use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::CoinbaseConfig;
use crate::connectors::{connect_with_backoff, TradeDedup};
use crate::events::{Event, EventMeta, Side, Venue};
use crate::ops::health::HealthRegistry;
use crate::ops::metrics::ConnectorMetrics;

pub async fn run(
    config: CoinbaseConfig,
    ingress_tx: mpsc::Sender<Event>,
    health: HealthRegistry,
    metrics: ConnectorMetrics,
    cancel: CancellationToken,
) {
    let mut dedup = TradeDedup::new(10_000);

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let (ws, session_id) = connect_with_backoff(
            &config.url,
            Venue::Coinbase,
            &health,
            &metrics,
            Duration::from_secs(60),
        )
        .await;

        let _ = ingress_tx
            .send(Event::SessionBoundary {
                meta: EventMeta::new(session_id),
                venue: Venue::Coinbase,
                session_id,
            })
            .await;

        let (mut sink, mut stream) = ws.split();

        // Subscribe to market_trades + heartbeats (always)
        let sub = serde_json::json!({
            "type": "subscribe",
            "product_ids": config.products,
            "channel": "market_trades"
        });
        let _ = sink.send(Message::Text(sub.to_string())).await;

        let hb_sub = serde_json::json!({
            "type": "subscribe",
            "channel": "heartbeats"
        });
        let _ = sink.send(Message::Text(hb_sub.to_string())).await;

        // Best-effort: subscribe to ticker for L1 BBO
        let ticker_sub = serde_json::json!({
            "type": "subscribe",
            "product_ids": config.products,
            "channel": "ticker"
        });
        let _ = sink.send(Message::Text(ticker_sub.to_string())).await;

        info!(venue = "coinbase", "subscribed to channels");

        loop {
            tokio::select! {
                biased;
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            health.record_venue_recv(Venue::Coinbase);
                            if let Err(e) = handle_message(
                                &text,
                                session_id,
                                &ingress_tx,
                                &metrics,
                                &mut dedup,
                            ).await {
                                debug!(venue = "coinbase", error = %e, "parse error");
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = sink.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            warn!(venue = "coinbase", "connection closed, reconnecting");
                            metrics.connected.with_label_values(&["coinbase"]).set(0);
                            break;
                        }
                        Some(Err(e)) => {
                            warn!(venue = "coinbase", error = %e, "ws error, reconnecting");
                            metrics.connected.with_label_values(&["coinbase"]).set(0);
                            break;
                        }
                        _ => {}
                    }
                }
                _ = cancel.cancelled() => {
                    info!(venue = "coinbase", "shutdown requested");
                    let _ = sink.close().await;
                    return;
                }
            }
        }
    }
}

#[derive(Deserialize, Debug)]
struct CoinbaseEnvelope {
    channel: Option<String>,
    events: Option<Vec<CoinbaseEvent>>,
}

#[derive(Deserialize, Debug)]
struct CoinbaseEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    trades: Option<Vec<CoinbaseTrade>>,
    tickers: Option<Vec<CoinbaseTicker>>,
}

#[derive(Deserialize, Debug)]
struct CoinbaseTrade {
    trade_id: Option<String>,
    product_id: Option<String>,
    price: Option<String>,
    size: Option<String>,
    side: Option<String>,
    time: Option<String>,
}

#[derive(Deserialize, Debug)]
struct CoinbaseTicker {
    product_id: Option<String>,
    best_bid: Option<String>,
    best_bid_quantity: Option<String>,
    best_ask: Option<String>,
    best_ask_quantity: Option<String>,
}

async fn handle_message(
    text: &str,
    session_id: Uuid,
    ingress_tx: &mpsc::Sender<Event>,
    metrics: &ConnectorMetrics,
    dedup: &mut TradeDedup,
) -> anyhow::Result<()> {
    let envelope: CoinbaseEnvelope = serde_json::from_str(text)?;
    let channel = match &envelope.channel {
        Some(c) => c.as_str(),
        None => return Ok(()),
    };

    let events = match &envelope.events {
        Some(e) => e,
        None => return Ok(()),
    };

    for event in events {
        match channel {
            "market_trades" => {
                if let Some(trades) = &event.trades {
                    for trade in trades {
                        let tid = trade
                            .trade_id
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string());
                        if !dedup.check_and_insert(&tid) {
                            continue;
                        }
                        metrics
                            .messages_total
                            .with_label_values(&["coinbase", "market_trades"])
                            .inc();
                        let exchange_ts = trade
                            .time
                            .as_ref()
                            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
                            .map(|dt| dt.with_timezone(&Utc));
                        let side = match trade.side.as_deref() {
                            Some("BUY") => Side::Buy,
                            Some("SELL") => Side::Sell,
                            _ => Side::Unknown,
                        };
                        let ev = Event::SpotTrade {
                            meta: EventMeta::new(session_id),
                            venue: Venue::Coinbase,
                            product: trade
                                .product_id
                                .clone()
                                .unwrap_or_else(|| "BTC-USD".into()),
                            price: parse_decimal(trade.price.as_deref()),
                            size: parse_decimal(trade.size.as_deref()),
                            side,
                            trade_id: tid,
                            exchange_ts,
                        };
                        let _ = ingress_tx.try_send(ev);
                    }
                }
            }
            "ticker" => {
                if let Some(tickers) = &event.tickers {
                    for ticker in tickers {
                        metrics
                            .messages_total
                            .with_label_values(&["coinbase", "ticker"])
                            .inc();
                        let bid_px = parse_decimal(ticker.best_bid.as_deref());
                        let ask_px = parse_decimal(ticker.best_ask.as_deref());
                        if bid_px == Decimal::ZERO || ask_px == Decimal::ZERO {
                            continue;
                        }
                        let ev = Event::SpotBbo {
                            meta: EventMeta::new(session_id),
                            venue: Venue::Coinbase,
                            product: ticker
                                .product_id
                                .clone()
                                .unwrap_or_else(|| "BTC-USD".into()),
                            bid_px,
                            bid_sz: parse_decimal(ticker.best_bid_quantity.as_deref()),
                            ask_px,
                            ask_sz: parse_decimal(ticker.best_ask_quantity.as_deref()),
                            exchange_ts: None,
                        };
                        let _ = ingress_tx.try_send(ev);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_decimal(s: Option<&str>) -> Decimal {
    s.and_then(|v| Decimal::from_str(v).ok())
        .unwrap_or(Decimal::ZERO)
}
