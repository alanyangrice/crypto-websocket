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

use crate::config::KrakenConfig;
use crate::connectors::{connect_with_backoff, TradeDedup};
use crate::events::{Event, EventMeta, Side, Venue};
use crate::ops::health::HealthRegistry;
use crate::ops::metrics::ConnectorMetrics;

pub async fn run(
    config: KrakenConfig,
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
            Venue::Kraken,
            &health,
            &metrics,
            Duration::from_secs(60),
        )
        .await;

        // Emit session boundary
        let _ = ingress_tx
            .send(Event::SessionBoundary {
                meta: EventMeta::new(session_id),
                venue: Venue::Kraken,
                session_id,
            })
            .await;

        let (mut sink, mut stream) = ws.split();

        // Subscribe to ticker (BBO trigger) and trade channels
        for symbol in &config.symbols {
            let ticker_sub = serde_json::json!({
                "method": "subscribe",
                "params": {
                    "channel": "ticker",
                    "symbol": [symbol],
                    "event_trigger": "bbo"
                }
            });
            let trade_sub = serde_json::json!({
                "method": "subscribe",
                "params": {
                    "channel": "trade",
                    "symbol": [symbol],
                    "snapshot": false
                }
            });
            if let Err(e) = sink.send(Message::Text(ticker_sub.to_string())).await {
                warn!(venue = "kraken", error = %e, "failed to send ticker subscribe");
                break;
            }
            if let Err(e) = sink.send(Message::Text(trade_sub.to_string())).await {
                warn!(venue = "kraken", error = %e, "failed to send trade subscribe");
                break;
            }
        }

        info!(venue = "kraken", "subscribed to channels");

        let mut ping_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            health.record_venue_recv(Venue::Kraken);
                            if let Err(e) = handle_message(
                                &text,
                                session_id,
                                &ingress_tx,
                                &metrics,
                                &mut dedup,
                            ).await {
                                debug!(venue = "kraken", error = %e, "parse error");
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = sink.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            warn!(venue = "kraken", "connection closed, reconnecting");
                            metrics.connected.with_label_values(&["kraken"]).set(0);
                            break;
                        }
                        Some(Err(e)) => {
                            warn!(venue = "kraken", error = %e, "ws error, reconnecting");
                            metrics.connected.with_label_values(&["kraken"]).set(0);
                            break;
                        }
                        _ => {}
                    }
                }
                _ = ping_interval.tick() => {
                    let _ = sink.send(Message::Ping(vec![])).await;
                }
                _ = cancel.cancelled() => {
                    info!(venue = "kraken", "shutdown requested");
                    let _ = sink.close().await;
                    return;
                }
            }
        }
    }
}

#[derive(Deserialize, Debug)]
struct KrakenEnvelope {
    channel: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct KrakenTickerData {
    symbol: String,
    bid: f64,
    bid_qty: f64,
    ask: f64,
    ask_qty: f64,
    timestamp: Option<String>,
}

#[derive(Deserialize, Debug)]
struct KrakenTradeData {
    symbol: String,
    side: Option<String>,
    qty: f64,
    price: f64,
    trade_id: Option<i64>,
    timestamp: Option<String>,
}

async fn handle_message(
    text: &str,
    session_id: Uuid,
    ingress_tx: &mpsc::Sender<Event>,
    metrics: &ConnectorMetrics,
    dedup: &mut TradeDedup,
) -> anyhow::Result<()> {
    let envelope: KrakenEnvelope = serde_json::from_str(text)?;

    let channel = match &envelope.channel {
        Some(c) => c.as_str(),
        None => return Ok(()),
    };

    let data = match &envelope.data {
        Some(d) => d,
        None => return Ok(()),
    };

    match channel {
        "ticker" => {
            let items: Vec<KrakenTickerData> = serde_json::from_value(data.clone())?;
            for item in items {
                metrics
                    .messages_total
                    .with_label_values(&["kraken", "ticker"])
                    .inc();
                let exchange_ts = item
                    .timestamp
                    .as_ref()
                    .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
                    .map(|dt| dt.with_timezone(&Utc));
                let meta = EventMeta::new(session_id);
                if let Some(ref ets) = exchange_ts {
                    let lag = (meta.recv_ts - ets).num_milliseconds() as f64;
                    metrics.lag_ms.with_label_values(&["kraken"]).observe(lag);
                }
                let event = Event::SpotBbo {
                    meta,
                    venue: Venue::Kraken,
                    product: normalize_product(&item.symbol),
                    bid_px: Decimal::from_str(&item.bid.to_string())
                        .unwrap_or(Decimal::ZERO),
                    bid_sz: Decimal::from_str(&item.bid_qty.to_string())
                        .unwrap_or(Decimal::ZERO),
                    ask_px: Decimal::from_str(&item.ask.to_string())
                        .unwrap_or(Decimal::ZERO),
                    ask_sz: Decimal::from_str(&item.ask_qty.to_string())
                        .unwrap_or(Decimal::ZERO),
                    exchange_ts,
                };
                let _ = ingress_tx.send(event).await;
            }
        }
        "trade" => {
            let items: Vec<KrakenTradeData> = serde_json::from_value(data.clone())?;
            for item in items {
                let tid = item
                    .trade_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| format!("{:.8}_{:.8}", item.price, item.qty));
                if !dedup.check_and_insert(&tid) {
                    continue;
                }
                metrics
                    .messages_total
                    .with_label_values(&["kraken", "trade"])
                    .inc();
                let exchange_ts = item
                    .timestamp
                    .as_ref()
                    .and_then(|ts| DateTime::parse_from_rfc3339(ts).ok())
                    .map(|dt| dt.with_timezone(&Utc));
                let side = match item.side.as_deref() {
                    Some("buy") => Side::Buy,
                    Some("sell") => Side::Sell,
                    _ => Side::Unknown,
                };
                let event = Event::SpotTrade {
                    meta: EventMeta::new(session_id),
                    venue: Venue::Kraken,
                    product: normalize_product(&item.symbol),
                    price: Decimal::from_str(&item.price.to_string())
                        .unwrap_or(Decimal::ZERO),
                    size: Decimal::from_str(&item.qty.to_string())
                        .unwrap_or(Decimal::ZERO),
                    side,
                    trade_id: tid,
                    exchange_ts,
                };
                let _ = ingress_tx.send(event).await;
            }
        }
        _ => {}
    }

    Ok(())
}

fn normalize_product(symbol: &str) -> String {
    symbol.replace('/', "-")
}
