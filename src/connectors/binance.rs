use chrono::{TimeZone, Utc};
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

use crate::config::BinanceConfig;
use crate::connectors::connect_with_backoff;
use crate::events::{Event, EventMeta, Venue};
use crate::ops::health::HealthRegistry;
use crate::ops::metrics::ConnectorMetrics;

pub async fn run(
    config: BinanceConfig,
    ingress_tx: mpsc::Sender<Event>,
    health: HealthRegistry,
    metrics: ConnectorMetrics,
    cancel: CancellationToken,
) {
    let url = format!("{}/ws/{}@markPrice@1s", config.url, config.symbol);

    loop {
        if cancel.is_cancelled() {
            break;
        }

        let (ws, session_id) = connect_with_backoff(
            &url,
            Venue::Binance,
            &health,
            &metrics,
            Duration::from_secs(60),
        )
        .await;

        let _ = ingress_tx
            .send(Event::SessionBoundary {
                meta: EventMeta::new(session_id),
                venue: Venue::Binance,
                session_id,
            })
            .await;

        let (mut sink, mut stream) = ws.split();

        info!(venue = "binance", "connected to mark price stream");

        loop {
            tokio::select! {
                biased;
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            health.record_venue_recv(Venue::Binance);
                            if let Err(e) = handle_message(
                                &text,
                                session_id,
                                &ingress_tx,
                                &metrics,
                            ).await {
                                debug!(venue = "binance", error = %e, "parse error");
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = sink.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            warn!(venue = "binance", "connection closed, reconnecting");
                            metrics.connected.with_label_values(&["binance"]).set(0);
                            break;
                        }
                        Some(Err(e)) => {
                            warn!(venue = "binance", error = %e, "ws error, reconnecting");
                            metrics.connected.with_label_values(&["binance"]).set(0);
                            break;
                        }
                        _ => {}
                    }
                }
                _ = cancel.cancelled() => {
                    info!(venue = "binance", "shutdown requested");
                    let _ = sink.close().await;
                    return;
                }
            }
        }
    }
}

#[derive(Deserialize, Debug)]
struct BinanceMarkPrice {
    #[serde(rename = "e")]
    event_type: Option<String>,
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "s")]
    symbol: Option<String>,
    #[serde(rename = "p")]
    mark_price: Option<String>,
    #[serde(rename = "i")]
    index_price: Option<String>,
    #[serde(rename = "r")]
    funding_rate: Option<String>,
    #[serde(rename = "T")]
    next_funding_time: Option<i64>,
}

async fn handle_message(
    text: &str,
    session_id: Uuid,
    ingress_tx: &mpsc::Sender<Event>,
    metrics: &ConnectorMetrics,
) -> anyhow::Result<()> {
    let data: BinanceMarkPrice = serde_json::from_str(text)?;

    metrics
        .messages_total
        .with_label_values(&["binance", "markPrice"])
        .inc();

    let exchange_ts = data
        .event_time
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single());

    let next_funding = data
        .next_funding_time
        .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
        .unwrap_or_else(Utc::now);

    let ev = Event::PerpMarkFunding {
        meta: EventMeta::new(session_id),
        symbol: data.symbol.unwrap_or_else(|| "BTCUSDT".into()),
        mark_px: parse_decimal(data.mark_price.as_deref()),
        index_px: parse_decimal(data.index_price.as_deref()),
        funding_rate: parse_decimal(data.funding_rate.as_deref()),
        next_funding_time: next_funding,
        exchange_ts,
    };
    let _ = ingress_tx.try_send(ev);

    Ok(())
}

fn parse_decimal(s: Option<&str>) -> Decimal {
    s.and_then(|v| Decimal::from_str(v).ok())
        .unwrap_or(Decimal::ZERO)
}
