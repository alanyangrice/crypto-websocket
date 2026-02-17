use chrono::{TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::DeribitConfig;
use crate::connectors::connect_with_backoff;
use crate::events::{CallPut, Event, EventMeta, Venue};
use crate::ops::health::HealthRegistry;
use crate::ops::metrics::ConnectorMetrics;

pub async fn run(
    config: DeribitConfig,
    ingress_tx: mpsc::Sender<Event>,
    health: HealthRegistry,
    metrics: ConnectorMetrics,
    cancel: CancellationToken,
) {
    loop {
        if cancel.is_cancelled() {
            break;
        }

        let (ws, session_id) = connect_with_backoff(
            &config.url,
            Venue::Deribit,
            &health,
            &metrics,
            Duration::from_secs(60),
        )
        .await;

        let _ = ingress_tx
            .send(Event::SessionBoundary {
                meta: EventMeta::new(session_id),
                venue: Venue::Deribit,
                session_id,
            })
            .await;

        let (mut sink, mut stream) = ws.split();

        // Fetch instruments and subscribe
        let instruments =
            match discover_and_subscribe(&config, &mut sink, &mut stream, session_id, &ingress_tx, &metrics)
                .await
            {
                Ok(i) => i,
                Err(e) => {
                    warn!(venue = "deribit", error = %e, "instrument discovery failed, reconnecting");
                    metrics.connected.with_label_values(&["deribit"]).set(0);
                    continue;
                }
            };

        info!(
            venue = "deribit",
            instrument_count = instruments.len(),
            "subscribed to option tickers"
        );

        // Subscribe to DVOL if available
        let dvol_sub = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 9999,
            "method": "public/subscribe",
            "params": {
                "channels": ["deribit_volatility_index.btc_usd"]
            }
        });
        let _ = sink.send(Message::Text(dvol_sub.to_string())).await;

        let mut refresh_interval =
            tokio::time::interval(Duration::from_secs(config.instrument_refresh_secs));
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(30));
        let mut rpc_id: u64 = 10000;

        loop {
            tokio::select! {
                msg = stream.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            health.record_venue_recv(Venue::Deribit);
                            if let Err(e) = handle_message(
                                &text,
                                session_id,
                                &ingress_tx,
                                &metrics,
                            ).await {
                                debug!(venue = "deribit", error = %e, "parse error");
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = sink.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            warn!(venue = "deribit", "connection closed, reconnecting");
                            metrics.connected.with_label_values(&["deribit"]).set(0);
                            break;
                        }
                        Some(Err(e)) => {
                            warn!(venue = "deribit", error = %e, "ws error, reconnecting");
                            metrics.connected.with_label_values(&["deribit"]).set(0);
                            break;
                        }
                        _ => {}
                    }
                }
                _ = heartbeat_interval.tick() => {
                    rpc_id += 1;
                    let test = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": rpc_id,
                        "method": "public/test",
                        "params": {}
                    });
                    let _ = sink.send(Message::Text(test.to_string())).await;
                }
                _ = refresh_interval.tick() => {
                    debug!(venue = "deribit", "refreshing instruments (periodic)");
                    // Re-discover (in a full impl we'd diff subscriptions)
                }
                _ = cancel.cancelled() => {
                    info!(venue = "deribit", "shutdown requested");
                    let _ = sink.close().await;
                    return;
                }
            }
        }
    }
}

#[derive(Deserialize, Debug)]
struct DeribitInstrument {
    instrument_name: String,
    #[serde(default)]
    is_active: bool,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    expiration_timestamp: i64,
    #[serde(default)]
    strike: Option<f64>,
    #[serde(default)]
    option_type: Option<String>,
    #[serde(default)]
    state: Option<String>,
}

async fn discover_and_subscribe(
    config: &DeribitConfig,
    sink: &mut crate::connectors::WsSink,
    stream: &mut crate::connectors::WsSource,
    session_id: Uuid,
    ingress_tx: &mpsc::Sender<Event>,
    _metrics: &ConnectorMetrics,
) -> anyhow::Result<Vec<String>> {
    // Request instruments
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "public/get_instruments",
        "params": {
            "currency": config.currency,
            "kind": "option"
        }
    });
    sink.send(Message::Text(req.to_string())).await?;

    // Wait for response
    let instruments = loop {
        if let Some(Ok(Message::Text(text))) = stream.next().await {
            let v: serde_json::Value = serde_json::from_str(&text)?;
            if v.get("id") == Some(&serde_json::Value::Number(1.into())) {
                if let Some(result) = v.get("result") {
                    let instruments: Vec<DeribitInstrument> =
                        serde_json::from_value(result.clone())?;
                    break instruments;
                }
            }
        }
    };

    // Filter: active + open + next N expiries + strike range + per-expiry cap
    let active: Vec<_> = instruments
        .into_iter()
        .filter(|i| {
            i.is_active
                && i.kind == "option"
                && i.state.as_deref() != Some("inactive")
                && i.strike.is_some()
        })
        .collect();

    // Collect unique expiries, sorted, take first N
    let mut expiries: Vec<i64> = active.iter().map(|i| i.expiration_timestamp).collect();
    expiries.sort();
    expiries.dedup();
    let now_ms = Utc::now().timestamp_millis();
    let future_expiries: Vec<i64> = expiries
        .into_iter()
        .filter(|&e| e > now_ms)
        .take(config.max_expiries)
        .collect();
    let _expiry_set: HashSet<i64> = future_expiries.iter().cloned().collect();

    // Group by expiry, cap per expiry
    let mut selected: Vec<String> = Vec::new();
    for expiry in &future_expiries {
        let mut candidates: Vec<_> = active
            .iter()
            .filter(|i| i.expiration_timestamp == *expiry)
            .collect();
        // Sort by distance from a reference (we don't have spot yet, use all)
        candidates.truncate(config.max_instruments_per_expiry);
        for inst in &candidates {
            selected.push(inst.instrument_name.clone());

            // Emit OptInstrument event
            let cp = match inst.option_type.as_deref() {
                Some("call") => CallPut::Call,
                _ => CallPut::Put,
            };
            let expiry_ts = Utc.timestamp_millis_opt(inst.expiration_timestamp).unwrap();
            let strike = Decimal::from_str(&inst.strike.unwrap_or(0.0).to_string())
                .unwrap_or(Decimal::ZERO);
            let ev = Event::OptInstrument {
                meta: EventMeta::new(session_id),
                instrument_name: inst.instrument_name.clone(),
                underlying: config.currency.clone(),
                expiry_ts,
                strike,
                cp,
            };
            let _ = ingress_tx.send(ev).await;
        }
    }

    // Subscribe to ticker for each selected instrument
    if !selected.is_empty() {
        let channels: Vec<String> = selected
            .iter()
            .map(|name| format!("ticker.{name}.100ms"))
            .collect();

        // Batch subscribe in chunks to avoid huge messages
        for chunk in channels.chunks(50) {
            let sub = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "public/subscribe",
                "params": {
                    "channels": chunk
                }
            });
            sink.send(Message::Text(sub.to_string())).await?;
        }
    }

    Ok(selected)
}

#[derive(Deserialize, Debug)]
struct DeribitNotification {
    method: Option<String>,
    params: Option<DeribitParams>,
}

#[derive(Deserialize, Debug)]
struct DeribitParams {
    channel: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct DeribitTickerData {
    instrument_name: Option<String>,
    best_bid_price: Option<f64>,
    best_ask_price: Option<f64>,
    mark_price: Option<f64>,
    mark_iv: Option<f64>,
    bid_iv: Option<f64>,
    ask_iv: Option<f64>,
    underlying_price: Option<f64>,
    open_interest: Option<f64>,
    timestamp: Option<i64>,
}

async fn handle_message(
    text: &str,
    session_id: Uuid,
    ingress_tx: &mpsc::Sender<Event>,
    metrics: &ConnectorMetrics,
) -> anyhow::Result<()> {
    let msg: serde_json::Value = serde_json::from_str(text)?;

    // Check if it's a subscription notification
    if msg.get("method").and_then(|m| m.as_str()) == Some("subscription") {
        if let Some(params) = msg.get("params") {
            let channel = params
                .get("channel")
                .and_then(|c| c.as_str())
                .unwrap_or("");

            if channel.starts_with("ticker.") {
                if let Some(data) = params.get("data") {
                    let ticker: DeribitTickerData = serde_json::from_value(data.clone())?;
                    metrics
                        .messages_total
                        .with_label_values(&["deribit", "ticker"])
                        .inc();
                    let exchange_ts = ticker
                        .timestamp
                        .and_then(|ms| Utc.timestamp_millis_opt(ms).single());
                    let ev = Event::OptTicker {
                        meta: EventMeta::new(session_id),
                        instrument_name: ticker.instrument_name.unwrap_or_default(),
                        bid_px: ticker.best_bid_price.map(|v| dec_from_f64(v)),
                        ask_px: ticker.best_ask_price.map(|v| dec_from_f64(v)),
                        mark_px: dec_from_f64(ticker.mark_price.unwrap_or(0.0)),
                        mark_iv: ticker.mark_iv,
                        bid_iv: ticker.bid_iv,
                        ask_iv: ticker.ask_iv,
                        underlying_index_px: dec_from_f64(
                            ticker.underlying_price.unwrap_or(0.0),
                        ),
                        open_interest: dec_from_f64(ticker.open_interest.unwrap_or(0.0)),
                        exchange_ts,
                    };
                    let _ = ingress_tx.send(ev).await;
                }
            } else if channel.starts_with("deribit_volatility_index") {
                if let Some(data) = params.get("data") {
                    let volatility = data
                        .get("volatility")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0);
                    let ts = data
                        .get("timestamp")
                        .and_then(|t| t.as_i64())
                        .and_then(|ms| Utc.timestamp_millis_opt(ms).single());
                    let ev = Event::DvolIndex {
                        meta: EventMeta::new(session_id),
                        value: volatility,
                        exchange_ts: ts,
                    };
                    let _ = ingress_tx.send(ev).await;
                }
            }
        }
    }

    Ok(())
}

fn dec_from_f64(v: f64) -> Decimal {
    Decimal::from_str(&v.to_string()).unwrap_or(Decimal::ZERO)
}
