use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

use crate::config::IvSurfaceConfig;
use crate::events::{Event, EventMeta};
use crate::ops::metrics::AppMetrics;

struct InstrumentMeta {
    expiry_ts: DateTime<Utc>,
    strike: Decimal,
}

struct TickerState {
    bid_px: Option<Decimal>,
    ask_px: Option<Decimal>,
    mark_px: Decimal,
    mark_iv: Option<f64>,
    bid_iv: Option<f64>,
    ask_iv: Option<f64>,
    underlying_index_px: Decimal,
    open_interest: Decimal,
    received_at: Instant,
}

pub async fn run(
    config: IvSurfaceConfig,
    mut input_rx: mpsc::Receiver<Event>,
    derived_tx: mpsc::Sender<Event>,
    app_metrics: AppMetrics,
    cancel: CancellationToken,
) {
    let mut instruments: HashMap<String, InstrumentMeta> = HashMap::new();
    let mut tickers: HashMap<String, TickerState> = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_millis(config.interval_ms));
    let staleness = Duration::from_millis(config.staleness_ms);
    let session_id = Uuid::new_v4();

    info!("iv_surface pipeline started");

    loop {
        tokio::select! {
            biased;
            Some(event) = input_rx.recv() => {
                match event {
                    Event::OptInstrument { instrument_name, expiry_ts, strike, .. } => {
                        instruments.insert(instrument_name, InstrumentMeta { expiry_ts, strike });
                    }
                    Event::OptTicker {
                        instrument_name,
                        bid_px, ask_px, mark_px,
                        mark_iv, bid_iv, ask_iv,
                        underlying_index_px, open_interest, ..
                    } => {
                        tickers.insert(instrument_name, TickerState {
                            bid_px, ask_px, mark_px,
                            mark_iv, bid_iv, ask_iv,
                            underlying_index_px, open_interest,
                            received_at: Instant::now(),
                        });
                    }
                    _ => {}
                }
            }
            _ = interval.tick() => {
                compute_and_emit(
                    &instruments,
                    &tickers,
                    staleness,
                    session_id,
                    &derived_tx,
                    &app_metrics,
                ).await;
            }
            _ = cancel.cancelled() => break,
        }
    }
    info!("iv_surface pipeline shutdown");
}

async fn compute_and_emit(
    instruments: &HashMap<String, InstrumentMeta>,
    tickers: &HashMap<String, TickerState>,
    staleness: Duration,
    session_id: Uuid,
    derived_tx: &mpsc::Sender<Event>,
    app_metrics: &AppMetrics,
) {
    let now = Instant::now();
    let recv_ts = Utc::now();

    // Group by expiry, applying staleness + liquidity filter
    let mut by_expiry: HashMap<DateTime<Utc>, Vec<(&str, &InstrumentMeta, &TickerState)>> =
        HashMap::new();

    for (name, meta) in instruments {
        if let Some(ticker) = tickers.get(name) {
            // Staleness filter
            if now.duration_since(ticker.received_at) >= staleness {
                continue;
            }
            // Liquidity filter: exclude instruments with no bid AND no ask AND OI=0
            let has_bid = ticker.bid_px.is_some();
            let has_ask = ticker.ask_px.is_some();
            let has_oi = ticker.open_interest > Decimal::ZERO;
            if !has_bid && !has_ask && !has_oi {
                continue;
            }
            by_expiry
                .entry(meta.expiry_ts)
                .or_default()
                .push((name.as_str(), meta, ticker));
        }
    }

    for (expiry_ts, items) in &by_expiry {
        if items.is_empty() {
            continue;
        }

        let underlying_index_px = items[0].2.underlying_index_px;

        // Collect IV points
        let mut iv_points: Vec<(Decimal, f64)> = Vec::new();
        for (_, meta, ticker) in items {
            if let Some(iv) = ticker.mark_iv {
                if iv > 0.0 {
                    iv_points.push((meta.strike, iv));
                }
            }
        }
        iv_points.sort_by(|a, b| a.0.cmp(&b.0));

        let n_points = items.len() as u32;
        let n_valid_points = iv_points.len() as u32;

        if n_valid_points == 0 {
            continue;
        }

        let min_strike = iv_points.first().map(|p| p.0).unwrap_or(Decimal::ZERO);
        let max_strike = iv_points.last().map(|p| p.0).unwrap_or(Decimal::ZERO);

        // ATM IV: bracket interpolation between two nearest strikes to underlying
        let atm_iv = compute_atm_iv(&iv_points, underlying_index_px);

        // spread_iv_atm: find the ATM instrument and compute ask_iv - bid_iv
        let spread_iv_atm = find_atm_spread(items, underlying_index_px);

        let expiry_label = expiry_ts.format("%Y%m%d").to_string();
        app_metrics
            .derived
            .iv_surface_points
            .with_label_values(&[&expiry_label])
            .set(n_valid_points as i64);

        let event = Event::IvSurfaceSnapshot {
            meta: EventMeta {
                seq: 0,
                session_id,
                recv_ts,
            },
            expiry_ts: *expiry_ts,
            atm_iv,
            iv_points,
            underlying_index_px,
            forward_proxy: None,
            n_points,
            n_valid_points,
            min_strike,
            max_strike,
            spread_iv_atm,
        };

        if derived_tx.try_send(event).is_err() {
            app_metrics.router.derived_drops.inc();
            debug!("derived_tx full, dropping IvSurfaceSnapshot");
        }
    }
}

fn compute_atm_iv(iv_points: &[(Decimal, f64)], underlying: Decimal) -> f64 {
    if iv_points.is_empty() {
        return 0.0;
    }
    if iv_points.len() == 1 {
        return iv_points[0].1;
    }

    // Find the two nearest strikes bracketing the underlying
    let mut below: Option<(Decimal, f64)> = None;
    let mut above: Option<(Decimal, f64)> = None;

    for &(strike, iv) in iv_points {
        if strike <= underlying {
            below = Some((strike, iv));
        }
        if strike > underlying && above.is_none() {
            above = Some((strike, iv));
        }
    }

    match (below, above) {
        (Some((s1, iv1)), Some((s2, iv2))) => {
            if s2 == s1 {
                return iv1;
            }
            // Linear interpolation weighted by distance
            let s1f: f64 = s1.to_string().parse().unwrap_or(0.0);
            let s2f: f64 = s2.to_string().parse().unwrap_or(1.0);
            let uf: f64 = underlying.to_string().parse().unwrap_or(0.0);
            let w = (uf - s1f) / (s2f - s1f);
            iv1 * (1.0 - w) + iv2 * w
        }
        (Some((_, iv)), None) => iv,
        (None, Some((_, iv))) => iv,
        (None, None) => 0.0,
    }
}

fn find_atm_spread(
    items: &[(&str, &InstrumentMeta, &TickerState)],
    underlying: Decimal,
) -> Option<f64> {
    let mut closest: Option<(&TickerState, Decimal)> = None;
    for (_, meta, ticker) in items {
        let dist = (meta.strike - underlying).abs();
        if closest.is_none() || dist < closest.unwrap().1 {
            closest = Some((ticker, dist));
        }
    }
    closest.and_then(|(ticker, _)| {
        match (ticker.ask_iv, ticker.bid_iv) {
            (Some(a), Some(b)) => Some(a - b),
            _ => None,
        }
    })
}
