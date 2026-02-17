use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use uuid::Uuid;

use crate::config::SpotMidConfig;
use crate::events::{Event, EventMeta, QualityFlag, Venue};
use crate::ops::metrics::AppMetrics;

struct VenueBbo {
    bid_px: Decimal,
    ask_px: Decimal,
    exchange_ts: Option<chrono::DateTime<chrono::Utc>>,
    received_at: Instant,
}

pub async fn run(
    config: SpotMidConfig,
    mut input_rx: mpsc::Receiver<Event>,
    derived_tx: mpsc::Sender<Event>,
    app_metrics: AppMetrics,
    cancel: CancellationToken,
) {
    let mut latest: HashMap<Venue, VenueBbo> = HashMap::new();
    let mut interval = tokio::time::interval(Duration::from_millis(config.interval_ms));
    let staleness = Duration::from_millis(config.staleness_ms);
    let threshold_bps = config.divergence_threshold_bps;
    let session_id = Uuid::new_v4();

    info!("spot_mid pipeline started");

    loop {
        tokio::select! {
            Some(event) = input_rx.recv() => {
                if let Event::SpotBbo { venue, bid_px, ask_px, exchange_ts, .. } = event {
                    latest.insert(venue, VenueBbo {
                        bid_px,
                        ask_px,
                        exchange_ts,
                        received_at: Instant::now(),
                    });
                }
            }
            _ = interval.tick() => {
                compute_and_emit(
                    &latest,
                    staleness,
                    threshold_bps,
                    session_id,
                    &derived_tx,
                    &app_metrics,
                ).await;
            }
            _ = cancel.cancelled() => break,
        }
    }
    info!("spot_mid pipeline shutdown");
}

async fn compute_and_emit(
    latest: &HashMap<Venue, VenueBbo>,
    staleness: Duration,
    threshold_bps: f64,
    session_id: Uuid,
    derived_tx: &mpsc::Sender<Event>,
    app_metrics: &AppMetrics,
) {
    let now = Instant::now();
    let recv_ts = Utc::now();

    let fresh: Vec<(Venue, Decimal)> = latest
        .iter()
        .filter(|(_, bbo)| now.duration_since(bbo.received_at) < staleness)
        .filter(|(_, bbo)| bbo.bid_px > Decimal::ZERO && bbo.ask_px > Decimal::ZERO)
        .map(|(venue, bbo)| {
            let mid = (bbo.bid_px + bbo.ask_px) / dec!(2);
            let mid = mid.round_dp(2);
            (*venue, mid)
        })
        .collect();

    if fresh.is_empty() {
        return;
    }

    let source_venues: Vec<Venue> = fresh.iter().map(|(v, _)| *v).collect();
    let mids: Vec<Decimal> = fresh.iter().map(|(_, m)| *m).collect();

    // Consolidated mid = median (with 2 venues, average)
    let s_mid = if mids.len() == 1 {
        mids[0]
    } else {
        let mut sorted = mids.clone();
        sorted.sort();
        if sorted.len() % 2 == 0 {
            let mid_idx = sorted.len() / 2;
            (sorted[mid_idx - 1] + sorted[mid_idx]) / dec!(2)
        } else {
            sorted[sorted.len() / 2]
        }
    };

    // Quality flag
    let quality_flag = if fresh.len() >= 2 {
        let min_mid = mids.iter().min().unwrap();
        let max_mid = mids.iter().max().unwrap();
        if *min_mid > Decimal::ZERO {
            let divergence_bps =
                ((*max_mid - *min_mid) / *min_mid * dec!(10000)).to_string().parse::<f64>().unwrap_or(0.0);
            app_metrics.derived.spot_divergence_bps.set(divergence_bps);
            if divergence_bps > threshold_bps {
                QualityFlag::Suspect
            } else {
                QualityFlag::Ok
            }
        } else {
            QualityFlag::Degraded
        }
    } else {
        QualityFlag::Degraded
    };

    // Compute source_age_ms_max deterministically from recv_ts
    let source_age_ms_max = latest
        .values()
        .filter_map(|bbo| {
            bbo.exchange_ts
                .map(|ets| (recv_ts - ets).num_milliseconds())
        })
        .max();

    let event = Event::SpotMid1s {
        meta: EventMeta {
            seq: 0,
            session_id,
            recv_ts,
        },
        s_mid,
        quality_flag,
        source_venues,
        source_age_ms_max,
        venue_mids: fresh,
    };

    // Non-blocking send to derived channel
    if derived_tx.try_send(event).is_err() {
        app_metrics.router.derived_drops.inc();
        debug!("derived_tx full, dropping SpotMid1s");
    }
}
