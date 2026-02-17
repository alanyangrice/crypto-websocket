use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::RedisSinkConfig;
use crate::events::Event;
use crate::ops::health::{HealthRegistry, SinkState};
use crate::ops::metrics::RedisMetrics;

pub async fn run(
    config: RedisSinkConfig,
    mut rx: mpsc::Receiver<Event>,
    health: HealthRegistry,
    metrics: RedisMetrics,
    cancel: CancellationToken,
) {
    if !config.enabled {
        info!("redis sink disabled");
        // Just drain the channel
        loop {
            tokio::select! {
                _ = rx.recv() => {}
                _ = cancel.cancelled() => return,
            }
        }
    }

    let client = match redis::Client::open(config.url.as_str()) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "failed to create redis client");
            health.set_redis_state(SinkState::Down);
            drain_channel(rx, cancel).await;
            return;
        }
    };

    let mut conn = match redis::aio::ConnectionManager::new(client).await {
        Ok(c) => {
            health.set_redis_state(SinkState::Healthy);
            metrics.connected.set(1);
            info!("redis sink connected");
            c
        }
        Err(e) => {
            error!(error = %e, "failed to connect to redis");
            health.set_redis_state(SinkState::Down);
            metrics.connected.set(0);
            drain_channel(rx, cancel).await;
            return;
        }
    };

    let mut pending: HashMap<String, Event> = HashMap::new();
    let mut health_interval =
        tokio::time::interval(Duration::from_millis(config.health_update_ms));

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(event) => {
                        let key = redis_key(&config.key_prefix, &event);
                        pending.insert(key, event);
                    }
                    None => break,
                }
            }
            _ = health_interval.tick() => {
                write_health(&config, &mut conn, &health, &metrics).await;
                continue;
            }
            _ = cancel.cancelled() => break,
        }

        // Coalesce: drain remaining without blocking
        while let Ok(event) = rx.try_recv() {
            let key = redis_key(&config.key_prefix, &event);
            pending.insert(key, event);
        }

        // Safety cap
        if pending.len() > config.max_pending_keys {
            metrics.pending_overflow_total.inc();
            warn!(
                pending_count = pending.len(),
                "redis pending overflow, clearing"
            );
            pending.clear();
            continue;
        }

        // Batch write
        if !pending.is_empty() {
            write_batch(&config, &mut conn, &mut pending, &metrics).await;
            pending.clear();
        }
    }

    // Final drain and flush
    while let Ok(event) = rx.try_recv() {
        let key = redis_key(&config.key_prefix, &event);
        pending.insert(key, event);
    }
    if !pending.is_empty() {
        write_batch(&config, &mut conn, &mut pending, &metrics).await;
    }

    info!("redis sink shutdown complete");
}

fn redis_key(prefix: &str, event: &Event) -> String {
    match event {
        Event::SpotBbo { venue, product, .. } => {
            format!("{prefix}:spot_bbo:{venue}:{product}")
        }
        Event::SpotMid1s { .. } => format!("{prefix}:spot_mid"),
        Event::IvSurfaceSnapshot { expiry_ts, .. } => {
            format!("{prefix}:iv_surface:{}", expiry_ts.timestamp())
        }
        Event::PerpMarkFunding { symbol, .. } => {
            format!("{prefix}:perp_mark_funding:{symbol}")
        }
        Event::DvolIndex { .. } => format!("{prefix}:dvol"),
        Event::SessionBoundary { venue, .. } => {
            format!("{prefix}:session_boundary:{venue}")
        }
        _ => format!("{prefix}:other:{}", event.event_type()),
    }
}

async fn write_batch(
    config: &RedisSinkConfig,
    conn: &mut redis::aio::ConnectionManager,
    pending: &mut HashMap<String, Event>,
    metrics: &RedisMetrics,
) {
    let start = std::time::Instant::now();
    let mut pipe = redis::pipe();

    for (key, event) in pending.iter() {
        let json = match serde_json::to_string(event) {
            Ok(j) => j,
            Err(_) => continue,
        };

        pipe.set(key, &json).ignore();

        // XADD for window streams
        match event {
            Event::SpotMid1s { .. } => {
                let stream_key = format!("{}:spot_mid:window", config.key_prefix);
                pipe.cmd("XADD")
                    .arg(&stream_key)
                    .arg("MAXLEN")
                    .arg("~")
                    .arg(config.spot_mid_window_len)
                    .arg("*")
                    .arg("data")
                    .arg(&json)
                    .ignore();
            }
            Event::IvSurfaceSnapshot { expiry_ts, .. } => {
                let stream_key = format!(
                    "{}:iv_surface:window:{}",
                    config.key_prefix,
                    expiry_ts.timestamp()
                );
                pipe.cmd("XADD")
                    .arg(&stream_key)
                    .arg("MAXLEN")
                    .arg("~")
                    .arg(config.iv_surface_window_len)
                    .arg("*")
                    .arg("data")
                    .arg(&json)
                    .ignore();
            }
            _ => {}
        }

        // Pub/Sub notification
        if config.pubsub_enabled {
            let notification = serde_json::json!({
                "type": event.event_type(),
                "key": key,
                "recv_ts": event.meta().recv_ts.to_rfc3339(),
            });
            pipe.publish(&config.pubsub_channel, notification.to_string())
                .ignore();
        }
    }

    let result: Result<(), redis::RedisError> = pipe.query_async(conn).await;
    let elapsed = start.elapsed().as_millis() as f64;

    match result {
        Ok(_) => {
            metrics.writes_total.inc();
            metrics
                .write_latency_ms
                .with_label_values(&["batch"])
                .observe(elapsed);
        }
        Err(e) => {
            metrics.failures_total.inc();
            warn!(error = %e, "redis batch write failed");
        }
    }
}

async fn write_health(
    config: &RedisSinkConfig,
    conn: &mut redis::aio::ConnectionManager,
    health: &HealthRegistry,
    _metrics: &RedisMetrics,
) {
    use redis::AsyncCommands;

    let snapshot = health.snapshot();
    let json = match serde_json::to_string(&snapshot) {
        Ok(j) => j,
        Err(_) => return,
    };

    let key = format!("{}:health", config.key_prefix);
    let result: Result<(), redis::RedisError> = conn.set(&key, &json).await;
    if let Err(e) = result {
        debug!(error = %e, "failed to write health to redis");
    }
}

async fn drain_channel(mut rx: mpsc::Receiver<Event>, cancel: CancellationToken) {
    loop {
        tokio::select! {
            _ = rx.recv() => {}
            _ = cancel.cancelled() => return,
        }
    }
}
