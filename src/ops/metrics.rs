use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

use tokio_util::sync::CancellationToken;

use crate::ops::health::HealthRegistry;

/// All application metrics live here for easy sharing.
#[derive(Clone)]
pub struct AppMetrics {
    pub registry: Registry,
    pub router: RouterMetrics,
    pub connector: ConnectorMetrics,
    pub parquet: ParquetMetrics,
    pub redis: RedisMetrics,
    pub derived: DerivedMetrics,
}

#[derive(Clone)]
pub struct RouterMetrics {
    pub events_total: IntCounter,
    pub redis_queue_drops: IntCounter,
    pub derived_drops: IntCounter,
    pub derived_processed: IntCounter,
}

#[derive(Clone)]
pub struct ConnectorMetrics {
    pub connected: IntGaugeVec,
    pub reconnects_total: IntCounterVec,
    pub messages_total: IntCounterVec,
    pub lag_ms: HistogramVec,
}

#[derive(Clone)]
pub struct ParquetMetrics {
    pub files_written_total: IntCounter,
    pub rows_written_total: IntCounterVec,
    pub flush_duration_ms: HistogramVec,
}

#[derive(Clone)]
pub struct RedisMetrics {
    pub writes_total: IntCounter,
    pub failures_total: IntCounter,
    pub write_latency_ms: HistogramVec,
    pub connected: prometheus::IntGauge,
    pub pending_overflow_total: IntCounter,
}

#[derive(Clone)]
pub struct DerivedMetrics {
    pub spot_divergence_bps: prometheus::Gauge,
    pub iv_surface_points: IntGaugeVec,
}

impl AppMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        // Router
        let router_events = IntCounter::new("md_router_events_total", "Total events routed").unwrap();
        let redis_queue_drops = IntCounter::new("md_redis_queue_drops_total", "Events dropped at Redis queue").unwrap();
        let derived_drops = IntCounter::new("md_derived_drops_total", "Derived events dropped").unwrap();
        let derived_processed = IntCounter::new("md_derived_processed_total", "Derived events processed").unwrap();
        registry.register(Box::new(router_events.clone())).unwrap();
        registry.register(Box::new(redis_queue_drops.clone())).unwrap();
        registry.register(Box::new(derived_drops.clone())).unwrap();
        registry.register(Box::new(derived_processed.clone())).unwrap();

        // Connector
        let connected = IntGaugeVec::new(Opts::new("md_connected", "Connector connected"), &["venue"]).unwrap();
        let reconnects_total = IntCounterVec::new(Opts::new("md_reconnects_total", "Reconnects"), &["venue"]).unwrap();
        let messages_total = IntCounterVec::new(Opts::new("md_messages_total", "Messages received"), &["venue", "channel"]).unwrap();
        let lag_ms = HistogramVec::new(
            HistogramOpts::new("md_lag_ms", "recv_ts - exchange_ts in ms")
                .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]),
            &["venue"],
        ).unwrap();
        registry.register(Box::new(connected.clone())).unwrap();
        registry.register(Box::new(reconnects_total.clone())).unwrap();
        registry.register(Box::new(messages_total.clone())).unwrap();
        registry.register(Box::new(lag_ms.clone())).unwrap();

        // Parquet
        let files_written = IntCounter::new("md_parquet_files_written_total", "Parquet files written").unwrap();
        let rows_written = IntCounterVec::new(Opts::new("md_parquet_rows_written_total", "Rows written"), &["stream"]).unwrap();
        let flush_duration = HistogramVec::new(
            HistogramOpts::new("md_parquet_flush_duration_ms", "Flush duration ms")
                .buckets(vec![10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0]),
            &["stream"],
        ).unwrap();
        registry.register(Box::new(files_written.clone())).unwrap();
        registry.register(Box::new(rows_written.clone())).unwrap();
        registry.register(Box::new(flush_duration.clone())).unwrap();

        // Redis
        let redis_writes = IntCounter::new("md_redis_writes_total", "Redis writes").unwrap();
        let redis_failures = IntCounter::new("md_redis_failures_total", "Redis write failures").unwrap();
        let redis_latency = HistogramVec::new(
            HistogramOpts::new("md_redis_write_latency_ms", "Redis write latency ms")
                .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0]),
            &["op"],
        ).unwrap();
        let redis_connected = prometheus::IntGauge::new("md_redis_connected", "Redis connected").unwrap();
        let pending_overflow = IntCounter::new("md_redis_pending_overflow_total", "Redis pending overflow").unwrap();
        registry.register(Box::new(redis_writes.clone())).unwrap();
        registry.register(Box::new(redis_failures.clone())).unwrap();
        registry.register(Box::new(redis_latency.clone())).unwrap();
        registry.register(Box::new(redis_connected.clone())).unwrap();
        registry.register(Box::new(pending_overflow.clone())).unwrap();

        // Derived
        let spot_div = prometheus::Gauge::new("md_spot_divergence_bps", "Spot divergence bps").unwrap();
        let iv_points = IntGaugeVec::new(Opts::new("md_iv_surface_points", "IV surface points"), &["expiry"]).unwrap();
        registry.register(Box::new(spot_div.clone())).unwrap();
        registry.register(Box::new(iv_points.clone())).unwrap();

        AppMetrics {
            registry,
            router: RouterMetrics {
                events_total: router_events,
                redis_queue_drops,
                derived_drops,
                derived_processed,
            },
            connector: ConnectorMetrics {
                connected,
                reconnects_total,
                messages_total,
                lag_ms,
            },
            parquet: ParquetMetrics {
                files_written_total: files_written,
                rows_written_total: rows_written,
                flush_duration_ms: flush_duration,
            },
            redis: RedisMetrics {
                writes_total: redis_writes,
                failures_total: redis_failures,
                write_latency_ms: redis_latency,
                connected: redis_connected,
                pending_overflow_total: pending_overflow,
            },
            derived: DerivedMetrics {
                spot_divergence_bps: spot_div,
                iv_surface_points: iv_points,
            },
        }
    }

    pub fn encode(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

/// Serve /metrics and /health on the given bind address.
pub async fn serve_http(
    bind: String,
    metrics: AppMetrics,
    health: HealthRegistry,
    cancel: CancellationToken,
) {
    use http_body_util::Full;
    use hyper::body::Bytes;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;

    let listener = match tokio::net::TcpListener::bind(&bind).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("failed to bind metrics server to {bind}: {e}");
            return;
        }
    };
    tracing::info!("metrics server listening on {bind}");

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let (stream, _) = match accept {
                    Ok(a) => a,
                    Err(e) => {
                        tracing::warn!("accept error: {e}");
                        continue;
                    }
                };
                let metrics = metrics.clone();
                let health = health.clone();
                let io = TokioIo::new(stream);
                tokio::spawn(async move {
                    let svc = service_fn(move |req: Request<hyper::body::Incoming>| {
                        let metrics = metrics.clone();
                        let health = health.clone();
                        async move {
                            match req.uri().path() {
                                "/metrics" => {
                                    let body = metrics.encode();
                                    Ok::<_, hyper::Error>(
                                        Response::builder()
                                            .header("content-type", "text/plain; charset=utf-8")
                                            .body(Full::new(Bytes::from(body)))
                                            .unwrap(),
                                    )
                                }
                                "/health" => {
                                    let snap = health.snapshot();
                                    let body = serde_json::to_string_pretty(&snap).unwrap_or_default();
                                    Ok(Response::builder()
                                        .header("content-type", "application/json")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap())
                                }
                                _ => Ok(Response::builder()
                                    .status(404)
                                    .body(Full::new(Bytes::from("not found")))
                                    .unwrap()),
                            }
                        }
                    });
                    if let Err(e) = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(io, svc)
                    .await
                    {
                        tracing::debug!("http connection error: {e}");
                    }
                });
            }
            _ = cancel.cancelled() => break,
        }
    }
    tracing::info!("metrics server shutdown");
}
