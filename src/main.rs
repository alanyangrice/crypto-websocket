#![allow(dead_code)]

mod config;
mod connectors;
mod events;
mod ops;
mod pipeline;
mod prices;
mod router;
mod sinks;

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::AppConfig;
use crate::ops::health::HealthRegistry;
use crate::ops::metrics::AppMetrics;
use crate::router::EventRouter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 0. Install TLS crypto provider before any connections
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    // 1. Init logging
    ops::logging::init();

    info!("md-ingest starting");

    // 2. Load config
    let config_path = parse_config_path();
    let config = AppConfig::load(&config_path)?;
    info!(config_path = %config_path, "configuration loaded");

    // 3. Create shared state
    let cancel = CancellationToken::new();
    let health = HealthRegistry::new();
    let metrics = AppMetrics::new();

    // 4. Create router + channels
    let (event_router, handles) = EventRouter::new(&config.router, metrics.router.clone());

    let ingress_tx = handles.ingress_tx;
    let derived_tx = handles.derived_tx;
    let parquet_rx = handles.parquet_rx;
    let redis_rx = handles.redis_rx;
    let spot_mid_rx = handles.spot_mid_rx;
    let iv_surface_rx = handles.iv_surface_rx;

    // 5. Spawn Parquet sink (singleton, non-restartable)
    let parquet_handle = {
        let pq_config = config.sinks.parquet.clone();
        let data_dir = config.general.data_dir.clone();
        let health = health.clone();
        let metrics = metrics.parquet.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            sinks::parquet::run(pq_config, data_dir, parquet_rx, health, metrics, cancel).await;
        })
    };

    // 6. Spawn Redis sink (restartable, non-fatal)
    let redis_handle = {
        let config = config.sinks.redis.clone();
        let health = health.clone();
        let metrics = metrics.redis.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            sinks::redis::run(config, redis_rx, health, metrics, cancel).await;
        })
    };

    // 7. Spawn Router
    let router_handle = {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            event_router.run(cancel).await;
        })
    };

    // 8. Spawn pipelines
    let spot_mid_handle = {
        let config = config.pipeline.spot_mid.clone();
        let derived_tx = derived_tx.clone();
        let app_metrics = metrics.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pipeline::spot_mid::run(config, spot_mid_rx, derived_tx, app_metrics, cancel).await;
        })
    };

    let iv_surface_handle = {
        let config = config.pipeline.iv_surface.clone();
        let derived_tx = derived_tx.clone();
        let app_metrics = metrics.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            pipeline::iv_surface::run(config, iv_surface_rx, derived_tx, app_metrics, cancel).await;
        })
    };

    // 9. Spawn connectors
    if config.connectors.kraken.enabled {
        let cfg = config.connectors.kraken.clone();
        let tx = ingress_tx.clone();
        let h = health.clone();
        let m = metrics.connector.clone();
        let c = cancel.clone();
        tokio::spawn(async move {
            connectors::kraken::run(cfg, tx, h, m, c).await;
        });
    }

    if config.connectors.coinbase.enabled {
        let cfg = config.connectors.coinbase.clone();
        let tx = ingress_tx.clone();
        let h = health.clone();
        let m = metrics.connector.clone();
        let c = cancel.clone();
        tokio::spawn(async move {
            connectors::coinbase::run(cfg, tx, h, m, c).await;
        });
    }

    if config.connectors.deribit.enabled {
        let cfg = config.connectors.deribit.clone();
        let tx = ingress_tx.clone();
        let h = health.clone();
        let m = metrics.connector.clone();
        let c = cancel.clone();
        tokio::spawn(async move {
            connectors::deribit::run(cfg, tx, h, m, c).await;
        });
    }

    if config.connectors.binance.enabled {
        let cfg = config.connectors.binance.clone();
        let tx = ingress_tx.clone();
        let h = health.clone();
        let m = metrics.connector.clone();
        let c = cancel.clone();
        tokio::spawn(async move {
            connectors::binance::run(cfg, tx, h, m, c).await;
        });
    }

    // 10. Spawn metrics HTTP server
    {
        let metrics = metrics.clone();
        let health = health.clone();
        let bind = config.general.metrics_bind.clone();
        let cancel = cancel.clone();
        tokio::spawn(async move {
            ops::metrics::serve_http(bind, metrics, health, cancel).await;
        });
    }

    info!("all tasks spawned, awaiting shutdown signal");

    // 11. Await Ctrl-C
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");

    // 12. Ordered shutdown
    cancel.cancel();

    // Wait for tasks to finish (with timeout)
    let _ = tokio::time::timeout(Duration::from_secs(30), async {
        let _ = router_handle.await;
        let _ = spot_mid_handle.await;
        let _ = iv_surface_handle.await;
        let _ = redis_handle.await;
        let _ = parquet_handle.await;
    })
    .await;

    info!("md-ingest shutdown complete");
    Ok(())
}

use std::time::Duration;

fn parse_config_path() -> String {
    let args: Vec<String> = std::env::args().collect();

    // Support: md-ingest config.toml
    //          md-ingest --config config.toml
    //          md-ingest -c config.toml
    //          md-ingest (defaults to config.toml)
    for i in 1..args.len() {
        if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
            return args[i + 1].clone();
        }
        if !args[i].starts_with('-') {
            return args[i].clone();
        }
    }
    "config.toml".into()
}
