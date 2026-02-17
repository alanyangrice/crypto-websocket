use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::config::RouterConfig;
use crate::events::Event;
use crate::ops::metrics::RouterMetrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Ingress,
    Derived,
}

pub struct EventRouter {
    ingress_rx: mpsc::Receiver<Event>,
    derived_rx: mpsc::Receiver<Event>,
    parquet_tx: mpsc::Sender<Event>,
    redis_tx: mpsc::Sender<Event>,
    spot_mid_tx: mpsc::Sender<Event>,
    iv_surface_tx: mpsc::Sender<Event>,
    debug_tx: Option<broadcast::Sender<Event>>,
    seq_counter: u64,
    metrics: RouterMetrics,
}

/// Handles returned when building the router, containing all channel endpoints
/// that other components need.
pub struct RouterHandles {
    pub ingress_tx: mpsc::Sender<Event>,
    pub derived_tx: mpsc::Sender<Event>,
    pub parquet_rx: mpsc::Receiver<Event>,
    pub redis_rx: mpsc::Receiver<Event>,
    pub spot_mid_rx: mpsc::Receiver<Event>,
    pub iv_surface_rx: mpsc::Receiver<Event>,
    pub debug_rx: Option<broadcast::Receiver<Event>>,
}

impl EventRouter {
    pub fn new(config: &RouterConfig, metrics: RouterMetrics) -> (Self, RouterHandles) {
        let (ingress_tx, ingress_rx) = mpsc::channel(config.ingress_buffer);
        let (derived_tx, derived_rx) = mpsc::channel(config.derived_buffer);
        let (parquet_tx, parquet_rx) = mpsc::channel(config.parquet_buffer);
        let (redis_tx, redis_rx) = mpsc::channel(config.redis_buffer);
        let (spot_mid_tx, spot_mid_rx) = mpsc::channel(config.pipeline_buffer);
        let (iv_surface_tx, iv_surface_rx) = mpsc::channel(config.pipeline_buffer);

        let (debug_tx, debug_rx) = {
            let (tx, rx) = broadcast::channel(1024);
            (Some(tx), Some(rx))
        };

        let router = Self {
            ingress_rx,
            derived_rx,
            parquet_tx,
            redis_tx,
            spot_mid_tx,
            iv_surface_tx,
            debug_tx,
            seq_counter: 0,
            metrics,
        };

        let handles = RouterHandles {
            ingress_tx,
            derived_tx,
            parquet_rx,
            redis_rx,
            spot_mid_rx,
            iv_surface_rx,
            debug_rx,
        };

        (router, handles)
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        tracing::info!("router started");
        loop {
            let (source, event) = tokio::select! {
                biased;
                Some(e) = self.ingress_rx.recv() => (Source::Ingress, e),
                Some(e) = self.derived_rx.recv() => (Source::Derived, e),
                _ = cancel.cancelled() => break,
            };
            self.route_event(source, event).await;

            // Fairness: after each ingress event, drain one derived if available
            if let Ok(derived) = self.derived_rx.try_recv() {
                self.route_event(Source::Derived, derived).await;
            }
        }

        // Drain remaining events before exiting
        tracing::info!("router draining remaining events");
        while let Ok(event) = self.ingress_rx.try_recv() {
            self.route_event(Source::Ingress, event).await;
        }
        while let Ok(event) = self.derived_rx.try_recv() {
            self.route_event(Source::Derived, event).await;
        }
        tracing::info!("router shutdown complete");
    }

    async fn route_event(&mut self, source: Source, mut event: Event) {
        self.seq_counter += 1;
        event.set_seq(self.seq_counter);
        self.metrics.events_total.inc();

        // CRITICAL PATH: Parquet gets everything, blocking
        if self.parquet_tx.send(event.clone()).await.is_err() {
            tracing::error!("parquet sink channel closed — fatal");
            return;
        }

        // BEST-EFFORT: Redis gets derived + latest state
        if self.should_cache(&event) {
            if self.redis_tx.try_send(event.clone()).is_err() {
                self.metrics.redis_queue_drops.inc();
            }
        }

        // Pipeline fanout: ONLY for raw ingress events
        if source == Source::Ingress {
            match &event {
                Event::SpotBbo { .. } => {
                    if let Err(_e) = self.spot_mid_tx.send(event.clone()).await {
                        debug!("spot_mid pipeline channel closed");
                    }
                }
                Event::OptTicker { .. } | Event::OptInstrument { .. } => {
                    if let Err(_e) = self.iv_surface_tx.send(event.clone()).await {
                        debug!("iv_surface pipeline channel closed");
                    }
                }
                _ => {}
            }
        } else {
            self.metrics.derived_processed.inc();
        }

        if let Some(ref debug_tx) = self.debug_tx {
            let _ = debug_tx.send(event);
        }
    }

    fn should_cache(&self, event: &Event) -> bool {
        matches!(
            event,
            Event::SpotBbo { .. }
                | Event::SpotMid1s { .. }
                | Event::IvSurfaceSnapshot { .. }
                | Event::PerpMarkFunding { .. }
                | Event::DvolIndex { .. }
                | Event::SessionBoundary { .. }
        )
    }
}
