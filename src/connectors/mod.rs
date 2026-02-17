pub mod binance;
pub mod coinbase;
pub mod deribit;
pub mod kraken;

use futures_util::stream::{SplitSink, SplitStream};
use lru::LruCache;
use rand::Rng;
use std::num::NonZeroUsize;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};
use uuid::Uuid;

use crate::events::Venue;
use crate::ops::health::{ConnectorState, HealthRegistry};
use crate::ops::metrics::ConnectorMetrics;

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type WsSink = SplitSink<WsStream, tokio_tungstenite::tungstenite::Message>;
pub type WsSource = SplitStream<WsStream>;

/// Exponential backoff with jitter for WS reconnections.
pub async fn connect_with_backoff(
    url: &str,
    venue: Venue,
    health: &HealthRegistry,
    metrics: &ConnectorMetrics,
    max_backoff: Duration,
) -> (WsStream, Uuid) {
    let mut delay = Duration::from_secs(1);
    let _session_id = loop {
        health.set_venue_state(venue, ConnectorState::Reconnecting);
        match connect_async(url).await {
            Ok((ws, _response)) => {
                let sid = Uuid::new_v4();
                health.set_venue_state(venue, ConnectorState::Connected);
                health.set_venue_session(venue, sid);
                metrics.connected.with_label_values(&[&venue.to_string()]).set(1);
                info!(venue = %venue, session_id = %sid, "websocket connected");
                return (ws, sid);
            }
            Err(e) => {
                metrics.reconnects_total.with_label_values(&[&venue.to_string()]).inc();
                health.increment_reconnects(venue);
                let jitter = rand::thread_rng().gen_range(0.75..1.25);
                let actual_delay = delay.mul_f64(jitter);
                warn!(
                    venue = %venue,
                    error = %e,
                    retry_in_ms = actual_delay.as_millis(),
                    "websocket connection failed, retrying"
                );
                sleep(actual_delay).await;
                delay = (delay * 2).min(max_backoff);
            }
        }
    };
}

/// Trade deduplication cache keyed by trade_id string.
pub struct TradeDedup {
    cache: LruCache<String, ()>,
}

impl TradeDedup {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: LruCache::new(NonZeroUsize::new(capacity).unwrap()),
        }
    }

    /// Returns true if this trade_id was NOT seen before (i.e., it's new).
    pub fn check_and_insert(&mut self, trade_id: &str) -> bool {
        if self.cache.contains(trade_id) {
            false
        } else {
            self.cache.put(trade_id.to_string(), ());
            true
        }
    }
}
