use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

use crate::events::Venue;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum ConnectorState {
    Connected,
    Reconnecting,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum SinkState {
    Healthy,
    Flushing,
    Degraded,
    Down,
    Fatal,
}

#[derive(Clone, Debug, Serialize)]
pub struct VenueHealth {
    pub state: ConnectorState,
    pub session_id: Option<Uuid>,
    pub last_recv_ts: Option<DateTime<Utc>>,
    pub reconnect_count: u64,
}

impl Default for VenueHealth {
    fn default() -> Self {
        Self {
            state: ConnectorState::Reconnecting,
            session_id: None,
            last_recv_ts: None,
            reconnect_count: 0,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct HealthSnapshot {
    pub venues: HashMap<String, VenueHealth>,
    pub parquet_state: SinkState,
    pub redis_state: SinkState,
    pub last_parquet_flush_ts: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct HealthRegistry {
    inner: Arc<RwLock<HealthInner>>,
}

struct HealthInner {
    venues: HashMap<Venue, VenueHealth>,
    parquet_state: SinkState,
    redis_state: SinkState,
    last_parquet_flush_ts: Option<DateTime<Utc>>,
}

impl HealthRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HealthInner {
                venues: HashMap::new(),
                parquet_state: SinkState::Healthy,
                redis_state: SinkState::Down,
                last_parquet_flush_ts: None,
            })),
        }
    }

    pub fn set_venue_state(&self, venue: Venue, state: ConnectorState) {
        let mut inner = self.inner.write().unwrap();
        let entry = inner.venues.entry(venue).or_default();
        entry.state = state;
    }

    pub fn set_venue_session(&self, venue: Venue, session_id: Uuid) {
        let mut inner = self.inner.write().unwrap();
        let entry = inner.venues.entry(venue).or_default();
        entry.session_id = Some(session_id);
    }

    pub fn record_venue_recv(&self, venue: Venue) {
        let mut inner = self.inner.write().unwrap();
        let entry = inner.venues.entry(venue).or_default();
        entry.last_recv_ts = Some(Utc::now());
    }

    pub fn increment_reconnects(&self, venue: Venue) {
        let mut inner = self.inner.write().unwrap();
        let entry = inner.venues.entry(venue).or_default();
        entry.reconnect_count += 1;
    }

    pub fn set_parquet_state(&self, state: SinkState) {
        self.inner.write().unwrap().parquet_state = state;
    }

    pub fn set_redis_state(&self, state: SinkState) {
        self.inner.write().unwrap().redis_state = state;
    }

    pub fn set_last_parquet_flush(&self, ts: DateTime<Utc>) {
        self.inner.write().unwrap().last_parquet_flush_ts = Some(ts);
    }

    pub fn snapshot(&self) -> HealthSnapshot {
        let inner = self.inner.read().unwrap();
        HealthSnapshot {
            venues: inner
                .venues
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            parquet_state: inner.parquet_state,
            redis_state: inner.redis_state,
            last_parquet_flush_ts: inner.last_parquet_flush_ts,
        }
    }
}
