use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    Kraken,
    Coinbase,
    Deribit,
    Binance,
}

impl std::fmt::Display for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Venue::Kraken => write!(f, "kraken"),
            Venue::Coinbase => write!(f, "coinbase"),
            Venue::Deribit => write!(f, "deribit"),
            Venue::Binance => write!(f, "binance"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallPut {
    Call,
    Put,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum QualityFlag {
    Ok,
    Degraded,
    Suspect,
}

impl std::fmt::Display for QualityFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QualityFlag::Ok => write!(f, "ok"),
            QualityFlag::Degraded => write!(f, "degraded"),
            QualityFlag::Suspect => write!(f, "suspect"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventMeta {
    pub seq: u64,
    pub session_id: Uuid,
    pub recv_ts: DateTime<Utc>,
}

impl EventMeta {
    pub fn new(session_id: Uuid) -> Self {
        Self {
            seq: 0,
            session_id,
            recv_ts: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Event {
    SpotBbo {
        meta: EventMeta,
        venue: Venue,
        product: String,
        bid_px: Decimal,
        bid_sz: Decimal,
        ask_px: Decimal,
        ask_sz: Decimal,
        exchange_ts: Option<DateTime<Utc>>,
    },
    SpotTrade {
        meta: EventMeta,
        venue: Venue,
        product: String,
        price: Decimal,
        size: Decimal,
        side: Side,
        trade_id: String,
        exchange_ts: Option<DateTime<Utc>>,
    },
    OptInstrument {
        meta: EventMeta,
        instrument_name: String,
        underlying: String,
        expiry_ts: DateTime<Utc>,
        strike: Decimal,
        cp: CallPut,
    },
    OptTicker {
        meta: EventMeta,
        instrument_name: String,
        bid_px: Option<Decimal>,
        ask_px: Option<Decimal>,
        mark_px: Decimal,
        mark_iv: Option<f64>,
        bid_iv: Option<f64>,
        ask_iv: Option<f64>,
        underlying_index_px: Decimal,
        open_interest: Decimal,
        exchange_ts: Option<DateTime<Utc>>,
    },
    PerpMarkFunding {
        meta: EventMeta,
        symbol: String,
        mark_px: Decimal,
        index_px: Decimal,
        funding_rate: Decimal,
        next_funding_time: DateTime<Utc>,
        exchange_ts: Option<DateTime<Utc>>,
    },
    DvolIndex {
        meta: EventMeta,
        value: f64,
        exchange_ts: Option<DateTime<Utc>>,
    },

    // --- Derived ---
    SpotMid1s {
        meta: EventMeta,
        s_mid: Decimal,
        quality_flag: QualityFlag,
        source_venues: Vec<Venue>,
        source_age_ms_max: Option<i64>,
        venue_mids: Vec<(Venue, Decimal)>,
    },
    IvSurfaceSnapshot {
        meta: EventMeta,
        expiry_ts: DateTime<Utc>,
        atm_iv: f64,
        iv_points: Vec<(Decimal, f64)>,
        underlying_index_px: Decimal,
        forward_proxy: Option<Decimal>,
        n_points: u32,
        n_valid_points: u32,
        min_strike: Decimal,
        max_strike: Decimal,
        spread_iv_atm: Option<f64>,
    },

    // --- Operational ---
    SessionBoundary {
        meta: EventMeta,
        venue: Venue,
        session_id: Uuid,
    },
}

impl Event {
    pub fn set_seq(&mut self, seq: u64) {
        match self {
            Event::SpotBbo { meta, .. }
            | Event::SpotTrade { meta, .. }
            | Event::OptInstrument { meta, .. }
            | Event::OptTicker { meta, .. }
            | Event::PerpMarkFunding { meta, .. }
            | Event::DvolIndex { meta, .. }
            | Event::SpotMid1s { meta, .. }
            | Event::IvSurfaceSnapshot { meta, .. }
            | Event::SessionBoundary { meta, .. } => {
                meta.seq = seq;
            }
        }
    }

    pub fn meta(&self) -> &EventMeta {
        match self {
            Event::SpotBbo { meta, .. }
            | Event::SpotTrade { meta, .. }
            | Event::OptInstrument { meta, .. }
            | Event::OptTicker { meta, .. }
            | Event::PerpMarkFunding { meta, .. }
            | Event::DvolIndex { meta, .. }
            | Event::SpotMid1s { meta, .. }
            | Event::IvSurfaceSnapshot { meta, .. }
            | Event::SessionBoundary { meta, .. } => meta,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            Event::SpotBbo { .. } => "spot_bbo",
            Event::SpotTrade { .. } => "spot_trade",
            Event::OptInstrument { .. } => "opt_instrument",
            Event::OptTicker { .. } => "opt_ticker",
            Event::PerpMarkFunding { .. } => "perp_mark_funding",
            Event::DvolIndex { .. } => "dvol_index",
            Event::SpotMid1s { .. } => "spot_mid_1s",
            Event::IvSurfaceSnapshot { .. } => "iv_surface_snapshot",
            Event::SessionBoundary { .. } => "session_boundary",
        }
    }

    /// Returns the venue for events that have one.
    pub fn venue(&self) -> Option<Venue> {
        match self {
            Event::SpotBbo { venue, .. }
            | Event::SpotTrade { venue, .. }
            | Event::SessionBoundary { venue, .. } => Some(*venue),
            Event::PerpMarkFunding { .. } => Some(Venue::Binance),
            Event::OptTicker { .. } | Event::OptInstrument { .. } | Event::DvolIndex { .. } => {
                Some(Venue::Deribit)
            }
            Event::SpotMid1s { .. } | Event::IvSurfaceSnapshot { .. } => None,
        }
    }
}
