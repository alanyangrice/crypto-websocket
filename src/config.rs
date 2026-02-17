use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub general: GeneralConfig,
    pub connectors: ConnectorsConfig,
    pub router: RouterConfig,
    pub pipeline: PipelineConfig,
    pub sinks: SinksConfig,
    pub supervisor: SupervisorConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GeneralConfig {
    pub data_dir: PathBuf,
    pub metrics_bind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorsConfig {
    pub kraken: KrakenConfig,
    pub coinbase: CoinbaseConfig,
    pub deribit: DeribitConfig,
    pub binance: BinanceConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KrakenConfig {
    pub enabled: bool,
    pub url: String,
    pub symbols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoinbaseConfig {
    pub enabled: bool,
    pub url: String,
    pub products: Vec<String>,
    pub jwt: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeribitConfig {
    pub enabled: bool,
    pub url: String,
    pub currency: String,
    pub max_expiries: usize,
    pub strike_range_factor: f64,
    pub max_instruments_per_expiry: usize,
    pub instrument_refresh_secs: u64,
    pub ticker_staleness_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceConfig {
    pub enabled: bool,
    pub url: String,
    pub symbol: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RouterConfig {
    pub ingress_buffer: usize,
    pub derived_buffer: usize,
    pub parquet_buffer: usize,
    pub redis_buffer: usize,
    pub pipeline_buffer: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    pub spot_mid: SpotMidConfig,
    pub iv_surface: IvSurfaceConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SpotMidConfig {
    pub interval_ms: u64,
    pub divergence_threshold_bps: f64,
    pub staleness_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IvSurfaceConfig {
    pub interval_ms: u64,
    pub staleness_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinksConfig {
    pub parquet: ParquetSinkConfig,
    pub redis: RedisSinkConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetSinkConfig {
    pub schema_version: String,
    pub buffer_rows: usize,
    pub flush_interval_ms: u64,
    pub price_scale: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisSinkConfig {
    pub enabled: bool,
    pub url: String,
    pub key_prefix: String,
    pub pubsub_enabled: bool,
    pub pubsub_channel: String,
    pub write_timeout_ms: u64,
    pub health_update_ms: u64,
    pub max_pending_keys: usize,
    pub spot_mid_window_len: usize,
    pub iv_surface_window_len: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SupervisorConfig {
    pub max_restarts_per_window: usize,
    pub restart_window_secs: u64,
}

impl AppConfig {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
