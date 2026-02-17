# md-ingest

A low-latency Rust service that ingests real-time BTC market data from multiple venues, computes derived analytics, and persists everything to a durable Parquet data lake and a Redis hot-state cache.

## Architecture

```
Connectors ─► mpsc Ingress ─► Router ─┬─► Parquet Sink  (durable cold store)
                                       ├─► Redis Sink    (best-effort hot cache)
Pipelines  ◄─── type-filtered channels │
    │                                  │
    └──► mpsc Derived (try_send) ──────┘
```

**Connectors** maintain persistent WebSocket connections to each venue, parse exchange-specific JSON into normalized events, and push them into the router's ingress channel.

**Router** assigns monotonic sequence numbers, fans out to the Parquet sink (blocking — never loses data), the Redis sink (best-effort — drops under backpressure), and type-filtered pipeline channels. Derived events arrive on a separate channel with fairness guarantees to prevent starvation.

**Pipelines** consume filtered events, compute derived streams, and send results back to the router via non-blocking `try_send()` to prevent deadlock.

**Sinks** persist data in two tiers:
- **Parquet** — atomic `.tmp` → `.parquet` rename, versioned paths (`data/v1/...`), ZSTD compression, watermark metadata
- **Redis** — coalescing write loop (latest-per-key), `XADD MAXLEN ~` rolling windows, periodic health publisher, Pub/Sub notifications

## Data Sources

| Venue | Channels | Data |
|-------|----------|------|
| **Kraken** | `ticker` (BBO), `trade` | Spot L1 + trades for BTC/USD |
| **Coinbase** | `ticker`, `market_trades`, `heartbeats` | Spot L1 + trades for BTC-USD |
| **Deribit** | `public/get_instruments`, `ticker.*`, DVOL | BTC option chain + implied vol |
| **Binance** | `btcusdt@markPrice@1s` | Perpetual mark price + funding rate |

## Derived Streams

- **`spot_mid_1s`** — Consolidated mid-price sampled every 1s across venues with quality flags (`Ok` / `Degraded` / `Suspect`) and staleness tracking
- **`iv_surface_snapshot`** — Per-expiry IV surface with ATM bracket interpolation, liquidity filtering, and quality statistics

## Quick Start

### Prerequisites

- Rust 1.75+ (`rustup` recommended)
- Redis 7+ (optional — service runs without it, setting sink to degraded)

### Build & Run

```bash
cargo build --release
./target/release/md-ingest config.toml
```

Or for development:

```bash
cargo run -- config.toml
```

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | Prometheus metrics |
| `GET /health` | JSON health snapshot (per-venue state, sink states) |

### Configuration

All settings live in `config.toml`. Key sections:

- `[connectors.*]` — enable/disable venues, set URLs and symbols
- `[router]` — channel buffer sizes (ingress, derived, parquet, redis, pipeline)
- `[pipeline.*]` — sampling intervals, staleness thresholds, divergence limits
- `[sinks.parquet]` — schema version, buffer size, flush interval, price scale
- `[sinks.redis]` — URL, key prefix, Pub/Sub toggle, coalescing limits, window lengths
- `[supervisor]` — restart budgets

### Redis Keys (when enabled)

```
GET  md:v1:spot_mid                     # latest consolidated mid
GET  md:v1:spot_bbo:kraken:BTC-USD      # latest Kraken BBO
GET  md:v1:iv_surface:{expiry_ts}       # latest IV surface for expiry
GET  md:v1:health                       # venue + sink states
XRANGE md:v1:spot_mid:window - + COUNT 60   # last 60s of mids
SUBSCRIBE md:v1:updates                 # push notifications
```

## Project Structure

```
src/
  main.rs              # entrypoint, task supervisor, ordered shutdown
  config.rs            # typed config (serde + toml)
  events.rs            # normalized event enum (Decimal prices, EventMeta)
  prices.rs            # centralized Decimal ↔ scaled i64 conversion
  router.rs            # mpsc router with Source tagging + fairness
  connectors/
    mod.rs             # connect_with_backoff, TradeDedup
    kraken.rs          # Kraken WS v2
    coinbase.rs        # Coinbase Advanced Trade WS
    deribit.rs         # Deribit JSON-RPC WS
    binance.rs         # Binance fstream WS
  pipeline/
    spot_mid.rs        # 1s consolidated spot mid
    iv_surface.rs      # periodic IV surface snapshots
  sinks/
    parquet.rs         # durable Parquet writer
    redis.rs           # best-effort Redis hot cache
  ops/
    metrics.rs         # Prometheus + HTTP server
    health.rs          # health state machine
    logging.rs         # tracing-subscriber JSON setup
```

## License

Private — not licensed for redistribution.
