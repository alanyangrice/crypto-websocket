#!/usr/bin/env python3
"""Evaluate collected md-ingest Parquet data."""

import json
import glob
import os
import sys
from datetime import timezone

import pandas as pd
import pyarrow.parquet as pq

DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "v1")
PRICE_SCALE = 1  # prices stored as strings, parsed to float


def load_stream(stream: str) -> pd.DataFrame:
    pattern = os.path.join(DATA_DIR, stream, "**", "*.parquet")
    files = glob.glob(pattern, recursive=True)
    if not files:
        return pd.DataFrame()
    df = pq.read_table(files).to_pandas()
    df["recv_ts"] = pd.to_datetime(df["recv_ts"], utc=True)
    return df.sort_values("recv_ts").reset_index(drop=True)


def parse_payload(df: pd.DataFrame, key: str) -> pd.DataFrame:
    records = []
    for raw in df["payload"]:
        d = json.loads(raw)
        records.append(d.get(key, d))
    return pd.DataFrame(records)


def fmt_ts(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S UTC")


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ── 1. Overview ──────────────────────────────────────────────

section("DATA COLLECTION OVERVIEW")

streams = sorted(
    d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))
)

overview_rows = []
for s in streams:
    files = glob.glob(os.path.join(DATA_DIR, s, "**", "*.parquet"), recursive=True)
    if not files:
        continue
    total_rows = sum(pq.read_metadata(f).num_rows for f in files)
    size_mb = sum(os.path.getsize(f) for f in files) / 1e6
    overview_rows.append(
        {"stream": s, "files": len(files), "rows": total_rows, "size_mb": round(size_mb, 1)}
    )

ov = pd.DataFrame(overview_rows)
print(ov.to_string(index=False))
print(f"\nTotal: {ov['files'].sum()} files, {ov['rows'].sum():,} rows, {ov['size_mb'].sum():.1f} MB")


# ── 2. Time coverage ────────────────────────────────────────

section("TIME COVERAGE (per stream)")

for s in streams:
    df = load_stream(s)
    if df.empty:
        continue
    t0, t1 = df["recv_ts"].min(), df["recv_ts"].max()
    span = t1 - t0
    hours = span.total_seconds() / 3600
    print(f"  {s:26s}  {fmt_ts(t0)}  →  {fmt_ts(t1)}  ({hours:.1f}h)")


# ── 3. Spot trades analysis ─────────────────────────────────

section("SPOT TRADES")

trades_raw = load_stream("spot_trade")
if not trades_raw.empty:
    tp = parse_payload(trades_raw, "SpotTrade")
    tp["price"] = tp["price"].astype(float)
    tp["size"] = tp["size"].astype(float)
    tp["exchange_ts"] = pd.to_datetime(tp["exchange_ts"], utc=True)
    tp["recv_ts"] = pd.to_datetime(tp.get("meta", tp).apply(
        lambda m: m["recv_ts"] if isinstance(m, dict) else trades_raw["recv_ts"]
    ), utc=True) if "meta" in tp.columns else trades_raw["recv_ts"].values
    tp["venue"] = tp["venue"]

    for venue in sorted(tp["venue"].unique()):
        vt = tp[tp["venue"] == venue]
        print(f"\n  [{venue}]")
        print(f"    Trades:     {len(vt):>10,}")
        print(f"    Price range: ${vt['price'].min():,.2f} – ${vt['price'].max():,.2f}")
        print(f"    VWAP:        ${(vt['price'] * vt['size']).sum() / vt['size'].sum():,.2f}")
        print(f"    Volume:      {vt['size'].sum():.6f} BTC  (${(vt['price'] * vt['size']).sum():,.0f} notional)")

        if "exchange_ts" in tp.columns:
            lag = (trades_raw.loc[vt.index, "recv_ts"] - vt["exchange_ts"]).dt.total_seconds() * 1000
            lag = lag.dropna()
            if len(lag) > 0:
                print(f"    Latency (recv - exchange):  p50={lag.median():.0f}ms  p95={lag.quantile(0.95):.0f}ms  p99={lag.quantile(0.99):.0f}ms")

    trades_1m = trades_raw.set_index("recv_ts").resample("1min").size()
    nonzero = trades_1m[trades_1m > 0]
    gaps = trades_1m[trades_1m == 0]
    print(f"\n  Trade rate:  {nonzero.mean():.0f} trades/min avg,  {nonzero.max()} max")
    if len(gaps) > 0:
        print(f"  Gap minutes (0 trades):  {len(gaps)} out of {len(trades_1m)}")
else:
    print("  No spot trade data found.")


# ── 4. Spot mid (1s) analysis ───────────────────────────────

section("SPOT MID (1s consolidated)")

mid_raw = load_stream("spot_mid_1s")
if not mid_raw.empty:
    mp = parse_payload(mid_raw, "SpotMid1s")
    mp["s_mid"] = mp["s_mid"].astype(float)
    mp["recv_ts"] = mid_raw["recv_ts"].values

    total_secs = (mid_raw["recv_ts"].max() - mid_raw["recv_ts"].min()).total_seconds()
    expected = int(total_secs)
    coverage = len(mp) / max(expected, 1) * 100

    print(f"  Observations:  {len(mp):,}")
    print(f"  Time span:     {total_secs/3600:.1f} hours ({expected:,} expected @ 1/s)")
    print(f"  Coverage:      {coverage:.1f}%")
    print(f"  Mid range:     ${mp['s_mid'].min():,.2f} – ${mp['s_mid'].max():,.2f}")
    print(f"  Mid mean:      ${mp['s_mid'].mean():,.2f}")

    mp["ret"] = mp["s_mid"].pct_change()
    print(f"  1s return vol: {mp['ret'].std() * 100:.4f}%")

    quality = mp["quality_flag"].value_counts()
    print(f"  Quality flags: {dict(quality)}")

    age = mp["source_age_ms_max"].dropna()
    if len(age) > 0:
        print(f"  Source age:    p50={age.median():.0f}ms  p95={age.quantile(0.95):.0f}ms  max={age.max():.0f}ms")
else:
    print("  No spot mid data found.")


# ── 5. BBO (best bid/offer) ─────────────────────────────────

section("SPOT BBO (best bid/offer)")

bbo_raw = load_stream("spot_bbo")
if not bbo_raw.empty:
    bp = parse_payload(bbo_raw, "SpotBbo")
    bp["bid"] = bp["bid_px"].astype(float)
    bp["ask"] = bp["ask_px"].astype(float)
    bp["recv_ts"] = bbo_raw["recv_ts"].values
    bp["venue"] = bp["venue"]
    bp["spread_bps"] = (bp["ask"] - bp["bid"]) / ((bp["ask"] + bp["bid"]) / 2) * 10000

    for venue in sorted(bp["venue"].unique()):
        vb = bp[bp["venue"] == venue]
        print(f"\n  [{venue}]")
        print(f"    Updates:     {len(vb):>10,}")
        rate = len(vb) / max((bbo_raw["recv_ts"].max() - bbo_raw["recv_ts"].min()).total_seconds(), 1)
        print(f"    Rate:        {rate:.1f} updates/sec")
        print(f"    Spread:      p50={vb['spread_bps'].median():.2f} bps  mean={vb['spread_bps'].mean():.2f} bps  p95={vb['spread_bps'].quantile(0.95):.2f} bps")
else:
    print("  No BBO data found.")


# ── 6. Perp mark / funding ──────────────────────────────────

section("PERPETUAL MARK PRICE & FUNDING")

perp_raw = load_stream("perp_mark_funding")
if not perp_raw.empty:
    pp = parse_payload(perp_raw, "PerpMarkFunding")
    pp["mark_price"] = pp["mark_px"].astype(float)
    pp["index_price"] = pp["index_px"].astype(float)
    pp["funding_rate"] = pp["funding_rate"].astype(float)
    pp["recv_ts"] = perp_raw["recv_ts"].values

    print(f"  Observations:  {len(pp):,}")
    rate = len(pp) / max((perp_raw["recv_ts"].max() - perp_raw["recv_ts"].min()).total_seconds(), 1)
    print(f"  Rate:          {rate:.1f} updates/sec")
    print(f"  Mark range:    ${pp['mark_price'].min():,.2f} – ${pp['mark_price'].max():,.2f}")
    basis_bps = (pp["mark_price"] - pp["index_price"]) / pp["index_price"] * 10000
    print(f"  Basis (mark-index): mean={basis_bps.mean():.2f} bps  p5={basis_bps.quantile(0.05):.2f} bps  p95={basis_bps.quantile(0.95):.2f} bps")
    print(f"  Funding rate:  {pp['funding_rate'].mean()*100:.6f}%  (8h annualized: {pp['funding_rate'].mean()*3*365*100:.2f}%)")
else:
    print("  No perp mark/funding data found.")


# ── 7. DVOL index ───────────────────────────────────────────

section("DVOL INDEX (Deribit Volatility)")

dvol_raw = load_stream("dvol_index")
if not dvol_raw.empty:
    dp = parse_payload(dvol_raw, "DvolIndex")
    dp["dvol"] = dp["value"].astype(float)
    dp["recv_ts"] = dvol_raw["recv_ts"].values

    print(f"  Observations:  {len(dp):,}")
    print(f"  DVOL range:    {dp['dvol'].min():.2f} – {dp['dvol'].max():.2f}")
    print(f"  DVOL mean:     {dp['dvol'].mean():.2f}")
else:
    print("  No DVOL data found.")


# ── 8. IV surface snapshots ─────────────────────────────────

section("IV SURFACE SNAPSHOTS")

iv_raw = load_stream("iv_surface_snapshot")
if not iv_raw.empty:
    ivp = parse_payload(iv_raw, "IvSurfaceSnapshot")
    ivp["atm_iv"] = ivp["atm_iv"].astype(float)
    ivp["recv_ts"] = iv_raw["recv_ts"].values

    n_expiries = ivp["expiry_ts"].nunique()
    print(f"  Snapshots:     {len(ivp):,}")
    print(f"  Expiries:      {n_expiries}")
    print(f"  ATM IV range:  {ivp['atm_iv'].min():.2f} – {ivp['atm_iv'].max():.2f}")

    for exp in sorted(ivp["expiry_ts"].unique()):
        sub = ivp[ivp["expiry_ts"] == exp]
        print(f"    {exp}  ATM IV: {sub['atm_iv'].mean():.2f} mean  ({len(sub)} snapshots)")
else:
    print("  No IV surface data found.")


# ── 9. Options flow ─────────────────────────────────────────

section("OPTIONS TICKERS")

opt_raw = load_stream("opt_ticker")
if not opt_raw.empty:
    print(f"  Total updates: {len(opt_raw):,}")
    rate = len(opt_raw) / max((opt_raw["recv_ts"].max() - opt_raw["recv_ts"].min()).total_seconds(), 1)
    print(f"  Rate:          {rate:.1f} updates/sec")

    op = parse_payload(opt_raw, "OptTicker")
    n_instruments = op["instrument_name"].nunique() if "instrument_name" in op.columns else "?"
    print(f"  Instruments:   {n_instruments}")
else:
    print("  No options ticker data found.")


# ── 10. Data quality summary ────────────────────────────────

section("DATA QUALITY SUMMARY")

issues = []
if not mid_raw.empty:
    total_secs = (mid_raw["recv_ts"].max() - mid_raw["recv_ts"].min()).total_seconds()
    if len(mid_raw) / max(total_secs, 1) < 0.9:
        issues.append(f"Spot mid coverage is {len(mid_raw)/max(total_secs,1)*100:.0f}% (< 90%)")

if not trades_raw.empty:
    gaps_1m = trades_raw.set_index("recv_ts").resample("1min").size()
    gap_count = (gaps_1m == 0).sum()
    if gap_count > 0:
        issues.append(f"{gap_count} minutes with zero trades")

if not bbo_raw.empty:
    bp2 = parse_payload(bbo_raw, "SpotBbo")
    bp2["spread"] = bp2["ask_px"].astype(float) - bp2["bid_px"].astype(float)
    crossed = (bp2["spread"] < 0).sum()
    if crossed > 0:
        issues.append(f"{crossed} crossed BBO quotes (bid > ask)")

if issues:
    for i in issues:
        print(f"  ⚠  {i}")
else:
    print("  ✓  No issues detected.")

print()
