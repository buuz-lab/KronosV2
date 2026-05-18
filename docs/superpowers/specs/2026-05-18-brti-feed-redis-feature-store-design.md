# BRTI Feed + Redis Feature Store — Phase 1 Design

**Date:** 2026-05-18  
**System:** BTC Kalshi Trading System (Kronos V2)  
**Scope:** Phase 1 data infrastructure — BRTI composite price feed and Redis feature store  
**Full spec:** `~/Downloads/btc_kalshi_trading_system.md`

---

## Context

Kalshi KXBTC markets resolve on **CF Benchmarks BRTI** (Bitcoin Real-Time Index), specifically the simple average of the 60 seconds before resolution (e.g. 6:30 PM EDT). This means all model inputs must use BRTI-derived candles, not Binance/Coinbase spot — which can diverge $50–200 during volatile periods.

Phase 1 builds the data foundation that everything else depends on:
- A live BRTI price composite from 3 constituent exchange WebSocket feeds
- A Redis-backed feature store that exposes resolution estimates and OHLCV candles to downstream components (Kronos model, signal fusion)

CF Benchmarks API access is pending. The architecture stubs the primary source so it plugs in with zero refactor when the API key arrives.

---

## Architecture

Three narrow classes with clean interfaces:

```
Coinbase WS ──┐
Kraken WS  ───┤──► BRTIAggregator ──► FeatureStore ──► Redis
Bitstamp WS ──┘         │
                   CF Benchmarks stub
                   (swaps in here as primary)
```

**Data flow:**
1. Each `ExchangeFeed` maintains a persistent WebSocket connection, normalizes ticks, pushes to an `asyncio.Queue[Tick]`
2. `BRTIAggregator` merges 3 queues, computes volume-weighted composite on each incoming tick, pushes `float` prices to an output queue
3. `FeatureStore` consumes the price queue, maintains an in-memory tick deque, resamples to OHLCV, writes everything to Redis

The in-memory deque does all computation; Redis is a durable read cache. Downstream code reads synchronously from Redis — it never touches the async layer.

---

## File Structure

```
btc_kalshi_system/
├── data/
│   ├── models.py              # Tick dataclass (shared between exchange_feed + aggregator)
│   ├── exchange_feed.py       # ExchangeFeed ABC + Coinbase/Kraken/Bitstamp subclasses
│   ├── brti_aggregator.py     # Volume-weighted composite + CF Benchmarks stub
│   └── feature_store.py       # Redis writer + synchronous read API
├── scripts/
│   └── validate_composite.py  # 10-minute live run, logs ticks to CSV, prints spread stats
├── tests/
│   ├── test_exchange_feed.py   # Unit tests for each exchange parser (no network)
│   ├── test_brti_aggregator.py # Unit tests for composite weighting + staleness
│   └── test_feature_store.py   # Unit tests for resolution estimate + OHLCV resampling
├── config.py
└── requirements.txt
```

---

## Component Designs

### `data/models.py`

```python
from dataclasses import dataclass

@dataclass
class Tick:
    exchange: str
    price: float
    volume: float    # 24h volume (or per-trade size for Bitstamp) — used for weighting
    timestamp: float # unix seconds
```

### `data/exchange_feed.py`

Abstract base class with exponential-backoff reconnection. Three concrete subclasses.

```python
class ExchangeFeed(ABC):
    RECONNECT_DELAYS = [1, 2, 4, 8, 16, 32, 60]  # seconds, capped at 60

    async def run(self, queue: asyncio.Queue[Tick]) -> None:
        """Entry point. Reconnects forever with exponential backoff."""

    @abstractmethod
    async def _connect_and_stream(self, queue: asyncio.Queue[Tick]) -> None:
        """Open WS, subscribe, parse, push Ticks. Raise on disconnect."""

class CoinbaseFeed(ExchangeFeed):
    # wss://advanced-trade-api.coinbase.com/ws/public
    # Subscribe: {"type": "subscribe", "channel": "ticker", "product_ids": ["BTC-USD"]}
    # Parse:     msg["events"][0]["tickers"][0] -> price, volume_24h

class KrakenFeed(ExchangeFeed):
    # wss://ws.kraken.com/v2
    # Subscribe: {"method": "subscribe", "params": {"channel": "ticker", "symbol": ["BTC/USD"]}}
    # Parse:     msg["data"][0] -> last, volume.today

class BitstampFeed(ExchangeFeed):
    # wss://ws.bitstamp.net
    # Subscribe: {"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}}
    # Parse:     msg["data"] -> price, amount (per-trade size, used as volume weight)
```

**Behaviors:**
- Each feed reconnects independently — one exchange going down doesn't affect others
- All three normalize to `Tick` before entering the queue — no exchange-specific logic leaks into the aggregator
- Health status exposed via `feed.is_connected: bool`

### `data/brti_aggregator.py`

```python
class BRTIAggregator:
    STALE_THRESHOLD_SECONDS = 5.0  # exclude ticks older than this from composite

    def __init__(self):
        self._latest: dict[str, Tick] = {}
        self._out_queue: asyncio.Queue[float] = asyncio.Queue()

    async def run(self, exchange_queues: list[asyncio.Queue[Tick]]) -> None:
        """Merge all exchange queues. Compute composite on each new tick."""
        tasks = [self._drain(q) for q in exchange_queues]
        await asyncio.gather(*tasks)

    async def _drain(self, queue: asyncio.Queue[Tick]) -> None:
        while True:
            tick = await queue.get()          # asyncio.Queue requires .get(), not async for
            self._latest[tick.exchange] = tick
            price = await self._cf_benchmarks_source()  # primary (None until implemented)
            if price is None:
                price = self._composite()               # fallback to composite
            if price is not None:
                await self._out_queue.put(price)

    def _composite(self) -> float | None:
        """
        Volume-weighted average of all exchanges with fresh ticks.
        Returns None if no exchanges have reported within STALE_THRESHOLD_SECONDS.
        Falls back to equal-weight average if total volume is 0.
        """

    # CF Benchmarks plug-in point:
    async def _cf_benchmarks_source(self) -> float | None:
        """
        Primary BRTI from CF Benchmarks REST/WS API.
        Returns None while unimplemented — composite is used as fallback.
        When CF Benchmarks API key arrives: open WS, parse 1-second ticks, return price.
        """
        return None

    @property
    def out_queue(self) -> asyncio.Queue[float]:
        return self._out_queue
```

**Staleness:** Any exchange tick older than 5 seconds is excluded from the weighted average. If all ticks are stale, `_composite()` returns `None` and FeatureStore skips that write cycle — preventing stale data from polluting the resolution estimate.

### `data/feature_store.py`

```python
class FeatureStore:
    TICK_BUFFER_SIZE = 7200  # 2 hours of second-level ticks

    def __init__(self, redis_url: str):
        self._tick_buffer: deque[tuple[float, float]] = deque(maxlen=self.TICK_BUFFER_SIZE)
        self._redis = redis.from_url(redis_url)

    # ── Async writer (called from main event loop) ─────────────────────────
    async def run(self, price_queue: asyncio.Queue[float]) -> None:
        while True:
            price = await price_queue.get()  # asyncio.Queue requires .get(), not async for
            self._tick_buffer.append((time.time(), price))
            self._flush_to_redis()

    def _flush_to_redis(self) -> None:
        now = time.time()
        pipe = self._redis.pipeline()
        # Tick list (newest first, trimmed to TICK_BUFFER_SIZE)
        pipe.lpush("brti:ticks", f"{now}:{self._tick_buffer[-1][1]}")
        pipe.ltrim("brti:ticks", 0, self.TICK_BUFFER_SIZE - 1)
        # Resolution estimate
        est = self._resolution_estimate()
        if est is not None:
            pipe.set("brti:resolution_estimate", est, ex=10)
        # OHLCV candles
        for tf in ["5min", "15min", "1h"]:
            df = self._resample(tf)
            if df is not None:
                pipe.set(f"brti:ohlcv:{tf}", df.to_json(), ex={"5min": 600, "15min": 1800, "1h": 7200}[tf])
        pipe.execute()

    # ── Synchronous read API (called by Kronos / signal fusion) ───────────
    def get_resolution_estimate(self) -> float | None:
        """60s rolling average — mirrors Kalshi resolution logic exactly."""
        val = self._redis.get("brti:resolution_estimate")
        return float(val) if val else None

    def get_ohlcv(self, timeframe: str) -> pd.DataFrame | None:
        """
        OHLCV DataFrame in Kronos format: [open, high, low, close, volume, amount].
        Returns None if insufficient data for the requested timeframe.
        """
        raw = self._redis.get(f"brti:ohlcv:{timeframe}")
        return pd.read_json(raw) if raw else None

    def get_raw_ticks(self, n_seconds: int) -> pd.Series | None:
        """Last n_seconds of tick prices as a pd.Series indexed by timestamp."""

    # ── Internal ──────────────────────────────────────────────────────────
    def _resolution_estimate(self) -> float | None:
        now = time.time()
        last_60 = [p for ts, p in self._tick_buffer if now - ts <= 60]
        return float(np.mean(last_60)) if last_60 else None

    def _resample(self, timeframe: str) -> pd.DataFrame | None:
        """
        Resample tick_buffer to OHLCV using pandas.
        Returns None if buffer has fewer ticks than one full candle period.
        Output columns: open, high, low, close, volume, amount (Kronos format).
        volume and amount are placeholder zeros in Phase 1 (no per-tick volume from composite).
        """
```

**Redis key schema:**

| Key | Type | TTL |
|-----|------|-----|
| `brti:ticks` | List (newest first) | None (LTRIM to 7200) |
| `brti:resolution_estimate` | String (float) | 10s |
| `brti:ohlcv:5min` | String (JSON DataFrame) | 600s |
| `brti:ohlcv:15min` | String (JSON DataFrame) | 1800s |
| `brti:ohlcv:1h` | String (JSON DataFrame) | 7200s |

**Note on volume in OHLCV:** The composite BRTI doesn't track per-tick trade volume (only 24h volume for weighting). Phase 1 OHLCV will have zero-filled `volume` and `amount` columns. This is a known limitation — correct volume comes from CF Benchmarks API or a per-trade WebSocket subscription. Kronos can ingest zero-volume candles; flag this when running calibration.

---

## Validation Script (`scripts/validate_composite.py`)

Runs the live feed for N minutes, logs all composite ticks to CSV, prints summary:

```
Usage: python scripts/validate_composite.py --minutes 10

Running composite feed for 10 minutes...
Ticks received: 598  (Coinbase: 211, Kraken: 193, Bitstamp: 194)
Composite range: $103,420 – $103,891
Exchange max spread (instantaneous): $14.20
Exchange avg spread: $4.10
Missing ticks (>2s gap): 2
OHLCV resampled (5min): 2 candles
OHLCV resampled (15min): 0 candles (need 15min of data)
Resolution estimate at end: $103,651.22
```

Once CF Benchmarks API arrives: re-run with `--compare-brti` to measure deviation from official BRTI. Target: < $20 average deviation.

---

## Testing Strategy

**Unit tests — no network, no Redis:**

- `test_exchange_feed.py`
  - For each feed class: feed a raw WebSocket JSON message → assert correct `Tick` (price, volume, exchange, timestamp)
  - Assert reconnect backoff sequence is correct (mock asyncio.sleep)

- `test_brti_aggregator.py`
  - Volume-weighted composite: inject 3 Ticks with known prices+volumes, assert composite matches expected formula
  - Staleness filtering: inject a Tick with timestamp 10s ago, assert it's excluded from composite
  - Equal-weight fallback: inject Ticks with volume=0, assert simple average

- `test_feature_store.py`
  - Resolution estimate: populate `_tick_buffer` with 60 ticks at known timestamps, assert `_resolution_estimate()` = mean
  - OHLCV resampling: populate buffer with 10 minutes of synthetic ticks, assert `_resample("5min")` returns 2 rows with correct OHLCV columns
  - Insufficient data: call `_resample("1h")` with only 5 minutes of data, assert returns None

**Integration smoke test:**
```bash
python -m btc_kalshi_system.data.feature_store --run-for 60
```
Runs full stack for 60 seconds, asserts: resolution estimate is non-None, at least 1 OHLCV candle exists for 5min timeframe.

---

## Known Limitations

1. **Zero volume in OHLCV:** Phase 1 composite uses 24h exchange volume for tick weighting only — not per-trade volume. OHLCV `volume`/`amount` columns will be zero. Acceptable for Kronos inference but must be noted when interpreting calibration results.

2. **Composite ≠ official BRTI:** The volume-weighted composite is highly correlated (~$5–20 deviation) but not identical to BRTI. CF Benchmarks uses a proprietary methodology including their own outlier filters. Model trained on composite may need recalibration when switching to official BRTI.

3. **3 of 5 BRTI constituents:** itBit and Gemini are excluded from Phase 1. These have lower volume and their WS APIs are less straightforward. Their exclusion biases the composite toward higher-volume exchanges (Coinbase dominant), which is directionally correct for BRTI weighting.

4. **No persistence of historical ticks:** `_tick_buffer` is in-memory. On restart, the feed starts fresh. Historical BRTI OHLCV for model training requires a separate data collection run (Phase 1b task in the spec).

---

## Dependencies

```
redis>=5.0           # Redis client
websockets>=12.0     # Async WebSocket client
aiohttp>=3.9         # HTTP (for CF Benchmarks REST stub)
pandas>=2.0
numpy>=1.26
python-dotenv
loguru               # Structured logging
pytest
pytest-asyncio
```

---

## Credentials (`.env`)

```
REDIS_URL=redis://localhost:6379
CF_BENCHMARKS_API_KEY=   # leave blank until API access arrives
```

---

## What Phase 1 Does NOT Include

- Derivatives feed (funding rate, OI, CVD) — Phase 1b
- Historical BRTI OHLCV backfill — Phase 1b  
- Kronos model inference — Phase 2
- Any Kalshi execution — Phase 5
