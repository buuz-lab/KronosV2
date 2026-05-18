# BRTI Feed + Redis Feature Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Phase 1 data infrastructure for the BTC/Kalshi trading system — a live BRTI composite price from Coinbase, Kraken, and Bitstamp WebSocket feeds feeding a Redis feature store that exposes 60-second resolution estimates and OHLCV candles in Kronos format.

**Architecture:** Three narrow async components — `ExchangeFeed` (one per exchange, normalizes WS ticks into `Tick` objects via a testable `parse_message` method), `BRTIAggregator` (volume-weights exchange ticks into a composite BRTI price with CF Benchmarks stub for future swap-in), and `FeatureStore` (maintains in-memory tick deque, resamples to OHLCV, writes to Redis). Downstream Kronos inference reads synchronously from Redis.

**Tech Stack:** Python 3.12, asyncio, `websockets>=12`, `redis-py>=5`, `pandas>=2`, `numpy`, `loguru`, `pytest` + `pytest-asyncio`, `fakeredis` (tests only)

**Design spec:** `docs/superpowers/specs/2026-05-18-brti-feed-redis-feature-store-design.md`

---

## File Map

| File | Status | Responsibility |
|------|--------|----------------|
| `config.py` | Create | All constants and env-loaded settings |
| `requirements.txt` | Create | Python dependencies |
| `.env.example` | Create | Credential template |
| `pyproject.toml` | Create | pytest configuration |
| `btc_kalshi_system/__init__.py` | Create | Package marker |
| `btc_kalshi_system/data/__init__.py` | Create | Package marker |
| `btc_kalshi_system/data/models.py` | Create | `Tick` dataclass — shared type between exchange_feed and aggregator |
| `btc_kalshi_system/data/exchange_feed.py` | Create | `ExchangeFeed` ABC + `CoinbaseFeed`, `KrakenFeed`, `BitstampFeed` subclasses |
| `btc_kalshi_system/data/brti_aggregator.py` | Create | `BRTIAggregator` — volume-weighted composite + CF Benchmarks stub |
| `btc_kalshi_system/data/feature_store.py` | Create | `FeatureStore` — Redis writer + synchronous read API |
| `scripts/validate_composite.py` | Create | Live 10-min validation script: CSV log + spread stats |
| `scripts/smoke_test.py` | Create | Integration test: full stack for N seconds, assert outputs |
| `tests/__init__.py` | Create | Package marker |
| `tests/data/__init__.py` | Create | Package marker |
| `tests/data/test_models.py` | Create | Tests for `Tick` dataclass |
| `tests/data/test_exchange_feed.py` | Create | Tests for each exchange's `parse_message` (no network) |
| `tests/data/test_brti_aggregator.py` | Create | Tests for composite weighting + staleness filtering |
| `tests/data/test_feature_store.py` | Create | Tests for resolution estimate + OHLCV resampling + Redis flush |

---

## Task 1: Project Scaffold

**Files:**
- Create: `requirements.txt`, `.env.example`, `pyproject.toml`, `config.py`
- Create: `btc_kalshi_system/__init__.py`, `btc_kalshi_system/data/__init__.py`
- Create: `tests/__init__.py`, `tests/data/__init__.py`

- [ ] **Step 1: Create directory structure**

```bash
cd "/Users/ezrakornberg/Kronos V2"
mkdir -p btc_kalshi_system/data tests/data scripts
touch btc_kalshi_system/__init__.py btc_kalshi_system/data/__init__.py
touch tests/__init__.py tests/data/__init__.py
```

- [ ] **Step 2: Write `requirements.txt`**

```text
# Runtime
websockets>=12.0
redis>=5.0
pandas>=2.0
numpy>=1.26
python-dotenv>=1.0
loguru>=0.7
aiohttp>=3.9

# Test
pytest>=8.0
pytest-asyncio>=0.23
fakeredis>=2.20
```

- [ ] **Step 3: Write `pyproject.toml`**

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 4: Write `.env.example`**

```dotenv
REDIS_URL=redis://localhost:6379
CF_BENCHMARKS_API_KEY=
```

- [ ] **Step 5: Write `config.py`**

```python
import os
from dotenv import load_dotenv

load_dotenv()

REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")

BRTI_TICK_BUFFER_SIZE: int = 7200          # 2 hours at ~1 tick/second
BRTI_STALE_THRESHOLD_SECONDS: float = 5.0  # exclude exchange ticks older than this
BRTI_RESOLUTION_WINDOW_SECONDS: int = 60   # rolling window for resolution estimate
RECONNECT_DELAYS: list[int] = [1, 2, 4, 8, 16, 32, 60]

COINBASE_WS_URL: str = "wss://advanced-trade-api.coinbase.com/ws/public"
KRAKEN_WS_URL: str = "wss://ws.kraken.com/v2"
BITSTAMP_WS_URL: str = "wss://ws.bitstamp.net"

REDIS_TTL_RESOLUTION_ESTIMATE: int = 10
REDIS_TTL_OHLCV: dict[str, int] = {"5min": 600, "15min": 1800, "1h": 7200}
OHLCV_TIMEFRAMES: list[str] = ["5min", "15min", "1h"]

CF_BENCHMARKS_API_KEY: str = os.getenv("CF_BENCHMARKS_API_KEY", "")
```

- [ ] **Step 6: Install dependencies**

```bash
cd "/Users/ezrakornberg/Kronos V2"
pip install -r requirements.txt
```

Expected: All packages install without errors.

- [ ] **Step 7: Verify pytest collects with no errors**

```bash
pytest --collect-only
```

Expected: "no tests ran" — no import or collection errors.

- [ ] **Step 8: Initialize git and commit**

```bash
git init
git add .
git commit -m "chore: project scaffold — config, requirements, test structure"
```

---

## Task 2: Tick Dataclass

**Files:**
- Create: `btc_kalshi_system/data/models.py`
- Test: `tests/data/test_models.py`

- [ ] **Step 1: Write failing tests**

`tests/data/test_models.py`:
```python
import time
from btc_kalshi_system.data.models import Tick


def test_tick_stores_all_fields():
    ts = time.time()
    tick = Tick(exchange="coinbase", price=103500.0, volume=15000.0, timestamp=ts)
    assert tick.exchange == "coinbase"
    assert tick.price == 103500.0
    assert tick.volume == 15000.0
    assert tick.timestamp == ts


def test_tick_equality():
    ts = 1716000000.0
    assert Tick("coinbase", 103500.0, 15000.0, ts) == Tick("coinbase", 103500.0, 15000.0, ts)


def test_tick_inequality_on_price():
    ts = 1716000000.0
    assert Tick("coinbase", 103500.0, 15000.0, ts) != Tick("coinbase", 103501.0, 15000.0, ts)
```

- [ ] **Step 2: Run test — verify FAIL**

```bash
pytest tests/data/test_models.py -v
```

Expected: `ModuleNotFoundError: No module named 'btc_kalshi_system.data.models'`

- [ ] **Step 3: Implement `btc_kalshi_system/data/models.py`**

```python
from dataclasses import dataclass


@dataclass
class Tick:
    exchange: str
    price: float
    volume: float    # 24h volume or per-trade size — used for composite weighting
    timestamp: float # unix seconds (time.time())
```

- [ ] **Step 4: Run test — verify PASS**

```bash
pytest tests/data/test_models.py -v
```

Expected:
```
tests/data/test_models.py::test_tick_stores_all_fields PASSED
tests/data/test_models.py::test_tick_equality PASSED
tests/data/test_models.py::test_tick_inequality_on_price PASSED
3 passed
```

- [ ] **Step 5: Commit**

```bash
git add btc_kalshi_system/data/models.py tests/data/test_models.py
git commit -m "feat: add Tick dataclass"
```

---

## Task 3: ExchangeFeed ABC + Parsers

**Files:**
- Create: `btc_kalshi_system/data/exchange_feed.py`
- Test: `tests/data/test_exchange_feed.py`

The `parse_message` method on each subclass is tested in isolation — no WebSocket connection, no network.

- [ ] **Step 1: Write failing tests**

`tests/data/test_exchange_feed.py`:
```python
import json
import pytest
from btc_kalshi_system.data.exchange_feed import CoinbaseFeed, KrakenFeed, BitstampFeed


# ── Coinbase ───────────────────────────────────────────────────────────────

def test_coinbase_parse_ticker_message():
    feed = CoinbaseFeed()
    msg = json.dumps({
        "channel": "ticker",
        "events": [{"type": "update", "tickers": [
            {"product_id": "BTC-USD", "price": "103500.00", "volume_24_h": "15234.5"}
        ]}]
    })
    tick = feed.parse_message(msg)
    assert tick is not None
    assert tick.exchange == "coinbase"
    assert tick.price == pytest.approx(103500.0)
    assert tick.volume == pytest.approx(15234.5)


def test_coinbase_returns_none_for_subscription_confirmation():
    feed = CoinbaseFeed()
    assert feed.parse_message(json.dumps({"channel": "subscriptions", "events": []})) is None


def test_coinbase_returns_none_for_non_update_event():
    feed = CoinbaseFeed()
    msg = json.dumps({"channel": "ticker", "events": [{"type": "snapshot", "tickers": []}]})
    assert feed.parse_message(msg) is None


# ── Kraken ─────────────────────────────────────────────────────────────────

def test_kraken_parse_ticker_message():
    feed = KrakenFeed()
    msg = json.dumps({
        "channel": "ticker",
        "type": "update",
        "data": [{"symbol": "BTC/USD", "last": 103500.0, "volume": 3252.6}]
    })
    tick = feed.parse_message(msg)
    assert tick is not None
    assert tick.exchange == "kraken"
    assert tick.price == pytest.approx(103500.0)
    assert tick.volume == pytest.approx(3252.6)


def test_kraken_returns_none_for_subscribe_response():
    feed = KrakenFeed()
    assert feed.parse_message(json.dumps({"method": "subscribe", "success": True})) is None


def test_kraken_returns_none_for_snapshot():
    feed = KrakenFeed()
    msg = json.dumps({"channel": "ticker", "type": "snapshot", "data": []})
    assert feed.parse_message(msg) is None


# ── Bitstamp ───────────────────────────────────────────────────────────────

def test_bitstamp_parse_trade_message():
    feed = BitstampFeed()
    msg = json.dumps({
        "event": "trade",
        "channel": "live_trades_btcusd",
        "data": {"price": 103500.0, "amount": 0.5}
    })
    tick = feed.parse_message(msg)
    assert tick is not None
    assert tick.exchange == "bitstamp"
    assert tick.price == pytest.approx(103500.0)
    assert tick.volume == pytest.approx(0.5)


def test_bitstamp_returns_none_for_subscription_succeeded():
    feed = BitstampFeed()
    msg = json.dumps({
        "event": "bts:subscription_succeeded",
        "data": {},
        "channel": "live_trades_btcusd"
    })
    assert feed.parse_message(msg) is None


def test_bitstamp_returns_none_for_heartbeat():
    feed = BitstampFeed()
    assert feed.parse_message(json.dumps({"event": "bts:heartbeat", "data": {}})) is None
```

- [ ] **Step 2: Run tests — verify FAIL**

```bash
pytest tests/data/test_exchange_feed.py -v
```

Expected: `ModuleNotFoundError: No module named 'btc_kalshi_system.data.exchange_feed'`

- [ ] **Step 3: Implement `btc_kalshi_system/data/exchange_feed.py`**

```python
import asyncio
import json
import time
from abc import ABC, abstractmethod

import websockets
from loguru import logger

from btc_kalshi_system.data.models import Tick
from config import RECONNECT_DELAYS


class ExchangeFeed(ABC):

    def __init__(self) -> None:
        self._connected = False

    @property
    @abstractmethod
    def ws_url(self) -> str: ...

    @abstractmethod
    def subscribe_message(self) -> dict: ...

    @abstractmethod
    def parse_message(self, raw: str) -> Tick | None:
        """Parse a raw WebSocket message string. Returns None if not a price tick."""

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def run(self, queue: asyncio.Queue) -> None:
        """Connect and stream forever, reconnecting with exponential backoff."""
        attempt = 0
        while True:
            try:
                await self._connect_and_stream(queue)
                attempt = 0
            except Exception as exc:
                self._connected = False
                delay = RECONNECT_DELAYS[min(attempt, len(RECONNECT_DELAYS) - 1)]
                logger.warning(f"{self.__class__.__name__} disconnected ({exc}), retry in {delay}s")
                await asyncio.sleep(delay)
                attempt += 1

    async def _connect_and_stream(self, queue: asyncio.Queue) -> None:
        async with websockets.connect(self.ws_url) as ws:
            self._connected = True
            logger.info(f"{self.__class__.__name__} connected")
            await ws.send(json.dumps(self.subscribe_message()))
            async for raw in ws:
                tick = self.parse_message(raw)
                if tick is not None:
                    await queue.put(tick)


class CoinbaseFeed(ExchangeFeed):

    @property
    def ws_url(self) -> str:
        return "wss://advanced-trade-api.coinbase.com/ws/public"

    def subscribe_message(self) -> dict:
        return {"type": "subscribe", "channel": "ticker", "product_ids": ["BTC-USD"]}

    def parse_message(self, raw: str) -> Tick | None:
        try:
            msg = json.loads(raw)
            if msg.get("channel") != "ticker":
                return None
            for event in msg.get("events", []):
                if event.get("type") != "update":
                    continue
                for ticker in event.get("tickers", []):
                    if ticker.get("product_id") == "BTC-USD":
                        return Tick(
                            exchange="coinbase",
                            price=float(ticker["price"]),
                            volume=float(ticker["volume_24_h"]),
                            timestamp=time.time(),
                        )
        except (KeyError, ValueError, json.JSONDecodeError):
            pass
        return None


class KrakenFeed(ExchangeFeed):

    @property
    def ws_url(self) -> str:
        return "wss://ws.kraken.com/v2"

    def subscribe_message(self) -> dict:
        return {
            "method": "subscribe",
            "params": {"channel": "ticker", "symbol": ["BTC/USD"]},
            "req_id": 1,
        }

    def parse_message(self, raw: str) -> Tick | None:
        try:
            msg = json.loads(raw)
            if msg.get("channel") != "ticker" or msg.get("type") != "update":
                return None
            for item in msg.get("data", []):
                if item.get("symbol") == "BTC/USD":
                    return Tick(
                        exchange="kraken",
                        price=float(item["last"]),
                        volume=float(item["volume"]),
                        timestamp=time.time(),
                    )
        except (KeyError, ValueError, json.JSONDecodeError):
            pass
        return None


class BitstampFeed(ExchangeFeed):

    @property
    def ws_url(self) -> str:
        return "wss://ws.bitstamp.net"

    def subscribe_message(self) -> dict:
        return {"event": "bts:subscribe", "data": {"channel": "live_trades_btcusd"}}

    def parse_message(self, raw: str) -> Tick | None:
        try:
            msg = json.loads(raw)
            if msg.get("event") != "trade":
                return None
            if msg.get("channel") != "live_trades_btcusd":
                return None
            data = msg["data"]
            return Tick(
                exchange="bitstamp",
                price=float(data["price"]),
                volume=float(data["amount"]),  # per-trade size, used as weight proxy
                timestamp=time.time(),
            )
        except (KeyError, ValueError, json.JSONDecodeError):
            pass
        return None
```

- [ ] **Step 4: Run tests — verify PASS**

```bash
pytest tests/data/test_exchange_feed.py -v
```

Expected:
```
tests/data/test_exchange_feed.py::test_coinbase_parse_ticker_message PASSED
tests/data/test_exchange_feed.py::test_coinbase_returns_none_for_subscription_confirmation PASSED
tests/data/test_exchange_feed.py::test_coinbase_returns_none_for_non_update_event PASSED
tests/data/test_exchange_feed.py::test_kraken_parse_ticker_message PASSED
tests/data/test_exchange_feed.py::test_kraken_returns_none_for_subscribe_response PASSED
tests/data/test_exchange_feed.py::test_kraken_returns_none_for_snapshot PASSED
tests/data/test_exchange_feed.py::test_bitstamp_parse_trade_message PASSED
tests/data/test_exchange_feed.py::test_bitstamp_returns_none_for_subscription_succeeded PASSED
tests/data/test_exchange_feed.py::test_bitstamp_returns_none_for_heartbeat PASSED
9 passed
```

- [ ] **Step 5: Commit**

```bash
git add btc_kalshi_system/data/exchange_feed.py tests/data/test_exchange_feed.py
git commit -m "feat: add ExchangeFeed ABC + Coinbase/Kraken/Bitstamp parsers"
```

---

## Task 4: BRTIAggregator

**Files:**
- Create: `btc_kalshi_system/data/brti_aggregator.py`
- Test: `tests/data/test_brti_aggregator.py`

- [ ] **Step 1: Write failing tests**

`tests/data/test_brti_aggregator.py`:
```python
import asyncio
import time
import pytest
from btc_kalshi_system.data.models import Tick
from btc_kalshi_system.data.brti_aggregator import BRTIAggregator


def fresh_tick(exchange: str, price: float, volume: float) -> Tick:
    return Tick(exchange=exchange, price=price, volume=volume, timestamp=time.time())


def stale_tick(exchange: str, price: float, volume: float) -> Tick:
    return Tick(exchange=exchange, price=price, volume=volume, timestamp=time.time() - 10.0)


# ── _composite ─────────────────────────────────────────────────────────────

def test_composite_volume_weighted():
    agg = BRTIAggregator()
    agg._latest = {
        "coinbase": fresh_tick("coinbase", 100.0, 1000.0),
        "kraken":   fresh_tick("kraken",   200.0, 3000.0),
    }
    # (100*1000 + 200*3000) / (1000+3000) = 700000/4000 = 175.0
    assert agg._composite() == pytest.approx(175.0)


def test_composite_excludes_stale_ticks():
    agg = BRTIAggregator()
    agg._latest = {
        "coinbase": fresh_tick("coinbase", 100.0, 1000.0),
        "kraken":   stale_tick("kraken",   200.0, 1000.0),  # stale: >5s old
    }
    assert agg._composite() == pytest.approx(100.0)  # only coinbase contributes


def test_composite_returns_none_when_all_stale():
    agg = BRTIAggregator()
    agg._latest = {
        "coinbase": stale_tick("coinbase", 100.0, 1000.0),
        "kraken":   stale_tick("kraken",   200.0, 1000.0),
    }
    assert agg._composite() is None


def test_composite_returns_none_when_no_ticks():
    agg = BRTIAggregator()
    assert agg._composite() is None


def test_composite_equal_weight_when_all_volumes_zero():
    agg = BRTIAggregator()
    agg._latest = {
        "coinbase": fresh_tick("coinbase", 100.0, 0.0),
        "kraken":   fresh_tick("kraken",   200.0, 0.0),
        "bitstamp": fresh_tick("bitstamp", 300.0, 0.0),
    }
    # All volumes zero → simple average → (100+200+300)/3 = 200.0
    assert agg._composite() == pytest.approx(200.0)


# ── _drain integration ─────────────────────────────────────────────────────

async def test_drain_emits_composite_price_per_tick():
    agg = BRTIAggregator()
    in_q: asyncio.Queue[Tick] = asyncio.Queue()

    await in_q.put(fresh_tick("coinbase", 100.0, 1000.0))
    await in_q.put(fresh_tick("kraken",   200.0, 1000.0))

    # Run _drain inline for 2 ticks (avoids spawning infinite task)
    for _ in range(2):
        tick = await in_q.get()
        agg._latest[tick.exchange] = tick
        price = agg._composite()
        if price is not None:
            await agg._out_queue.put(price)

    assert agg.out_queue.qsize() == 2
    first = await agg.out_queue.get()
    assert first == pytest.approx(100.0)  # coinbase only
    second = await agg.out_queue.get()
    assert second == pytest.approx(150.0)  # (100*1000 + 200*1000) / 2000
```

- [ ] **Step 2: Run tests — verify FAIL**

```bash
pytest tests/data/test_brti_aggregator.py -v
```

Expected: `ModuleNotFoundError: No module named 'btc_kalshi_system.data.brti_aggregator'`

- [ ] **Step 3: Implement `btc_kalshi_system/data/brti_aggregator.py`**

```python
import asyncio
import time

from loguru import logger

from btc_kalshi_system.data.models import Tick
from config import BRTI_STALE_THRESHOLD_SECONDS


class BRTIAggregator:
    """
    Merges exchange ticks into a volume-weighted composite BRTI price.
    CF Benchmarks plugs in by implementing _cf_benchmarks_source().
    """

    def __init__(self) -> None:
        self._latest: dict[str, Tick] = {}
        self._out_queue: asyncio.Queue[float] = asyncio.Queue()

    async def run(self, exchange_queues: list[asyncio.Queue]) -> None:
        """Drain all exchange queues concurrently. Emit composite price on each tick."""
        await asyncio.gather(*[self._drain(q) for q in exchange_queues])

    async def _drain(self, queue: asyncio.Queue) -> None:
        while True:
            tick = await queue.get()
            self._latest[tick.exchange] = tick
            price = await self._cf_benchmarks_source()  # None in Phase 1
            if price is None:
                price = self._composite()
            if price is not None:
                await self._out_queue.put(price)

    def _composite(self) -> float | None:
        """
        Volume-weighted average of exchanges with fresh ticks.
        Falls back to simple average when all volumes are zero.
        Returns None if no fresh ticks are available.
        """
        now = time.time()
        fresh = {
            e: t for e, t in self._latest.items()
            if now - t.timestamp < BRTI_STALE_THRESHOLD_SECONDS
        }
        if not fresh:
            return None
        total_vol = sum(t.volume for t in fresh.values())
        if total_vol == 0.0:
            return sum(t.price for t in fresh.values()) / len(fresh)
        return sum(t.price * t.volume / total_vol for t in fresh.values())

    async def _cf_benchmarks_source(self) -> float | None:
        """
        Primary BRTI from CF Benchmarks REST/WS API.
        Returns None while unimplemented — composite is used as fallback.
        When CF Benchmarks API key arrives: open WS, parse 1-second ticks, return price here.
        """
        return None

    @property
    def out_queue(self) -> asyncio.Queue:
        return self._out_queue
```

- [ ] **Step 4: Run tests — verify PASS**

```bash
pytest tests/data/test_brti_aggregator.py -v
```

Expected:
```
tests/data/test_brti_aggregator.py::test_composite_volume_weighted PASSED
tests/data/test_brti_aggregator.py::test_composite_excludes_stale_ticks PASSED
tests/data/test_brti_aggregator.py::test_composite_returns_none_when_all_stale PASSED
tests/data/test_brti_aggregator.py::test_composite_returns_none_when_no_ticks PASSED
tests/data/test_brti_aggregator.py::test_composite_equal_weight_when_all_volumes_zero PASSED
tests/data/test_brti_aggregator.py::test_drain_emits_composite_price_per_tick PASSED
6 passed
```

- [ ] **Step 5: Commit**

```bash
git add btc_kalshi_system/data/brti_aggregator.py tests/data/test_brti_aggregator.py
git commit -m "feat: add BRTIAggregator with volume-weighted composite + CF Benchmarks stub"
```

---

## Task 5: FeatureStore

**Files:**
- Create: `btc_kalshi_system/data/feature_store.py`
- Test: `tests/data/test_feature_store.py`

- [ ] **Step 1: Write failing tests**

`tests/data/test_feature_store.py`:
```python
import time
from collections import deque

import pandas as pd
import pytest
import fakeredis

from btc_kalshi_system.data.feature_store import FeatureStore


def make_store() -> FeatureStore:
    """FeatureStore backed by an in-memory FakeRedis — no real Redis needed."""
    store = FeatureStore.__new__(FeatureStore)
    store._tick_buffer = deque(maxlen=7200)
    store._redis = fakeredis.FakeRedis()
    return store


# ── _resolution_estimate ───────────────────────────────────────────────────

def test_resolution_estimate_averages_last_60s_only():
    store = make_store()
    now = time.time()
    for i in range(60):
        store._tick_buffer.append((now - 120 + i, 100.0))  # old (>60s ago)
    for i in range(60):
        store._tick_buffer.append((now - 59 + i, 200.0))   # fresh (≤60s ago)
    assert store._resolution_estimate() == pytest.approx(200.0)


def test_resolution_estimate_returns_none_when_buffer_empty():
    store = make_store()
    assert store._resolution_estimate() is None


def test_resolution_estimate_includes_all_ticks_within_60s():
    store = make_store()
    now = time.time()
    store._tick_buffer.extend([
        (now - 59, 100.0),
        (now - 30, 200.0),
        (now - 1,  300.0),
    ])
    assert store._resolution_estimate() == pytest.approx(200.0)  # mean(100, 200, 300)


# ── _resample ──────────────────────────────────────────────────────────────

def _populate_two_candles(store: FeatureStore) -> None:
    """Fill buffer with 10 ticks (1/min) forming 2 complete 5-min candles."""
    base_ts = pd.Timestamp("2026-01-01 00:00:00", tz="UTC").timestamp()
    prices = [100.0, 101.0, 99.0, 102.0, 103.0,   # candle 1: min 0-4
              200.0, 201.0, 199.0, 202.0, 203.0]  # candle 2: min 5-9
    for i, price in enumerate(prices):
        store._tick_buffer.append((base_ts + i * 60, price))


def test_resample_returns_correct_columns():
    store = make_store()
    _populate_two_candles(store)
    df = store._resample("5min")
    assert df is not None
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "amount"]


def test_resample_ohlcv_values_candle_1():
    store = make_store()
    _populate_two_candles(store)
    df = store._resample("5min")
    assert df is not None
    assert len(df) == 2
    c1 = df.iloc[0]
    assert c1["open"]   == pytest.approx(100.0)
    assert c1["high"]   == pytest.approx(103.0)
    assert c1["low"]    == pytest.approx(99.0)
    assert c1["close"]  == pytest.approx(103.0)
    assert c1["volume"] == pytest.approx(0.0)
    assert c1["amount"] == pytest.approx(0.0)


def test_resample_returns_none_when_fewer_than_2_ticks():
    store = make_store()
    store._tick_buffer.append((time.time(), 100.0))
    assert store._resample("5min") is None


def test_resample_returns_none_for_empty_buffer():
    store = make_store()
    assert store._resample("5min") is None


# ── get_raw_ticks ──────────────────────────────────────────────────────────

def test_get_raw_ticks_returns_ticks_within_window():
    store = make_store()
    now = time.time()
    store._tick_buffer.extend([
        (now - 120, 100.0),  # old — excluded
        (now - 30,  200.0),  # within 60s
        (now - 1,   300.0),  # within 60s
    ])
    result = store.get_raw_ticks(60)
    assert result is not None
    assert len(result) == 2
    assert list(result.values) == pytest.approx([200.0, 300.0])


def test_get_raw_ticks_returns_none_when_no_data_in_window():
    store = make_store()
    store._tick_buffer.append((time.time() - 120, 100.0))
    assert store.get_raw_ticks(60) is None


# ── _flush_to_redis → read API ─────────────────────────────────────────────

def test_flush_writes_resolution_estimate_to_redis():
    store = make_store()
    now = time.time()
    for i in range(60):
        store._tick_buffer.append((now - 59 + i, 150.0))
    store._flush_to_redis()
    assert store.get_resolution_estimate() == pytest.approx(150.0, abs=1.0)


def test_flush_writes_ohlcv_to_redis():
    store = make_store()
    _populate_two_candles(store)
    store._flush_to_redis()
    df = store.get_ohlcv("5min")
    assert df is not None
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "amount"]
    assert len(df) >= 1


def test_get_resolution_estimate_returns_none_before_flush():
    store = make_store()
    assert store.get_resolution_estimate() is None


def test_get_ohlcv_returns_none_before_flush():
    store = make_store()
    assert store.get_ohlcv("5min") is None
```

- [ ] **Step 2: Run tests — verify FAIL**

```bash
pytest tests/data/test_feature_store.py -v
```

Expected: `ModuleNotFoundError: No module named 'btc_kalshi_system.data.feature_store'`

- [ ] **Step 3: Implement `btc_kalshi_system/data/feature_store.py`**

```python
import asyncio
import time
from collections import deque

import numpy as np
import pandas as pd
import redis
from loguru import logger

from config import (
    BRTI_RESOLUTION_WINDOW_SECONDS,
    BRTI_TICK_BUFFER_SIZE,
    OHLCV_TIMEFRAMES,
    REDIS_TTL_OHLCV,
    REDIS_TTL_RESOLUTION_ESTIMATE,
    REDIS_URL,
)

_FREQ_MAP = {"5min": "5min", "15min": "15min", "1h": "1h"}


class FeatureStore:
    """
    Async writer: consumes float prices from BRTIAggregator.out_queue,
    maintains a tick deque, and writes to Redis on every new price.

    Sync read API: get_resolution_estimate(), get_ohlcv(), get_raw_ticks()
    called by Kronos engine and signal fusion.

    volume/amount columns are 0.0 in Phase 1 — the composite feed provides no
    per-tick volume. This is a known limitation noted in the design spec.
    """

    def __init__(self, redis_url: str = REDIS_URL) -> None:
        self._tick_buffer: deque[tuple[float, float]] = deque(maxlen=BRTI_TICK_BUFFER_SIZE)
        self._redis = redis.from_url(redis_url)

    # ── Async writer ───────────────────────────────────────────────────────

    async def run(self, price_queue: asyncio.Queue) -> None:
        while True:
            price = await price_queue.get()
            self._tick_buffer.append((time.time(), price))
            self._flush_to_redis()

    def _flush_to_redis(self) -> None:
        if not self._tick_buffer:
            return
        ts, price = self._tick_buffer[-1]
        pipe = self._redis.pipeline()

        pipe.lpush("brti:ticks", f"{ts}:{price}")
        pipe.ltrim("brti:ticks", 0, BRTI_TICK_BUFFER_SIZE - 1)

        est = self._resolution_estimate()
        if est is not None:
            pipe.set("brti:resolution_estimate", est, ex=REDIS_TTL_RESOLUTION_ESTIMATE)

        for tf in OHLCV_TIMEFRAMES:
            df = self._resample(tf)
            if df is not None:
                pipe.set(f"brti:ohlcv:{tf}", df.to_json(), ex=REDIS_TTL_OHLCV[tf])

        pipe.execute()

    # ── Synchronous read API ───────────────────────────────────────────────

    def get_resolution_estimate(self) -> float | None:
        """60s rolling BRTI average — mirrors Kalshi resolution logic exactly."""
        val = self._redis.get("brti:resolution_estimate")
        return float(val) if val else None

    def get_ohlcv(self, timeframe: str) -> pd.DataFrame | None:
        """
        OHLCV DataFrame in Kronos format: [open, high, low, close, volume, amount].
        Returns None if insufficient data has accumulated since startup.
        """
        raw = self._redis.get(f"brti:ohlcv:{timeframe}")
        return pd.read_json(raw) if raw else None

    def get_raw_ticks(self, n_seconds: int) -> pd.Series | None:
        """Last n_seconds of BRTI prices as pd.Series indexed by UTC timestamp."""
        now = time.time()
        ticks = [(ts, p) for ts, p in self._tick_buffer if now - ts <= n_seconds]
        if not ticks:
            return None
        timestamps, prices = zip(*ticks)
        return pd.Series(
            list(prices),
            index=pd.to_datetime(list(timestamps), unit="s", utc=True),
        )

    # ── Internal ──────────────────────────────────────────────────────────

    def _resolution_estimate(self) -> float | None:
        now = time.time()
        recent = [p for ts, p in self._tick_buffer if now - ts <= BRTI_RESOLUTION_WINDOW_SECONDS]
        return float(np.mean(recent)) if recent else None

    def _resample(self, timeframe: str) -> pd.DataFrame | None:
        if len(self._tick_buffer) < 2:
            return None
        timestamps, prices = zip(*self._tick_buffer)
        df = pd.DataFrame(
            {"price": list(prices)},
            index=pd.to_datetime(list(timestamps), unit="s", utc=True),
        )
        ohlcv = df["price"].resample(_FREQ_MAP[timeframe]).agg(
            open="first", high="max", low="min", close="last"
        ).dropna()
        if ohlcv.empty:
            return None
        ohlcv["volume"] = 0.0
        ohlcv["amount"] = 0.0
        return ohlcv[["open", "high", "low", "close", "volume", "amount"]]
```

- [ ] **Step 4: Run tests — verify PASS**

```bash
pytest tests/data/test_feature_store.py -v
```

Expected:
```
tests/data/test_feature_store.py::test_resolution_estimate_averages_last_60s_only PASSED
tests/data/test_feature_store.py::test_resolution_estimate_returns_none_when_buffer_empty PASSED
tests/data/test_feature_store.py::test_resolution_estimate_includes_all_ticks_within_60s PASSED
tests/data/test_feature_store.py::test_resample_returns_correct_columns PASSED
tests/data/test_feature_store.py::test_resample_ohlcv_values_candle_1 PASSED
tests/data/test_feature_store.py::test_resample_returns_none_when_fewer_than_2_ticks PASSED
tests/data/test_feature_store.py::test_resample_returns_none_for_empty_buffer PASSED
tests/data/test_feature_store.py::test_get_raw_ticks_returns_ticks_within_window PASSED
tests/data/test_feature_store.py::test_get_raw_ticks_returns_none_when_no_data_in_window PASSED
tests/data/test_feature_store.py::test_flush_writes_resolution_estimate_to_redis PASSED
tests/data/test_feature_store.py::test_flush_writes_ohlcv_to_redis PASSED
tests/data/test_feature_store.py::test_get_resolution_estimate_returns_none_before_flush PASSED
tests/data/test_feature_store.py::test_get_ohlcv_returns_none_before_flush PASSED
13 passed
```

- [ ] **Step 5: Run full test suite — verify no regressions**

```bash
pytest -v
```

Expected: All 25 tests pass (3 + 9 + 6 + 13 across all test files).

- [ ] **Step 6: Commit**

```bash
git add btc_kalshi_system/data/feature_store.py tests/data/test_feature_store.py
git commit -m "feat: add FeatureStore — Redis writer + resolution estimate + OHLCV resampling"
```

---

## Task 6: Validation Script

**Files:**
- Create: `scripts/validate_composite.py`

No automated test — this script requires live internet and real WebSocket connections.

- [ ] **Step 1: Implement `scripts/validate_composite.py`**

```python
"""
Run the BRTI composite feed for N minutes and print statistics.

Usage:
    python scripts/validate_composite.py --minutes 10
    python scripts/validate_composite.py --minutes 10 --csv /tmp/brti_ticks.csv
"""

import argparse
import asyncio
import csv
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from btc_kalshi_system.data.brti_aggregator import BRTIAggregator
from btc_kalshi_system.data.exchange_feed import BitstampFeed, CoinbaseFeed, KrakenFeed
from btc_kalshi_system.data.models import Tick


async def run_validation(minutes: int, csv_path: str | None) -> None:
    coinbase_q: asyncio.Queue[Tick] = asyncio.Queue()
    kraken_q:   asyncio.Queue[Tick] = asyncio.Queue()
    bitstamp_q: asyncio.Queue[Tick] = asyncio.Queue()

    agg = BRTIAggregator()
    composite_prices: list[float] = []
    exchange_tick_counts: dict[str, int] = {"coinbase": 0, "kraken": 0, "bitstamp": 0}
    tick_log: list[dict] = []
    stop_event = asyncio.Event()

    async def drain_exchange(name: str, queue: asyncio.Queue[Tick]) -> None:
        while True:
            tick = await queue.get()
            exchange_tick_counts[name] += 1
            agg._latest[tick.exchange] = tick
            price = agg._composite()
            if price is not None:
                await agg.out_queue.put(price)

    async def collect_composite() -> None:
        while True:
            price = await agg.out_queue.get()
            composite_prices.append(price)
            tick_log.append({"timestamp": time.time(), "composite": price})

    async def timeout() -> None:
        await asyncio.sleep(minutes * 60)
        stop_event.set()

    print(f"Running BRTI composite feed for {minutes} minute(s)...")
    print("Exchanges: Coinbase, Kraken, Bitstamp")
    print("-" * 50)

    tasks = [
        asyncio.create_task(CoinbaseFeed().run(coinbase_q)),
        asyncio.create_task(KrakenFeed().run(kraken_q)),
        asyncio.create_task(BitstampFeed().run(bitstamp_q)),
        asyncio.create_task(drain_exchange("coinbase", coinbase_q)),
        asyncio.create_task(drain_exchange("kraken", kraken_q)),
        asyncio.create_task(drain_exchange("bitstamp", bitstamp_q)),
        asyncio.create_task(collect_composite()),
        asyncio.create_task(timeout()),
    ]

    await stop_event.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    total = len(composite_prices)
    print(f"\nTicks received (composite): {total}")
    for name, count in exchange_tick_counts.items():
        print(f"  {name.capitalize()}: {count} ticks")

    if total > 0:
        print(f"Composite range: ${min(composite_prices):,.2f} – ${max(composite_prices):,.2f}")
        print(f"Final composite price:     ${composite_prices[-1]:,.2f}")
        window = composite_prices[-60:] if len(composite_prices) >= 60 else composite_prices
        print(f"Resolution estimate (last {len(window)} prices avg): ${sum(window)/len(window):,.2f}")
        latest_per_exchange = {
            e: t.price for e, t in agg._latest.items()
        }
        if len(latest_per_exchange) >= 2:
            spread = max(latest_per_exchange.values()) - min(latest_per_exchange.values())
            print(f"Final cross-exchange spread: ${spread:,.2f}")

    if csv_path and tick_log:
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "composite"])
            writer.writeheader()
            writer.writerows(tick_log)
        print(f"\nTick log written to: {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate BRTI composite feed")
    parser.add_argument("--minutes", type=int, default=10)
    parser.add_argument("--csv", type=str, default=None)
    args = parser.parse_args()
    asyncio.run(run_validation(args.minutes, args.csv))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Syntax-check the script**

```bash
cd "/Users/ezrakornberg/Kronos V2"
python -m py_compile scripts/validate_composite.py && echo "OK"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add scripts/validate_composite.py
git commit -m "feat: add validate_composite.py — live BRTI feed validation (10-min run, CSV log)"
```

---

## Task 7: Integration Smoke Test

**Files:**
- Create: `scripts/smoke_test.py`

Runs the full stack (3 feeds → aggregator → feature store → Redis) for N seconds. Requires a running Redis instance and live internet.

- [ ] **Step 1: Start Redis**

```bash
redis-server --daemonize yes
redis-cli ping
```

Expected: `PONG`

- [ ] **Step 2: Implement `scripts/smoke_test.py`**

```python
"""
Integration smoke test — runs the full BRTI → Redis stack for N seconds.

Requires: Redis running at REDIS_URL, live internet.

Usage:
    python scripts/smoke_test.py --seconds 30

Exit code: 0 = pass, 1 = fail.
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from btc_kalshi_system.data.brti_aggregator import BRTIAggregator
from btc_kalshi_system.data.exchange_feed import BitstampFeed, CoinbaseFeed, KrakenFeed
from btc_kalshi_system.data.feature_store import FeatureStore
from btc_kalshi_system.data.models import Tick


async def run_smoke(seconds: int) -> bool:
    coinbase_q: asyncio.Queue[Tick] = asyncio.Queue()
    kraken_q:   asyncio.Queue[Tick] = asyncio.Queue()
    bitstamp_q: asyncio.Queue[Tick] = asyncio.Queue()

    agg = BRTIAggregator()
    store = FeatureStore()
    stop_event = asyncio.Event()

    async def timeout() -> None:
        await asyncio.sleep(seconds)
        stop_event.set()

    tasks = [
        asyncio.create_task(CoinbaseFeed().run(coinbase_q)),
        asyncio.create_task(KrakenFeed().run(kraken_q)),
        asyncio.create_task(BitstampFeed().run(bitstamp_q)),
        asyncio.create_task(agg.run([coinbase_q, kraken_q, bitstamp_q])),
        asyncio.create_task(store.run(agg.out_queue)),
        asyncio.create_task(timeout()),
    ]

    print(f"Running full BRTI → Redis stack for {seconds}s...")
    await stop_event.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    passed = True

    est = store.get_resolution_estimate()
    if est is None:
        print("FAIL  resolution_estimate is None — no ticks received in 60s window")
        passed = False
    else:
        print(f"PASS  resolution_estimate = ${est:,.2f}")

    tick_count = len(store._tick_buffer)
    if tick_count == 0:
        print("FAIL  tick buffer is empty — no prices processed")
        passed = False
    else:
        print(f"PASS  {tick_count} ticks in buffer")

    contributed = set(agg._latest.keys())
    if len(contributed) == 0:
        print("FAIL  no exchanges contributed ticks")
        passed = False
    else:
        status = "PASS" if len(contributed) >= 2 else "WARN"
        print(f"{status}  {len(contributed)} exchange(s) contributed: {contributed}")

    return passed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    ok = asyncio.run(run_smoke(args.seconds))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run smoke test**

```bash
cd "/Users/ezrakornberg/Kronos V2"
python scripts/smoke_test.py --seconds 30
```

Expected output:
```
Running full BRTI → Redis stack for 30s...
PASS  resolution_estimate = $103,651.22
PASS  87 ticks in buffer
PASS  3 exchange(s) contributed: {'coinbase', 'kraken', 'bitstamp'}
```

If `resolution_estimate` is None: the 60-second window hasn't filled yet — rerun with `--seconds 90`.

If only 2 exchanges contribute: Bitstamp trades infrequently in calm markets — this is acceptable.

- [ ] **Step 4: Run full unit test suite — final pass**

```bash
pytest -v
```

Expected: All 25 unit tests pass.

- [ ] **Step 5: Final commit**

```bash
git add scripts/smoke_test.py
git commit -m "feat: add smoke_test.py — full-stack BRTI → Redis integration test"
```

---

## Spec Coverage

| Spec requirement | Task |
|----------------|------|
| Composite fallback: Coinbase, Kraken, Bitstamp WebSocket feeds | Task 3 |
| Volume-weighted composite BRTI price | Task 4 |
| Staleness filtering (exclude ticks > 5s old) | Task 4 |
| Equal-weight fallback when all volumes are zero | Task 4 |
| CF Benchmarks primary source plug-in stub | Task 4 |
| In-memory tick deque (7200 entries = 2 hours) | Task 5 |
| `get_resolution_estimate()` → 60s rolling average | Task 5 |
| `get_ohlcv(timeframe)` → Kronos-format DataFrame | Task 5 |
| `get_raw_ticks(n_seconds)` → pd.Series | Task 5 |
| Redis persistence (ticks, resolution estimate, OHLCV per timeframe) | Task 5 |
| OHLCV columns: [open, high, low, close, volume, amount] | Task 5 |
| Exponential backoff reconnect on WS disconnect | Task 3 |
| All constants in `config.py` | Task 1 |
| Validation script with spread stats + CSV log | Task 6 |
| Integration smoke test | Task 7 |

**Known Phase 1 limitations (documented in design spec, not gaps):**
- `volume`/`amount` OHLCV columns are zero — composite feed has no per-tick volume
- 3 of 5 BRTI constituents (itBit + Gemini excluded, lower volume, Phase 2b)
- Tick buffer resets on restart — historical BRTI accumulation is Phase 1b
