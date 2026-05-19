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

    # _drain runs forever; timeout after it consumes both queued ticks and blocks
    try:
        await asyncio.wait_for(agg._drain(in_q), timeout=0.5)
    except asyncio.TimeoutError:
        pass  # expected — drain blocked waiting for a third tick

    assert agg.out_queue.qsize() == 2
    first = await agg.out_queue.get()
    assert first == pytest.approx(100.0)   # coinbase only
    second = await agg.out_queue.get()
    assert second == pytest.approx(150.0)  # (100*1000 + 200*1000) / 2000
