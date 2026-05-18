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
