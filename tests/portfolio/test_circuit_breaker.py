from unittest.mock import MagicMock, PropertyMock

import pytest

from btc_kalshi_system.execution.router import ClientState
from btc_kalshi_system.portfolio.circuit_breaker import (
    MAX_DAILY_DRAWDOWN_DOLLARS,
    MIN_CALIBRATOR_SAMPLES,
    ROLLING_EDGE_WINDOW,
    BreakerStatus,
    CircuitBreaker,
    TripReason,
)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def make_breaker(
    daily_pnl: float = 0.0,
    router_state: ClientState = ClientState.PRIMARY,
    edge_len: int = 0,
    edge_value: float = 0.01,
    calibrator_samples: int = MIN_CALIBRATOR_SAMPLES,
) -> CircuitBreaker:
    monitor = MagicMock()
    monitor.get_daily_pnl.return_value = daily_pnl

    router = MagicMock()
    type(router).state = PropertyMock(return_value=router_state)

    edge_tracker = MagicMock()
    edge_tracker.__len__ = MagicMock(return_value=edge_len)
    edge_tracker.current_edge.return_value = edge_value

    calibrator = MagicMock()
    type(calibrator).n_samples = PropertyMock(return_value=calibrator_samples)

    return CircuitBreaker(
        monitor=monitor,
        edge_tracker=edge_tracker,
        router=router,
        calibrator=calibrator,
    )


# ------------------------------------------------------------------
# All clear
# ------------------------------------------------------------------

def test_all_clear_returns_not_tripped():
    cb = make_breaker(
        daily_pnl=0.0,
        router_state=ClientState.PRIMARY,
        edge_len=ROLLING_EDGE_WINDOW,
        edge_value=0.01,
        calibrator_samples=MIN_CALIBRATOR_SAMPLES,
    )
    status = cb.check()
    assert not status.tripped
    assert status.reason is None
    assert status.message is None


# ------------------------------------------------------------------
# BOTH_CLIENTS_FAILED
# ------------------------------------------------------------------

def test_trips_on_both_clients_failed():
    cb = make_breaker(router_state=ClientState.BOTH_FAILED)
    status = cb.check()
    assert status.tripped
    assert status.reason is TripReason.BOTH_CLIENTS_FAILED


# ------------------------------------------------------------------
# Daily drawdown
# ------------------------------------------------------------------

def test_trips_on_daily_drawdown_exceeded():
    cb = make_breaker(daily_pnl=-(MAX_DAILY_DRAWDOWN_DOLLARS + 0.01))
    status = cb.check()
    assert status.tripped
    assert status.reason is TripReason.DAILY_DRAWDOWN


def test_does_not_trip_at_exact_drawdown_boundary():
    # < -200 trips; exactly -200 does not
    cb = make_breaker(daily_pnl=-MAX_DAILY_DRAWDOWN_DOLLARS)
    status = cb.check()
    assert not status.tripped


# ------------------------------------------------------------------
# Negative rolling edge
# ------------------------------------------------------------------

def test_trips_on_negative_rolling_edge_with_enough_trades():
    cb = make_breaker(edge_len=ROLLING_EDGE_WINDOW, edge_value=-0.01)
    status = cb.check()
    assert status.tripped
    assert status.reason is TripReason.NEGATIVE_ROLLING_EDGE


def test_does_not_trip_on_negative_edge_with_too_few_trades():
    cb = make_breaker(edge_len=ROLLING_EDGE_WINDOW - 1, edge_value=-0.01)
    status = cb.check()
    assert not status.tripped


def test_does_not_trip_on_positive_edge():
    cb = make_breaker(edge_len=ROLLING_EDGE_WINDOW, edge_value=0.02)
    status = cb.check()
    assert not status.tripped


# ------------------------------------------------------------------
# Calibrator insufficient
# ------------------------------------------------------------------

def test_trips_on_calibrator_insufficient_samples():
    cb = make_breaker(calibrator_samples=MIN_CALIBRATOR_SAMPLES - 1)
    status = cb.check()
    assert status.tripped
    assert status.reason is TripReason.CALIBRATOR_INSUFFICIENT


def test_does_not_trip_at_exactly_min_calibrator_samples():
    cb = make_breaker(calibrator_samples=MIN_CALIBRATOR_SAMPLES)
    status = cb.check()
    assert not status.tripped


# ------------------------------------------------------------------
# Check order: BOTH_CLIENTS_FAILED takes priority over drawdown
# ------------------------------------------------------------------

def test_both_failed_takes_priority_over_drawdown():
    cb = make_breaker(
        router_state=ClientState.BOTH_FAILED,
        daily_pnl=-(MAX_DAILY_DRAWDOWN_DOLLARS + 50),
    )
    status = cb.check()
    assert status.reason is TripReason.BOTH_CLIENTS_FAILED


# ------------------------------------------------------------------
# is_tripped convenience wrapper
# ------------------------------------------------------------------

def test_is_tripped_returns_true_when_tripped():
    cb = make_breaker(router_state=ClientState.BOTH_FAILED)
    assert cb.is_tripped() is True


def test_is_tripped_returns_false_when_clear():
    cb = make_breaker()
    assert cb.is_tripped() is False
