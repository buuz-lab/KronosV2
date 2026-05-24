"""
Tests for CalibrationDriftMonitor.

Uses fakeredis so no live Redis is needed.
"""

import math
from collections import deque

import fakeredis
import pytest

from btc_kalshi_system.signal.calibration_drift_monitor import (
    CalibrationDriftMonitor,
    DRIFT_WINDOW,
    DRIFT_ALERT_MULTIPLIER,
    DRIFT_CONSECUTIVE_ALERT,
    _KEY_HISTORY,
    _KEY_BASELINE,
    _KEY_ALERT_COUNT,
)


def make_monitor() -> CalibrationDriftMonitor:
    """CalibrationDriftMonitor backed by a fresh fakeredis instance."""
    monitor = CalibrationDriftMonitor.__new__(CalibrationDriftMonitor)
    monitor._redis = fakeredis.FakeRedis(decode_responses=True)
    monitor._history = deque(maxlen=DRIFT_WINDOW)
    monitor._total_count = 0
    return monitor


def _record_n_good(monitor: CalibrationDriftMonitor, n: int) -> None:
    """Record n trades with Brier score of 0.25 each (prob=0.5, outcome=1)."""
    for _ in range(n):
        monitor.record(calibrated_prob=0.5, outcome=1)


def _record_window_with_brier(monitor: CalibrationDriftMonitor, brier_per_trade: float) -> None:
    """Record DRIFT_WINDOW trades that produce the given per-trade Brier score.

    We use outcome=1 and calibrated_prob = 1 - sqrt(brier_per_trade).
    """
    prob = 1.0 - math.sqrt(brier_per_trade)
    for _ in range(DRIFT_WINDOW):
        monitor.record(calibrated_prob=prob, outcome=1)


# ── Test 1 ─────────────────────────────────────────────────────────────────────

def test_first_window_sets_baseline_alert_count_stays_0():
    """Record exactly DRIFT_WINDOW trades — baseline is set, alert_count stays 0."""
    monitor = make_monitor()
    _record_n_good(monitor, DRIFT_WINDOW)

    # Baseline must be set
    baseline = monitor.baseline_brier()
    assert baseline is not None
    assert baseline == pytest.approx(0.25)  # (0.5 - 1)^2 = 0.25

    # First window never triggers an alert
    raw_count = monitor._redis.get(_KEY_ALERT_COUNT)
    assert int(raw_count) == 0
    assert not monitor.is_drifting()


# ── Test 2 ─────────────────────────────────────────────────────────────────────

def test_second_window_bad_increments_alert_count():
    """Second window at 1.4× baseline → alert_count == 1."""
    monitor = make_monitor()

    # First window — establishes baseline of 0.25
    _record_n_good(monitor, DRIFT_WINDOW)
    baseline = monitor.baseline_brier()
    assert baseline == pytest.approx(0.25)

    # Second window — Brier = 0.25 * 1.4 = 0.35 (exceeds 1.3× threshold)
    bad_brier = baseline * 1.4
    _record_window_with_brier(monitor, bad_brier)

    alert_count = int(monitor._redis.get(_KEY_ALERT_COUNT))
    assert alert_count == 1
    assert not monitor.is_drifting()  # Need 3 consecutive, only have 1


# ── Test 3 ─────────────────────────────────────────────────────────────────────

def test_three_consecutive_bad_windows_is_drifting():
    """Three consecutive bad windows → is_drifting() returns True."""
    monitor = make_monitor()

    # First window — baseline = 0.25
    _record_n_good(monitor, DRIFT_WINDOW)
    baseline = monitor.baseline_brier()
    assert baseline == pytest.approx(0.25)

    bad_brier = baseline * 1.4  # 0.35 — exceeds 1.3× threshold

    # Three more bad windows
    for _ in range(3):
        _record_window_with_brier(monitor, bad_brier)

    assert monitor.is_drifting()
    assert int(monitor._redis.get(_KEY_ALERT_COUNT)) == 3


# ── Test 4 ─────────────────────────────────────────────────────────────────────

def test_good_window_resets_alert_count():
    """After 3 bad windows, a good window resets alert_count to 0."""
    monitor = make_monitor()

    # First window — baseline = 0.25
    _record_n_good(monitor, DRIFT_WINDOW)
    baseline = monitor.baseline_brier()

    bad_brier = baseline * 1.4

    # Three bad windows → drifting
    for _ in range(3):
        _record_window_with_brier(monitor, bad_brier)

    assert monitor.is_drifting()

    # Good window — Brier = 0.25 (== baseline, under 1.3× threshold)
    _record_n_good(monitor, DRIFT_WINDOW)

    assert not monitor.is_drifting()
    assert int(monitor._redis.get(_KEY_ALERT_COUNT)) == 0


# ── Test 5 ─────────────────────────────────────────────────────────────────────

def test_current_brier_zero_before_window_fills():
    """Recording fewer than DRIFT_WINDOW trades → current_brier() returns 0.0."""
    monitor = make_monitor()

    for i in range(DRIFT_WINDOW - 1):
        monitor.record(calibrated_prob=0.5, outcome=1)
        assert monitor.current_brier() == pytest.approx(0.0), (
            f"Expected 0.0 after {i + 1} trades, got {monitor.current_brier()}"
        )

    # Exactly at window fill it should return the actual score
    monitor.record(calibrated_prob=0.5, outcome=1)
    assert monitor.current_brier() == pytest.approx(0.25)
