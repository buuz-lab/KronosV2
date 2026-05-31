"""
Regime confidence tracker for Kronos V2.

Confidence-stratified Gate 2 shadow analysis and candle_features logger health.

Usage:
    python3 scripts/regime_confidence_tracker.py [--db trades.db] [--days N]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--db",
        default="trades.db",
        help="Path to trades.db (default: trades.db)",
    )
    p.add_argument(
        "--days",
        type=int,
        default=30,
        help="Limit queries to this many days back (default: 30)",
    )
    return p.parse_args()


def _open(db_path: str) -> sqlite3.Connection:
    if not Path(db_path).exists():
        sys.exit(f"Database not found: {db_path}")
    return sqlite3.connect(db_path)


# ── Section 1: Overall regime model live stats ────────────────────────────────


def section_regime_live_stats(conn: sqlite3.Connection, days: int) -> None:
    print("=== REGIME MODEL LIVE STATS ===")

    total = conn.execute(
        f"""SELECT COUNT(*) FROM trades
            WHERE regime_prob IS NOT NULL
              AND DATE(timestamp) >= DATE('now', '-{days} days')"""
    ).fetchone()[0]

    print(f"Trades with regime_prob (last {days}d) : {total}")

    if total == 0:
        print("  (no regime_prob data yet)")
        print()
        return

    # Agreement: regime direction matches Kronos (trade) direction
    agreed = conn.execute(
        f"""SELECT COUNT(*) FROM trades
            WHERE regime_prob IS NOT NULL
              AND DATE(timestamp) >= DATE('now', '-{days} days')
              AND (
                (direction = 1 AND regime_prob >= 0.5)
                OR (direction = 0 AND regime_prob < 0.5)
              )"""
    ).fetchone()[0]

    agree_pct = 100.0 * agreed / total if total > 0 else 0.0
    print(f"  Agreement with Kronos direction  : {agree_pct:.1f}%  (regime direction == trade direction)")

    # Win rate on agreements
    agree_resolved = conn.execute(
        f"""SELECT COUNT(*), ROUND(100.0*AVG(outcome), 1)
            FROM trades
            WHERE regime_prob IS NOT NULL
              AND outcome IS NOT NULL
              AND DATE(timestamp) >= DATE('now', '-{days} days')
              AND (
                (direction = 1 AND regime_prob >= 0.5)
                OR (direction = 0 AND regime_prob < 0.5)
              )"""
    ).fetchone()
    n_agree = agree_resolved[0] or 0
    wr_agree = agree_resolved[1] or 0.0

    # Win rate on disagreements
    disagree_resolved = conn.execute(
        f"""SELECT COUNT(*), ROUND(100.0*AVG(outcome), 1)
            FROM trades
            WHERE regime_prob IS NOT NULL
              AND outcome IS NOT NULL
              AND DATE(timestamp) >= DATE('now', '-{days} days')
              AND (
                (direction = 1 AND regime_prob < 0.5)
                OR (direction = 0 AND regime_prob >= 0.5)
              )"""
    ).fetchone()
    n_disagree = disagree_resolved[0] or 0
    wr_disagree = disagree_resolved[1] or 0.0

    print(f"  Win rate on agreements           : {wr_agree}%  (n={n_agree})")
    print(f"  Win rate on disagreements        : {wr_disagree}%  (n={n_disagree})")
    print(f"  (Note: Gate 2 is shadow mode — disagreements do NOT block trades)")
    print()


# ── Section 2: Confidence-stratified accuracy ─────────────────────────────────


def section_confidence_stratified(conn: sqlite3.Connection, days: int) -> None:
    print("=== CONFIDENCE-STRATIFIED ACCURACY ===")

    buckets = [
        ("low   (<0.10)",  "ABS(regime_prob - 0.5) < 0.10"),
        ("med   (0.10-0.20)", "ABS(regime_prob - 0.5) >= 0.10 AND ABS(regime_prob - 0.5) < 0.20"),
        ("high  (0.20-0.30)", "ABS(regime_prob - 0.5) >= 0.20 AND ABS(regime_prob - 0.5) < 0.30"),
        ("very  (>0.30)",  "ABS(regime_prob - 0.5) >= 0.30"),
    ]

    header = f"{'Confidence bucket':<24s} {'n':>6s}  {'win_pct':>8s}  {'agrees_with_kronos':>18s}"
    print(header)

    for label, cond in buckets:
        row = conn.execute(
            f"""SELECT
                  COUNT(*),
                  ROUND(100.0*AVG(outcome), 1),
                  ROUND(100.0*AVG(CASE
                    WHEN (direction = 1 AND regime_prob >= 0.5)
                      OR (direction = 0 AND regime_prob < 0.5) THEN 1.0 ELSE 0.0
                  END), 1)
                FROM trades
                WHERE regime_prob IS NOT NULL
                  AND outcome IS NOT NULL
                  AND DATE(timestamp) >= DATE('now', '-{days} days')
                  AND {cond}"""
        ).fetchone()
        n = row[0] or 0
        win_pct = f"{row[1]}%" if row[1] is not None else "N/A"
        agrees = f"{row[2]}%" if row[2] is not None else "N/A"
        print(f"{label:<24s} {n:>6d}  {win_pct:>8s}  {agrees:>18s}")

    print(f"(Only rows with regime_prob IS NOT NULL AND outcome IS NOT NULL)")
    print()


# ── Section 3: Gate 2 shadow disagreements ────────────────────────────────────


def section_gate2_shadow(conn: sqlite3.Connection, days: int) -> None:
    print("=== GATE 2 SHADOW DISAGREEMENTS ===")

    total = conn.execute(
        f"""SELECT COUNT(*) FROM trades
            WHERE regime_prob IS NOT NULL
              AND DATE(timestamp) >= DATE('now', '-{days} days')
              AND (
                (direction = 1 AND regime_prob < 0.5)
                OR (direction = 0 AND regime_prob >= 0.5)
              )"""
    ).fetchone()[0]

    print(f"Total disagreements (last {days}d) : {total}")

    if total == 0:
        print("  (no disagreements yet)")
        print()
        return

    # High-confidence: regime_prob > 0.7 (when direction=0) or regime_prob < 0.3 (when direction=1)
    high_conf = conn.execute(
        f"""SELECT COUNT(*), ROUND(100.0*AVG(outcome), 1)
            FROM trades
            WHERE regime_prob IS NOT NULL
              AND outcome IS NOT NULL
              AND DATE(timestamp) >= DATE('now', '-{days} days')
              AND (
                (direction = 1 AND regime_prob < 0.3)
                OR (direction = 0 AND regime_prob > 0.7)
              )"""
    ).fetchone()

    med_conf = conn.execute(
        f"""SELECT COUNT(*), ROUND(100.0*AVG(outcome), 1)
            FROM trades
            WHERE regime_prob IS NOT NULL
              AND outcome IS NOT NULL
              AND DATE(timestamp) >= DATE('now', '-{days} days')
              AND (
                (direction = 1 AND regime_prob >= 0.3 AND regime_prob < 0.4)
                OR (direction = 0 AND regime_prob > 0.6 AND regime_prob <= 0.7)
              )"""
    ).fetchone()

    low_conf = conn.execute(
        f"""SELECT COUNT(*), ROUND(100.0*AVG(outcome), 1)
            FROM trades
            WHERE regime_prob IS NOT NULL
              AND outcome IS NOT NULL
              AND DATE(timestamp) >= DATE('now', '-{days} days')
              AND (
                (direction = 1 AND regime_prob >= 0.4 AND regime_prob < 0.5)
                OR (direction = 0 AND regime_prob > 0.5 AND regime_prob <= 0.6)
              )"""
    ).fetchone()

    def _fmt(row):
        n = row[0] or 0
        wr = f"{row[1]}%" if row[1] is not None else "N/A"
        return n, wr

    n_high, wr_high = _fmt(high_conf)
    n_med, wr_med = _fmt(med_conf)
    n_low, wr_low = _fmt(low_conf)

    print(f"  High-confidence (>0.70 away) : {n_high}  — win_pct {wr_high}")
    print(f"  Med-confidence  (0.60-0.70)  : {n_med}  — win_pct {wr_med}")
    print(f"  Low-confidence  (<0.60)      : {n_low}  — win_pct {wr_low}")
    print()


# ── Section 4: Candle features logger health ──────────────────────────────────


def section_candle_features_health(conn: sqlite3.Connection) -> None:
    print("=== CANDLE FEATURES LOGGER HEALTH ===")

    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "candle_features" not in tables:
        print("  (candle_features table not yet created — logger not running)")
        print()
        return

    total = conn.execute("SELECT COUNT(*) FROM candle_features").fetchone()[0]
    print(f"Total rows logged               : {total}")

    if total == 0:
        print("  (no rows yet — logger may have just started)")
        print()
        return

    date_range = conn.execute(
        "SELECT MIN(DATE(candle_ts)), MAX(DATE(candle_ts)) FROM candle_features"
    ).fetchone()
    print(f"Date range                      : {date_range[0]} to {date_range[1]}")

    up = conn.execute(
        "SELECT COUNT(*) FROM candle_features WHERE btc_direction = 1"
    ).fetchone()[0]
    down = conn.execute(
        "SELECT COUNT(*) FROM candle_features WHERE btc_direction = 0"
    ).fetchone()[0]
    up_pct = 100.0 * up / total
    down_pct = 100.0 * down / total
    print(f"BTC up candles                  : {up} ({up_pct:.1f}%)")
    print(f"BTC down candles                : {down} ({down_pct:.1f}%)")

    feat_stale = conn.execute(
        "SELECT COUNT(*) FROM candle_features WHERE features_stale = 1"
    ).fetchone()[0]
    deribit_stale = conn.execute(
        "SELECT COUNT(*) FROM candle_features WHERE deribit_stale = 1"
    ).fetchone()[0]
    fs_pct = 100.0 * feat_stale / total
    ds_pct = 100.0 * deribit_stale / total
    print(f"Features stale                  : {feat_stale} ({fs_pct:.1f}%)")
    print(f"Deribit stale                   : {deribit_stale} ({ds_pct:.1f}%)")

    last_24h = conn.execute(
        """SELECT COUNT(*) FROM candle_features
           WHERE logged_at >= DATETIME('now', '-1 day')"""
    ).fetchone()[0]
    print(f"Rows last 24h                   : {last_24h}  (expected ~96)")
    print()


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    args = parse_args()
    conn = _open(args.db)
    try:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "trades" not in tables:
            print("trades table does not exist in the database — nothing to report.")
            return

        section_regime_live_stats(conn, args.days)
        section_confidence_stratified(conn, args.days)
        section_gate2_shadow(conn, args.days)
        section_candle_features_health(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
