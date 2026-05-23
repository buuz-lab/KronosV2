"""
Win rate analysis by entry price bucket.

Usage:
    python3 scripts/win_rate_by_price.py              # 10¢ buckets
    python3 scripts/win_rate_by_price.py --bucket 5   # 5¢ buckets
    python3 scripts/win_rate_by_price.py --dir yes    # YES→UP only
    python3 scripts/win_rate_by_price.py --dir no     # NO→DOWN only
"""
import argparse
import sqlite3

DB = "trades.db"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=int, default=10, help="Bucket size in cents")
    parser.add_argument("--dir", choices=["yes", "no", "both"], default="both")
    parser.add_argument("--min-trades", type=int, default=5, help="Min trades to show bucket")
    args = parser.parse_args()

    dir_filter = ""
    if args.dir == "yes":
        dir_filter = "AND direction = 1"
    elif args.dir == "no":
        dir_filter = "AND direction = 0"

    conn = sqlite3.connect(DB)
    rows = conn.execute(f"""
        SELECT
            (fill_price_cents / {args.bucket}) * {args.bucket}  AS bucket_low,
            (fill_price_cents / {args.bucket}) * {args.bucket} + {args.bucket} - 1 AS bucket_high,
            COUNT(*)                                             AS trades,
            SUM(CASE WHEN outcome = 1 THEN 1 ELSE 0 END)        AS wins,
            ROUND(SUM(pnl_dollars), 2)                          AS net_pnl,
            ROUND(AVG(fill_price_cents), 1)                     AS avg_price
        FROM trades
        WHERE outcome IS NOT NULL
          AND fill_price_cents IS NOT NULL
          {dir_filter}
        GROUP BY bucket_low
        ORDER BY bucket_low
    """).fetchall()
    conn.close()

    dir_label = {"yes": "YES→UP only", "no": "NO→DOWN only", "both": "All directions"}[args.dir]
    print(f"\nWin Rate by Entry Price — {dir_label}  (bucket={args.bucket}¢, min={args.min_trades} trades)")
    print(f"  {'Price Range':<14} {'Trades':>7} {'Wins':>6} {'Win%':>7} {'Net P&L':>10}  {'Avg Price':>10}")
    print("  " + "-" * 62)

    for bucket_low, bucket_high, trades, wins, net_pnl, avg_price in rows:
        if trades < args.min_trades:
            continue
        win_pct = wins * 100 / trades
        bar = "█" * int(win_pct / 5)
        flag = " ✓" if win_pct >= 55 else (" ✗" if win_pct < 45 else "")
        print(
            f"  {bucket_low:>3}¢ – {bucket_high:>3}¢     "
            f"{trades:>7} {wins:>6} {win_pct:>6.1f}%"
            f" {net_pnl:>10.2f}  {avg_price:>8.1f}¢  {bar}{flag}"
        )

    total = conn = sqlite3.connect(DB).execute(f"""
        SELECT COUNT(*), SUM(CASE WHEN outcome=1 THEN 1 ELSE 0 END), ROUND(SUM(pnl_dollars),2)
        FROM trades WHERE outcome IS NOT NULL AND fill_price_cents IS NOT NULL {dir_filter}
    """).fetchone()
    if total[0]:
        print("  " + "-" * 62)
        print(f"  {'TOTAL':<14} {total[0]:>7} {total[1]:>6} {total[1]*100/total[0]:>6.1f}%  {total[2]:>10.2f}")
    print()


if __name__ == "__main__":
    main()
