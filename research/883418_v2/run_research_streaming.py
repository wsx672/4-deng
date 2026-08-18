from __future__ import annotations

"""Memory-safe entry point for the 883418 V2 research runner.

It keeps only a few market sessions in memory while the full-history data is
cached as Parquet files on the GitHub Actions runner.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd

import run_research as rr


class LazyFrames:
    def get(self, date: pd.Timestamp, default: Any = None) -> pd.DataFrame:
        path = rr.cache_file_for_date(date)
        if not path.exists():
            return default if default is not None else pd.DataFrame()
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            rr.log(f"Could not read cached frame for {date:%Y-%m-%d}: {exc}")
            return default if default is not None else pd.DataFrame()


def fetch_one_market_day_stream(date: pd.Timestamp) -> tuple[pd.Timestamp, dict[str, Any]]:
    cache_path: Path = rr.cache_file_for_date(date)
    if cache_path.exists():
        try:
            count = len(pd.read_parquet(cache_path, columns=["ts_code"]))
        except Exception:
            count = rr.MIN_STOCKS
        return date, {
            "cached": True,
            "daily_rows": count,
            "basic_rows": count,
            "merged_rows": count,
        }

    date_str = date.strftime("%Y%m%d")
    daily = rr.api_call(
        "daily",
        fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        trade_date=date_str,
    )
    basic = rr.api_call(
        "daily_basic",
        fields=(
            "ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,"
            "total_share,float_share,free_share,total_mv,circ_mv"
        ),
        trade_date=date_str,
    )
    if daily.empty or basic.empty:
        return date, {
            "cached": False,
            "daily_rows": len(daily),
            "basic_rows": len(basic),
            "merged_rows": 0,
        }

    basic = basic.drop(columns=["close"], errors="ignore")
    frame = daily.merge(
        basic,
        on=["ts_code", "trade_date"],
        how="inner",
        validate="one_to_one",
    )
    numeric_cols = [
        "open", "high", "low", "close", "pre_close", "change", "pct_chg",
        "vol", "amount", "turnover_rate", "turnover_rate_f", "volume_ratio",
        "total_share", "float_share", "free_share", "total_mv", "circ_mv",
    ]
    for col in numeric_cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame[frame["ts_code"].str.endswith((".SH", ".SZ"), na=False)].copy()
    frame = frame.dropna(subset=["ts_code", "pct_chg", "total_mv", "close"])
    frame = frame[(frame["total_mv"] > 0) & (frame["close"] > 0)]
    if not frame.empty:
        frame.to_parquet(cache_path, index=False, compression="zstd")
    return date, {
        "cached": False,
        "daily_rows": len(daily),
        "basic_rows": len(basic),
        "merged_rows": len(frame),
    }


def fetch_market_days_stream(
    dates: list[pd.Timestamp],
) -> tuple[LazyFrames, pd.DataFrame]:
    rr.log(f"Fetching {len(dates):,} market sessions with {rr.MAX_WORKERS} workers")
    quality_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=rr.MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_one_market_day_stream, date): date
            for date in dates
        }
        completed = 0
        for future in as_completed(futures):
            date = futures.pop(future)
            try:
                out_date, quality = future.result()
                quality_rows.append({"trade_date": out_date, **quality})
            except Exception as exc:
                rr.log(f"Market data failed for {date:%Y-%m-%d}: {str(exc)[:240]}")
                quality_rows.append({
                    "trade_date": date,
                    "cached": False,
                    "daily_rows": 0,
                    "basic_rows": 0,
                    "merged_rows": 0,
                    "error": str(exc).replace(rr.TOKEN, "***")[:500],
                })
            completed += 1
            if completed % 100 == 0 or completed == len(dates):
                rr.log(f"Fetched {completed:,}/{len(dates):,} sessions")
    quality = pd.DataFrame(quality_rows).sort_values("trade_date")
    return LazyFrames(), quality


rr.fetch_market_days = fetch_market_days_stream


if __name__ == "__main__":
    rr.main()
