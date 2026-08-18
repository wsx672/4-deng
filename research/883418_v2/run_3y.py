from __future__ import annotations

import os
from collections import defaultdict, deque
from typing import Any

import numpy as np
import pandas as pd

# 固定研究范围：最近3年；只复刻“每日总市值最小200只”，不再运行100/300只对照。
os.environ.setdefault("START_DATE", "20230819")
os.environ.setdefault("END_DATE", "20260819")
os.environ.setdefault("BASE_N", "200")
os.environ.setdefault("TUSHARE_PROXY_URL", "https://fast.xiaodefa.cn")

import run_research as research


VARIANT = "total_mv_bottom200_daily"


def process_market_200(
    dates: list[pd.Timestamp],
    frames: dict[pd.Timestamp, pd.DataFrame],
    stock_basic: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """只构造每日总市值最小200只股票的等权微盘组合。"""
    previous_members: list[str] = []
    index_level = 1000.0
    synthetic_level: dict[str, float] = {}
    price_hist: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=60))
    ret_hist: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=60))
    daily_rows: list[dict[str, Any]] = []
    membership_rows: list[dict[str, Any]] = []
    latest_members = pd.DataFrame()

    if stock_basic.empty:
        meta_names = pd.Series(dtype=object)
        meta_market = pd.Series(dtype=object)
        meta_list_date = pd.Series(dtype=object)
    else:
        stock_meta = stock_basic.drop_duplicates("ts_code").set_index("ts_code")
        meta_names = stock_meta.get("name", pd.Series(dtype=object))
        meta_market = stock_meta.get("market", pd.Series(dtype=object))
        meta_list_date = stock_meta.get("list_date", pd.Series(dtype=object))

    research.log("Constructing only the daily bottom-200 total-market-cap portfolio")
    sorted_dates = sorted(dates)
    for idx, date in enumerate(sorted_dates):
        frame = frames.get(date, pd.DataFrame())
        if frame.empty or len(frame) < research.MIN_STOCKS:
            continue

        frame = frame.copy().set_index("ts_code", drop=False)
        frame["ret"] = pd.to_numeric(frame["pct_chg"], errors="coerce") / 100.0

        # 用每日涨跌幅连续复合成技术价格序列，避免除权导致均线/广度失真。
        for code, ret, close in frame[["ts_code", "ret", "close"]].itertuples(index=False, name=None):
            if not np.isfinite(ret):
                continue
            previous_level = synthetic_level.get(code)
            current_level = (
                float(close)
                if previous_level is None or not np.isfinite(previous_level)
                else float(previous_level * (1.0 + ret))
            )
            if current_level <= 0 or not np.isfinite(current_level):
                current_level = float(close)
            synthetic_level[code] = current_level
            price_hist[code].append(current_level)
            ret_hist[code].append(float(ret))

        selected = research.choose_members(
            frame.reset_index(drop=True),
            n=200,
            field="total_mv",
            min_age_days=0,
            date=date,
            stock_meta=stock_basic,
        )

        # T日收盘确定成分，组合收益使用T-1日已知成分在T日的等权收益，避免未来函数。
        realized = frame.reindex(previous_members)["ret"].dropna() if previous_members else pd.Series(dtype=float)
        portfolio_ret = (
            float(realized.mean())
            if len(realized) >= max(30, int(len(previous_members) * 0.50))
            else np.nan
        )
        if np.isfinite(portfolio_ret):
            index_level *= 1.0 + portfolio_ret

        overlap = len(set(previous_members) & set(selected))
        denominator = max(1, max(len(previous_members), len(selected)))
        turnover = 1.0 - overlap / denominator if previous_members else np.nan

        market_amount = float(frame["amount"].sum(skipna=True))
        base = frame.reindex(selected).dropna(subset=["ret", "total_mv"])
        member_count = len(base)
        selected_amount = float(base["amount"].sum(skipna=True))

        row: dict[str, Any] = {
            "trade_date": date,
            "market_stock_count": len(frame),
            "market_amount": market_amount,
            "market_total_mv": float(frame["total_mv"].sum(skipna=True)),
            f"ret__{VARIANT}": portfolio_ret,
            f"level__{VARIANT}": index_level,
            f"turnover__{VARIANT}": turnover,
            f"member_count__{VARIANT}": len(selected),
            "base_member_count": member_count,
            "selected_amount": selected_amount,
            "amount_share": selected_amount / market_amount if market_amount > 0 else np.nan,
            "median_turnover": float(base["turnover_rate"].median(skipna=True)),
            "median_total_mv": float(base["total_mv"].median(skipna=True)),
            "max_total_mv": float(base["total_mv"].max(skipna=True)),
            "min_total_mv": float(base["total_mv"].min(skipna=True)),
        }

        illiq = np.abs(base["ret"].to_numpy(dtype=float)) / np.maximum(
            base["amount"].to_numpy(dtype=float), 1e-9
        )
        illiq = illiq[np.isfinite(illiq)]
        row["illiq_median"] = float(np.median(illiq)) if illiq.size else np.nan
        row["illiq90"] = float(np.quantile(illiq, 0.90)) if illiq.size >= 20 else np.nan
        row["liquidity_fragility"] = (
            row["illiq90"] / row["illiq_median"]
            if row["illiq_median"] and np.isfinite(row["illiq_median"])
            else np.nan
        )

        above20: list[bool] = []
        above60: list[bool] = []
        for code in selected:
            history = np.asarray(price_hist[code], dtype=float)
            if history.size >= 20:
                above20.append(bool(history[-1] > np.nanmean(history[-20:])))
            if history.size >= 60:
                above60.append(bool(history[-1] > np.nanmean(history[-60:])))
        row["b20"] = float(np.mean(above20)) if len(above20) >= 50 else np.nan
        row["b60"] = float(np.mean(above60)) if len(above60) >= 50 else np.nan
        row["b20_valid_count"] = len(above20)
        row["b60_valid_count"] = len(above60)

        matrix = research.rolling_return_matrix(ret_hist, selected, 20)
        avg_corr, pc1_share = research.correlation_and_pc1(matrix)
        row["avg_corr20"] = avg_corr
        row["pc1_share20"] = pc1_share
        row["extreme_up_rate"] = float((base["pct_chg"] >= 9.8).mean()) if member_count else np.nan
        row["extreme_down_rate"] = float((base["pct_chg"] <= -9.8).mean()) if member_count else np.nan
        row["advance_ratio"] = float((base["ret"] > 0).mean()) if member_count else np.nan

        sorted_base = base.sort_values(["total_mv", "ts_code"])
        for rank, (code, values) in enumerate(sorted_base.iterrows(), start=1):
            membership_rows.append(
                {
                    "trade_date": date,
                    "rank": rank,
                    "ts_code": code,
                    "name": meta_names.get(code, ""),
                    "market": meta_market.get(code, ""),
                    "list_date": meta_list_date.get(code, ""),
                    "close": values.get("close"),
                    "pct_chg": values.get("pct_chg"),
                    "amount": values.get("amount"),
                    "turnover_rate": values.get("turnover_rate"),
                    "total_mv": values.get("total_mv"),
                    "circ_mv": values.get("circ_mv"),
                }
            )

        latest_members = sorted_base.reset_index(drop=True).copy()
        latest_members["trade_date"] = date
        latest_members["name"] = latest_members["ts_code"].map(meta_names)
        latest_members["market"] = latest_members["ts_code"].map(meta_market)
        latest_members["list_date"] = latest_members["ts_code"].map(meta_list_date)
        latest_members.insert(0, "rank", np.arange(1, len(latest_members) + 1))

        daily_rows.append(row)
        previous_members = selected
        if (idx + 1) % 100 == 0 or idx + 1 == len(sorted_dates):
            research.log(f"Processed {idx + 1:,}/{len(sorted_dates):,} sessions")

    daily = pd.DataFrame(daily_rows).sort_values("trade_date").reset_index(drop=True)
    membership = pd.DataFrame(membership_rows)
    return daily, membership, latest_members


# 覆盖通用脚本中的多规模构造函数，确保本入口只运行200只方案。
research.process_market = process_market_200


if __name__ == "__main__":
    research.main()
