from __future__ import annotations

import json
import math
import os
import sys
import time
import traceback
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import tushare as ts
from scipy import stats


PROXY_URL = os.getenv("TUSHARE_PROXY_URL", "https://fast.xiaodefa.cn")
TOKEN = os.environ["TUSHARE_TOKEN"].strip()
START_DATE = os.getenv("START_DATE", "20180101")
END_DATE = os.getenv("END_DATE", datetime.now().strftime("%Y%m%d"))
OUT = Path(os.getenv("OUTPUT_DIR", "research_output"))
CACHE = Path(os.getenv("CACHE_DIR", ".cache/883418_v2"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
MIN_STOCKS = int(os.getenv("MIN_STOCKS", "150"))
BASE_N = int(os.getenv("BASE_N", "200"))
BASE_COST_BPS = float(os.getenv("BASE_COST_BPS", "10"))

OUT.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)

ts.set_token(TOKEN)
pro = ts.pro_api()
pro._DataApi__http_url = PROXY_URL


@dataclass
class ApiStats:
    calls: int = 0
    retries: int = 0
    errors: int = 0
    rows: int = 0


API_STATS: dict[str, ApiStats] = defaultdict(ApiStats)


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def safe_json_value(value: Any) -> Any:
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Path):
        return str(value)
    return value


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=safe_json_value), encoding="utf-8")


def api_call(api_name: str, fields: str = "", max_attempts: int = 7, **params: Any) -> pd.DataFrame:
    stats_obj = API_STATS[api_name]
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        stats_obj.calls += 1
        try:
            method = getattr(pro, api_name)
            df = method(fields=fields, **params) if fields else method(**params)
            if df is None:
                df = pd.DataFrame()
            stats_obj.rows += len(df)
            return df
        except Exception as exc:
            last_error = exc
            stats_obj.errors += 1
            if attempt + 1 >= max_attempts:
                break
            stats_obj.retries += 1
            delay = min(20.0, 0.8 * (2**attempt)) + np.random.uniform(0.0, 0.4)
            msg = str(exc).replace(TOKEN, "***")
            log(f"{api_name} retry {attempt + 1}/{max_attempts - 1}: {msg[:180]}")
            time.sleep(delay)
    assert last_error is not None
    raise RuntimeError(f"{api_name} failed after {max_attempts} attempts: {str(last_error).replace(TOKEN, '***')}")


def normalize_dates(df: pd.DataFrame, column: str = "trade_date") -> pd.DataFrame:
    if not df.empty and column in df.columns:
        df = df.copy()
        df[column] = pd.to_datetime(df[column].astype(str), format="%Y%m%d", errors="coerce")
    return df


def fetch_static_data() -> tuple[list[pd.Timestamp], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    log("Fetching trading calendar, stock metadata, 883418 and benchmark history")
    calendar = api_call(
        "trade_cal",
        fields="exchange,cal_date,is_open,pretrade_date",
        exchange="SSE",
        start_date=START_DATE,
        end_date=END_DATE,
        is_open="1",
    )
    if calendar.empty:
        raise RuntimeError("trade_cal returned no open dates")
    calendar["cal_date"] = pd.to_datetime(calendar["cal_date"].astype(str), format="%Y%m%d")
    dates = sorted(pd.Timestamp(x) for x in calendar["cal_date"].dropna().unique())

    stocks: list[pd.DataFrame] = []
    for status in ("L", "D", "P"):
        try:
            frame = api_call(
                "stock_basic",
                fields="ts_code,symbol,name,market,exchange,list_status,list_date,delist_date",
                exchange="",
                list_status=status,
            )
            if not frame.empty:
                stocks.append(frame)
        except Exception as exc:
            log(f"stock_basic status={status} unavailable: {str(exc)[:150]}")
    stock_basic = pd.concat(stocks, ignore_index=True).drop_duplicates("ts_code", keep="first") if stocks else pd.DataFrame()
    if not stock_basic.empty:
        stock_basic["list_date_dt"] = pd.to_datetime(stock_basic["list_date"].astype(str), format="%Y%m%d", errors="coerce")
        stock_basic["delist_date_dt"] = pd.to_datetime(stock_basic["delist_date"].astype(str), format="%Y%m%d", errors="coerce")

    ths = pd.DataFrame()
    try:
        ths = api_call("ths_daily", ts_code="883418.TI", start_date=START_DATE, end_date=END_DATE)
        ths = normalize_dates(ths)
        if not ths.empty:
            ths = ths.sort_values("trade_date").drop_duplicates("trade_date")
            log(f"ths_daily 883418.TI returned {len(ths):,} rows")
        else:
            log("ths_daily 883418.TI returned empty; synthetic index will be target")
    except Exception as exc:
        log(f"ths_daily 883418.TI unavailable: {str(exc)[:200]}; synthetic index will be target")

    benchmark = api_call(
        "index_daily",
        fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        ts_code="000300.SH",
        start_date=START_DATE,
        end_date=END_DATE,
    )
    benchmark = normalize_dates(benchmark).sort_values("trade_date").drop_duplicates("trade_date")
    return dates, stock_basic, ths, benchmark


def cache_file_for_date(date: pd.Timestamp) -> Path:
    return CACHE / f"market_{date.strftime('%Y%m%d')}.parquet"


def fetch_one_market_day(date: pd.Timestamp) -> tuple[pd.Timestamp, pd.DataFrame, dict[str, Any]]:
    date_str = date.strftime("%Y%m%d")
    cache_path = cache_file_for_date(date)
    if cache_path.exists():
        frame = pd.read_parquet(cache_path)
        return date, frame, {"cached": True, "daily_rows": len(frame), "basic_rows": len(frame), "merged_rows": len(frame)}

    daily = api_call(
        "daily",
        fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        trade_date=date_str,
    )
    basic = api_call(
        "daily_basic",
        fields="ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,total_share,float_share,free_share,total_mv,circ_mv",
        trade_date=date_str,
    )
    if daily.empty or basic.empty:
        return date, pd.DataFrame(), {"cached": False, "daily_rows": len(daily), "basic_rows": len(basic), "merged_rows": 0}

    basic = basic.drop(columns=["close"], errors="ignore")
    frame = daily.merge(basic, on=["ts_code", "trade_date"], how="inner", validate="one_to_one")
    numeric_cols = [
        "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount",
        "turnover_rate", "turnover_rate_f", "volume_ratio", "total_share", "float_share", "free_share",
        "total_mv", "circ_mv",
    ]
    for col in numeric_cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame[frame["ts_code"].str.endswith((".SH", ".SZ"), na=False)].copy()
    frame = frame.dropna(subset=["ts_code", "pct_chg", "total_mv", "close"])
    frame = frame[(frame["total_mv"] > 0) & (frame["close"] > 0)]
    if not frame.empty:
        frame.to_parquet(cache_path, index=False, compression="zstd")
    return date, frame, {"cached": False, "daily_rows": len(daily), "basic_rows": len(basic), "merged_rows": len(frame)}


def fetch_market_days(dates: list[pd.Timestamp]) -> tuple[dict[pd.Timestamp, pd.DataFrame], pd.DataFrame]:
    log(f"Fetching {len(dates):,} market sessions with {MAX_WORKERS} workers")
    frames: dict[pd.Timestamp, pd.DataFrame] = {}
    quality_rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_one_market_day, date): date for date in dates}
        completed = 0
        for future in as_completed(futures):
            date = futures[future]
            try:
                out_date, frame, quality = future.result()
                frames[out_date] = frame
                quality_rows.append({"trade_date": out_date, **quality})
            except Exception as exc:
                log(f"Market data failed for {date:%Y-%m-%d}: {str(exc)[:240]}")
                quality_rows.append({"trade_date": date, "cached": False, "daily_rows": 0, "basic_rows": 0, "merged_rows": 0, "error": str(exc).replace(TOKEN, "***")[:500]})
            completed += 1
            if completed % 100 == 0 or completed == len(dates):
                log(f"Fetched {completed:,}/{len(dates):,} sessions")
    return frames, pd.DataFrame(quality_rows).sort_values("trade_date")


def rolling_return_matrix(ret_hist: dict[str, deque[float]], members: list[str], lookback: int = 20) -> np.ndarray:
    usable = [code for code in members if len(ret_hist[code]) >= max(10, lookback // 2)]
    if len(usable) < 20:
        return np.empty((0, 0))
    matrix = np.full((lookback, len(usable)), np.nan, dtype=float)
    for j, code in enumerate(usable):
        values = np.asarray(list(ret_hist[code])[-lookback:], dtype=float)
        matrix[-len(values):, j] = values
    valid_cols = np.sum(np.isfinite(matrix), axis=0) >= max(10, int(lookback * 0.75))
    matrix = matrix[:, valid_cols]
    if matrix.shape[1] < 20:
        return np.empty((0, 0))
    col_means = np.nanmean(matrix, axis=0)
    inds = np.where(~np.isfinite(matrix))
    matrix[inds] = np.take(col_means, inds[1])
    return matrix


def correlation_and_pc1(matrix: np.ndarray) -> tuple[float, float]:
    if matrix.size == 0 or matrix.shape[0] < 10 or matrix.shape[1] < 20:
        return np.nan, np.nan
    centered = matrix - matrix.mean(axis=0, keepdims=True)
    std = centered.std(axis=0, ddof=1)
    keep = std > 1e-12
    centered = centered[:, keep]
    std = std[keep]
    if centered.shape[1] < 20:
        return np.nan, np.nan
    z = centered / std
    n = z.shape[1]
    total_corr_sum = float(np.square(z.sum(axis=1)).sum() / max(1, z.shape[0] - 1))
    avg_corr = (total_corr_sum - n) / (n * (n - 1))
    singular_values = np.linalg.svd(z, full_matrices=False, compute_uv=False)
    eigenvalues = np.square(singular_values) / max(1, z.shape[0] - 1)
    pc1_share = float(eigenvalues[0] / eigenvalues.sum()) if eigenvalues.sum() > 0 else np.nan
    return float(avg_corr), pc1_share


def choose_members(frame: pd.DataFrame, n: int, field: str = "total_mv", min_age_days: int = 0, date: pd.Timestamp | None = None, stock_meta: pd.DataFrame | None = None) -> list[str]:
    eligible = frame.copy()
    if min_age_days > 0 and date is not None and stock_meta is not None and not stock_meta.empty:
        list_dates = stock_meta.set_index("ts_code")["list_date_dt"]
        age = eligible["ts_code"].map(list_dates)
        eligible = eligible[(pd.Timestamp(date) - age).dt.days >= min_age_days]
    eligible = eligible.dropna(subset=[field]).sort_values([field, "ts_code"], ascending=[True, True])
    return eligible.head(n)["ts_code"].tolist()


def process_market(dates: list[pd.Timestamp], frames: dict[pd.Timestamp, pd.DataFrame], stock_basic: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    variants = {
        "total_mv_bottom100_daily": {"n": 100, "field": "total_mv", "age": 0, "freq": "D"},
        "total_mv_bottom200_daily": {"n": 200, "field": "total_mv", "age": 0, "freq": "D"},
        "total_mv_bottom300_daily": {"n": 300, "field": "total_mv", "age": 0, "freq": "D"},
        "circ_mv_bottom200_daily": {"n": 200, "field": "circ_mv", "age": 0, "freq": "D"},
        "total_mv_bottom200_age60": {"n": 200, "field": "total_mv", "age": 60, "freq": "D"},
        "total_mv_bottom200_monthly": {"n": 200, "field": "total_mv", "age": 0, "freq": "M"},
    }
    prev_members: dict[str, list[str]] = {name: [] for name in variants}
    current_month: dict[str, tuple[int, int] | None] = {name: None for name in variants}
    level_by_variant = {name: 1000.0 for name in variants}
    synthetic_level: dict[str, float] = {}
    price_hist: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=60))
    ret_hist: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=60))
    daily_rows: list[dict[str, Any]] = []
    membership_rows: list[dict[str, Any]] = []
    latest_members = pd.DataFrame()

    meta_names = stock_basic.set_index("ts_code")["name"] if not stock_basic.empty else pd.Series(dtype=object)
    meta_market = stock_basic.set_index("ts_code")["market"] if not stock_basic.empty else pd.Series(dtype=object)
    meta_list_date = stock_basic.set_index("ts_code")["list_date"] if not stock_basic.empty else pd.Series(dtype=object)

    log("Constructing daily smallest-cap portfolios and factor primitives")
    for idx, date in enumerate(sorted(dates)):
        frame = frames.get(date, pd.DataFrame())
        if frame.empty or len(frame) < MIN_STOCKS:
            continue
        frame = frame.copy().set_index("ts_code", drop=False)
        frame["ret"] = frame["pct_chg"] / 100.0
        for code, ret, close in frame[["ts_code", "ret", "close"]].itertuples(index=False, name=None):
            if not np.isfinite(ret):
                continue
            previous = synthetic_level.get(code)
            current_level = float(close) if previous is None or not np.isfinite(previous) else float(previous * (1.0 + ret))
            if current_level <= 0 or not np.isfinite(current_level):
                current_level = float(close)
            synthetic_level[code] = current_level
            price_hist[code].append(current_level)
            ret_hist[code].append(float(ret))

        row: dict[str, Any] = {
            "trade_date": date,
            "market_stock_count": len(frame),
            "market_amount": float(frame["amount"].sum(skipna=True)),
            "market_total_mv": float(frame["total_mv"].sum(skipna=True)),
        }
        selected_by_variant: dict[str, list[str]] = {}
        for name, config in variants.items():
            rebalance = config["freq"] == "D" or current_month[name] != (date.year, date.month) or not prev_members[name]
            if rebalance:
                selected = choose_members(frame.reset_index(drop=True), int(config["n"]), str(config["field"]), int(config["age"]), date, stock_basic)
                current_month[name] = (date.year, date.month)
            else:
                selected = [code for code in prev_members[name] if code in frame.index]
            selected_by_variant[name] = selected
            prior = prev_members[name]
            realized = frame.reindex(prior)["ret"].dropna() if prior else pd.Series(dtype=float)
            portfolio_ret = float(realized.mean()) if len(realized) >= max(30, int(len(prior) * 0.5)) else np.nan
            if np.isfinite(portfolio_ret):
                level_by_variant[name] *= 1.0 + portfolio_ret
            overlap = len(set(prior) & set(selected))
            denom = max(1, max(len(prior), len(selected)))
            turnover = 1.0 - overlap / denom if prior else np.nan
            row[f"ret__{name}"] = portfolio_ret
            row[f"level__{name}"] = level_by_variant[name]
            row[f"turnover__{name}"] = turnover
            row[f"member_count__{name}"] = len(selected)
            prev_members[name] = selected

        base_members = selected_by_variant["total_mv_bottom200_daily"]
        base = frame.reindex(base_members).dropna(subset=["ret", "total_mv"])
        member_count = len(base)
        row["base_member_count"] = member_count
        row["selected_amount"] = float(base["amount"].sum(skipna=True))
        row["amount_share"] = row["selected_amount"] / row["market_amount"] if row["market_amount"] > 0 else np.nan
        row["median_turnover"] = float(base["turnover_rate"].median(skipna=True))
        row["median_total_mv"] = float(base["total_mv"].median(skipna=True))
        row["max_total_mv"] = float(base["total_mv"].max(skipna=True))
        row["min_total_mv"] = float(base["total_mv"].min(skipna=True))
        illiq = np.abs(base["ret"].to_numpy(dtype=float)) / np.maximum(base["amount"].to_numpy(dtype=float), 1e-9)
        illiq = illiq[np.isfinite(illiq)]
        row["illiq_median"] = float(np.median(illiq)) if illiq.size else np.nan
        row["illiq90"] = float(np.quantile(illiq, 0.90)) if illiq.size >= 20 else np.nan
        row["liquidity_fragility"] = row["illiq90"] / row["illiq_median"] if row["illiq_median"] and np.isfinite(row["illiq_median"]) else np.nan

        above20: list[bool] = []
        above60: list[bool] = []
        for code in base_members:
            history = np.asarray(price_hist[code], dtype=float)
            if history.size >= 20:
                above20.append(bool(history[-1] > np.nanmean(history[-20:])))
            if history.size >= 60:
                above60.append(bool(history[-1] > np.nanmean(history[-60:])))
        row["b20"] = float(np.mean(above20)) if len(above20) >= 50 else np.nan
        row["b60"] = float(np.mean(above60)) if len(above60) >= 50 else np.nan
        row["b20_valid_count"] = len(above20)
        row["b60_valid_count"] = len(above60)
        avg_corr, pc1_share = correlation_and_pc1(rolling_return_matrix(ret_hist, base_members, 20))
        row["avg_corr20"] = avg_corr
        row["pc1_share20"] = pc1_share
        row["extreme_up_rate"] = float((base["pct_chg"] >= 9.8).mean()) if member_count else np.nan
        row["extreme_down_rate"] = float((base["pct_chg"] <= -9.8).mean()) if member_count else np.nan
        row["advance_ratio"] = float((base["ret"] > 0).mean()) if member_count else np.nan

        for rank, (code, values) in enumerate(base.sort_values(["total_mv", "ts_code"]).iterrows(), start=1):
            membership_rows.append({
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
            })
        latest_members = base.reset_index(drop=True).copy()
        latest_members["trade_date"] = date
        latest_members["name"] = latest_members["ts_code"].map(meta_names)
        latest_members["market"] = latest_members["ts_code"].map(meta_market)
        latest_members["list_date"] = latest_members["ts_code"].map(meta_list_date)
        latest_members = latest_members.sort_values(["total_mv", "ts_code"])
        latest_members.insert(0, "rank", np.arange(1, len(latest_members) + 1))
        daily_rows.append(row)
        if (idx + 1) % 250 == 0 or idx + 1 == len(dates):
            log(f"Processed {idx + 1:,}/{len(dates):,} sessions")

    return pd.DataFrame(daily_rows).sort_values("trade_date").reset_index(drop=True), pd.DataFrame(membership_rows), latest_members


def to_return_series(df: pd.DataFrame, pct_candidates: list[str], close_col: str = "close") -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    indexed = df.set_index("trade_date").sort_index()
    for col in pct_candidates:
        if col in indexed.columns:
            return pd.to_numeric(indexed[col], errors="coerce") / 100.0
    return pd.to_numeric(indexed[close_col], errors="coerce").pct_change() if close_col in indexed.columns else pd.Series(dtype=float)


def rsi(series: pd.Series, period: int = 6) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def rolling_robust_z(series: pd.Series, window: int = 252, min_periods: int = 126) -> pd.Series:
    median = series.rolling(window, min_periods=min_periods).median()
    mad = (series - median).abs().rolling(window, min_periods=min_periods).median()
    return ((series - median) / (1.4826 * mad.replace(0, np.nan))).clip(-3, 3)


def forward_mdd(close: pd.Series, horizon: int) -> pd.Series:
    arr = close.to_numpy(dtype=float)
    out = np.full(len(arr), np.nan)
    for i in range(len(arr) - horizon):
        base = arr[i]
        future = arr[i + 1:i + horizon + 1]
        if np.isfinite(base) and base != 0 and np.isfinite(future).any():
            out[i] = np.nanmin(future / base - 1.0)
    return pd.Series(out, index=close.index)


def build_factor_frame(daily: pd.DataFrame, ths: pd.DataFrame, benchmark: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    frame = daily.set_index("trade_date").sort_index().copy()
    base_ret = frame["ret__total_mv_bottom200_daily"].astype(float)
    base_level = frame["level__total_mv_bottom200_daily"].astype(float)
    target_name = "synthetic_total_mv_bottom200"
    target_ret = base_ret.copy()
    target_close = base_level.copy()
    if not ths.empty:
        ths_idx = ths.set_index("trade_date").sort_index()
        ths_ret = to_return_series(ths, ["pct_change", "pct_chg"])
        ths_close = pd.to_numeric(ths_idx["close"], errors="coerce") if "close" in ths_idx.columns else pd.Series(dtype=float)
        if ths_ret.reindex(frame.index).notna().sum() >= 250:
            target_name = "883418.TI"
            target_ret = ths_ret.reindex(frame.index)
            target_close = ths_close.reindex(frame.index)
            frame["ths_ret"] = target_ret
            frame["ths_close"] = target_close

    bench_ret = to_return_series(benchmark, ["pct_chg", "pct_change"]).reindex(frame.index)
    benchmark_idx = benchmark.set_index("trade_date").sort_index()
    bench_close = pd.to_numeric(benchmark_idx["close"], errors="coerce").reindex(frame.index)
    frame["target_ret"] = target_ret
    frame["target_close"] = target_close
    frame["benchmark_ret"] = bench_ret
    frame["benchmark_close"] = bench_close
    frame["ma20"] = target_close.rolling(20, min_periods=20).mean()
    frame["ma60"] = target_close.rolling(60, min_periods=60).mean()
    frame["ma120"] = target_close.rolling(120, min_periods=120).mean()
    frame["trend_price_ma60"] = target_close / frame["ma60"] - 1.0
    frame["trend_ma20_ma60"] = frame["ma20"] / frame["ma60"] - 1.0
    frame["trend_ma60_slope"] = frame["ma60"].pct_change(20)
    frame["ret5"] = target_close.pct_change(5)
    frame["ret20"] = target_close.pct_change(20)
    frame["ret60"] = target_close.pct_change(60)
    frame["rs20"] = target_close.pct_change(20) - bench_close.pct_change(20)
    frame["rs60"] = target_close.pct_change(60) - bench_close.pct_change(60)
    frame["delta_b20_5"] = frame["b20"].diff(5)
    frame["liquidity_expansion"] = frame["amount_share"].rolling(5, min_periods=5).mean() - frame["amount_share"].rolling(20, min_periods=20).mean()
    frame["rv5"] = target_ret.rolling(5, min_periods=5).std(ddof=1) * np.sqrt(252)
    frame["rv20"] = target_ret.rolling(20, min_periods=20).std(ddof=1) * np.sqrt(252)
    frame["downside_vol20"] = target_ret.clip(upper=0).pow(2).rolling(20, min_periods=20).mean().pow(0.5) * np.sqrt(252)
    frame["volshock"] = frame["rv5"] / frame["rv20"].replace(0, np.nan)
    total_energy = target_ret.pow(2).rolling(20, min_periods=20).sum()
    downside_energy = target_ret.clip(upper=0).pow(2).rolling(20, min_periods=20).sum()
    frame["downside_share"] = downside_energy / total_energy.replace(0, np.nan)
    frame["rsi6"] = rsi(target_close, 6)
    for horizon in (5, 10, 20):
        frame[f"fwd_ret_{horizon}"] = target_close.shift(-horizon) / target_close - 1.0
        frame[f"fwd_mdd_{horizon}"] = forward_mdd(target_close, horizon)
    return frame, target_name


def safe_corr(x: pd.Series, y: pd.Series, method: str = "pearson") -> float:
    valid = pd.concat([x, y], axis=1).replace([np.inf, -np.inf], np.nan).dropna()
    if len(valid) < 30 or valid.iloc[:, 0].nunique() < 3 or valid.iloc[:, 1].nunique() < 3:
        return np.nan
    return float(valid.iloc[:, 0].corr(valid.iloc[:, 1], method=method))


def newey_west_tstat(values: pd.Series, lags: int) -> float:
    x = pd.Series(values).dropna().to_numpy(dtype=float)
    n = len(x)
    if n < 30:
        return np.nan
    mean = x.mean()
    demeaned = x - mean
    variance = np.dot(demeaned, demeaned) / n
    for lag in range(1, min(lags, n - 1) + 1):
        gamma = np.dot(demeaned[lag:], demeaned[:-lag]) / n
        variance += 2.0 * (1.0 - lag / (lags + 1.0)) * gamma
    se = math.sqrt(max(variance, 0.0) / n)
    return float(mean / se) if se > 0 else np.nan


def monthly_walk_forward_predictions(frame: pd.DataFrame, factor: str, label: str, horizon: int) -> pd.DataFrame:
    data = frame[[factor, label]].replace([np.inf, -np.inf], np.nan).copy()
    result_rows: list[dict[str, Any]] = []
    valid_dates = data.index
    months = pd.Series(valid_dates.to_period("M"), index=valid_dates)
    for month in months.drop_duplicates().sort_values():
        month_dates = months[months == month].index
        first_date = month_dates.min()
        earlier = valid_dates[valid_dates < first_date]
        if len(earlier) == 0:
            continue
        train = data.loc[:earlier.max()].iloc[:-horizon].dropna().tail(756)
        if len(train) < 252:
            continue
        x = train[factor].to_numpy(dtype=float)
        y = train[label].to_numpy(dtype=float)
        x_mean = x.mean()
        x_std = x.std(ddof=1)
        if not np.isfinite(x_std) or x_std <= 1e-12:
            continue
        xz = (x - x_mean) / x_std
        beta = np.cov(xz, y, ddof=1)[0, 1] / max(np.var(xz, ddof=1), 1e-12)
        alpha = y.mean() - beta * xz.mean()
        for date in month_dates:
            value = data.at[date, factor]
            actual = data.at[date, label]
            if not np.isfinite(value) or not np.isfinite(actual):
                continue
            result_rows.append({"trade_date": date, "prediction": alpha + beta * ((value - x_mean) / x_std), "actual": actual, "beta": beta})
    return pd.DataFrame(result_rows).set_index("trade_date") if result_rows else pd.DataFrame()


def evaluate_factors(frame: pd.DataFrame) -> pd.DataFrame:
    factor_cols = [
        "trend_price_ma60", "trend_ma20_ma60", "trend_ma60_slope", "rs20", "rs60", "b20", "b60",
        "delta_b20_5", "amount_share", "liquidity_expansion", "median_turnover", "illiq_median", "illiq90",
        "liquidity_fragility", "avg_corr20", "pc1_share20", "rv20", "downside_vol20", "volshock",
        "downside_share", "ret5", "rsi6", "extreme_up_rate", "extreme_down_rate", "advance_ratio",
    ]
    rows: list[dict[str, Any]] = []
    for horizon in (5, 10, 20):
        label = f"fwd_ret_{horizon}"
        risk_label = f"fwd_mdd_{horizon}"
        for factor in factor_cols:
            pearson = safe_corr(frame[factor], frame[label], "pearson")
            spearman = safe_corr(frame[factor], frame[label], "spearman")
            mdd_corr = safe_corr(frame[factor], frame[risk_label], "spearman")
            preds = monthly_walk_forward_predictions(frame, factor, label, horizon)
            if preds.empty:
                oos_corr = oos_spearman = oos_hit = spread = spread_t = np.nan
                oos_n = 0
            else:
                oos_corr = safe_corr(preds["prediction"], preds["actual"], "pearson")
                oos_spearman = safe_corr(preds["prediction"], preds["actual"], "spearman")
                oos_hit = float((np.sign(preds["prediction"]) == np.sign(preds["actual"])).mean())
                q_low, q_high = preds["prediction"].quantile([0.30, 0.70])
                long_actual = preds.loc[preds["prediction"] >= q_high, "actual"]
                short_actual = preds.loc[preds["prediction"] <= q_low, "actual"]
                spread = float(long_actual.mean() - short_actual.mean()) if len(long_actual) and len(short_actual) else np.nan
                spread_series = pd.Series(index=preds.index, dtype=float)
                spread_series.loc[long_actual.index] = long_actual
                spread_series.loc[short_actual.index] = -short_actual
                spread_t = newey_west_tstat(spread_series, lags=horizon)
                oos_n = len(preds)
            rows.append({
                "horizon_days": horizon,
                "factor": factor,
                "full_pearson": pearson,
                "full_spearman": spearman,
                "spearman_with_forward_mdd": mdd_corr,
                "oos_pearson": oos_corr,
                "oos_spearman": oos_spearman,
                "oos_direction_hit_rate": oos_hit,
                "oos_top30_minus_bottom30": spread,
                "oos_spread_newey_west_t": spread_t,
                "oos_observations": oos_n,
            })
    return pd.DataFrame(rows)


def performance_metrics(returns: pd.Series, positions: pd.Series, turnover: pd.Series) -> dict[str, float]:
    ret = returns.fillna(0.0).astype(float)
    equity = (1.0 + ret).cumprod()
    n = len(ret)
    years = n / 252.0 if n else np.nan
    total_return = equity.iloc[-1] - 1.0 if n else np.nan
    annual_return = equity.iloc[-1] ** (1.0 / years) - 1.0 if n and years > 0 and equity.iloc[-1] > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(252) if n > 1 else np.nan
    sharpe = ret.mean() / ret.std(ddof=1) * np.sqrt(252) if n > 1 and ret.std(ddof=1) > 0 else np.nan
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min()) if n else np.nan
    calmar = annual_return / abs(max_drawdown) if np.isfinite(annual_return) and max_drawdown < 0 else np.nan
    active_ret = ret[positions > 0]
    return {
        "total_return": float(total_return),
        "annual_return": float(annual_return),
        "annual_volatility": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": max_drawdown,
        "calmar": float(calmar),
        "win_rate_when_invested": float((active_ret > 0).mean()) if len(active_ret) else np.nan,
        "average_position": float(positions.mean()),
        "annual_turnover": float(turnover.sum() / max(years, 1e-9)) if n else np.nan,
        "position_changes": int((turnover > 1e-9).sum()),
        "observations": int(n),
    }


def build_ablation(frame: pd.DataFrame, cost_bps: float = BASE_COST_BPS) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    z = pd.DataFrame(index=frame.index)
    for col in [
        "trend_price_ma60", "trend_ma20_ma60", "trend_ma60_slope", "rs20", "rs60", "b20", "b60",
        "delta_b20_5", "amount_share", "liquidity_expansion", "illiq90", "liquidity_fragility",
        "avg_corr20", "pc1_share20", "volshock", "downside_vol20", "extreme_down_rate", "ret5",
    ]:
        z[col] = rolling_robust_z(frame[col])
    components = pd.DataFrame(index=frame.index)
    components["trend"] = z[["trend_price_ma60", "trend_ma20_ma60", "trend_ma60_slope"]].mean(axis=1)
    components["relative_strength"] = z[["rs20", "rs60"]].mean(axis=1)
    components["breadth"] = z[["b20", "b60", "delta_b20_5"]].mean(axis=1)
    components["liquidity"] = pd.concat([z["amount_share"], z["liquidity_expansion"], -z["illiq90"], -z["liquidity_fragility"]], axis=1).mean(axis=1)
    components["risk_penalty"] = pd.concat([z["avg_corr20"], z["pc1_share20"], z["volshock"], z["downside_vol20"], z["extreme_down_rate"]], axis=1).mean(axis=1)
    components["conditional_reversal"] = (-z["ret5"]).clip(lower=0).where((components["trend"] > 0) & frame["rsi6"].between(20, 50), 0.0)
    scores = pd.DataFrame(index=frame.index)
    scores["M0_trend"] = components["trend"]
    scores["M1_trend_rs"] = components[["trend", "relative_strength"]].mean(axis=1)
    scores["M2_add_breadth"] = components[["trend", "relative_strength", "breadth"]].mean(axis=1)
    scores["M3_add_liquidity"] = components[["trend", "relative_strength", "breadth", "liquidity"]].mean(axis=1)
    scores["M4_add_risk"] = pd.concat([components[["trend", "relative_strength", "breadth", "liquidity"]], -components["risk_penalty"].rename("risk")], axis=1).mean(axis=1)
    scores["M5_add_reversal"] = scores["M4_add_risk"] + 0.35 * components["conditional_reversal"]
    positions = pd.DataFrame(index=frame.index)
    gate = (frame["trend_price_ma60"] > 0) & (frame["trend_ma60_slope"] > -0.02)
    for model in scores.columns:
        score = scores[model]
        q40 = score.rolling(252, min_periods=126).quantile(0.40).shift(1)
        q60 = score.rolling(252, min_periods=126).quantile(0.60).shift(1)
        pos = pd.Series(0.0, index=frame.index)
        pos[(score > q40) & gate] = 0.5
        pos[(score > q60) & gate] = 1.0
        if model == "M0_trend":
            pos = ((frame["trend_price_ma60"] > 0) & (frame["trend_ma60_slope"] > 0)).astype(float)
        positions[model] = pos

    asset_ret = frame["target_ret"].fillna(0.0)
    strategy_returns = pd.DataFrame(index=frame.index)
    summary_rows: list[dict[str, Any]] = []
    valid_start = frame[["target_ret", "ma120"]].dropna().index.min()
    sample_dates = frame.index[frame.index >= valid_start]
    oos_start = sample_dates[int(len(sample_dates) * 0.60)] if len(sample_dates) > 10 else sample_dates.min()
    for model in positions.columns:
        executed = positions[model].shift(1).fillna(0.0)
        turnover = executed.diff().abs().fillna(executed.abs())
        net_ret = executed * asset_ret - turnover * cost_bps / 10000.0
        strategy_returns[model] = net_ret
        for period, mask in {"full": frame.index >= valid_start, "oos_last40pct": frame.index >= oos_start}.items():
            summary_rows.append({"model": model, "period": period, "cost_bps": cost_bps, **performance_metrics(net_ret[mask], executed[mask], turnover[mask])})
    bh_pos = pd.Series(1.0, index=frame.index)
    bh_turnover = pd.Series(0.0, index=frame.index)
    bh_turnover.iloc[0] = 1.0
    bh_ret = asset_ret - bh_turnover * cost_bps / 10000.0
    strategy_returns["BuyHold"] = bh_ret
    for period, mask in {"full": frame.index >= valid_start, "oos_last40pct": frame.index >= oos_start}.items():
        summary_rows.append({"model": "BuyHold", "period": period, "cost_bps": cost_bps, **performance_metrics(bh_ret[mask], bh_pos[mask], bh_turnover[mask])})
    summary = pd.DataFrame(summary_rows)
    equity = (1.0 + strategy_returns.fillna(0.0)).cumprod()
    equity.index.name = "trade_date"
    details = pd.concat({"component": components, "score": scores, "position_signal": positions, "net_return": strategy_returns}, axis=1)
    return summary, equity, details


def cost_stress_test(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for cost in (5.0, 10.0, 20.0, 30.0):
        summary, _, _ = build_ablation(frame, cost_bps=cost)
        rows.append(summary[summary["period"] == "oos_last40pct"])
    return pd.concat(rows, ignore_index=True)


def replication_metrics(daily: pd.DataFrame, ths: pd.DataFrame) -> pd.DataFrame:
    if ths.empty:
        return pd.DataFrame([{"variant": "883418.TI unavailable", "observations": 0, "correlation": np.nan, "tracking_error_ann": np.nan, "r2": np.nan}])
    ths_ret = to_return_series(ths, ["pct_change", "pct_chg"]).rename("actual")
    daily_idx = daily.set_index("trade_date")
    rows: list[dict[str, Any]] = []
    for column in [c for c in daily.columns if c.startswith("ret__")]:
        variant = column.removeprefix("ret__")
        aligned = pd.concat([ths_ret, daily_idx[column].rename("synthetic")], axis=1).dropna()
        if len(aligned) < 30:
            continue
        diff = aligned["synthetic"] - aligned["actual"]
        beta, intercept, r_value, _, _ = stats.linregress(aligned["actual"], aligned["synthetic"])
        rows.append({
            "variant": variant,
            "observations": len(aligned),
            "correlation": aligned["synthetic"].corr(aligned["actual"]),
            "tracking_error_ann": diff.std(ddof=1) * np.sqrt(252),
            "mean_excess_ann": diff.mean() * 252,
            "rmse_daily": float(np.sqrt(np.mean(np.square(diff)))),
            "beta_vs_883418": beta,
            "alpha_daily": intercept,
            "r2": r_value**2,
        })
    return pd.DataFrame(rows).sort_values(["correlation", "tracking_error_ann"], ascending=[False, True])


def make_summary(target_name: str, factor_eval: pd.DataFrame, replication: pd.DataFrame, ablation: pd.DataFrame, latest_signal: pd.DataFrame, quality: pd.DataFrame) -> str:
    lines = [
        "# 883418微盘股第二版因子研究", "", f"- 数据区间：{START_DATE}—{END_DATE}", f"- 目标序列：{target_name}",
        f"- 代理地址：{PROXY_URL}", "- 基础复刻：每日收盘按总市值从小到大选取200只沪深A股，等权持有至下一交易日。",
        "- Token通过临时环境变量注入，未写入代码、结果或日志。", "", "## 数据质量", "",
        f"- 计划交易日：{len(quality):,}", f"- 合并后有效行情日：{int((quality['merged_rows'] >= MIN_STOCKS).sum()):,}",
        f"- 合并后股票数中位数：{quality['merged_rows'].median():,.0f}", "",
    ]
    if not replication.empty:
        lines += ["## 复刻结果前五名", "", replication.head(5).to_markdown(index=False), ""]
    if not factor_eval.empty:
        ranked = factor_eval.sort_values("oos_spearman", ascending=False).groupby("horizon_days", as_index=False).head(5)
        lines += ["## 各预测期样本外因子前五名", "", ranked.to_markdown(index=False), ""]
    if not ablation.empty:
        oos = ablation[ablation["period"] == "oos_last40pct"].sort_values("sharpe", ascending=False)
        lines += ["## 消融回测（样本外后40%）", "", oos.to_markdown(index=False), ""]
    if not latest_signal.empty:
        lines += ["## 最新信号", "", latest_signal.to_markdown(index=False), ""]
    lines += [
        "## 研究限制", "",
        "1. 复刻采用每日总市值最小200只这一明确规则，并测试流通市值、规模数量、上市满60日和月度调仓等变体；不等于同花顺未公开的精确编制细则。",
        "2. 个股日收益使用Tushare daily接口的pct_chg连续复合构造技术价格序列；生产版仍应使用adj_factor交叉核验。",
        "3. 指数回测采用下一交易日生效并计入单边交易成本，未完整模拟涨跌停无法成交、冲击成本和实际组合容量，不构成收益承诺。",
        "4. 单一时间序列的因子显著性容易受到制度与风格切换影响，应优先看样本外方向、消融增量和不同成本下的稳定性。", "",
    ]
    return "\n".join(lines)


def main() -> None:
    started = time.time()
    metadata: dict[str, Any] = {
        "sdk_version": getattr(ts, "__version__", "unknown"), "proxy_url": PROXY_URL, "start_date": START_DATE,
        "end_date": END_DATE, "base_n": BASE_N, "max_workers": MAX_WORKERS, "token_persisted": False,
    }
    try:
        dates, stock_basic, ths, benchmark = fetch_static_data()
        frames, quality = fetch_market_days(dates)
        daily, membership, latest_members = process_market(dates, frames, stock_basic)
        if daily.empty:
            raise RuntimeError("No valid daily synthetic index records were constructed")
        factor_frame, target_name = build_factor_frame(daily, ths, benchmark)
        factor_eval = evaluate_factors(factor_frame)
        replication = replication_metrics(daily, ths)
        ablation, equity, backtest_details = build_ablation(factor_frame, BASE_COST_BPS)
        cost_stress = cost_stress_test(factor_frame)
        latest_date = factor_frame.index.max()
        latest_flat = backtest_details.loc[[latest_date]].copy()
        latest_flat.columns = ["__".join(map(str, col)) for col in latest_flat.columns]
        latest_flat.insert(0, "target_name", target_name)
        latest_flat.insert(0, "trade_date", latest_date)
        latest_signal = latest_flat.reset_index(drop=True)

        quality.to_csv(OUT / "data_quality.csv", index=False, encoding="utf-8-sig")
        daily.to_csv(OUT / "synthetic_index_variants.csv", index=False, encoding="utf-8-sig")
        replication.to_csv(OUT / "replication_comparison.csv", index=False, encoding="utf-8-sig")
        factor_frame.reset_index().to_csv(OUT / "factor_daily.csv", index=False, encoding="utf-8-sig")
        factor_eval.to_csv(OUT / "factor_evaluation.csv", index=False, encoding="utf-8-sig")
        ablation.to_csv(OUT / "ablation_backtest.csv", index=False, encoding="utf-8-sig")
        cost_stress.to_csv(OUT / "cost_stress.csv", index=False, encoding="utf-8-sig")
        equity.reset_index().to_csv(OUT / "equity_curves.csv", index=False, encoding="utf-8-sig")
        backtest_details.reset_index().to_parquet(OUT / "backtest_details.parquet", index=False, compression="zstd")
        latest_signal.to_csv(OUT / "latest_signal.csv", index=False, encoding="utf-8-sig")
        latest_members.to_csv(OUT / "members_latest.csv", index=False, encoding="utf-8-sig")
        membership.to_parquet(OUT / "members_history.parquet", index=False, compression="zstd")
        stock_basic.to_csv(OUT / "stock_basic.csv", index=False, encoding="utf-8-sig")
        if not ths.empty:
            ths.to_csv(OUT / "883418_ths_daily.csv", index=False, encoding="utf-8-sig")
        benchmark.to_csv(OUT / "benchmark_000300.csv", index=False, encoding="utf-8-sig")
        summary = make_summary(target_name, factor_eval, replication, ablation, latest_signal, quality)
        (OUT / "summary.md").write_text(summary, encoding="utf-8")
        metadata.update({
            "target_name": target_name, "calendar_days": len(dates), "valid_synthetic_days": len(daily),
            "factor_rows": len(factor_frame), "membership_rows": len(membership), "latest_trade_date": latest_date,
            "elapsed_seconds": time.time() - started, "api_stats": {name: asdict(value) for name, value in API_STATS.items()},
            "output_files": sorted(path.name for path in OUT.iterdir()),
        })
        write_json(OUT / "metadata.json", metadata)
        log(f"Research complete in {(time.time() - started) / 60:.1f} minutes; target={target_name}")
        print(summary)
    except Exception as exc:
        metadata.update({
            "status": "failed", "error_type": type(exc).__name__, "error": str(exc).replace(TOKEN, "***"),
            "traceback": traceback.format_exc().replace(TOKEN, "***"), "elapsed_seconds": time.time() - started,
            "api_stats": {name: asdict(value) for name, value in API_STATS.items()},
        })
        write_json(OUT / "failure.json", metadata)
        raise


if __name__ == "__main__":
    main()
