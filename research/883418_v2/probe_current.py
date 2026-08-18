from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import tushare as ts

TOKEN = os.environ["TUSHARE_TOKEN"].strip()
PROXY_URL = os.getenv("TUSHARE_PROXY_URL", "https://fast.xiaodefa.cn")
OUT = Path(os.getenv("OUTPUT_DIR", "probe_output"))
OUT.mkdir(parents=True, exist_ok=True)

ts.set_token(TOKEN)
pro = ts.pro_api()
pro._DataApi__http_url = PROXY_URL


def clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def frame_preview(frame: pd.DataFrame) -> dict[str, Any]:
    preview: dict[str, Any] = {
        "rows": int(len(frame)),
        "columns": list(frame.columns),
    }
    if not frame.empty:
        preview["first"] = {k: clean_value(v) for k, v in frame.iloc[0].to_dict().items()}
        preview["last"] = {k: clean_value(v) for k, v in frame.iloc[-1].to_dict().items()}
    return preview


def probe(name: str, func: Callable[[], pd.DataFrame]) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        frame = func()
        elapsed = time.perf_counter() - started
        return {"ok": True, "elapsed_seconds": round(elapsed, 3), **frame_preview(frame)}
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return {
            "ok": False,
            "elapsed_seconds": round(elapsed, 3),
            "error_type": type(exc).__name__,
            "error": str(exc).replace(TOKEN, "***")[:1000],
        }


result: dict[str, Any] = {
    "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    "sdk_version": getattr(ts, "__version__", "unknown"),
    "proxy_url": PROXY_URL,
    "token_persisted": False,
    "endpoints": {},
}

calendar = probe(
    "trade_cal",
    lambda: pro.trade_cal(
        exchange="SSE",
        start_date="20260801",
        end_date="20260818",
        is_open="1",
        fields="exchange,cal_date,is_open,pretrade_date",
    ),
)
result["endpoints"]["trade_cal"] = calendar
latest_trade_date = "20260818"
if calendar.get("ok") and calendar.get("rows", 0):
    candidates = []
    for row_key in ("first", "last"):
        row = calendar.get(row_key) or {}
        value = row.get("cal_date")
        if value:
            candidates.append(str(value))
    if candidates:
        latest_trade_date = max(candidates)
result["latest_trade_date"] = latest_trade_date

result["endpoints"]["ths_daily_883418"] = probe(
    "ths_daily_883418",
    lambda: pro.ths_daily(
        ts_code="883418.TI",
        start_date="20200101",
        end_date="20260818",
    ),
)
result["endpoints"]["ths_member_883418"] = probe(
    "ths_member_883418",
    lambda: pro.ths_member(ts_code="883418.TI"),
)
result["endpoints"]["daily_latest"] = probe(
    "daily_latest",
    lambda: pro.daily(
        trade_date=latest_trade_date,
        fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
    ),
)
result["endpoints"]["daily_basic_latest"] = probe(
    "daily_basic_latest",
    lambda: pro.daily_basic(
        trade_date=latest_trade_date,
        fields="ts_code,trade_date,close,turnover_rate,total_mv,circ_mv",
    ),
)
result["endpoints"]["stock_basic_listed"] = probe(
    "stock_basic_listed",
    lambda: pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,market,exchange,list_status,list_date",
    ),
)

(OUT / "probe.json").write_text(
    json.dumps(result, ensure_ascii=False, indent=2, default=str),
    encoding="utf-8",
)
print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
