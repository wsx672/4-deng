from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import requests

CODE = "883418"
OUT = Path("research_output")
OUT.mkdir(parents=True, exist_ok=True)


def get_v_cookie(session: requests.Session) -> str | None:
    urls = [
        "https://raw.githubusercontent.com/Zhu-Aemon/slingshot/b85f26f4f6d3770b996802067add33317f7501bc/api/jqka/js/jqka_v.js",
        "https://raw.githubusercontent.com/Zhu-Aemon/slingshot/main/api/jqka/js/jqka_v.js",
    ]
    js_path = Path("/tmp/jqka_v.js")
    for url in urls:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            js_path.write_text(response.text, encoding="utf-8")
            command = [
                "node",
                "-e",
                "const fs=require('fs'); eval(fs.readFileSync(process.argv[1],'utf8')); console.log(get_v());",
                str(js_path),
            ]
            result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=30)
            value = result.stdout.strip().splitlines()[-1].strip()
            if value:
                return value
        except Exception as exc:
            print(f"cookie generation failed for {url}: {exc}", file=sys.stderr)
    return None


def extract_json(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError(f"No JSON object found; prefix={text[:200]!r}")
    return json.loads(text[start : end + 1])


def reconstruct(data: dict[str, Any]) -> list[dict[str, Any]]:
    price_factor = float(data.get("priceFactor") or 1)
    raw_price = str(data["price"]).split(",")
    groups = [raw_price[i : i + 4] for i in range(0, len(raw_price), 4)]
    groups = [g for g in groups if len(g) == 4]

    def to_int(value: str) -> int:
        return 0 if value == "" else int(value)

    prices: list[tuple[float, float, float, float]] = []
    for group in groups:
        low = to_int(group[0])
        open_ = low + to_int(group[1])
        high = low + to_int(group[2])
        close = low + to_int(group[3])
        prices.append((open_ / price_factor, high / price_factor, low / price_factor, close / price_factor))

    volumes = [to_int(v) for v in str(data.get("volumn", "")).split(",")]
    dates_short = str(data["dates"]).split(",")
    years: list[str] = []
    for item in data.get("sortYear", []):
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            years.extend([str(item[0])] * int(item[1]))
        elif isinstance(item, dict):
            year = item.get("year") or item.get("0")
            count = item.get("num") or item.get("count") or item.get("1")
            if year is not None and count is not None:
                years.extend([str(year)] * int(count))
    if not years:
        raise ValueError("sortYear was empty or unrecognized")

    n = min(len(prices), len(volumes), len(dates_short), len(years))
    rows: list[dict[str, Any]] = []
    for i in range(n):
        open_, high, low, close = prices[i]
        rows.append(
            {
                "date": f"{years[i]}{dates_short[i]}",
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volumes[i],
            }
        )
    return rows


def main() -> None:
    session = requests.Session()
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    ]
    v_cookie = get_v_cookie(session)
    attempts: list[dict[str, Any]] = []
    successful_rows: list[dict[str, Any]] | None = None
    successful_adjust: str | None = None

    for adjust in ("00", "01", "02"):
        for ua in user_agents:
            for use_cookie in (True, False):
                url = f"https://d.10jqka.com.cn/v6/line/hs_{CODE}/{adjust}/all.js"
                headers = {
                    "Accept": "*/*",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Referer": f"https://q.10jqka.com.cn/gn/detail/code/{CODE}/",
                    "User-Agent": ua,
                }
                cookies = {"historystock": CODE, "spversion": "20130314"}
                if use_cookie and v_cookie:
                    cookies["v"] = v_cookie
                started = time.time()
                try:
                    response = session.get(url, headers=headers, cookies=cookies, timeout=45)
                    prefix = response.text[:160]
                    record: dict[str, Any] = {
                        "adjust": adjust,
                        "use_cookie": use_cookie,
                        "status": response.status_code,
                        "length": len(response.content),
                        "elapsed": round(time.time() - started, 3),
                        "prefix": re.sub(r"\s+", " ", prefix),
                    }
                    response.raise_for_status()
                    data = extract_json(response.text)
                    rows = reconstruct(data)
                    record.update(
                        {
                            "parsed_rows": len(rows),
                            "first_date": rows[0]["date"] if rows else None,
                            "last_date": rows[-1]["date"] if rows else None,
                            "first_close": rows[0]["close"] if rows else None,
                            "last_close": rows[-1]["close"] if rows else None,
                            "keys": sorted(data.keys()),
                        }
                    )
                    attempts.append(record)
                    if len(rows) >= 100:
                        successful_rows = rows
                        successful_adjust = adjust
                        break
                except Exception as exc:
                    attempts.append(
                        {
                            "adjust": adjust,
                            "use_cookie": use_cookie,
                            "error": type(exc).__name__,
                            "message": str(exc)[:500],
                            "elapsed": round(time.time() - started, 3),
                        }
                    )
            if successful_rows is not None:
                break
        if successful_rows is not None:
            break

    result = {
        "code": CODE,
        "cookie_generated": bool(v_cookie),
        "success": successful_rows is not None,
        "successful_adjust": successful_adjust,
        "row_count": len(successful_rows or []),
        "first_row": (successful_rows or [None])[0],
        "last_row": (successful_rows or [None])[-1],
        "attempts": attempts,
    }
    (OUT / "ths_probe.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    if successful_rows:
        with (OUT / "883418_history.json").open("w", encoding="utf-8") as handle:
            json.dump(successful_rows, handle, ensure_ascii=False)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not successful_rows:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
