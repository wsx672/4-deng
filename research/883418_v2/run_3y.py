from __future__ import annotations

import os

# 最近三年：2023-08-19 至 2026-08-19。
# 2023-08-19如非交易日，trade_cal会自动从之后第一个交易日开始返回。
os.environ.setdefault("START_DATE", "20230819")
os.environ.setdefault("END_DATE", "20260819")
os.environ.setdefault("BASE_N", "200")
os.environ.setdefault("TUSHARE_PROXY_URL", "https://fast.xiaodefa.cn")

from run_research import main


if __name__ == "__main__":
    main()
