# 883418微盘股第二版因子研究（最近3年）

## 固定研究口径

- 数据区间：2023-08-19至2026-08-19；实际使用区间以交易日历和可取得的最新完整交易日为准。
- 真实指数：优先调用 `pro.ths_daily(ts_code="883418.TI")`。
- 复刻规则：每个交易日收盘后，在沪深A股中按 `daily_basic.total_mv` 从小到大排序，选取最小200只。
- 权重：等权。
- 收益归属：T日收盘确定的新成分，自T+1交易日起生效；T日组合收益使用T-1日已知成分计算，防止未来函数。
- 不再运行100只、300只、流通市值、月度调仓等规模或规则对照。
- 预测周期：未来5日、10日、20日收益及同期最大回撤。
- 回测：信号滞后一日执行，并进行交易成本压力测试。

## Tushare代理配置

脚本按以下方式初始化：

```python
import tushare as ts

ts.set_token(os.environ["TUSHARE_TOKEN"])
pro = ts.pro_api()
pro._DataApi__http_url = "https://fast.xiaodefa.cn"
```

密钥只通过环境变量传入，不应写入代码、日志或公开仓库。

## 运行

```bash
cd research/883418_v2
python -m pip install -r requirements.txt
export TUSHARE_TOKEN="你的临时Token"
python run_3y.py
```

Windows PowerShell：

```powershell
cd research/883418_v2
python -m pip install -r requirements.txt
$env:TUSHARE_TOKEN="你的临时Token"
python run_3y.py
```

## 主要输出

- `883418_ths_daily.csv`：883418真实指数行情（接口可用时）
- `synthetic_index_variants.csv`：200只复刻指数日序列
- `members_history.parquet`：每日200只历史成分
- `members_latest.csv`：最新一期200只成分
- `factor_daily.csv`：每日因子值
- `factor_evaluation.csv`：5/10/20日因子样本外评价
- `ablation_backtest.csv`：M0—M5消融回测
- `cost_stress.csv`：不同交易成本压力测试
- `equity_curves.csv`：净值曲线
- `summary.md`：自动研究摘要

本项目用于分析学习，不构成投资建议或收益承诺。
