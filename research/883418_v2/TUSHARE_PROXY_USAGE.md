# Tushare Pro 高权限代理调用约定

本项目后续默认通过 Tushare SDK 和以下代理地址访问数据：

```text
https://fast.xiaodefa.cn
```

## 1. 常规 Pro 接口

密钥必须从环境变量读取，不写入代码或仓库。

```python
import os
import tushare as ts

TOKEN = os.environ["TUSHARE_TOKEN"]
ts.set_token(TOKEN)
pro = ts.pro_api()
pro._DataApi__http_url = "https://fast.xiaodefa.cn"

df = pro.daily(
    ts_code="000001.SZ",
    start_date="20260101",
    end_date="20260110",
)
```

## 2. `pro_bar` 等模块级接口

必须显式传入 `api=pro`，确保沿用已经设置代理的客户端。

```python
import os
import tushare as ts

TOKEN = os.environ["TUSHARE_TOKEN"]
ts.set_token(TOKEN)
pro = ts.pro_api()
pro._DataApi__http_url = "https://fast.xiaodefa.cn"

df = ts.pro_bar(
    ts_code="002594.SZ",
    api=pro,
    start_date="20180101",
    end_date="20181011",
    adj="qfq",
)
```

## 3. 实时接口

`realtime_quote`、`realtime_tick`、`realtime_list` 还必须设置事件校验地址。

```python
import os
import tushare as ts
from tushare.stock import cons as ct

TOKEN = os.environ["TUSHARE_TOKEN"]
ts.set_token(TOKEN)
pro = ts.pro_api()
pro._DataApi__http_url = "https://fast.xiaodefa.cn"
ct.verify_token_url = "https://fast.xiaodefa.cn/dataapi/sdk-event"

df = ts.realtime_quote(ts_code="600000.SH,000001.SZ,000001.SH")
```

## 4. 安全规则

- 密钥仅通过 `TUSHARE_TOKEN` 环境变量或 CI Secret 注入。
- 禁止提交 `.env`、Token、请求头或带鉴权参数的日志。
- 日志中只允许记录代理地址、接口名称、耗时、行数和错误类型。
- 用户曾在聊天中直接发送过密钥，研究结束后建议在服务端轮换。

## 5. 883418研究默认数据口径

- 真实指数：优先使用 `pro.ths_daily(ts_code="883418.TI")`。
- 当前成分：使用 `pro.ths_member(ts_code="883418.TI")`，但不得把当前成员倒填为历史成员。
- 复刻指数：T日收盘按总市值或流通市值排序，选取最小N只，T+1生效。
- 用户指定基准：每日总市值最小200只，等权，默认排除北交所、上市满60日、保留ST作为敏感性基准。
- 全市场字段：优先使用15,000积分可调用的专业批量接口；字段不足时回退到 `daily + daily_basic`。
