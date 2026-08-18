# Tushare Pro 代理连接约定

本项目以后调用 Tushare Pro 时，默认采用以下连接方式；访问令牌只从环境变量读取，不写入源码、日志、报告或公开仓库。

```python
import os
import tushare as ts

TOKEN = os.environ["TUSHARE_TOKEN"]
ts.set_token(TOKEN)
pro = ts.pro_api()
pro._DataApi__http_url = "https://fast.xiaodefa.cn"
```

## `pro_bar` 等模块级接口

模块级接口需要显式传入已经设置代理地址的 `pro`：

```python
df = ts.pro_bar(
    ts_code="002594.SZ",
    api=pro,
    start_date="20180101",
    end_date="20181011",
    adj="qfq",
)
```

## 实时行情接口

调用 `realtime_quote`、`realtime_tick`、`realtime_list` 前，额外设置 SDK 事件验证地址：

```python
from tushare.stock import cons as ct

ct.verify_token_url = "https://fast.xiaodefa.cn/dataapi/sdk-event"
df = ts.realtime_quote(ts_code="600000.SH,000001.SZ,000001.SH")
```

## 安全约定

- Token 只通过 `TUSHARE_TOKEN` 环境变量或密钥管理服务注入。
- 不在 Python 文件、Notebook、Git 提交、日志和报告中保存明文 Token。
- 研究代码输出元数据时，仅记录 SDK 版本、代理地址和接口调用统计。
- 公开聊天或仓库中出现过的 Token 应在任务完成后轮换。
