# Predict 做市策略：预览与实盘

按 MarketID 加载真实结果名称、token 和盘口。通过独立 Python 模块启动 Hummingbot Clock 与连接器，不是 StrategyV2 Controller。

## 自动选择方向

`mid = (best_bid + best_ask) / 2`，选择 `mid > threshold` 的结果，默认阈值 0.5。
严格大于，等于阈值不选；多个结果满足时选择最高中间价，同价优先保留当前结果。
空盘口、交叉盘口、过期行情、断线、关闭市场及不满足报价范围/深度限制的盘口不参与选择。
名称来自市场接口，不要求叫 YES/NO 或 Up/Down。

实盘选中结果后挂买单，有可用库存时同时挂卖单。切换方向先撤销本策略旧单，等待终态和成交核对，再重新报价。
旧结果只挂库存卖单，不再买入。没有结果超过阈值时停止买入，但健康盘口上的已有库存仍可挂卖单。
**策略管理该市场在此账户中的全部库存，包括启动前已持有的份额。** 不自动拆分、合并、赎回或市价平仓。

## 启动

在仓库根目录、已编译完成的 Hummingbot Python 环境执行。仅能运行 REST probe 的环境不一定能运行 Clock。
将下面的 `123` 换成真实 MarketID。密钥通过环境变量传入，不写入配置和订单状态文件。

PowerShell：

```powershell
$env:PREDICT_API_KEY = "你的 API Key"
$env:PREDICT_PRIVATE_KEY = "你的签名私钥"
$env:PREDICT_ACCOUNT_ADDRESS = "你的 Predict Account 地址"
```

使用普通 EOA 时移除 `PREDICT_ACCOUNT_ADDRESS`；Predict Account 用户需填写该账户地址和对应签名私钥。
账户必须有 USDT 以及相应 USDT / ERC-1155 交易授权。程序不会自动发送授权交易。
参见 [Predict 官方交易与授权文档](https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0)。

先预览（不需要私钥，不提交订单）：

```bash
python -m scripts.predict_market_maker --market-id 123 --auto-select --threshold 0.5 --config conf/scripts/predict_market_maker.live.example.json --duration 60
```

启用真实交易：

```bash
python -m scripts.predict_market_maker --market-id 123 --auto-select --threshold 0.5 --config conf/scripts/predict_market_maker.live.example.json --live
```

`--live` 使用真实余额签名并提交真实订单；未传时始终只读。实盘必须传入配置文件。
示例每单 5 份、目标库存 10 份、两个结果合计库存上限 20 份。参数用于功能测试，不是收益保证。
`max_inventory` 是份额限制，不是累计花费或亏损上限，反复买卖会持续产生交易及费用。

固定结果时，用 `--outcome-name "实际名称"` 或 `--token-id "实际token"` 替代 `--auto-select`，三者互斥。
`--threshold` 仅对自动选择生效。`--interval` 默认 2 秒，`--duration` 默认持续运行。

## 报价与资金

```text
inventory_ratio = clamp((inventory - target_inventory) / max_inventory, -1, 1)
center = mid - inventory_skew * inventory_ratio
bid = 向下取 tick(center - half_spread)
ask = 向上取 tick(center + half_spread)
```

`half_spread=0.01` 表示绝对价格差 0.01。实盘使用 `LIMIT_MAKER` / post-only。
买单按可用 USDT、费用及两个结果合计剩余份额额度裁剪，卖单按可用库存裁剪。
低于最小数量/金额直接跳过，不扩大订单来凑最低金额。

| 参数 | 含义 |
|---|---|
| `order_amount` | 每侧目标份额 |
| `target_inventory` | 用于报价偏移的单结果目标库存 |
| `max_inventory` | 实盘两个结果合计份额上限；预览为所选结果上限 |
| `inventory_skew` | 库存偏移强度 |
| `refresh_seconds` | 报价最长保持时间，默认 30 秒 |
| `refresh_price_ticks` | 触发撤旧换新的价格变化 tick 数 |
| `min_order_amount` / `min_order_notional` | 最小份额 / 最小 USDT 金额 |
| `amount_step` | 数量取整步长 |
| `min_book_spread` / `min_level_amount` | 最小盘口价差 / 买卖一深度 |
| `min_mid_price` / `max_mid_price` | 允许报价的中间价范围，默认 0.02 至 0.98 |
| `max_book_age_seconds` | 行情有效期，默认 30 秒 |
| `preview_usdt` / `preview_shares` | 仅预览使用的静态模拟资金，实盘忽略 |

预览不模拟成交、盈亏或切换后的库存流转，只展示当前所选结果的虚拟报价。
实盘输出 `mode=LIVE`、实际结果名称、活动订单 ID、链上余额及目标报价。
`SUBMITTED` 表示有跟踪中的订单，不代表成交。
成交采用 REST 订单接口的累计 `amountFilled`，按增量去重；不再调用 `eth_getLogs` 扫描成交。
日志标记 `fill_source=REST_CUMULATIVE`；成交价按委托价、手续费按市场费率估算，不能用作精确收益记录。
余额仍通过链上 `balanceOf` 查询。REST 成交状态不等于链上结算证明，库存以实际余额为准。
实盘日志的 `connector_status` 和 `market_data_stream_connected` 分别显示连接器就绪条件与行情连接状态。
订单事件或后台轮询会暂时使账户未就绪；策略先等待账户对账，再判断是否撤单。
`WAIT_CANCEL_CONFIRMATION` 表示撤换流程中仍有被跟踪的订单，不代表撤单已经成功。

## 撤单、恢复与停止

使用专用账户，避免其他程序/手工订单改变同一市场敞口。同账户同市场只运行一个实例。
默认状态文件：`data/predict-maker-<账户地址>-<MarketID>.json`，也可用 `--state-file` 指定。
文件有进程锁，签名订单哈希在 HTTP 下单前原子保存；重启恢复并先处理旧单，不重复提交结果未知的订单。
**重启必须复用同一状态文件，不能删文件或换路径绕过未解决订单。** 不同路径之间没有分布式互斥。
这是在途订单恢复记录，不是完整成交历史数据库。

Ctrl+C 或运行时间到期会停止新报价，尝试撤单、核对并保存状态，最多等待 60 秒。
退出超时会报错并保留恢复记录，不会宣称撤单成功。网络故障或交易结果未知时停止新增报价。
恢复订单若一直无法查询，程序保守等待/报错，需要人工核查哈希，不会假设其未成交并重新下单。

`--order-ttl` 默认 60 秒，控制签名订单过期时间。
REST 撤单只移除订单簿中的订单，不使签名在链上失效；已经匹配的结算可能延迟完成，短过期时间不能替代成交核对。
当前未实现链上取消交易。参见 [官方撤单说明](https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0)。

## 验证范围

```bash
python -m unittest discover -s test/hummingbot/strategy/predict_market_making -v
python -m unittest discover -s test/hummingbot/connector/exchange/predict -v
```

离线测试覆盖阈值边界、库存限制、方向切换等待撤单、断线撤单、退出超时及文件锁/账户身份校验。
开发环境缺少完整 Hummingbot 依赖，尚未完成真实 Clock、私有账户、签名下单、成交和撤单端到端验证。
本次实现没有替用户启动实盘，也没有提交真实订单。
