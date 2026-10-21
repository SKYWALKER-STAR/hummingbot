# Predict 做市报价预览

这是第一阶段的只读做市策略宿主：运行真实 Predict 连接器、Hummingbot Clock 和实时订单簿，
计算报价并展示刷新决策。程序不请求私钥、不调用 buy/sell/cancel，不提交任何订单。
它不是成交回测，也不是 Hummingbot 内置 CLI 的 StrategyV2 Controller；使用下面的 Python 模块入口启动。

## 启动

使用之前能够运行 `scripts.predict_market_probe` 的 Hummingbot Python 环境，在仓库根目录运行。
先通过探测脚本查明实际 MarketID 和结果名称。市场不要求命名为 YES/NO。

PowerShell：

```powershell
$env:PREDICT_API_KEY = "你的 API Key"
python -m scripts.predict_market_maker --market-id 123 --outcome-name "实际结果名称" --duration 60
```

WSL / Linux：

```bash
export PREDICT_API_KEY="你的 API Key"
python -m scripts.predict_market_maker --market-id 123 --outcome-name "实际结果名称" --duration 60
```

将示例 `123` 和名称替换成真实值。名称区分大小写；名称重复时改用 `--token-id`，不能同时传两个选择器。
不传 `--duration` 会一直运行，Ctrl+C 结束。默认每 2 秒输出一次，可用 `--interval` 修改。
首次等待连接器和 WS 就绪超过 90 秒会报错，可用 `--startup-timeout` 调整。

使用配置文件预览双边报价：

```bash
python -m scripts.predict_market_maker --market-id 123 --outcome-name "实际结果名称" --config conf/scripts/predict_market_maker.example.json --duration 60
```

示例文件配置了 **100 模拟 USDT 和 50 模拟份额**，这些不是钱包查询结果。
不指定文件时默认 **100 模拟 USDT、0 份额**，因此不会生成卖单。
所有模拟余额在运行期间保持不变；程序不模拟成交、收益、订单排队或真实资金占用。
示例参数用于验证逻辑，不是经过收益验证的交易参数。

## 报价逻辑

```text
mid = (best_bid + best_ask) / 2
inventory_ratio = clamp((inventory - target_inventory) / max_inventory, -1, 1)
center = mid - inventory_skew × inventory_ratio
bid = 向下取整(center - half_spread)
ask = 向上取整(center + half_spread)
```

价格使用市场 tick；额外限制买价低于当前卖一、卖价高于当前买一。
`half_spread=0.01` 指绝对概率价格差 0.01，不是相对中间价的 1%。
买单按剩余仓位额度和含费用的资金预算裁剪，卖单不超过库存和可用份额。
数量向下取整，达不到最小份额/金额直接跳过，不扩大订单去满足最小值。

| 参数 | 含义 |
|---|---|
| `order_amount` | 每侧目标份额 |
| `target_inventory` / `max_inventory` | 目标库存 / 最大库存份额 |
| `inventory_skew` | 库存比率为 1 时的绝对价格偏移 |
| `refresh_seconds` | 虚拟报价最长保持时间 |
| `refresh_price_ticks` | 触发替换的价格变化 tick 数 |
| `min_order_amount` / `min_order_notional` | 策略侧最小份额 / 最小 USDT 金额 |
| `amount_step` | 策略侧数量步长 |
| `min_book_spread` / `min_level_amount` | 最小盘口价差 / 买卖一最小深度 |
| `min_mid_price` / `max_mid_price` | 允许报价的中间价范围 |
| `max_book_age_seconds` | 最近收到有效盘口后的最大等待秒数 |
| `preview_usdt` / `preview_shares` | 明确指定的模拟资金 / 库存 |

金额、份额和价格参数可用 JSON 字符串表示十进制数，避免浮点误差。
只支持列出的参数及示例文件中的字段；未知字段会报错，防止参数拼写错误被忽略。

## 输出与刷新

stdout 每行一个 JSON 对象，连接日志写入 stderr。包含真实结果名、token、买卖一、
报价中心、目标报价、保持中的虚拟报价、决策和跳过原因。

- `QUOTE`：首次建立虚拟报价。
- `HOLD`：保持原虚拟报价；目标报价可能有小于阈值的变化。
- `REPLACE`：预览撤旧换新决策（不执行真实撤单或下单）。
- `WITHDRAW`：清空虚拟报价，原因可能是市场关闭、行情过期、断线或预算不满足。
- `SKIP`：当前没有可用报价。

即使价格变化未达到阈值，旧报价变为穿过盘口或数量额度减少也会触发替换。
只读阶段无需等待真实撤单确认；接入实盘时必须实现“撤单 → 确认 → 对账 → 重新报价”，
不能把预览中的瞬时替换直接用于真实订单。

WS 收到消息不代表行情一定新鲜：策略还检查有效盘口接收时间。安静市场长时间没有新盘口时也可能保守撤出。
市场改名仍绑定原 token，不自动切换结果。当前仅支持连接器能够识别的二元条件市场。

## 测试

```bash
python -m unittest discover -s test/hummingbot/strategy/predict_market_making -v
```

单元测试使用独立 Decimal 引擎和模拟连接器，可在普通 Python 环境执行；真实 Clock/WS 运行仍需要完整 Hummingbot 环境。
后续实盘接入需增加真实余额、订单状态管理、持久化恢复和停止时撤单，当前入口没有开启实盘的参数。
