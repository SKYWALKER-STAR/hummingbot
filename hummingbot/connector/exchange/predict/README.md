# Predict.fun 连接器

本连接器位于 Hummingbot 源码中，独立于 `predictbot` 的全局配置。
提供 MarketID 动态加载、REST/WS 盘口、EOA/Predict Account 认证、SDK 签名、
LIMIT/LIMIT_MAKER、API 撤单、余额预留、订单跟踪和链上成交对账。
当前为待实盘验收的初版；离线测试不代表已通过官方连接器认证。

## 环境

在已安装并编译的 Hummingbot 环境中安装：

```bash
python -m pip install -r hummingbot/connector/exchange/predict/requirements.txt
```

使用主网 API key；交易还需要私钥、已有的 USDT/ERC-1155 合约授权及相应资产。
连接器不会自动授权、转账、拆分、合并或赎回。私钥通过 Hummingbot 加密配置或环境变量传入，勿写入源码。
SDK 默认 BNB RPC 必须支持历史区块和过滤 `OrderFilled` 日志。

## 按 MarketID 加载（只读）

在仓库根目录、Hummingbot Python 环境运行：

```bash
python -m scripts.predict_market_probe 123 456
```

`123`、`456` 是示例，替换为实际市场 ID。脚本读取环境变量 `PREDICT_API_KEY`，
输出真实市场名、结果名、交易对、token ID 和各结果最优买卖价，不下单。

```python
connector = PredictExchange(
    predict_api_key=api_key,
    market_ids=[market_id],
    trading_required=False,
)
await connector.initialize_markets()
for outcome in connector.get_market_outcomes(market_id):
    print(outcome.name, outcome.token_id, outcome.trading_pair)

# 名称必须来自 API，而不是预设 YES/NO。
pair = connector.get_trading_pair(market_id, outcome_name=actual_name)
# 重名时使用 token_id 或 index_set 精确选择；名称匹配区分大小写。
pair = connector.get_trading_pair(market_id, token_id=actual_token_id)
```

交易对使用稳定身份 `PREDICT_{MarketID}_{indexSet}-USDT`，实际显示名称在 `outcome.name`。
例如 API 返回 Team A/Team B、Up/Down 或中文名称，均保留原样。名称改变不会造成资产身份改变；
token 身份改变则停止该市场的交易，不悄悄替换。

API 的市场盘口以 `indexSet=1` 为基准；`indexSet=2` 通过交换买卖方向并取 `1-price` 转换。
方向不依赖名称，也不依赖 outcomes 数组的顺序。精度读取 `decimalPrecision`，使用 Decimal。
当前支持每个 MarketID 对应两个结果、indexSet 为 1/2 的条件市场。
若 API 返回其他结构会明确拒绝，不能把多结果盘口错误地按二元互补换算。

## 动态增删

```python
new_outcomes = await connector.add_market(other_market_id)
await connector.refresh_market(market_id)
await connector.remove_market(other_market_id)
```

运行前和运行中均可添加。每个市场只订阅一份原始盘口，映射到两个结果。
已有未完成订单或持仓时拒绝删除。添加过程中若初始盘口请求失败会抛异常，
已加载的结果仍可见；重试同一 `add_market` 会补齐尚未完成的盘口，不重复订阅。

## 交易接入

```python
connector = PredictExchange(
    predict_api_key=api_key,
    predict_private_key=private_key,
    predict_account_address=predict_account_address,  # EOA 传空字符串
    market_ids=[market_id],
    trading_required=True,
    order_ttl=300,
)
await connector.initialize_markets()
pair = connector.get_trading_pair(market_id, outcome_name=actual_name)
```

将 connector 注册到 Hummingbot Clock/策略框架后，由框架调用网络生命周期与 tick。
等待 `connector.ready`，并在策略内使用标准接口：

```python
from decimal import Decimal
from hummingbot.core.data_type.common import OrderType

# 示例只说明 API，数量和价格需由策略决定。
order_id = connector.buy(pair, Decimal("10"), OrderType.LIMIT_MAKER, Decimal("0.5"))
connector.cancel(pair, order_id)
```

普通限价单可能吃单；`LIMIT_MAKER` 映射为 Predict `isPostOnly`。
不支持市价单。费用按市场参数保守预留，实际成交费用从链上事件读取。
CLI `connect predict` 可保存凭据及逗号分隔的 MarketID；自动化脚本直接传 `market_ids`。
不应同时运行旧 predictbot 对同一钱包进行主动交易，以免两个策略管理同一资产。

## 订单与恢复语义

- Hummingbot 的 exchange_order_id 保存签名订单 hash；API 数字订单 ID 在撤单时查询。
- 下单超时/5xx 保留 hash，暂停新订单，通过查询核查，不盲目生成新订单重试。
- API 撤单只移除盘口，不等于链上撤销签名。采用短期有效订单；需要强制链上失效时应另行执行链上撤单。
- 撮合通知不直接作为最终成交。账户 WS 触发对账，按 hash 查询链上 `OrderFilled`，
  等待两个区块后产生 TradeUpdate，使用 transactionHash/logIndex 去重。
- 轮询也查询同一成交来源，补齐 WS 丢失事件；完整成交价格和费用不按限价猜测。
- 未确认结算、RPC 失败、API/链上成交数量不一致时暂停新单，保留占用。
- 使用 Hummingbot MarketsRecorder 保存/恢复 tracking_states；其中包含订单 hash 和已处理成交。
  自定义宿主也必须持久化这些状态。加载旧订单时会自动补充相应 MarketID。
- 对于服务器始终返回 404 的不确定提交，连接器保留订单并暂停新单，须人工核查，
  不会仅凭 404 推断资金已安全释放。
- 默认主网 WS 来自官方文档。使用 `domain="predict_testnet"` 时需显式提供同网络的 `ws_url`；
  不会悄悄连接主网 WS。

## 验证

```bash
python -m unittest discover -s test/hummingbot/connector/exchange/predict -v
```

`test_predict_core.py` 可在具备 aiohttp 的普通 Python 环境运行，覆盖名称/方向/精度、
分页、JWT 并发刷新、超时不重发及链上成交解析。
`test_predict_exchange.py` 需要完整编译后的 Hummingbot 环境，覆盖连接器加载、刷新、
订单与余额生命周期。缺少依赖时显式跳过，不能把跳过视为通过。

尚需在目标运行环境完成：SDK 实际签名、账户权限、真实 WS 协议、RPC 日志范围限制、
下单/部分成交/撤单竞争、进程重启以及极小额交易的端到端验收。
# REST rate limits

The REST client uses 90% of the supplied app quotas: trading 900/minute
(quota 1,000), general reads/auth 450/minute (quota 500), search 108/minute
(quota 120), and trading burst 72/second (quota 80). Requests are paced
evenly rather than sent in a burst. Order reads count as general requests;
order placement/removal count as trading requests.

Clients using the same API token and domain in the same event loop share
the limiter. Separate processes, event loops, and other tokens belonging to
the same app do not share this local accounting; their combined traffic
must fit the app quota.

HTTP 429 pauses all REST buckets for the `Retry-After` duration (seconds or
HTTP date), falling back to 60 seconds when absent/invalid. The failed
request is still reported to the caller; trading POSTs are not automatically
replayed. This limiter does not cover blockchain RPC or WebSocket limits.
