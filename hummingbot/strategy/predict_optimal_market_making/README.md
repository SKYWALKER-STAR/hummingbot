# Predict 最优做市：论文模型、同步采集、后台校准和实盘

这是独立新增的策略包，通过 `python -m scripts.predict_optimal_market_maker` 启动
Hummingbot Clock 和现有 Predict 连接器，不是 StrategyV2 Controller，也不通过交互式 `start` 启动。
没有修改 Hummingbot 核心代码、原 Predict 连接器或原做市策略。

运行模式：

* `solve` / `simulate`：离线论文模型复现，不连接账户。
* `run`：行情采集、只读报价、后台校准同时运行；不传 `--live` 不签名、不下单。
* `run --live`：行情与订单事件采集、后台校准、真实 post-only 挂单同时运行。
* `calibrate`：从落盘历史重新拟合并验证参数，生成候选模型。
* `export`：把原始事件归档转换为便于 DuckDB 查询的 Parquet。

开发验证没有提交真实订单。离线通过不能代替服务器上的完整连接器、私有账户和交易验证。

## 1. 模型与论文的对应关系

来源：Dominik Feil、Max Nendel，*Optimal Market Making in Prediction Markets*，
[arXiv:2607.17991v1](https://arxiv.org/abs/2607.17991v1)，尤其第 2 节及第 4.1～4.4 节。

状态为归一化时间 `t`、市场概率 `p`、库存 `q`。价格以盘口中间价作为概率代理，
没有额外的预测信号，也不把该概率当作真实获胜概率的保证。

```text
logit(p) = log(p / (1-p))
latent_sigma(t,p) = sigma0 + sigma1*t^eta + sigma2/(1+logit(p)^2)
price_sigma(t,p) = p*(1-p)*latent_sigma(t,p)

activity(t,p) = (a0 + a1*expm1(xi*t)/expm1(xi))*sqrt(p*(1-p))
k(t) = k0 + (k1-k0)*expm1(kappa*t)/expm1(kappa)

lambda_buy = activity * (2*pi/(pi+p))^nu * exp(-k(t)*(p-pi))
lambda_sell = activity * (2*(1-pi)/(2-pi-p))^nu * exp(-k(t)*(pi-p))

terminal V(1,p,q) = -gamma_terminal*q^2*p*(1-p)
running penalty = gamma*q^2*price_sigma(t,p)^2

z_buy = (V(t,p,q)-V(t,p,q+trade_size))/trade_size
z_sell = (V(t,p,q)-V(t,p,q-trade_size))/trade_size

bid = argmax_pi lambda_buy(t,p,pi) * (p-pi-z_buy)
ask = argmax_pi lambda_sell(t,p,pi) * (pi-p-z_sell)
```

求解的是论文去掉现金维度后的三维 HJB，而非固定价差加线性库存偏移。
数值实现采用反向时间、隐式 Euler、二阶价格差分、截断价格域的齐次 Neumann 边界。
非线性方程用 Howard 策略迭代求解，而非论文演示的朴素固定点迭代：
较激进报价的强度很大，朴素迭代会发散；两者针对同一个隐式离散方程。
每步检查有限性与收敛，不收敛就失败，不把无效表交给实盘。
实时对价值函数做三线性插值，然后在合法 maker ticks 上分别优化两侧报价。

### 论文复现与实盘适配的区别

| 项目 | 离线论文模式 | 实盘模式 |
|---|---|---|
| 库存 | 默认 `[-100,100]`、每笔 10 份 | 固定 outcome，`[0,q_max]`，禁止裸卖空 |
| 初始参数 | 论文 Table 1 | 示例参数为起点，尚未经你的市场校准 |
| 下单量 | 固定 `trade_size` | 同样固定；不足时跳过，不裁成模型外的小单 |
| 候选价格 | `[0,1]` 均匀网格 | `0 < price < 1`，满足 tick 且不吃对手盘 |
| 资金 | 论文不约束现金 | 可用余额、预留现金、单笔成本上限 |
| 双 outcome | 等价为单资产可做空模型 | 只交易显式指定 token；不自动拆分、合并、赎回 |
| 截止时间 | 模型终点为结算 | 可提前停止交易和撤单，不自动市价清仓 |

实盘库存上限同时检查该市场两侧持仓总和，但只为指定 outcome 挂单。
其他 outcome 的库存不会自动卖出。使用专用账户、固定市场和固定 token。
现金不足、post-only 限制、实际市场跳价等会破坏论文理想模型的最优性。
当前实现没有把现金作为第四维重新求解，也没有把实际手续费并入 HJB 收益项；
预算检查会保守预留费用。风险偏好 `gamma`、`gamma_terminal` 由你选择，不从行情中拟合。

## 2. 环境和文件

在已经编译完成、可正常运行 Predict 连接器的 Linux Hummingbot Python 环境中执行。
模型依赖 `numpy`、`scipy`（均为仓库现有声明依赖）；行情和交易使用连接器现有依赖。
只运行 `--help`、校准或读取模型不需要加载 Cython Clock；`solve` 需要 SciPy。

```text
hummingbot/strategy/predict_optimal_market_making/
  model.py                   HJB、插值、合成 Monte Carlo
  recorder.py                有界队列、gzip JSONL、曝光窗口
  calibration.py             拟合、时间留出验证、参数筛选
  config.py                  严格配置校验、带时区时间解析
  connector.py               策略专用观察子类，不改原连接器
  execution.py               maker 报价、撤换、恢复约束
  examples/live.json         可复制的配置模板
scripts/predict_optimal_market_maker.py
test/hummingbot/strategy/predict_optimal_market_making/
```

在仓库根目录执行后文命令。示例 ID `123`、token `123456` 不是实际可用标识。
先用已有探测工具找出市场、结果名称和 token：

```bash
export PREDICT_API_KEY='YOUR_API_KEY'
python -m scripts.predict_market_probe 123
```

## 3. Linux 持久化目录

建议与代码分开保存，目录所有者应为服务运行用户。例如管理员先创建：

```bash
sudo install -d -o hummingbot -g hummingbot -m 0750 /var/lib/predict-mm
sudo install -d -o hummingbot -g hummingbot -m 0750 /etc/predict-mm
```

将 `hummingbot` 替换为实际服务用户/组。一个配置、市场和账户建议使用一个独立数据根目录。

```text
/var/lib/predict-mm/market-123/
  raw/date=YYYY-MM-DD/market_id=123/events-HH-SESSION.jsonl.gz
  models/MODEL_VERSION.npz             初始 HJB 表缓存
  calibration/report-TIMESTAMP.json    每次拟合结果（含拒绝原因）
  calibration/MODEL_VERSION.npz        验证并求解成功的候选表
  calibration/active.json              自动激活记录和上下文
  state/ADDRESS-123.json               签名订单恢复记录及锁文件
```

日期和小时统一使用 UTC。原始文件按小时、会话轮换，写入线程每秒 flush/fsync。
队列满、磁盘失败会置故障标志，主循环停止并进入撤单清理；提交前也检查采集健康。
这不构成交易所订单与日志的分布式原子事务，断电仍可能损失最后一小段日志；
订单恢复依赖独立的、提交前原子持久化的 `state`，不能用行情日志替代。

不要在运行中压缩、删除或移动当前小时文件；备份已关闭的小时文件和状态目录。
突然断电可能留下不完整 gzip，离线读入会明确报错，需恢复备份或人工修复，不会静默跳过。
监控磁盘容量，按实际每日增长量制定保留策略；程序不会自动删除原始数据。

## 4. 配置和时间单位

```bash
cp hummingbot/strategy/predict_optimal_market_making/examples/live.json /etc/predict-mm/market-123.json
```

**启动前必须修改三个时间字段，不能直接使用模板日期：**

* `start_utc`：该模型的时间原点，通常为选定的开始观察/做市时间。
* `resolution_utc`：预计事件结算时间，模型风险计算的终点。
* `trading_cutoff_utc`：停止新单并撤销在途订单的时间，必须不晚于结算时间。

必须显式带 `Z` 或 `+08:00` 等时区。要求 `start < cutoff <= resolution`。
当前连接器的市场对象没有可靠的结算时间字段，所以这里必须人工配置；
市场延期或规则变化时，先停止进程、确认撤单，再更新配置重启，不自动猜测日期。
所有模型时间按 `horizon = resolution - start` 归一化为 `[0,1]`。
`a0/a1` 单位是每个完整 horizon 的固定份额等价成交次数；`sigma` 是该 horizon 单位的波动。
不能将每秒拟合参数直接填入归一化模型。

| 配置 | 含义 |
|---|---|
| `model.q_min` | 实盘/预览必须为 0 |
| `model.q_max` | 模型库存上界，实盘也作为该市场两侧合计份额上限 |
| `model.trade_size` | 固定每侧下单份额；库存范围必须是它的整数倍，数量精度为 0.000001 |
| `model.sigma0/1/2, eta` | 潜在 logit 波动及其时间/概率形状 |
| `model.a0/a1, xi` | 基础/时间变化活动强度 |
| `model.nu, k0/k1, kappa` | 边界衰减和成交率对价差的敏感度 |
| `model.gamma` | 持仓期间风险惩罚 |
| `model.gamma_terminal` | 剩余库存的二元结算风险惩罚 |
| `model.time_steps/price_steps/quote_steps` | 求解网格；加密后应检查结果变化和耗时 |
| `model.p_min` | 求解价格截断；实盘中间价越界则停报 |
| `model.tolerance/max_iterations` | 非线性求解精度及迭代上限 |
| `interval_seconds` | 主循环周期；实盘每轮调用现有账户对账，受连接器限速约束 |
| `refresh_seconds/refresh_ticks` | 最长挂单保持时间、触发换价的 ticks 数 |
| `max_book_age_seconds` | 允许行情年龄 |
| `min_level_amount` | 买卖一最小深度 |
| `min_order_amount/min_order_notional` | 最小下单份额和金额，不自动放大订单凑门槛 |
| `max_cash_per_order` | 单个买单的估算成本上限，不是累计投入或亏损上限 |
| `cash_reserve` | 可用 USDT 中不用于新买单的预留金额 |
| `calibration_seconds` | 后台拟合间隔，默认 900 秒 |
| `calibration_min_windows/min_fills` | 最少有效窗口/训练等价成交量，另外检查验证样本和双侧成交 |
| `auto_activate_calibration` | 默认 false：拟合、验证和生成候选表，但不自动替换；true 才自动切换 |
| `preview_usdt/preview_shares` | 只读报价的静态余额，不是成交模拟器 |
| `startup_timeout` | 启动就绪等待上限 |
| `order_ttl` | 签名订单有效秒数，默认 300、至少 180 |
| `max_quote_ticks` | 最大完整 tick 网格规模，过细市场停报，避免阻塞实时循环 |

示例为每单 5 份、库存上限 20 份、现金预留 5 USDT。
这些是功能示例，不是推荐仓位或经校准的生产参数。

## 5. 先跑论文模型

不需要 API Key 或私钥：

```bash
python -m scripts.predict_optimal_market_maker solve \
  --output /var/lib/predict-mm/paper/table.npz

python -m scripts.predict_optimal_market_maker simulate \
  --table /var/lib/predict-mm/paper/table.npz \
  --paths 100 --steps 500 --seed 7 \
  --output /var/lib/predict-mm/paper/simulation.json
```

默认 `solve` 使用论文 Table 1 系数、允许做空；`--model /path/model.json` 可传仅含 `ModelConfig`
字段的 JSON（不是整个实盘配置）。默认网格不宣称精确复现论文表格数值，需做网格/时间步收敛检查。
模拟输出平均 PnL、标准差、终端绝对库存、95% 损失 VaR 和 ES。
模型在 logit 空间使用正确的 Ito 漂移，结算为 `Bernoulli(p_T)`。
最优和 myopic 策略使用相同的外生价格路径与结算随机数、相同库存上下界；
成交使用各自强度采样。论文对照组未强加相同库存上限，因此这里的结果不能直接与论文 Table 2 等同。
时间步内买卖顺序和多次成交是离散近似，缩小步长后再评估模拟结果。
小样本命令用于冒烟验证，论文量级统计需增加到 10000 条路径并检查计算耗时。

## 6. 同步采集、校准与真实挂单

先只读验证行情和配置：

```bash
python -m scripts.predict_optimal_market_maker run \
  --market-id 123 --token-id 123456 \
  --config /etc/predict-mm/market-123.json \
  --data-dir /var/lib/predict-mm/market-123 \
  --duration 120
```

只读模式没有真实订单曝光/成交，校准一般会报告样本不足；它可以采集波动数据，
但当前保守的联合校准流程要等真实挂单数据充分才生成成交模型候选。

实盘使用环境变量，密钥不要写入 JSON、命令参数或 Git：

```bash
export PREDICT_PRIVATE_KEY='YOUR_SIGNING_PRIVATE_KEY'
export PREDICT_ACCOUNT_ADDRESS='YOUR_PREDICT_ACCOUNT_ADDRESS'

python -m scripts.predict_optimal_market_maker run \
  --market-id 123 --token-id 123456 \
  --config /etc/predict-mm/market-123.json \
  --data-dir /var/lib/predict-mm/market-123 \
  --live
```

普通 EOA 按现有连接器约定省略 `PREDICT_ACCOUNT_ADDRESS`。
账户需具备现有连接器要求的资金、ERC-20/ERC-1155 授权，程序不自动发送授权交易。
不要让原做市策略或手工交易同时操作相同账户、市场。

如果希望验证通过后**自动使用新校准参数**，把配置中的 `auto_activate_calibration` 改为 `true`。
这是与真实挂单同步运行的后台任务，不需要停止采集器或另起一个交易进程。
尚无足够数据时继续使用显式配置的初始模型，不伪造“已校准”状态。

### 订单执行与恢复

1. 启动按账户/市场锁定状态文件，恢复签名哈希，检查市场上是否有状态文件中未知的 OPEN 订单。
2. 恢复的在途订单先撤销并对账；没有确认终态前不提交替代单。
3. 每轮核对状态及真实余额；盘口失效、市场关闭、截止时间到达、采集故障均停止新增报价。
4. 报价变化、定期刷新、部分成交或模型版本切换，会进入撤旧换新流程。
5. 对完整固定份额执行资金和库存检查，以 `LIMIT_MAKER` 提交。每次提交前重新检查行情/余额。
6. 下单签名哈希在 HTTP 提交前由现有连接器通过回调原子保存。超时结果未知时保留原哈希对账，不换盐重下。
7. 连续累计 3 次未能建立跟踪订单会触发提交熔断，停止新增单，需人工检查并重启。

报价不保证双侧都有：零持仓不能卖、库存满不能买、金额不足或模型收益非正时也跳过。
没有自动买入初始库存、强制清仓、市价单、裸卖空或 YES/NO 联合最优化。

**重启必须保持同一个数据根目录和状态文件。** 不得删除状态文件、换 token 或换目录绕过恢复。
文件锁只对相同文件路径有效，不是跨服务器分布式锁。
SIGINT/SIGTERM 会停止新增单，继续运行 Clock，最多等待 60 秒撤单/对账，再退出。
无法确认撤单时会报错且保留状态，不会宣称成功。网络故障期间无法保证立即撤单。
REST 移除订单不等于让签名链上失效；过期时间也不能替代成交核对。

## 7. 数据和校准的真实含义

归档为同一条有序事件流：`schema_version`、`session_id`、`sequence`、
`received_utc_ns`、`received_monotonic_ns`、`market_id`、`kind`、`data`。

| kind | 内容 |
|---|---|
| `session_start` | 模式、token、公开市场元数据、非秘密配置和初始模型版本 |
| `orderbook` | 连接器接受的市场级全深度快照及交易所更新时间；不把互补盘口重复存两次 |
| `book_sample` | 约每 5 秒从新行情取样的中间价、UTC 和单调时钟 |
| `connection/connection_gap` | 主循环观察到的连接状态及数据不可用原因 |
| `market_metadata/market_status` | 公开市场规则和交易状态的变化 |
| `submit_request/submit_result/submission_uncertain` | 提交意图、跟踪结果、签名哈希及不确定状态 |
| `cancel_request/cancel_response` | 撤单请求与 REST 响应；响应不是订单终态确认 |
| `order_status` | REST 累计成交、状态、相对上次观察的成交量 |
| `exposure` | 两次观察间的挂单暴露窗口、成交量、样本是否合格 |
| `decision` | 报价、跳过原因、库存、时间、模型版本和执行动作 |
| `calibration/model_activated/shutdown` | 拟合验证报告、激活和停止信息 |

只采集连接器接受的行情快照，不是底层所有 WS 原始包；旧时间戳消息可能已被连接器丢弃。
行情断流回调会立即作废曝光窗口；tick、费率或结算合约标志变化后停报，需核对并重启。
现有连接器不订阅公共逐笔成交，因此这里没有虚构公共成交数据。
公开订单簿聚合深度也不能确定本单精确排队位置。

### 成交记录限制

当前连接器用 REST `amountFilled` 累计差分，成交价按委托价估算，maker 费用由连接器估算。
事件明确标记 `REST_CUMULATIVE`、`LOCAL_REST_OBSERVATION`、`ESTIMATED`；
不能据此生成精确成交时间、精确手续费或链上结算证明。
首次观察恢复订单只建立基线，之前的成交不算新样本；成功提交确认后即开始观察新订单，
不只保留存活到首轮 REST 的订单。重复累计成交不会重复计数。

校准采用：`count = 窗口新增成交份额 / trade_size`，是固定份额等价成交量，
不是交易所逐笔成交次数。REST 到达时间造成区间删失，模型属于近似的 quasi-Poisson 拟合。
只使用完整固定数量、盘口健康、最长 60 秒、窗口内中间价变化不超过两个 tick 的窗口；
部分成交后的剩余小单、断线和过期数据窗口保留原始记录但不用于拟合。
这种筛选也可能产生样本选择偏差：报告通过不等于成交模型无偏或已验证真实收益。

### 在线校准流程

* 内存保留最近最多 200000 条价格样本及 200000 个合格曝光窗口，重启后重新积累。
* 定时在后台线程拟合，主循环继续采集和管理订单，不在实时循环求解 HJB。
* 按时间约 70%/30% 拆分训练/验证，排除横跨分界的曝光窗口，不随机打乱消息。
* 固定 `nu`，只拟合平稳子模型 `a0` 和 `k0=k1`，`a1=0`；不强行拟合所有时间形状参数。
* 要求足够训练和验证成交、买卖双侧成交、至少 0.002 的价差变化覆盖。
* logit 波动候选仅在训练价格样本足够时估计，并单独通过后续价格样本似然验证；
  不能用成交似然宣称波动模型验证成功。
* 验证成交似然必须改善，最优 `k` 不得卡在搜索边界，活动系数和波动跳变不得过大。
* 候选重新求解成功后保存独立版本。自动激活开启时才切换；已有挂单先撤销再按新模型报价。
* 同配置、同 market/token 的自动激活模型可从 `active.json` 在重启时恢复；上下文不匹配不复用。

本版每次报告的留出验证仍可能因反复调参而过拟合，并非独立长期样本外收益评估。
现有策略没有覆盖过的挂单距离，也不会因采集时间增长就自动变得可识别；不自动进行风险探索挂单。

离线重新校准（只读历史，不下单、不激活）：

```bash
python -m scripts.predict_optimal_market_maker calibrate \
  --market-id 123 --token-id 123456 \
  --config /etc/predict-mm/market-123.json \
  --data-dir /var/lib/predict-mm/market-123 \
  --output /var/lib/predict-mm/offline-candidate/report.json
```

只合并公开配置完全相同的 LIVE 会话。通过时另存 `report.npz`；不通过只写拒绝报告。
离线读入前应停止进程或复制已关闭的归档文件，不读取仍写入中的 gzip。
历史量大时应按市场/时间裁分数据根目录；离线拟合目前将合格特征读入内存。

## 8. Parquet 与 DuckDB 分析

可选安装：

```bash
python -m pip install -r hummingbot/strategy/predict_optimal_market_making/requirements-analysis.txt

python -m scripts.predict_optimal_market_maker export \
  --data-dir /var/lib/predict-mm/market-123 --market-id 123 \
  --output-dir /var/lib/predict-mm/processed/export-001
```

输出目录必须为空，避免重跑混入旧分片。每个事件类型独立目录，10000 条一片。
顶层列为统一事件字段，`payload_json` 保留事件内容，避免不同市场导致 schema 漂移。
DuckDB 例如：

```sql
SELECT json_extract_string(payload_json, '$.side') AS side,
       sum(try_cast(json_extract_string(payload_json, '$.seconds') AS DOUBLE)) AS exposure_seconds,
       sum(try_cast(json_extract_string(payload_json, '$.filled_volume') AS DOUBLE)) AS filled_shares
FROM read_parquet('/var/lib/predict-mm/processed/export-001/exposure/*.parquet')
WHERE json_extract_string(payload_json, '$.eligible') = 'true'
GROUP BY side;
```

行情、订单和决策可以按 session/time 关联。成交后中间价变化可离线计算，但基准时间只能标为
REST 观察时间，不能当作精确成交时刻的 markout。

## 9. systemd 部署示例

`/etc/predict-mm/credentials.env`（由服务用户可读，建议权限 0600）：

```text
PREDICT_API_KEY=YOUR_API_KEY
PREDICT_PRIVATE_KEY=YOUR_PRIVATE_KEY
PREDICT_ACCOUNT_ADDRESS=YOUR_ACCOUNT_ADDRESS
```

示例单元 `/etc/systemd/system/predict-optimal-mm.service`：

```ini
[Unit]
Description=Predict HJB maker and data recorder
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=hummingbot
Group=hummingbot
WorkingDirectory=/opt/hummingbot
EnvironmentFile=/etc/predict-mm/credentials.env
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/hummingbot/.venv/bin/python -m scripts.predict_optimal_market_maker run --market-id 123 --token-id 123456 --config /etc/predict-mm/market-123.json --data-dir /var/lib/predict-mm/market-123 --live
KillSignal=SIGTERM
TimeoutStopSec=90
Restart=on-failure
RestartSec=30
UMask=0027

[Install]
WantedBy=multi-user.target
```

将代码目录、虚拟环境路径、用户和 ID 替换为实际值。如果用 conda，`ExecStart` 填 conda 环境
的 Python 绝对路径，不依赖交互 shell 的 `conda activate`。
先用不带 `--live` 的命令核对；完成配置后管理员可执行：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now predict-optimal-mm
journalctl -u predict-optimal-mm -f
sudo systemctl stop predict-optimal-mm
```

`Restart=on-failure` 会持续重试错误配置；观察日志并修复，不要通过删除状态文件排错。

### Docker 部署

镜像必须包含本次新增策略和脚本，并保持原有编译扩展和 Predict 运行依赖可用。
仅拉取旧镜像不会包含新策略。将宿主机目录映射为持久卷，例如在现有服务配置中加入：

```yaml
volumes:
  - /var/lib/predict-mm:/data/predict-mm
  - /etc/predict-mm:/etc/predict-mm:ro
stop_grace_period: 90s
```

容器内启动同一个模块，将 `--data-dir` 改为 `/data/predict-mm/market-123`。
用 `env_file` 或部署平台的秘密注入机制提供环境变量，确认宿主机目录 UID/GID 对容器用户可写。
入口进程应直接运行 Python 或使用 `exec`，确保 SIGTERM 能送达；不要把采集数据只写进容器层。

## 10. 验证和已知边界

```bash
python -m unittest discover -s test/hummingbot/strategy/predict_optimal_market_making -v
python -m unittest discover -s test/hummingbot/strategy/predict_market_making -v
```

新增测试覆盖终端条件、互补对称性、库存偏移、网格加密、模型序列化、确定性模拟、
资金和库存限制、tick、部分成交撤换、断线撤单、停止超时保留状态、模型切换、
未知 OPEN 订单拒绝、采集故障、累计去重和已知参数的校准验证。
离线测试不需要私钥，不连接交易所。

开发环境另有一个原策略测试因缺少旧模板 `conf/scripts/predict_market_maker.example.json` 失败；
该文件不属于本次新增策略，未修改原测试或原策略来规避此问题。

仍需要你在真实 Linux 环境验证：Clock、RPC/限速、账户授权、post-only 拒单、
真实部分成交和撤单、重启恢复、SIGTERM、磁盘满故障及长时资源占用。
目前没有真实账户端到端验证，也没有承诺论文模拟收益在 Predict 实盘成立。
