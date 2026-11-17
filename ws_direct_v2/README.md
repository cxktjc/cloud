# v9711 WS-DIRECT v2：按实际持仓数使用 50% / 30% / 20% 资金

本版本对应 2026-11-16 原对话的 v2 任务。以相邻 `ws_direct_v1` 为工程基础，
参考 `baseline_503020_stop25_20261116` 的三仓资金比例，独立保存所有新版本输出。

## 下单金额

金额基数是 **trade 自己账本的 realized_balance**，不是 signal 资金、交易所钱包余额或含浮盈权益。
新账本初始资金 15U；重启继续现有资金。signal 和 trade 各自复利、各自维护仓位，
正常启动不会导入另一方或 v1 的资金和仓位。

| trade 当前实际持仓数 | 下一笔新挂单的理论金额 |
| --- | --- |
| 0 | trade 资金 × 0.5 |
| 1 | trade 资金 × 0.3 |
| 2 | trade 资金 × 0.2 |
| ≥3 | 普通单通常停止新挂；到期前最后 3m 的预挂例外、下述补挂单均使用 0.2 档 |

这是按**当前持仓数量**选择档位，不是永久给仓位编号。部分成交计一仓；
UNKNOWN/尚未确认平掉的仓位仍计数。两仓减为一仓后新单使用 0.3，减为空仓后使用 0.5。
已有持仓不补仓、不改量。

- 持仓数量改变时，未成交的旧档挂单先撤单，精确终态确认后再按新档挂单。
- 撤单或下单回报丢失时保留原身份和占位，等待账户流/精确查询，不盲目重复下单。
- 同一 bar 内重新挂单使用持久化递增代次，重启也不会复用旧 clientOrderId。
- 持仓档位未改变时，资金浮动不会反复撤改同一笔已挂订单。下一笔新单使用最新资金。
- 8 个竞争挂单使用挂出时所见的持仓数；挂单数不当作已成交仓数。
- 最低 5U、币对 minNotional/minQty 和 stepSize 向上对齐规则继续生效。
  例如 15U 资金的三个理论档是 7.5/4.5/3U，后两档实际至少 5U。
  面板逐单显示理论金额和实际委托金额。

**并发成交边界：**多笔已到交易所的旧档挂单可能在撤单确认前成交，真实成交全部记账。
50/30/20 不是严格的一倍总敞口限制；最低金额、并发成交和 v1 的到期前预挂例外均可能提高敞口。
精确保证每次成交恰好依次为 50/30/20，需要改变多挂单执行结构，不属于本次金额替换。

## signal 持有、trade 未持有的补挂单

trade 按 **signal 该笔持仓的原入场限价**补挂 BUY LIMIT/GTC，跨 bar 保留，
直到该 signal 持仓结束才撤掉未成交单。不套用普通挂单的价差门槛或每根 bar 到期撤单，
也不参加普通挂单的排名替换。signal 确认入场时，若已有同笔同价挂单，直接保留原订单身份。

- **最多额外 2 单**，不占普通 8 单名额，不影响普通挂单的分档计数；面板分开显示。
- trade 有 3～5 仓时也可补挂；**达到 6 仓即停止补挂，并发起撤掉已有补挂单**。
  第 6 仓成交入账后，后台发送闸门同时禁止继续发送已排队的补挂单。
- 撤单未确认、下单 UNKNOWN 都继续占用额外名额，不重复发单；重启续接原身份。
- 缺口超过 2 个时，先补 signal 入场较早的持仓，同时间按币对排序；有空位后继续补。
- 新补挂按 trade 当时实际仓数使用 50/30/20，最低 5U 和交易所精度规则生效。
  档位变化时沿用精确撤旧再换新；部分成交后已算 trade 持仓，撤掉余单，不给已有仓位补量。
- 保留行情、账户流和精度就绪检查，以及平仓后 60 秒保护和强平/手动平仓保护。
  signal 短暂不可读或过期时不新增补挂，也不据此猜测持仓已结束。

已到交易所的订单可能在撤单确认前继续成交，真实成交全部记账；“6 仓停止补挂”
是发送和撤单规则，不能抹掉撤单竞态中已经发生的成交。成交后的到期平仓沿用该 signal
持仓的原到期时间，仍提前 1 秒开始，不从补挂成交时重新计算持仓期。

## signal 与参考回测的关系

signal 改为独立三仓模型，同一完成 bar 内按币对顺序依次使用 50/30/20，
按完成 3m 数据确认模型入场，使用上一根完成 close × (1−2.5%) 提前生成限价，K=4。
trade 用内部 WS 的实际到达顺序执行，成交及收益由精确交易所回报确认；signal 不是实际成交证明。

trade 的定时平仓在持仓最后一根 bar 结束前 **1 秒**开始，原计划到期时间保持不变。
平仓使用 SELL LIMIT/GTC `priceMatch=QUEUE`：每次确认挂出后等 **5 秒**，
未全部成交则撤单，精确确认终态、结算累计成交后，以剩余仓位重新挂 QUEUE。
**包括首单在内最多挂 12 次**；第 12 次撤单确认后，剩余数量用 MARKET 兜底。
提前全部成交即停止；明确拒绝 QUEUE 时直接进入原有 MARKET 兜底。
撤单/查询/网络延迟会增加总耗时，状态不明时继续核对原单，避免重叠平仓。
轮数和订单身份持久化，重启接着执行；旧 60 秒 QUEUE 单按原订单身份恢复到 5 秒等待。
v2 固定执行 5 秒 × 12 次，旧 `.env` 中的 `V9711_LIVE_CLOSE_LIMIT_SECONDS` 不再生效。
保留原有显式关闭开关：`V9711_LIVE_CLOSE_LIMIT_ENABLED=0` 时直接 MARKET，默认开启限价流程。
此变更只影响 trade；signal 仍按完成 bar 确认模型退出。**没有新增参考目录的 25% 止损。**
参考脚本另使用当前 bar open 生成入场价、固定 slot 权重及含持仓估值权益，
因此本版本不是该回测脚本的逐项实盘复刻，也不声称复现其收益。没有进行新策略回测或调参。

## 2026-11-17 入场与 QV 更新

参考 `v9891_fixed_d025_strategy_20261117` 的固定偏移及 QV 机制，按用户确认保留实盘
**上一根完成 close** 基准，不取当前 bar open，不增加 25% 硬止损。

- 入场限价：上一根完成 close × 0.975。
- QV20：当前下单 bar 之前 20 个 3m 时间格的 quote_volume 均值，min_periods=10；
  单币缺失格或非有限 QV 填 0，与参考加载器一致，不把跨缺口的 20 条记录当作 20 根连续 bar。
- 每根 bar 对完整配置币池（并包含状态中新增的币对）的正数有限 QV20 取 q90，
  用当前及之前共 24 个 q90 的均值作为入场 QV 上限。`QV20 <= 阈值` 可入选。
  阈值由完成历史逐根重建；不受价格接近挂单线的候选数量影响，不按月份或重启清零。
- 完整平滑需要下单 bar 之前 **43 根连续完成 bar**。缓存至少 **96 根（4小时48分）**，
  旧 `.env` 的 `HISTORY_BARS=40` 自动提升至 96；数据库原有 24 小时保留期足够。
  全市场整根 bar 缺失时暂停新信号入场，不退回固定阈值；会重试有界历史窗口补缺。
  既有信号到期 close 仍可结算，trade 原有持仓平仓独立运行。
- 普通挂单分档：前 5 单 **≤1.85%**，第 6～8 单 **≤1.65%**，满 8 单替换 **≤1.45%**。
  按用户明确修正的数值收紧准入，减少距离过远的新增普通挂单。
  优势仍严格大于 0.5 个百分点；例外变为新单严格小于 1.45%、旧单严格大于 1.65%。
  旧的 `V9711_LIVE_{BOOTSTRAP_PLACE,FILL_PLACE,PLACE}_GAP` 覆盖值不再生效。
  仅因距离扩大不自动撤掉当前 bar 的有效旧挂单。补挂单继续走原入场限价和独立名额规则。

已有 signal 账本自动识别上一版精确模型参数，先保存原始字节备份
`signal_portfolio.json.pre_d025_cs_qv.bak`，再记录 `strategy_effective_bar_open_ms`。
**重启当时的下单 bar 保留旧入场规则，下一根完整 3m bar 起使用新规则**；
停机期间更早的补算 bar 仍按原参数计算。旧已处理 bar 的 checkpoint 冻结，不追溯重算已有交易。
保留 session、资金、原持仓价格/数量/到期时间及交易记录，不修改 trade 的原始订单身份和账本。
无法识别或损坏的账本会保留并报错，不重建空账本。

升级时先停止原 signal 和 trade，再在 cloud 仓库 `git pull --ff-only`，从 `ws_direct_v2`
重启两者。不删除 `var/state`，不运行同步资金/导入仓位工具。signal 面板及预案
`portfolio.qv` 显示实际阈值、有效币数、历史分位数数量和生效 bar。

## WS 与文件路径

```text
完成 3m → signal 提前发布本 bar 全候选模板 → desired_orders_ws.json
aggTrade → trade 内存准入/持久化身份/派发 → 内存价格镜像 → 异步 latest.json → signal
```

trade 不读 `latest.json` 决定开仓，文件写出在该 tick 决策之后；保留独立网络工作通道、
8 个普通挂单分档、最多 2 个独立补挂单、撤旧再换新、价格新鲜度/重连代次保护、
账户流确认、手动/强平恢复及交易 WS 保活。
signal/trade 面板按变化刷新，最多每 3 秒一次。

运行路径由 **v2 代码所在目录**推导；旧环境变量、启动工作目录不会重定向账本。
以下以 `/opt/v9711-live/cloud/ws_direct_v2` 安装位置为例：

| 用途 | 路径 |
| --- | --- |
| 实盘 signal 模型账本 | `ws_direct_v2/var/state/signal_portfolio.json` |
| signal 预案 | `ws_direct_v2/var/state/desired_orders_ws.json` |
| trade 账本 / 精确身份 | `ws_direct_v2/var/state/execution_ledger.json` / `execution_intents.json` |
| trade 反馈和日志 | `ws_direct_v2/var/state/execution_feedback.json` / `trade.log` |
| signal 日志 | `ws_direct_v2/var/state/signal.log` |
| v2 价格镜像 | `ws_direct_v2/var/binance_ticker_price/latest.json` |
| v2 精度缓存 | `ws_direct_v2/var/exchange_info_cache.json`（后台自动刷新） |
| 共用完成 3m 状态，只读 | `cloud/var/klines_3m_status.json` |
| 共用账户流，只读 | `cloud/var/binance_account_stream/` |
| 显式 `--paper` 的账本 | `ws_direct_v2/var/paper_state/`（两个进程都加） |
| 私有设置 | `ws_direct_v2/.env`（只需凭据/MySQL，不填写文件路径） |

订单 client 前缀独立使用 `v9712l_`，信号 contract 使用 `ws_direct_v2`。
旧版计划不兼容。只给本版本的 WS 写价格镜像；继续运行现有 3m/agg/account collectors。

## 服务器启动

把完整 `ws_direct_v2` 目录放到 `cloud` 下，复用 `cloud/.venv`。依赖见 `requirements.txt`。
首次填写本版本 `.env`，之后不需要为了找账本反复修改环境变量：

```bash
cd /opt/v9711-live/cloud/ws_direct_v2
cp -n .env.example .env
chmod 600 .env
# 填好本版本 .env 的 Binance 与 MySQL 凭据
/opt/v9711-live/cloud/.venv/bin/python -B -m v9711_live.execution.runner --check-inputs
/opt/v9711-live/cloud/.venv/bin/python -B -m v9711_live.run_signal --check-inputs
```

**切换前停止旧 trade，在交易所手动撤掉旧挂单、平掉旧仓位即可。**
v2 不读取、复制或清理旧执行账本；旧文件中的仓位、UNKNOWN 或精确订单残留均不阻挡启动，
不需要删除或修复旧账本。旧目录仅保留 trade 进程锁，避免旧 trade 与 v2 同时运行。
首次启动创建独立 15U 实盘账本；
可证明的首次空账本写入中断会自动继续。已有账本损坏、缺半份或身份不符时不会重置。

两个终端分别运行（trade 命令会提交真实订单）：

```bash
cd /opt/v9711-live/cloud/ws_direct_v2
/opt/v9711-live/cloud/.venv/bin/python -B -u -m v9711_live.run_signal
```

```bash
cd /opt/v9711-live/cloud/ws_direct_v2
/opt/v9711-live/cloud/.venv/bin/python -B -u -m v9711_live.execution.runner --enable-live --no-dry-run
```

模拟必须在两个命令后都加 `--paper`，trade 不加实盘开关。禁止模拟写入 live state。
同时只运行一个写 v2 价格镜像的 trade。systemd 模板独立命名为
`v9711-ws-v2-signal.service` / `v9711-ws-v2-trade.service`，不会覆盖 v1 的 unit 文件。
安装模板不会启动服务。

## 可选的一次性手动账本同步

正常运行不自动同步 signal 资金/仓位。如操作员明确需要一次性同步，先停止 signal/trade，
核对模型仓位与实际 Binance 持仓一致，预览后再应用。该工具只改本地记账，不执行 BUY/SELL；
不会把模型仓位变成交易所已成交订单。原执行文件保存在 `var/state/execution_backups/`。

```bash
/opt/v9711-live/cloud/.venv/bin/python -B -m scripts.sync_signal_to_trade
/opt/v9711-live/cloud/.venv/bin/python -B -m v9711_live.execution.runner --sync-signal-ledger
```

保留 v1 已有的 `--price SYMBOL=PRICE` 预览及 runner 的 `--sync-price SYMBOL=PRICE` 应用参数。
应用后退出，正常启动不再加同步参数；trade 继续独立维护资金与持仓。
历史兼容的迁移模块保留供离线维护；正常 v2 启动不调用 v1 自动接管流程。

## 本地验证

```powershell
& D:\program\python.exe -B -m unittest discover -s tests -q
```

请在本目录运行。测试使用临时状态及模拟交易通道，含本地回环 WebSocket。
完整验证与限制见 [VALIDATION_V2.md](VALIDATION_V2.md)。没有启动真实下单或修改生产账本。
