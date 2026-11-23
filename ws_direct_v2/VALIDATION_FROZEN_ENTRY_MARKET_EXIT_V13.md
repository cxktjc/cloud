# 冻结入场依据与到期 MARKET 平仓：2026-11-23

运行标识：`20261123-frozen-entry-market-exit-v13`。改动仅在 cloud 仓库 `ws_direct_v2`。
测试全部使用临时账本、离线交易所和假 socket，没有发出真实订单或修改生产状态。

## 入场行为

owner 首次准入时，以其当前决策视图中的有效 tick/bid0 判断分档或替换条件，
冻结该精确 client ID 的价格、门槛、来源及 `entry_quote_policy=admission_frozen`。
删除登记前读取最新 ingress 报价的二次比价；worker 在落盘回执及 socket 就绪后，
使用 owner 发布的冻结依据，不再读取 WS 当前价格、报价时效或连接状态。
因此已准入的订单不会仅因等待期间反弹、报价变旧或行情断线而被发送端拒绝。

新入场仍要求首次准入行情有效。worker 继续检查信号有效期/bar、账户状态、容量、
精确身份和撤单状态；持久化失败或信号撤回仍阻止发单。普通单距离严格超过 2.1%
的独立撤单规则保留；若 owner 已撤销该单准入，worker 也不发送。
补挂单继续保留原价格、数量、独立名额及 signal 生命周期。

单笔 WS 与 REST batch 都使用冻结依据。面板新单标签改为 `准入满足=tick/depth/all`，
补挂显示 `准入满足=signal_follow`；旧单无新策略标记时保留原标签。
兼容字段 `entry_send_quote` 保存的也是准入快照，不能把它解读为最新价格复核。
实际发送日志使用 `entry admission retained`，计时仍记录真实 socket 发送时刻。

## 到期平仓

`TIMED_EXIT_LEAD_SECONDS=0`，原计划 `exit_due_epoch` 不变，到期后直接 SELL MARKET。
不再提前 1 秒，不再新建 QUEUE 平仓单或执行 5 秒 × 12 次限价重挂。
旧环境变量不能重新开启限价流程。既有持仓的价格、数量及到期时间保持原值。

待平仓的最近到期时间参与 owner 唤醒，不再仅等待普通 250ms 维护周期。
已有平仓 CID 继续精确核对；明确拒绝的 MARKET 按原冷却重试，避免到期唤醒忙转。
唤醒不等于交易所立即成交：本机调度、精确状态核对、身份落盘及网络仍需要时间。

重启发现旧 QUEUE 时，立即推进精确撤单/成交核对，只对确认剩余数量发 MARKET。
撤单结果不明、提交结果不明或缺交易所订单号时保留原 CID 查询，不重叠发平仓单。
原有账户手动/强平识别及实际成交记账保持生效，普通入场撤单的 1.5 秒间隔保留。

## 验证

使用 `D:\program\python.exe -B -m unittest discover -s tests`：
**613 项通过，33.820 秒**。覆盖：

- 阻塞真实账本写入期间报价反弹或断线，已经准入的单仍只发送一次；发送路径读取 WS 会使测试失败。
- 单笔、batch 分别保留各 CID 最初的 tick/depth/all 依据。
- 信号撤回、账户过期、精确身份缺失或写盘失败仍阻止发送。
- 到期前不发平仓单，到期直接 MARKET；重复调用先等精确成交，不重复平仓。
- 旧 QUEUE 部分成交后只市价平剩余数量；撤单不明或订单号未知时不重复平仓。
- 到期唤醒选择最近待平仓时间，跳过在途平仓并遵守拒绝冷却。
- 旧 QUEUE 的立即撤单截止时间不逐轮延后，也不因此反复保存账本。

本地完整日志：`var/validation_v2/v13_full_tests.log`（忽略文件）。

## 离线负载抽查及限制

```powershell
& D:\program\python.exe -B -m tests.benchmark_entry_latency_v7 --samples 8 --history 1000 --signed-socket --burst --pressure-rate 36000 --cpus 2 --near-symbols 130 --timing-history 5000 --require-ms 10 --output var/validation_v2/v13_smoke.json
```

固定两核、真实本机持久化、签名假 socket、130 个近价背景币、5000 条计时历史。
本次仅为 2 秒短负载抽查，实际接收 35,939 条/秒，共 71,878 条；入队/决策各 2,012，
过载、过载丢弃及过期积压拒绝均为 0，无错误、重复币对、重复 CID 或未发送目标币。

| 计时范围 | 样本数 | P50 ms | P95 ms | 最大 ms | ≤10ms |
|---|---:|---:|---:|---:|---:|
| 指定的同时触发订单 | 8 | 11.862 | 12.332 | 12.473 | 0/8 |
| 含背景发单的全部订单 | 18 | 11.507 | 19.015 | 24.282 | 6/18 |

脚本退出码 2 表示严格 10ms 目标未通过，完整性检查通过。
本次没有证明持续高压或生产环境逐笔 10ms 达标，也不把 v12 的历史压测当作 v13 验收。
数据留在 `var/validation_v2/v13_smoke.json` 和 `v13_smoke.log`。

## 更新

先正常停止旧 trade，再在 cloud 仓库 `git pull --ff-only`，从 `ws_direct_v2` 用原命令启动 trade。
signal 无需重启；不删除 state。确认面板 build 为本文件的 v13 运行标识，启动日志显示
`close policy=MARKET at exit_due_epoch; timed_exit_lead=0.0s`。
增量账本及备份方式沿用 [v12 说明](VALIDATION_INGRESS_JOURNAL_V12.md)：备份整个 state 目录。
