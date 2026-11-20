# v2 账户历史查询、热币行情和运行诊断（2026-11-20）

本轮基线 `fb1035d`。用户确认云端为该提交、2 核 CPU，trade 进程 CPU 83.7%。14:08:23 UTC 面板为 0 仓、1 有效普通单、无待撤/补挂，接收 6,671/s、决策 1,046/s、队列 47、累计未变买一 6,191,022、过载丢弃 21,436。后者与先前截图一致，不能把这一时刻解释为仍在新增 overflow。

纠正前述吞吐比较：接收数含经过有效性校验后被跳过的未变买一；决策数只含实际完成 owner 回调的事件。二者本来可以差很大。应结合入队速率、实际完成速率、过期/旧代次拒绝、队列趋势和过载丢弃增量诊断。已有 overflow/stale 历史仍是真实问题，没有移除或改写这些计数。

## 代码复查发现与改动

- `AccountStreamView.lookup_order()` 原来每笔查询均读取、排序并解析整个 orders 表；`_fills()` 每次读取、排序并解析整个 fills 表。先前压力工具用假账户查询，未覆盖历史数据增长的维护成本。本次已知订单 ID 用主键 `SYMBOL:orderId` 查询；仅 CID 时用币对主键范围查询。成交按 `SYMBOL:orderId:` 范围读取，之后仍检查精确 symbol/order/client 身份，合并累计成交和标准/LITE 去重。订单 1 的范围不会读到订单 10。
- 账户健康快照沿用项目的文件 revision 缓存，内容不变时复用解析结果；每次调用仍按当前时间验证 TTL、账号及 ready/connected/live。文件丢失、替换为坏 JSON 不沿用旧健康状态。
- 原有快路径刻意让已挂出币的每次变价都完整规划。热门已挂币高频更新时，即便不会撤单/替换，也反复规划。本次也对已挂出、已持有及已经正确归属的补挂检查“有无动作”。已有单越过 2.1%、新满足替换优势、资金档变化、精确成交回报、补挂失效/满六仓、信号文件变化等仍进入完整路径。
- 增加入队、未变买一、过期拒绝等速率。面板增加 owner 行情处理和维护的 P95/最大耗时，以及最近一次超过 50ms 的主要阶段。反馈内另有最近 5 次慢阶段、限定长度的统计、构建标签及源文件路径/摘要；不逐条刷慢日志。

未更改信号、QV、挂单门槛、仓位规则、满 8 单先发替换、补挂名额、1.5s 撤单间隔和平仓。未增大队列、未放宽 0.5s 限制、未合并变价事件。AccountStreamView 仍只读数据库，未迁移或改写采集器数据库。

## 补充压力对照

同一离线工具，800 币、3s、10,000 帧/s，报价集中于前 8 个已挂币；20% tick / 80% bid0，价格交替变化。账户数据库另含 5,000 笔无关历史订单和 5,000 笔历史成交，使用真实 SQLite 查询及精确归属核对；账本、镜像和 fsync 为真实临时文件，交易所下单模拟。无 profiler、未并行跑测试。基线在独立 Python 进程装载 `fb1035d` 的 stream 和 ws_direct 类，其他条件相同。

| 指标 | 基线 | 本次 |
|---|---:|---:|
| 接收帧数 | 30,000 | 30,000 |
| 实际入队 | 24,022 | 24,003 |
| 完成决定处理 | 1,747 | 24,003 |
| 过期拒绝 | 22,275 | 0 |
| 队列峰值 | 7,263 | 521 |
| 接收至回调完成 P95 / P99 | 500 / 516ms | 31 / 47ms |
| 测量期间维护 P95 | 392.873ms | 27.527ms |
| overflow / overload_discarded | 0 / 0 | 0 / 0 |

本次 5,997 帧是原有连续未变买一去重；其余 24,003 个入队事件全部处理，未借去掉变价提高吞吐。基线积压时 `overloaded` 会暂停未变去重，因而比本次多入队 19 帧；不是本次增加了合并规则。两组最终均为 8 个普通挂单、8 次模拟下单。

本次 owner 行情回调最近 512 样本 P95 为 0.082ms（不含排队）；整体接收至回调完成仍使用上表的 31ms。运行统计还保留初始化维护，包含首次登记/fsync，故它的维护 P95/最大与表中只统计测量期间维护的口径不同。Windows monotonic 粒度约 15.625ms；owner 工作阶段采用高精度 perf_counter。

这些数据没有真实 WS/TLS 接收、服务器磁盘/CPU 争用或用户账户真实数据；能证明修复可复现开销，不能证明它们是该服务器的全部瓶颈，也不承诺云端同样延迟。新增运行分段用于继续定位真实慢阶段。

## 回归与应用

**429 项通过**。本轮新增 10 项覆盖：无关历史订单/成交不会被解析、订单 ID 前缀隔离及 CID 恢复、标准/LITE 成交去重、健康缓存失效/到期、真实入队与未变计数区别、诊断样本有界、热门已挂币无动作跳过/跨线替换/距离撤单、补挂首次建立及满六仓撤销。原精确身份、UNKNOWN、手动平仓、重复下单及满三仓回归均通过。旧手工 SQLite 测试 fixture 补齐采集器原有 key 主键，没有要求线上迁移。

```powershell
& D:\program\python.exe -B -m unittest discover -s tests
& D:\program\python.exe -B -m tests.benchmark_ws_pressure --rate 10000 --seconds 3 --pattern changing_effective --nonblocking --near-symbols 800 --hot-symbols 8 --account-history 5000
```

忽略目录中的证据为 `var/validation_v2/owner_v3_full_tests.log`、`owner_v3_baseline_hot_sql.json`、`owner_v3_hot_sql.json`。

停止旧 trade、更新 cloud、重启 trade。signal、warmup 和采集器无需重启，不清理账本或账户数据。启动日志和反馈的运行标签应为 `20261120-owner-stream-v3`。

```bash
cd /opt/v9711-live/cloud
git pull --ff-only
cd ws_direct_v2
V9711_LIVE_ENTRY_BATCH=0 /opt/v9711-live/cloud/.venv/bin/python -B -u -m v9711_live.execution.runner --enable-live --no-dry-run
```

更新后看入队与决策速率、过期拒绝速率及过载丢弃增量，并看“处理耗时/最近慢处理”。`orders_forced` 为订单/强制平仓核对，`positions_manual` 为持仓/手动平仓核对，`entry_plan` 为入场计划，`ledger` 为账本落盘，`feedback` 为反馈落盘，`render` 为面板输出。累计计数重启清零本身不作为提速证据。未部署生产服务器。
