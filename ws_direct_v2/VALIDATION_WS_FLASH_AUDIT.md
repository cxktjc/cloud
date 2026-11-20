# v2 WS 急跌反弹链路检查（2026-11-20）

检查基线：`ef09c20`。本轮只扩展离线检查工具、记录发现，未修改交易执行、信号或配置默认值，未连接交易所或部署服务器。

验证：三组无 profiler 压力测量、独立 profiler 诊断、四项假传输探针均完成；行情/深度/下单时间相关 61 项回归通过，两份检查脚本 AST 校验通过。无需因本次检查重启 trade 或 signal。

结论：仍有明显提速空间。当前优化对“绝大部分币远离限价”有效；大量币同时进入候选区间时，重复筛选、精度检查和同步落盘会阻塞行情 owner。另复现了发送复核读取已处理旧价的问题，快速反弹时应同时修正行情新鲜度。

## 实际链路与等待位置

`WS recv → JSON 解析 → 公用 FIFO → owner 更新 processed → 读取信号/处理网络回报 → 候选排名及准入 → 登记精确订单身份与占用槽位并落盘 → 网络线程发送前复核 → WS 单笔 / REST 批量 → 回报 → owner 记账`

- 新行情直接唤醒 owner，不是固定等 0.5s 再判断。维护工作在同一个 owner 中同步执行，每次完成后约 250ms 再安排下一次；250ms 是维护间隔，不是每笔下单的固定延迟。
- 行情、订单回报处理和维护竞争同一个 owner。回报完成还可能触发立即维护。维护中的全池候选检查、账本保存、反馈构造会占用行情处理时间。
- 队列年龄超过默认 0.5s 的事件被显式拒绝，未静默覆盖急跌/反弹事件。年龄检查在决定处理事件之前；回调还可能继续耗时，因此“接收到完成判断”统计可以超过 0.5s。
- 当前下单准入没有等待 price JSON 或 signal 面板刷新；接收线程独立，价格镜像在决定处理后异步写入。面板时间、请求发送时间、交易所回报时间需要区分。

相关源码：`execution/ws_direct.py:226,272`、`execution/ws_market.py:231`、`execution/runner.py:853`（均在 `v9711_live/` 下）。

## 大量币同时接近限价的补测

800 币、两种行情源、独立镜像线程、真实临时目录及账本 fsync、非阻塞网络调度。订单请求使用离线替身。已去掉测试 fixture 对 `time.time()` 的 Mock 调用记录开销，保留稳定 bar 时钟。三组依次运行，没有并行跑性能任务。

`changing_effective` 模式让收到的价格在基准上下交替，双源取小后的有效判断价也改变；没有仅靠数量变化去重降低负载。近价基准为限价上方 1%，远价为 3%；8 笔普通单在计时前已模拟准入，期间观察持续处理压力。**这不是急跌成交率测试，也不代表实际市场必然同时有 800 个合格候选。**

| 场景 | 输入/秒 | 总输入/入队 | 完成判断 | 排队超时拒绝 | 接收至判断完成 P95 | 维护 P95 |
|---|---:|---:|---:|---:|---:|---:|
| 8 币近价、792 币远价 | 5,000 | 15,000 | 15,000 | 0 | 16ms | 24.855ms |
| 800 币近价 | 5,000 | 15,000 | 1,935 | 13,065 | 515ms | 79.756ms |
| 800 币近价 | 1,000 | 3,000 | 1,975 | 1,025 | 500ms | 79.483ms |

三组均无队列溢出、无异常，`unchanged=0`；超时拒绝是现有保护机制在工作，不是网络断线。这也说明只看溢出次数或平时 P95 会漏掉近价行情的处理压力。

这些是本机 Windows Python 3.11.5 的工程压力数据，不能直接代替 Ubuntu 云机速度。此解释器 monotonic 使用 GetTickCount64，分辨率约 15.625ms；表中接收延迟不能解释为精确到 1ms。维护时长和下文模拟发送点使用高精度 perf_counter 测量。没有测交易所 RTT、交易所入簿、排队成交或收益。

独立 cProfile 诊断（800 近价、500 条/秒、2s；有 profiler 开销，不能当作吞吐基准）：

- 550 次 `on_tick`、7 次维护，调用 `prepare_limit` 9,960 次，累计约 0.966s。
- `prepare_limit` 内部先读 metadata，再经 `price_text`、`validate_order` 各读一次；共 29,880 次 `_symbol_meta`，每次仍会探测文件状态、校验和转换字段。
- 整次诊断中 `file_stamp` 累计约 0.617s，包含信号文件探测和大量重复 metadata 探测；缓存 JSON 解码并没有消除这些探测。
- 临时候选已满足物理上限、价差也不足以替换时，仍反复做完整 preflight。周期维护还会对全池近价候选重复计算，快路径也对 shortlist 重复计算。

相关源码：`execution/client.py:325,356,379,396`、`execution/runner.py:905`、`execution/ws_direct.py:191,137`。signal 心跳/组合变化只要改写文件，当前代码也会清空并重建模板排名，即使候选模板本身未变化；此项属于源码确认，未在上述固定信号压力测试中模拟。

## 首笔派发的批量屏障

独立探针替换了 WS、REST 两个发送入口和 worker 预热，真实 adapter 的最终准入复核仍运行。这里的“发送点”是**假传输函数入口**，不含真实连接、签名、socket send 或回报 RTT。

测量起点是一次准入维护调用开始，报价在此之前已准备好；不能把以下数字称作 WS 接收到实盘下单延迟。

| 本次同时准入 | 派发方式 | 首笔到达假发送点 | 最后到达假发送点 |
|---|---|---:|---:|
| 1 笔 | 默认单笔 WS 路径 | 32.140ms | 32.140ms |
| 8 笔 | 默认批量，分 5+3 | 98.581ms | 99.783ms |
| 8 笔 | 仅探针内关闭批量，逐笔派发 | 32.109ms | 106.663ms |

- 每个新普通单先 `reserve_entry_client_id()` 保存两份文件，再 `register()` 保存两份文件，共 4 次 fsync，之后才允许网络请求。这保证重启时保留精确身份与槽位，不能简单删除。
- 默认 `entry_plan()` 收集完整计划，8 笔都登记完才开始派发 5+3 两个任务。探针中首批派发前已完成 34 次 fsync：前置维护 2 次，8 笔登记 32 次；不同 worker 的两个批次先后到达存在调度差异。
- 逐笔派发可让第一笔登记完成后马上交给预热 WS worker，无须等后面 7 笔登记。对首笔有帮助；不等于最后一笔或全部回报一定更快。
- 2..5 笔的默认传输实际是 REST batchOrders，不使用已预热的交易 WS。HTTP helper 每次新建 urllib opener，没有连接池；因此还可能付出新建 HTTPS 连接的耗时，此部分未做真实网络测量。

相关源码：`execution/nonblocking_io.py:363`、`execution/runner.py:578,608`、`execution/client.py:91,106,116`、`execution/batch_entry.py:40`、`exchange/binance_network.py:20`。

## 快速反弹时复核可能仍看到旧价

可重复的离线场景，限价 1.00：

1. tick 从 1.03 降到 0.99，owner 开始处理并登记订单。
2. 在第一次账本保存前人为阻塞 owner 80ms，模拟慢磁盘/主线程阻塞。
3. 独立接收线程收到 tick=1.03、bid0=1.03，均已经超出 1.85% 准入范围；反弹事件已入队。
4. 订单发送前复核仍使用 `processed` 中的 tick=0.99，通过准入。探针约在反弹已收到 13.285ms 后到达假发送点。

`received_rebound_but_guard_used_old_quote=true`。这不是单纯面板延迟：`latest_ingress` 已经是新价，但变化后的新价尚未被 owner 消费。book 的“相同价格刷新时间”机制不会把变化后的新价直接交给复核。

相关源码：`execution/ws_market.py:130,191,397`、`execution/ws_direct.py:69`。当前队列 0.5s 保护发生在回调之前，不能防止回调登记期间新收到反弹而最终复核仍看到旧价。

修正方向：owner 继续按事件顺序处理状态，发送前另外读取已接收的最新有效 tick/bid0，并校验连接代次、有效性和时效。保留用户指定的“任意一个满足即准入”，不能改成两者都满足，也不能把旧急跌价锁存成永久准入资格。这样能阻止反弹后才发出、继续悬挂的订单；是否抓到急跌仍取决于前段速度及交易所入簿时间。

## 建议实施顺序

1. **重复 preflight 与全池维护（性能优先）**：将精度 metadata 读成带 revision 的内存快照；每次准备订单复用同一份 metadata；缓存按 symbol、限价、资金档、metadata revision 的结果。候选先做便宜的价差/替换资格判断，真正可能操作的候选才做 preflight，而且必须先通过 preflight 才撤旧单。资金、持仓数、模板、metadata 变化时失效。
2. **发送复核最新行情（与性能改造一同完成）**：修正上面的旧价复核问题，为接收、排队、实际发送分别保留时间。发送点不能只靠回报时间推断。也不能靠放宽 0.5s 过期保护制造“提速”。
3. **首笔立即派发**：优先使用已有预热 WS worker，登记一笔后尽快派发，取消整组登记屏障；对比第一笔、最后一笔、ACK 和限流表现，再决定批量是否只保留为可选路径。
4. **减少维护阻塞及落盘次数**：无变化账本不反复重写；只因心跳变化不重建模板排名。需要新身份/占用槽位的关键持久化应设计成可恢复事务，验证每个中断点无重复下单后再合并；显示和非关键反馈从关键路径移开。不能直接把全部保存改成异步。
5. 前述改造后，再测 100ms、300ms、1s 回弹窗口和突发队列，若仍有积压，再考虑按币保序的关键穿越事件优先调度。必须保留急跌与反弹两条变化，不能简单“只留最新价”把短促机会覆盖掉。

替换规则另外有业务等待：旧单需要精确确认终态才放新单；如果撤单通道正在 1.5s 间隔中或前面还有撤单，会自然超过 1s 的机会窗口。空闲通道的第一笔撤单不是固定先等 1.5s。本轮遵守此前保留节流的要求，不建议偷偷缩短它。已有预挂单才可能在下跌时直接成交；事后收到一次低价并不保证还能成交于该价格。

## 复现

从本目录执行，所有账户、状态和市场输入都是临时 fixture，不读生产账本：

```powershell
& D:\program\python.exe -B -m tests.benchmark_ws_pressure --rate 5000 --seconds 3 --pattern changing_effective --nonblocking --near-symbols 8 --output var/validation_v2/flash_audit_mostly_far_5000.json
& D:\program\python.exe -B -m tests.benchmark_ws_pressure --rate 5000 --seconds 3 --pattern changing_effective --nonblocking --near-symbols 800 --output var/validation_v2/flash_audit_all_near_5000.json
& D:\program\python.exe -B -m tests.benchmark_ws_pressure --rate 1000 --seconds 3 --pattern changing_effective --nonblocking --near-symbols 800 --output var/validation_v2/flash_audit_all_near_1000.json
& D:\program\python.exe -B -m tests.benchmark_flash_entry --output var/validation_v2/flash_audit_transport_probe.json
```

两组 800 近价压力测试由于检测到排队超时，按工具设计返回 exit=1；不是脚本异常。原始输出和 profiler 文件保留在本地忽略目录 `var/validation_v2/flash_audit_*`，报告保留本次汇总证据。原来的旧版本压力报告使用不同 fixture 时钟/价格模式，不能跨报告直接按数字比较改进幅度。
