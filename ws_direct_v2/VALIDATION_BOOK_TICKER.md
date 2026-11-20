# tick / bid0 OR 挂单验证（2026-11-20）

## 实现

- 相同币池、相同分组的独立 aggTrade/bookTicker WS 接入，同一 owner 队列。
- best bid `b` 和量 `B`，按 `u` 排除重复/倒退；分来源时间、连接和重连代次校验。
- 有效两路最小价格用于普通准入、候选排名、替换和 >2.1% 距离撤单。
- WS 单笔及 REST batch 发送前重新检查当次准入门槛；替换仍保留原优势要求。
- 临时报价不合格且请求未发出时允许后续重新评估，UNKNOWN 实际发送继续精确占位。
- tick-only 估值/镜像、signal/warmup、独立固定数量补挂、容量与 1.5 秒撤单间隔保留。

## 检查

从 `ws_direct_v2` 执行：

```powershell
& D:\program\python.exe -B -m unittest discover -s tests -v
```

结果：**324 项测试通过**。日志（忽略文件）：`var/validation_v2/book_ticker_tests.log`。

新增 19 项包括：

- 两路最小值、tick-only 估值及镜像、只有 book 就绪即可运行、单路断线与两路失效。
- 零买量、重复 updateId、重连前排队消息、报价过期及 book 事件不污染价格镜像。
- 只有 tick 合格、只有 bid 合格、两路都不合格、两路同时事件仍只登记/发送一笔。
- 两路均超 2.1% 才撤、行情都缺失时保留订单、补挂不因 bid 远离或仓数变化撤改。
- 满 8 单新旧都按双路口径比较、撤后补入和发送前仍满足 0.22pp 优势。
- 实际 WS 单笔/REST batch adapter 的发送前检查、报价暂时失格后允许重新准入。
- 24 组确定性随机双路行情及 book 断线场景中，增量 shortlist 与全量选择完全一致。
- 实际 consumer 的订阅 URL 和 bookTicker 解析，以及独立双路连接的分组/币池一致。

全部验证使用临时账本、模拟网络和离线报文，不依赖生产账户/真实订单。
现有 305 项回归继续覆盖订单身份、重试上限、重启、资金档位、部分成交和原撤单排队节奏。
其中丢 ACK 重启测试固定到 bar 内第 30 秒，避免实际测试运行时间临近 bar 结束时，模拟 +8 秒跨 bar 导致偶发失败；生产过期与对账逻辑未因此调整。
本地模拟验证不代替服务器网络连通性或实盘成交验证。

## trade 每组 100 币 + SUBSCRIBE（2026-11-20）

- trade 的 aggTrade/bookTicker 均按最多 100 币分组，连接不带 `?streams=` 的原端点，
  每次建立连接后发送一次本组完整 `SUBSCRIBE`，使用数值请求 ID；重连重新订阅。
- `result: null` 的匹配确认只记录日志，不标记行情就绪、不刷新行情时间。
  错误响应触发现有失效/重连流程；仍支持原始事件及 `data` 包装格式。
- 默认 DNS、直连、自动 pong、30s ping、TLS 验证及现有重试/时效要求保留。
  独立 ticker collector 默认继续 URL 订阅，诊断工具仍保留历史 200 币分组边界。
- 647 币的实际双路 receiver 调度验证：每路 7 组，大小为 100×6+47，全币池无遗漏。
- 本次命令：`& D:\program\python.exe -B -m unittest tests.test_book_ticker_v2 tests.test_ws_direct tests.test_ws_pressure_v2 tests.test_ticker_readiness tests.test_ws_connection_probe -q`。
  **109 项通过**，含真实本地回环 WS 的两种流/两种报文格式，以及仅 ACK 无行情、
  重连重订阅、订阅拒绝等验证；未发送真实订单，未修改生产账本。
- 服务器是否仍会异常断线需更新后观察；本地测试不证明此前故障根因已消除。

订阅协议依据：[Binance USDⓈ-M Live Subscribing/Unsubscribing](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams)。

## 启动串行连接、depth 优先（2026-11-20）

- 首次启动先依次连接全部 bookTicker 组，再依次连接 aggTrade 组。每组完成握手和
  SUBSCRIBE 发送后等待 1s，才发起下一组；已有组继续接收，状态线程不等待这些间隔。
- 首轮失败会记录原错误并释放启动等待，间隔 1s 后继续其他组，失败组按原策略独立重连。
  启动完成信号与行情就绪分离，订阅确认仍不授权使用行情；后续重连不走该首次启动队列。
- 停止时取消启动等待/间隔及所有已发起的 receiver，不在停止后继续启动新连接。
- 相关回归命令：`& D:\program\python.exe -B -m unittest tests.test_book_ticker_v2.BookReceiverTests tests.test_book_ticker_v2.SubscribeReceiverTests tests.test_ticker_readiness tests.test_ws_pressure_v2.ReceiverRecoveryTests tests.test_ws_direct -q`。
  **59 项通过**。包括 647 币双路 14 组顺序、13 次间隔、真实 1s 间隔、订阅发送未完成时
  保持等待、首轮握手失败继续、启动握手/间隔中停止，以及原有超时/重连/解析回归。
  测试使用临时状态及本地回环 WS，未发送真实订单或修改生产状态。
