# 挂单满足来源与 depth 连接诊断（2026-11-20）

## 改动

- 普通单分别检查有效 tick / bid0 与这笔单实际门槛，标注 tick / depth / all；不把最低价来源冒充满足来源。
- 保留登记报价快照；WS 单笔在重连/最终发送复核时重新分类，REST batch 逐成员分类。
  worker 只返回快照，owner 收到 ACK 后持久化 `entry_send_quote`，不从网络线程改账本。
- 登记、发送前和成功日志带满足来源及精确 CID；当前挂单显示登记/发送阶段，旧订单 unknown，补挂 signal_follow。
- 代码核查两路均调用 `websocket_connect_kwargs()`，显式 `proxy=None`；没有配置 12334 转发。
  新增测试在 HTTPS_PROXY/ALL_PROXY 指向 12334 时确认 book 实际连接参数依然为 proxy=None。
- 移除 bookTicker 失败后向 tick 专属 /market 路径轮换的行为，固定使用官方 /public/stream。
- 按 stream 区分连接日志，增加端点、直连状态、分组错误与收包过滤计数。

## 验证

```powershell
& D:\program\python.exe -B -m unittest discover -s tests -v
```

结果：330 项测试通过；日志（忽略文件）：`var/validation_v2/entry_quote_labels_tests.log`。
本轮新增 6 项：分档/替换边界与 all 判定；重连前后来源变化及 owner 持久化；实际 batch adapter 逐单来源；
补挂/旧单明确标识；book 端点重试范围；超时日志显示过滤与直连状态。
已有 book 真实 consumer 解析测试加上恶意 12334 代理环境，断言连接参数显式禁用代理。

本机另做了公开 BTC bookTicker 直连探测（无账户、无凭据、无下单），本机网络下 /public 与 /market 均在握手阶段超时；
该结果不能代表 cloud 网络，不能用来推断 cloud 此次报错原因。尚未取得 cloud 报错行或在服务器验证连通性。
官方依据：[Individual Symbol Book Ticker Streams](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public#individual-symbol-book-ticker-streams)。
