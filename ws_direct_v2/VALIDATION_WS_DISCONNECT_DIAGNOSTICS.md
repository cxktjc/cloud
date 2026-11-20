# depth 异常断线：证据与只读对照（2026-11-20）

本次仅增加独立诊断模块及测试，不改变 trade 的订阅、重连、线程、准入或下单逻辑。

用户提供的 14:54:58—14:56:58 UTC 日志中，depth group 1/2/3 共出现 12 次异常断线。
每次均已握手成功并收到有效行情；`messages` 与 `valid` 总数均为 20,018。
存活时间最短 3.102 秒、中位 4.7765 秒、最长 9.148 秒；每次断线后退避 30 秒。
因此这不是“启动从未收到 depth”，而是“已恢复后很快再次丢失连接”，退避进一步延长不可用时间。

接收库在 `recv()` 末尾通过 `raise protocol.close_exc from recv_exc` 保留底层异常。
原 trade 日志只保存最外层异常文字；无法区分 TCP reset、EOF、TLS 错误等，也无法据此确定服务器端、
网络路径或本地调度哪一方导致断线。3—9 秒与配置的首个客户端 ping（30 秒）、行情静默上限（30 秒）
及定期重连（23 小时）不匹配。没有关闭帧也不同于正常主动关闭；不把 30 秒退避误当断线原因。
参考：[websockets 异常说明](https://websockets.readthedocs.io/en/15.0/faq/common.html)。

## 服务器同机对照

在已停止 trade 的同一目录运行，默认观察 20 秒，只打印诊断，不写账本、不连接账户、不发送交易请求：

```bash
cd /opt/v9711-live/cloud/ws_direct_v2
/opt/v9711-live/cloud/.venv/bin/python -B -u -m collectors.ws_connection_probe --group 2 --seconds 20
```

同时连接四组公开行情，使用原有心跳、超时、4096 接收缓冲与 `proxy=None`：

| case | 对照意义 |
| --- | --- |
| depth_group_url | 原第 2 组最多 200 币、原 public combined URL |
| depth_single_url | 单 BTCUSDT、同 public 入口 |
| tick_group_url | 同组币对、market aggTrade 入口 |
| depth_group_subscribe | 同组币对、public 入口，改用一条公开 SUBSCRIBE 请求 |

输出包含连接 IP、握手/首包时间、匹配事件数、订阅 ACK、连接存活时间、最后收包距今、异常链、
transport/parser 异常与整个诊断循环最大调度延迟。只在 `connected`、`first_data`、`summary` 时输出，
不逐帧打印。四组各自只连接一次，观察结束或断线即结束，不修改策略重试策略。
正常观察结束也必须结合 `matching_events` 判断，握手但零行情不当作行情正常。

判读：组订阅失败而单币稳定，可继续查组合订阅/负载；两种 depth 组订阅行为不同，可继续查订阅方式；
depth 均失败而 tick 稳定，可继续查 public 路径；独立进程稳定而 trade 复现，则继续查 trade 内部影响。
这些是缩小范围的证据，不自动证明具体哪一方存在故障。20 秒稳定也不证明长期稳定。

本机直接连接的三组先导对照均在 10 秒握手超时、零数据，未复现服务器“已收数据后断开”，
因此不拿本机网络结果替代服务器现场。

## 验证

`D:\program\python.exe -B -m unittest tests.test_ws_connection_probe tests.test_book_ticker_v2.DualMarketTests tests.test_book_ticker_v2.WSRecoveryHistoryTests tests.test_book_ticker_v2.BookReceiverTests tests.test_ws_pressure_v2.ReceiverRecoveryTests`

31 项通过。其中新增 6 项验证异常链与 IP、公开订阅请求、握手失败、观察结束、四组对照及异常链环保护。
这是离线诊断行为回归，服务器根因仍待同机输出。
