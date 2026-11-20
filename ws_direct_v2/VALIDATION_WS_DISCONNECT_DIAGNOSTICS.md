# depth 异常断线：证据与只读对照（2026-11-20）

本次仅增加独立诊断模块及测试，不改变 trade 的订阅、重连、线程、准入或下单逻辑。

用户提供的 14:54:58—14:56:58 UTC 日志中，depth group 1/2/3 共出现 12 次异常断线。
每次均已握手成功并收到有效行情；`messages` 与 `valid` 总数均为 20,018。
存活时间最短 3.102 秒、中位 4.7765 秒、最长 9.148 秒；每次断线后退避 30 秒。
因此这不是“启动从未收到 depth”，而是“已恢复后很快再次丢失连接”，退避进一步延长不可用时间。

接收库会将底层异常链接到 `ConnectionClosed`；17.1 使用 `recv_exc`，17.4 改为 `connection_exc`。
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
这是离线诊断行为回归，不证明服务器行情连接正常。

## 15:09 UTC 服务器对照与后续 curl 输出

用户在 trade 停止时运行四组对照：

| case | 存活秒数 | 数据/匹配事件 | 底层证据 |
| --- | --- | --- | --- |
| depth_group_url | 7.069 | 1277/1277 | EOF，帧还差 172 字节 |
| depth_single_url | 10.062 | 1868/1868 | EOF，帧还差 94 字节 |
| depth_group_subscribe | 10.318 | 2816/2815，含 1 个订阅 ACK | EOF，帧还差 153 字节 |
| tick_group_url | 12.056 | 1792/1792 | EOF，帧还差 181 字节 |

四个连接 IP 不同；事件循环最大延迟 3.003ms。之后单 BTCUSDT 的 curl HTTP Upgrade 对照
返回 `http=101 peer=16.76.74.58 seconds=8.931882 bytes=1139537`，在 20 秒观察结束前已经退出。
这表明独立于 trade，且独立于 Python/websockets，也能在同机复现连接提前结束。
trade 的写盘、撤单、订单规划，以及 200 币分组和 combined URL，并非复现该现象的必要条件。
仍不能仅凭 EOF 指定是币安节点、云主机网络还是路径中设备所致；需要 TCP/TLS 关闭方向或异地对照。
curl 不是完整的 WS 客户端，不自动响应 WS ping，不能当长期可用性测试；本次 8.93 秒的现象
应与 Python 客户端的无关闭帧、帧中 EOF 证据一起判读。

用户确认此前只 git pull 和重启 trade，没有执行 pip/系统升级；不把观察到的 websockets 17.4
当作“最近升级导致”的证据。原始诊断工具读取旧私有字段，因此该版本下 `transport_error=[]`
不能证明底层没有异常；外层 exceptions 与 protocol_error 的 EOF 证据不受影响。
工具现兼容两个字段，新增一项对应测试；诊断相关共 32 项通过。交易/重连代码没有变化。

## 固定 IP 的 TLS 关闭通知与控制帧对照

后续 curl 对同一 IP 的详细日志显示：证书为有效的 `*.binance.com`，校验通过，协商 TLS 1.3；
`HTTP 101` 后收到 877,800 字节，9.250451 秒时出现入站 `TLS alert, close notify`。
这次是远端 TLS 终止端发出关闭通知，并非 curl 的 20 秒观察超时。TLS 终止端可能是服务入口/负载均衡，
仅凭通知无法确定它的内部关闭理由，不能直接推断成封 IP、连接限额或普通丢包。
参考：[TLS 1.3 closure alerts](https://www.rfc-editor.org/rfc/rfc8446.html#section-6.1)。

curl 的手工 Upgrade 测试不会自动回 WebSocket pong，所以不能把它当作完备的 WS 心跳测试。
新增同一 BTCUSDT、同一官方 public 入口的两路 Python 对照，两路均由库自动回复 pong。
默认用正常系统 DNS，各自输出实际入口 IP：

```bash
/opt/v9711-live/cloud/.venv/bin/python -B -u -m collectors.ws_connection_probe \
  --heartbeat-compare --seconds 20
```

也可固定到同一入口 IP，便于控制 DNS 返回地址的差异（仍不保证连接落到同一个后端实例）：

```bash
/opt/v9711-live/cloud/.venv/bin/python -B -u -m collectors.ws_connection_probe \
  --heartbeat-compare --peer-ip 16.76.74.58 --seconds 20
```

一条连接保留 30 秒客户端 ping，另一条每 2 秒主动 ping。每条记录入站/出站的 PING/PONG/CLOSE
及 EOF 顺序；出站记录表示库生成了帧，不证明服务器已经收到。记录保留最近 32 个控制事件，
总计数独立累加；不格式化或输出行情数据帧。`observation_end_requested=true` 表示主动结束观察，
不要把随后本地生成的正常 CLOSE 当作远端先关。固定 IP 仍保留 fstream.binance.com 的 SNI 和证书校验。

这可以继续检查漏回服务器 ping、主动保活差异及对端关闭顺序；2 秒心跳稳定一次只能作为后续验证线索。
未将诊断用的 2 秒参数应用到 trade。新增控制日志与两路配置、SNI 的回归和本地真实服务主动 ping/
确认 pong 的集成测试；诊断相关共 37 项通过。
