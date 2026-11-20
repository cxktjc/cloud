# 手动平仓与延迟入场记账（2026-11-20）

## 已确认的代码缺陷

`_register_fill` 以本地处理时刻设置 entry_epoch，手动平仓筛选又使用
max(order_bar_open_ms, entry_epoch*1000, external_evidence_since_ms)。
若入场回报/精确查询晚于实际手动平仓才被本地记账，就会把已发生的平仓和归零证据一起挡掉。
已在临时账户 SQLite/账本复现：真实 BUY → 手动 SELL+LONG=0 → 延迟记入该 BUY，旧逻辑保留幽灵持仓。

## 修复

- 新仓保存 `entry_fill_time_ms`：精确入场 BUY 的成交时间；正常 FILLED/PARTIALLY_FILLED 回包可用其 updateTime。
  CANCELED 回包的更新时间不等于成交时刻，不据此恢复入场时间。
- 旧仓只有在账户库已存在归零候选证据时，才读取同一 CID/OID 的精确 BUY 成交时间，重新核对完整证据。
  不额外访问交易所，不发 BUY/SELL，不从币对合计数量猜入场。
- 保留同步/导入屏障、至少足量外部 SELL 与 LONG=0、排除自有平仓单等限制，按原规则记录真实退出 VWAP 及收益。
- 无法匹配时打印时间下界、账户数量/归零时间、匹配量与缺量，沿用每币警告去重/60 秒节流。

## 验证

353 项回归通过，命令：

```powershell
& D:\program\python.exe -B -m unittest discover -s tests -v
```

新增 6 项：旧仓延迟记账恢复与幂等结算；新仓记入交易所时间；错误入场身份不允许降低屏障；
同步屏障不能被旧成交绕过；归零早于错误起点的诊断；CANCELED 更新时间/其他订单成交不能伪装入场时间。
专项测试使用临时文件与真实本地 AccountStore，broker 被禁止调用。
日志（忽略文件）：`var/validation_v2/manual_close_timing_tests.log`。

## XPINUSDT 当前证据范围

用户日志表明 2026-11-20 06:32:47 UTC 账户流见 LONG=0，订单成交累计 6851、均价 0.000731。
日志没有毫秒交易时间、last_quantity、精确订单身份或 trade 持仓时间字段，尚不能确认此仓命中上述缺陷。
另复现了归零时间比 SELL 时间早 1ms 时旧证据查询会排除 SELL，暂未放宽该上界，等待账户库输出确认。
本次没有连接 cloud、修改其账本或按照聊天日志手动结算；真实持仓清理需服务器拉取后由原 owner 核验完整证据。
