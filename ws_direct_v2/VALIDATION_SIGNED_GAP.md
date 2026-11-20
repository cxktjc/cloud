# 候选价差排序检查（2026-11-20）

- 发现 signal 面板 `signal_diagnostics.nearest_candidates` 使用 `abs(gap)` 选前三，
  例如 -8% 的候选可能被 +0.1%、+1%、+2% 挤出显示。已改为 `(gap, symbol)`，
  标题改为“按价差从小到大，低于限价优先”。
- signal 实时候选发布 `signal_logic.select_orders` 使用 `gap <= place_gap`，包含所有负价差，
  不用绝对值，也不受面板前三限制；新候选决策按 `(gap, symbol)` 排序。
  序列化按币名排序以稳定文件内容，不限制候选，也不作为 trade 的入场优先级。
- trade 的 `_update_rank` 最小堆、`_fast_snapshot` 短名单、`select_physical_orders` 候选及换单排序，
  均使用原始带符号价差。`Row.from_dict` 重新从现价和限价计算价差；tick/depth 发送前复核同样使用有符号上限判断。
- 低于限价会直接满足价格条件，但仍要求候选/信号/行情有效、资金精度可执行、无重复身份或重开阻止，
  并保留持仓容量、8 普通挂单、独立补挂、换单优势、精确撤单终态及 1.5 秒间隔。
  本次没有给负价差额外增加突破容量或无优势换单的例外。
- 撤单队列仍按绝对价差安排先后，这是原先指定的撤单节奏，独立于入场优先级，本次未改。
- signal 持仓模型通过已完成 K 线确认，不读取这三行面板的排序。本次不改历史模型、QV、入场限价、
  账本或实盘账户；当前报价低于限价只表示可提交买入限价单，不直接记为模型/交易所成交。

验证命令：

```powershell
& D:\program\python.exe -B -m unittest tests.test_signed_gap_v2 -v
& D:\program\python.exe -B -m unittest discover -s tests -v
```

新增 6 项验证通过：负/零/正价差面板排名、-20% 信号发布、9 选 8 的负价差优先、
新旧均为负值时按有符号优势换单、tick/depth/all 负值发送资格、真实 owner 短名单和模拟下单顺序。
完整 374 项回归通过。原始日志：`var/validation_v2/signed_gap_tests.log`。
