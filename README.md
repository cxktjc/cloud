# v9711-live-ubuntu

这是 `v9711` 实盘 signal/trade 的独立 Ubuntu 部署目录。它不依赖原工作区 `D:\0606`、其他策略目录或原来的 `deliverables...` 包名。

## 运行职责

| 服务 | 作用 | 数据 |
|---|---|---|
| `collectors.ticker_ws` | Binance USD-M `aggTrade` WebSocket，高频更新本机币对价格 | `var/binance_ticker_price/latest.json` |
| `collectors.binance_futures_3m_ws` | 3m K线 WebSocket，写完成K线 | MySQL `binance_futures.klines_3m` |
| `collectors.kline_aggregator_15m` | 每15m检查并合成15m，同时只检查最近24小时 | MySQL `binance_futures.klines_15m` |
| `collectors.binance_account_stream` | 账户事件、订单和成交本地账本 | `var/binance_account_stream/events.sqlite3` |
| `v9711_live.run_signal` | 读取本地3m状态、MySQL K线和高频价格文件，发布 signal 计划 | `var/state/desired_orders.json` |
| `v9711_live.execution.runner` | 消费计划并执行真实订单 | `var/state/execution_*.json` |

signal 仍然不直接连接行情 WebSocket。高频 WebSocket 由 `ticker_ws` 负责，signal 通过原子替换的本地 JSON 高频消费；原有按价格内容去重和高频 stat 探测仍然保留，因此同一份价格不会重复计算。

## 数据保留口径

- `klines_3m`：按 `KLINE_RETENTION_HOURS=24` 清理，只保留最近约1天。
- `klines_15m`：aggregator 同样清理和检查最近24小时，启动扫描默认96根15m窗口。
- `latest.json`：只保留当前一份价格快照，WebSocket采集不追加价格历史，也不创建价格历史表。
- `events.sqlite3`、`var/state/`：**不能按1天清理**。这里保存真实持仓、订单身份、成交归属、复利余额和恢复游标。
- 当前 `config/symbols.txt` 是从本地 v9711 当前3m状态导出的647个策略币对；不要在正式运行中随意覆盖它。需要更新时先测试新币对是否有足够40根完成3m预热。

## Ubuntu 安装

假设部署路径是 `/opt/v9711-live/cloud`，并且服务器有一个可连接的 MySQL。服务器上不要放整个 `D:\0606`。

```bash
sudo mkdir -p /opt/v9711-live/cloud
sudo chown -R "$USER":"$USER" /opt/v9711-live/cloud
cd /opt/v9711-live/cloud
git clone <你的私有仓库地址> .
# 或者第一次从本机复制后：git init / git remote add / git push
bash deploy/install_ubuntu.sh
cp .env.example .env
chmod 600 .env
nano .env
```

如果服务器 Python 不是 `python3`，可以：

```bash
PYTHON_BIN=python3.11 bash deploy/install_ubuntu.sh
```

## MySQL 初始化

`.env` 至少填写：

```text
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_ADMIN_USER=root
MYSQL_ADMIN_PASSWORD=...
MYSQL_USER=root
MYSQL_PASSWORD=...
```

然后执行：

```bash
bash deploy/init_mysql.sh
```

脚本只创建 `binance_futures` 数据库及以下策略K线表：

- `binance_futures.klines_3m`
- `binance_futures.klines_15m`
- `binance_futures.strategy_symbols`

Binance行情、账户和交易连接全部走服务器自身的直连HTTPS/WSS，不读取代理环境变量，也不使用本地代理端口。MySQL仍需要自己的数据库连接配置；不要把密码提交到 Git。

## 币对文件

当前文件已经随仓库提供：

```bash
wc -l config/symbols.txt
head config/symbols.txt
```

更新成交易所当前所有交易中的 USD-M 永续 USDT 币对：

```bash
.venv/bin/python -B -m scripts.update_symbols
```

从某个本地3m状态恢复策略当前币对：

```bash
.venv/bin/python -B -m scripts.update_symbols \
  --from-status var/klines_3m_status.json
```

更新后应先让3m collector运行，等每个币对有40根完成K线，再让signal正式接管。币对文件的变更会被3m collector周期检查并重建订阅。

## 先不下真实订单的启动顺序

每个命令都在仓库根目录执行：

```bash
# 1. 价格WS
.venv/bin/python -B -u -m collectors.ticker_ws

# 2. 3m K线WS
.venv/bin/python -B -u -m collectors.binance_futures_3m_ws \
  --symbols-file config/symbols.txt \
  --retention-hours 24 \
  --status-file var/klines_3m_status.json \
  --aggregator-trigger-file var/kline_aggregator_15m.trigger

# 3. 15m合成（可选但保留，和3m配合）
.venv/bin/python -B -u -m collectors.kline_aggregator_15m \
  --symbols-file config/symbols.txt \
  --retention-hours 24 \
  --startup-check-windows 96 \
  --trigger-file var/kline_aggregator_15m.trigger

# 4. 账户事件；需要API key，但不会下单
.venv/bin/python -B -u -m collectors.binance_account_stream \
  --output-dir var/binance_account_stream --env-file .env

# 5. signal
.venv/bin/python -B -u -m v9711_live.run_signal

# 6. trade先模拟，不开真实订单
.venv/bin/python -B -u -m v9711_live.execution.runner
```

建议先检查：

```bash
cat var/binance_ticker_price/latest.json | python3 -m json.tool | head
cat var/klines_3m_status.json | python3 -m json.tool | head
cat var/state/desired_orders.json | python3 -m json.tool | head -80
```

trade 模拟正常、账户事件为 `live/ready/connected`、价格和3m状态连续稳定后，才执行真实交易：

```bash
.venv/bin/python -B -u -m v9711_live.execution.runner \
  --enable-live --no-dry-run
```

不要同时运行两套trade，也不要在旧电脑和服务器同时使用同一组 `v9711l_*` 订单。

## systemd

把服务文件中的 `User=ubuntu` 改成实际运行用户，并确认部署路径确实是 `/opt/v9711-live/cloud`：

```bash
sed -i "s/^User=ubuntu$/User=$USER/" deploy/systemd/*.service
bash deploy/install_systemd.sh

sudo systemctl enable --now v9711-ticker.service
sudo systemctl enable --now v9711-3m.service
sudo systemctl enable --now v9711-15m.service
sudo systemctl enable --now v9711-account.service
sudo systemctl enable --now v9711-signal.service

# 确认下面几项后再开真实trade：
systemctl status v9711-ticker v9711-3m v9711-15m v9711-account v9711-signal
journalctl -u v9711-signal -f

# 最后才允许真实执行器
sudo systemctl enable --now v9711-trade.service
```

停止或升级时：

```bash
sudo systemctl stop v9711-trade.service v9711-signal.service v9711-account.service v9711-15m.service v9711-3m.service v9711-ticker.service
# 备份 var/state 和 var/binance_account_stream 后再更新代码
git pull --ff-only
.venv/bin/pip install -r requirements.txt
sudo systemctl start v9711-ticker.service v9711-3m.service v9711-15m.service v9711-account.service v9711-signal.service
```

## Git

本地目录已经单独初始化为 Git 仓库，并已推送到 `git@github.com:cxktjc/cloud.git` 的 `main` 分支：

```bash
git pull --ff-only origin main
```

`.env`、运行数据库、日志、signal/trade状态和价格快照都被 `.gitignore` 排除。服务器正式接管前，必须先备份并核对 `var/state/`、真实未决订单和账户持仓；不能通过删除状态文件来“清空”或重新开始。

## 重要检查

1. 所有Binance服务使用直连HTTPS/WSS，不需要配置代理地址或本地代理端口。
2. 3m collector 的数据库表必须有 `open_time_ms` 主键索引；初始化SQL已提供。
3. WebSocket价格采集使用 `aggTrade`，按200个币对一组连接，默认每100ms合并写一次快照；如果服务器带宽/磁盘压力较大，可把 `TICKER_PUBLISH_INTERVAL` 调到0.2或0.5。
4. `exchangeInfo`更新币对后，新币对需要先积累warmup；signal不会因为文件更新就虚构历史K线。
5. 1天行情只够短时重启恢复。停机超过1天，不能静默跳过缺失bar；应先补数据或保持trade停用。
6. 本包没有复制其他策略目录；批量LIMIT下单辅助和交易适配器已内置到 `v9711_live/exchange/`。


## ??WS????????????

`ticker_ws`???? `/market/stream`???????????????15????????? `/public/stream`?? `/stream` ?????????????????
?????????????????? `aggTrade` ????????????????

- `connecting ... endpoint=...`????????????
- `handshake complete; waiting for first valid aggTrade`???????????
- `DATA READY ... first_symbol=...`????????????
- `received_messages=0 valid=0 waiting_for=first_data`?????????15?????????
- ??????????30????/?????? `.env` ? `TICKER_READ_TIMEOUT=300` ????????????????????????

?????????????signal/trade?????????????????? `.env` ??????????? `TICKER_WS_BASE` ??????

```bash
.venv/bin/python -B -u -m collectors.ticker_ws \
  --ws-base wss://fstream.binance.com/market/stream \
  --symbols-file config/symbols.txt \
  --output var/binance_ticker_price/latest.json \
  --read-timeout 30
```

?????????????????????????????????????????????????????????????????????

## ?????????

signal?3m???15m???????????????????? `.env`?
????????? `MYSQL_PASSWORD`???????????? `MYSQL_ADMIN_PASSWORD`?
???????????? > ???????? > ?? `.env`?
????????????Git?`git pull`??????????? `.env`?

signal????? `--mysql-password`??? `--mysql-host`?`--mysql-port`?
`--mysql-user`?`--mysql-database`?3m?agg??????????
account??SQLite?trade?????????WS??JSON????MySQL???
??????????????????? ALTER USER ???MySQL??????

### Cloud????????

??????cloud?MySQL???????????????????????
signal?? `--mysql-password`?3m?agg?????????????
??? > ?????? > ??.env > cloud??????
???????.env??????????????? `MYSQL_PASSWORD` ?
`MYSQL_ADMIN_PASSWORD`?git pull??????????.env?
??????????????MySQL????????
???????????????????????????????

## Signal 3m handoff diagnostics

Default file probes: 20ms; maintenance: 250ms; in-memory warmup: 40 completed
3m bars (about two hours). File probes do not query MySQL every 20ms.
The 3m collector publishes status only after each subscription group reaches
90% completed-bar coverage. Database rows alone do not imply a published status.
The agg process publishes 15m status, not the required 3m handoff.

Producer defaults now share signal's configured status/price paths. Explicit
CLI paths still take priority. New .env templates do not hardcode /opt paths;
existing private .env files are never overwritten by git pull.

Read-only check (no state writes or MySQL queries):

```bash
.venv/bin/python -B -m v9711_live.run_signal --check-inputs
```

Compare status.path with the collector log: published 3m status ... path=...
To pin both processes to the same checkout, from the repository root:

```bash
# Restart the existing collector rather than running a duplicate.
.venv/bin/python -B -u -m collectors.binance_futures_3m_ws --status-file "$PWD/var/klines_3m_status.json"
.venv/bin/python -B -u -m v9711_live.run_signal --status-file "$PWD/var/klines_3m_status.json" --price-file "$PWD/var/binance_ticker_price/latest.json"
```

Waiting status now includes the exact resolved path and rejection reason.
Stale, misaligned, or unfinished bars remain ineligible for new trading.

## Correct server checkout: /opt/v9711-live/cloud

All six systemd templates and .env.example use /opt/v9711-live/cloud.
The Python package still derives defaults from its actual checkout when there
are no environment overrides. Existing private .env files are not overwritten.
Update stale V9711_DEPLOY_ROOT and market file overrides in the private .env.
Do not replace the entire .env: retain credentials and check any existing state
and account-stream paths before restarting a real executor. Changing a path
does not migrate its ledger; do not start trade on an empty replacement ledger.

For immediate signal path correction without changing state ownership:

```bash
cd /opt/v9711-live/cloud
.venv/bin/python -B -m v9711_live.run_signal --check-inputs --status-file "$PWD/var/klines_3m_status.json" --price-file "$PWD/var/binance_ticker_price/latest.json"
```

If using systemd, reinstall the edited templates and daemon-reload. Existing
installed service files will not change merely by pulling repository updates.
Only restart the required services; do not launch duplicate trade processes.


## Newest-first startup backfill and signal display

The aggregator now repairs/aggregates the retained startup windows newest-first,
so recent 3m history needed for signal warmup is downloaded before older history.
Each unavailable window yields after a bounded coverage/repair attempt and is
retried later; a missing old window no longer waits indefinitely ahead of the
remaining backlog. Newly completed live windows take priority between attempts.
The latest 15m status file never moves backwards when an older window completes.
OHLC aggregation itself still uses chronological child candles.

Signal redraws only on changed content and at most once every 3 seconds by
default. Intermediate display changes are coalesced into the latest frame.
File polling, calculation, desired-order publication, and trade display timing
are unchanged. Optional private .env setting:

```dotenv
V9711_LIVE_SIGNAL_MIN_REFRESH_SECONDS=3
```

After pulling this update, restart only the aggregator and signal processes.
Do not clear state or restart a second trade instance.
