"""Read-only path/clock diagnostics; never writes state or queries the database."""
from __future__ import annotations
import time
from pathlib import Path
from datetime import datetime, timezone
from . import config as cfg
from .market_data import StatusReader, PriceReader


def status_reason(status, error="", *, now=None):
    if status is None:
        return error or "3m status missing or invalid JSON/schema/fields"
    now = time.time() if now is None else now
    if status.open_time_ms % cfg.BAR_MS:
        return "3m open_time_ms is not aligned"
    available = (status.open_time_ms + cfg.BAR_MS)/1000
    expiry = (status.open_time_ms + cfg.ORDER_BAR_LIFETIME_MS)/1000
    if now < available-5:
        return "3m not completed or local clock behind"
    if now >= expiry:
        return "3m status expired: latest completed bar not published, or reading an old file"
    return "ok"


def check_inputs():
    status_reader=StatusReader(cfg.STATUS_FILE)
    status=status_reader.read()
    price_reader=PriceReader(cfg.PRICE_FILE)
    price=price_reader.read()
    def describe(path):
        path=Path(path).expanduser().resolve()
        try:
            stat=path.stat()
            return {"path":str(path),"exists":True,"bytes":stat.st_size,
                    "mtime_utc":datetime.fromtimestamp(stat.st_mtime,timezone.utc).isoformat()}
        except OSError as exc:
            return {"path":str(path),"exists":False,"error":str(exc)}
    return {
        "clock_utc":datetime.now(timezone.utc).isoformat(),
        "deploy_root":str(cfg.ROOT),"cwd":str(Path.cwd()),
        "status":{**describe(cfg.STATUS_FILE),"reason":status_reason(status,status_reader._json.error),
                  "bar_open_ms":status.open_time_ms if status else None,
                  "bar_open_utc":datetime.fromtimestamp(status.open_time_ms/1000,timezone.utc).isoformat() if status else None,
                  "symbols":status.symbol_count if status else 0},
        "price":{**describe(cfg.PRICE_FILE),"healthy":price_reader.last_read_healthy,
                 "reason":price_reader.last_read_error,"sequence":price.sequence if price else None},
        "mysql":{"host":cfg.MYSQL_HOST,"port":cfg.MYSQL_PORT,"database":cfg.MYSQL_DATABASE,
                 "queried":False},
        "file_probe_seconds":min(cfg.STATUS_POLL_SECONDS,cfg.PRICE_POLL_SECONDS),
        "maintenance_seconds":cfg.SIGNAL_MAINTENANCE_SECONDS,"history_bars":cfg.HISTORY_BARS,
        "note":"3m writes rows to MySQL; signal also requires a fresh completed-batch status JSON. Collector explicit CLI paths must match these resolved paths."
    }
