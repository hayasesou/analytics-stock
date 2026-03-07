from __future__ import annotations

from datetime import datetime
import time
import traceback
from zoneinfo import ZoneInfo

from src.config import load_runtime_secrets, load_yaml_config
from src.jobs.agents import run_agents_once
from src.jobs.crypto_marketdata import run_crypto_marketdata
from src.jobs.daily import run_daily
from src.jobs.edge_radar import run_edge_radar
from src.jobs.research import run_research
from src.jobs.weekly import run_weekly
from src.storage.db import NeonRepository


def _is_after_or_equal(now: datetime, hour: int, minute: int) -> bool:
    if now.hour > hour:
        return True
    if now.hour == hour and now.minute >= minute:
        return True
    return False


def _safe_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def _resolve_edge_radar_schedule(cfg: dict[str, object]) -> dict[str, int | bool]:
    edge_cfg = cfg.get("edge_radar", {})
    if not isinstance(edge_cfg, dict):
        edge_cfg = {}
    equities_cfg = edge_cfg.get("equities", {})
    if not isinstance(equities_cfg, dict):
        equities_cfg = {}
    crypto_cfg = edge_cfg.get("crypto", {})
    if not isinstance(crypto_cfg, dict):
        crypto_cfg = {}
    return {
        "enabled": bool(edge_cfg.get("enabled", True)),
        "equities_hour_jst": _safe_int(equities_cfg.get("run_hour_jst"), 20, 0, 23),
        "equities_minute_jst": _safe_int(equities_cfg.get("run_minute_jst"), 10, 0, 59),
        "crypto_minute_jst": _safe_int(crypto_cfg.get("run_minute_jst"), 5, 0, 59),
    }


def _resolve_crypto_marketdata_schedule(cfg: dict[str, object]) -> dict[str, int | bool]:
    crypto_cfg = cfg.get("crypto_marketdata", {})
    if not isinstance(crypto_cfg, dict):
        crypto_cfg = {}
    return {
        "enabled": bool(crypto_cfg.get("enabled", True)),
        "minute_jst": _safe_int(crypto_cfg.get("run_minute_jst"), 3, 0, 59),
    }


def run_scheduler(poll_seconds: int = 20, tz_name: str = "Asia/Tokyo") -> None:
    tz = ZoneInfo(tz_name)
    secrets = load_runtime_secrets()
    cfg = load_yaml_config()
    edge_schedule = _resolve_edge_radar_schedule(cfg)
    crypto_marketdata_schedule = _resolve_crypto_marketdata_schedule(cfg)
    repo = NeonRepository(secrets.database_url)
    last_edge_crypto_slot: tuple[int, int, int, int] | None = None
    last_edge_equities_date = None
    last_crypto_marketdata_slot: tuple[int, int, int, int] | None = None

    while True:
        now = datetime.now(tz)

        # Daily: run once after 20:00 JST if no run exists for this JST date.
        if _is_after_or_equal(now, 20, 0):
            if not repo.has_run_for_date("daily", now.date(), tz_name=tz_name):
                run_daily()

        # Weekly: Saturday after 06:30 JST, run once for this JST date.
        if now.weekday() == 5 and _is_after_or_equal(now, 6, 30):
            if not repo.has_run_for_date("weekly", now.date(), tz_name=tz_name):
                run_weekly()

        # Research loop: once after 07:00 JST when weekly run exists.
        if _is_after_or_equal(now, 7, 0):
            if not repo.has_run_for_date("research", now.date(), tz_name=tz_name):
                run_research()

        # Crypto marketdata: hourly once after configured minute.
        if bool(crypto_marketdata_schedule["enabled"]):
            marketdata_minute = int(crypto_marketdata_schedule["minute_jst"])
            if now.minute >= marketdata_minute:
                marketdata_slot = (now.year, now.month, now.day, now.hour)
                if last_crypto_marketdata_slot != marketdata_slot:
                    try:
                        run_crypto_marketdata()
                        last_crypto_marketdata_slot = marketdata_slot
                    except Exception:  # noqa: BLE001
                        print("[scheduler] crypto_marketdata_failed", flush=True)
                        traceback.print_exc()

        # Edge Radar:
        # - equities: daily once after configured JST time.
        # - crypto: hourly once after configured minute.
        if bool(edge_schedule["enabled"]):
            eq_hour = int(edge_schedule["equities_hour_jst"])
            eq_minute = int(edge_schedule["equities_minute_jst"])
            crypto_minute = int(edge_schedule["crypto_minute_jst"])

            if _is_after_or_equal(now, eq_hour, eq_minute) and last_edge_equities_date != now.date():
                try:
                    run_edge_radar(scope="equities")
                    last_edge_equities_date = now.date()
                except Exception:  # noqa: BLE001
                    print("[scheduler] edge_radar_equities_failed", flush=True)
                    traceback.print_exc()

            if now.minute >= crypto_minute:
                crypto_slot = (now.year, now.month, now.day, now.hour)
                if last_edge_crypto_slot != crypto_slot:
                    try:
                        run_edge_radar(scope="crypto")
                        last_edge_crypto_slot = crypto_slot
                    except Exception:  # noqa: BLE001
                        print("[scheduler] edge_radar_crypto_failed", flush=True)
                        traceback.print_exc()

        # Agent queue processor: lightweight tick each loop.
        run_agents_once(limit=20)

        time.sleep(max(poll_seconds, 5))
