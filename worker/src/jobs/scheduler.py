from __future__ import annotations

from datetime import datetime
import time
from zoneinfo import ZoneInfo

from src.config import load_runtime_secrets
from src.jobs.daily import run_daily
from src.jobs.weekly import run_weekly
from src.storage.db import NeonRepository


def _is_after_or_equal(now: datetime, hour: int, minute: int) -> bool:
    if now.hour > hour:
        return True
    if now.hour == hour and now.minute >= minute:
        return True
    return False


def run_scheduler(poll_seconds: int = 20, tz_name: str = "Asia/Tokyo") -> None:
    tz = ZoneInfo(tz_name)
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)

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

        time.sleep(max(poll_seconds, 5))
