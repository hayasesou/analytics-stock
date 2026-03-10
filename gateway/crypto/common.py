from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_symbol(symbol: str, venue: str) -> str:
    raw = str(symbol).strip()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    base = raw.split(".", 1)[0].strip().upper()
    if not base:
        return base

    venue_norm = str(venue).strip().lower()
    if "hyper" in venue_norm and base.endswith("USDT"):
        return base[:-4]
    return base


def normalize_side(side: str) -> str:
    raw = str(side).strip().upper()
    mapping = {
        "BUY": "BUY",
        "BUY_TO_COVER": "BUY",
        "SELL": "SELL",
        "SELL_SHORT": "SELL",
    }
    return mapping.get(raw, raw)


def opposite_side(side: str) -> str:
    if str(side).strip().upper() == "BUY":
        return "SELL"
    return "BUY"
