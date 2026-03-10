from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any


TERMINAL_STATUSES = {"filled", "rejected", "canceled", "expired", "error"}
PENDING_STATUSES = {"new", "sent", "ack", "partially_filled", "accepted"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def normalize_symbol(symbol: str) -> str:
    raw = str(symbol).strip().upper()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    if "." in raw:
        raw = raw.split(".", 1)[0]
    return raw.strip()


def normalize_side(side: str) -> str:
    mapping = {
        "BUY": "BUY",
        "BUY_TO_COVER": "BUY",
        "SELL": "SELL",
        "SELL_SHORT": "SELL",
    }
    return mapping.get(str(side).strip().upper(), str(side).strip().upper())


def is_terminal(status: str) -> bool:
    return str(status).strip().lower() in TERMINAL_STATUSES


def standardize_status(raw_status: str, *, filled_qty: float, remaining_qty: float, requested_qty: float) -> str:
    normalized = str(raw_status).strip().lower()
    mapping = {
        "presubmitted": "sent",
        "submitted": "sent",
        "pendingsubmit": "ack",
        "pendingcancel": "ack",
        "apicancelled": "canceled",
        "cancelled": "canceled",
        "inactive": "rejected",
        "filled": "filled",
        "partiallyfilled": "partially_filled",
    }
    value = mapping.get(normalized, normalized)

    if value in {"sent", "ack"} and filled_qty > 0 and remaining_qty > 0:
        return "partially_filled"
    if filled_qty >= max(0.0, requested_qty) and requested_qty > 0:
        return "filled"
    if value in TERMINAL_STATUSES | PENDING_STATUSES:
        return value
    return "error"


def action_from_side(side: str) -> str:
    return "BUY" if normalize_side(side) == "BUY" else "SELL"
