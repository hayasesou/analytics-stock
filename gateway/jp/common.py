from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
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
    return raw


def normalize_side(side: str) -> str:
    raw = str(side).strip().upper()
    mapping = {
        "BUY": "BUY",
        "BUY_TO_COVER": "BUY",
        "SELL": "SELL",
        "SELL_SHORT": "SELL",
    }
    return mapping.get(raw, raw)


def normalize_margin_type(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"cash", "genbutsu"}:
        return "cash"
    if raw in {"margin_open", "shinyo_new", "new"}:
        return "margin_open"
    if raw in {"margin_close", "shinyo_close", "close"}:
        return "margin_close"
    return "cash"
