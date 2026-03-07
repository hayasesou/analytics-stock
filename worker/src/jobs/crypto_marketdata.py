from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from src.config import load_runtime_secrets, load_yaml_config
from src.integrations.binance import BinanceMarketClient
from src.integrations.hyperliquid import HyperliquidMarketClient
from src.storage.db import NeonRepository
from src.types import CryptoDataQualitySnapshot, CryptoMarketSnapshot


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_int(value: Any, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    if minimum is not None:
        out = max(minimum, out)
    if maximum is not None:
        out = min(maximum, out)
    return out


def _to_float(value: Any, default: float, minimum: float | None = None, maximum: float | None = None) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        out = default
    if minimum is not None:
        out = max(minimum, out)
    if maximum is not None:
        out = min(maximum, out)
    return out


def _default_pairs() -> list[dict[str, Any]]:
    return [
        {
            "symbol": "BTC",
            "binance_symbol": "BTCUSDT",
            "hyperliquid_symbol": "BTC",
            "collect_spot": True,
            "collect_perp": True,
            "binance_enabled": True,
            "hyperliquid_enabled": True,
        },
        {
            "symbol": "ETH",
            "binance_symbol": "ETHUSDT",
            "hyperliquid_symbol": "ETH",
            "collect_spot": True,
            "collect_perp": True,
            "binance_enabled": True,
            "hyperliquid_enabled": True,
        },
    ]


def _resolve_pairs(raw_pairs: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_pairs, list) or not raw_pairs:
        raw_pairs = _default_pairs()

    output: list[dict[str, Any]] = []
    for raw in raw_pairs:
        if not isinstance(raw, dict):
            continue
        symbol = str(raw.get("symbol") or "").strip().upper()
        if not symbol:
            continue

        binance_symbol = str(raw.get("binance_symbol") or "").strip().upper()
        if not binance_symbol:
            binance_symbol = f"{symbol}USDT"

        hyperliquid_symbol = str(raw.get("hyperliquid_symbol") or "").strip().upper()
        if not hyperliquid_symbol:
            hyperliquid_symbol = symbol

        output.append(
            {
                "symbol": symbol,
                "binance_symbol": binance_symbol,
                "hyperliquid_symbol": hyperliquid_symbol,
                "collect_spot": _to_bool(raw.get("collect_spot"), True),
                "collect_perp": _to_bool(raw.get("collect_perp"), True),
                "binance_enabled": _to_bool(raw.get("binance_enabled"), True),
                "hyperliquid_enabled": _to_bool(raw.get("hyperliquid_enabled"), True),
            }
        )

    if not output:
        return _default_pairs()
    return output


def _resolve_crypto_marketdata_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    root = cfg.get("crypto_marketdata", {})
    if not isinstance(root, dict):
        root = {}

    binance_cfg = root.get("binance", {})
    if not isinstance(binance_cfg, dict):
        binance_cfg = {}
    hyper_cfg = root.get("hyperliquid", {})
    if not isinstance(hyper_cfg, dict):
        hyper_cfg = {}
    dq_cfg = root.get("data_quality", {})
    if not isinstance(dq_cfg, dict):
        dq_cfg = {}

    return {
        "enabled": _to_bool(root.get("enabled"), True),
        "run_minute_jst": _to_int(root.get("run_minute_jst"), 3, minimum=0, maximum=59),
        "quality_window_sec": _to_int(root.get("quality_window_sec"), 60, minimum=10),
        "pairs": _resolve_pairs(root.get("pairs")),
        "binance": {
            "enabled": _to_bool(binance_cfg.get("enabled"), True),
            "rest_timeout_sec": _to_float(binance_cfg.get("rest_timeout_sec"), 5.0, minimum=1.0),
            "ws_timeout_sec": _to_float(binance_cfg.get("ws_timeout_sec"), 1.5, minimum=0.2),
        },
        "hyperliquid": {
            "enabled": _to_bool(hyper_cfg.get("enabled"), True),
            "rest_timeout_sec": _to_float(hyper_cfg.get("rest_timeout_sec"), 5.0, minimum=1.0),
            "ws_timeout_sec": _to_float(hyper_cfg.get("ws_timeout_sec"), 1.5, minimum=0.2),
        },
        "data_quality": {
            "max_missing_ratio": _to_float(dq_cfg.get("max_missing_ratio"), 0.25, minimum=0.0, maximum=1.0),
            "max_latency_ms": _to_float(dq_cfg.get("max_latency_ms"), 3000.0, minimum=1.0),
            "lookback_minutes": _to_int(dq_cfg.get("lookback_minutes"), 60, minimum=1),
        },
    }


def _required_fields_for_quality(snapshot: CryptoMarketSnapshot) -> tuple[str, ...]:
    if snapshot.market_type == "perp":
        return ("mid", "spread_bps", "funding_rate")
    return ("mid", "spread_bps")


def _build_quality_snapshot(
    snapshot: CryptoMarketSnapshot,
    *,
    quality_window_sec: int,
    max_missing_ratio: float,
    max_latency_ms: float,
) -> CryptoDataQualitySnapshot:
    required_fields = _required_fields_for_quality(snapshot)
    sample_count = len(required_fields)
    missing_count = 0
    for field_name in required_fields:
        if getattr(snapshot, field_name) is None:
            missing_count += 1
    missing_ratio = float(missing_count / sample_count) if sample_count > 0 else 0.0

    latency_p95_ms = float(snapshot.latency_ms) if snapshot.latency_ms is not None else None
    latency_for_gate = latency_p95_ms if latency_p95_ms is not None else (max_latency_ms + 1.0)
    eligible_for_edge = missing_ratio <= max_missing_ratio and latency_for_gate <= max_latency_ms

    ws_failover_count = 0
    if bool((snapshot.data_quality or {}).get("ws_failed")):
        ws_failover_count = 1

    return CryptoDataQualitySnapshot(
        exchange=snapshot.exchange,
        symbol=snapshot.symbol,
        market_type=snapshot.market_type,
        window_start=snapshot.observed_at - timedelta(seconds=max(10, int(quality_window_sec))),
        window_end=snapshot.observed_at,
        sample_count=sample_count,
        missing_count=missing_count,
        missing_ratio=missing_ratio,
        latency_p95_ms=latency_p95_ms,
        ws_failover_count=ws_failover_count,
        eligible_for_edge=eligible_for_edge,
        details={
            "market_type": snapshot.market_type,
            "source_mode": snapshot.source_mode,
            "required_fields": list(required_fields),
        },
    )


def run_crypto_marketdata(
    now: datetime | None = None,
) -> dict[str, Any]:
    cfg = load_yaml_config()
    resolved_cfg = _resolve_crypto_marketdata_cfg(cfg)
    if not bool(resolved_cfg["enabled"]):
        return {"enabled": False, "inserted_snapshots": 0, "inserted_data_quality": 0, "eligible_for_edge": 0}

    if now is None:
        observed_now_utc = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        observed_now_utc = now.replace(tzinfo=timezone.utc)
    else:
        observed_now_utc = now.astimezone(timezone.utc)

    tz_name = str(cfg.get("timezone", "Asia/Tokyo"))
    observed_now_jst = observed_now_utc.astimezone(ZoneInfo(tz_name))

    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)

    binance_cfg = resolved_cfg["binance"]
    hyper_cfg = resolved_cfg["hyperliquid"]
    binance_client = None
    hyperliquid_client = None
    if bool(binance_cfg["enabled"]):
        binance_client = BinanceMarketClient(
            rest_timeout_sec=float(binance_cfg["rest_timeout_sec"]),
            ws_timeout_sec=float(binance_cfg["ws_timeout_sec"]),
        )
    if bool(hyper_cfg["enabled"]):
        hyperliquid_client = HyperliquidMarketClient(
            rest_timeout_sec=float(hyper_cfg["rest_timeout_sec"]),
            ws_timeout_sec=float(hyper_cfg["ws_timeout_sec"]),
        )

    snapshots: list[CryptoMarketSnapshot] = []
    errors: list[dict[str, str]] = []
    for pair in resolved_cfg["pairs"]:
        if binance_client is not None and bool(pair.get("binance_enabled", True)):
            symbol = str(pair["binance_symbol"]).strip().upper()
            if bool(pair["collect_spot"]):
                try:
                    snapshots.append(
                        binance_client.fetch_market_snapshot(
                            symbol=symbol,
                            market_type="spot",
                            observed_at=observed_now_utc,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append({"exchange": "binance", "symbol": symbol, "market_type": "spot", "error": str(exc)})
            if bool(pair["collect_perp"]):
                try:
                    snapshots.append(
                        binance_client.fetch_market_snapshot(
                            symbol=symbol,
                            market_type="perp",
                            observed_at=observed_now_utc,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append({"exchange": "binance", "symbol": symbol, "market_type": "perp", "error": str(exc)})

        if hyperliquid_client is not None and bool(pair.get("hyperliquid_enabled", True)) and bool(pair["collect_perp"]):
            symbol = str(pair["hyperliquid_symbol"]).strip().upper()
            try:
                snapshots.append(
                    hyperliquid_client.fetch_market_snapshot(
                        symbol=symbol,
                        market_type="perp",
                        observed_at=observed_now_utc,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                errors.append({"exchange": "hyperliquid", "symbol": symbol, "market_type": "perp", "error": str(exc)})

    inserted_snapshots = repo.insert_crypto_market_snapshots(snapshots)

    dq_cfg = resolved_cfg["data_quality"]
    quality_rows = [
        _build_quality_snapshot(
            snapshot,
            quality_window_sec=int(resolved_cfg["quality_window_sec"]),
            max_missing_ratio=float(dq_cfg["max_missing_ratio"]),
            max_latency_ms=float(dq_cfg["max_latency_ms"]),
        )
        for snapshot in snapshots
    ]
    inserted_data_quality = repo.insert_crypto_data_quality_snapshots(quality_rows)

    eligible_rows = repo.fetch_crypto_market_inputs_for_edge(
        max_missing_ratio=float(dq_cfg["max_missing_ratio"]),
        max_latency_ms=float(dq_cfg["max_latency_ms"]),
        lookback_minutes=int(dq_cfg["lookback_minutes"]),
        limit=500,
    )

    return {
        "enabled": True,
        "observed_at_jst": observed_now_jst.isoformat(),
        "inserted_snapshots": inserted_snapshots,
        "inserted_data_quality": inserted_data_quality,
        "eligible_for_edge": len(eligible_rows),
        "errors": errors,
    }
