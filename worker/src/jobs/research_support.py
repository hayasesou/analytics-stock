from __future__ import annotations

from datetime import date
from typing import Any

DEFAULT_AGENT_TASK_TYPES = [
    "strategy_design",
    "coding",
    "feature_engineering",
    "risk_evaluation",
    "orchestration",
]


def _to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_rating_set(values: Any, default: list[str]) -> set[str]:
    if not isinstance(values, list):
        values = list(default)
    normalized = {str(value).strip().upper() for value in values if str(value).strip()}
    return normalized or {str(value).strip().upper() for value in default}


def _strategy_status_for_rating(rating: str, overlay_cfg: dict[str, Any]) -> str:
    if not bool(overlay_cfg.get("enabled", True)):
        return "candidate"
    allow_ratings = _normalize_rating_set(overlay_cfg.get("screening_allow_ratings"), default=["A", "B"])
    candidate_status = str(overlay_cfg.get("screening_pass_status", "candidate"))
    blocked_status = str(overlay_cfg.get("screening_block_status", "draft"))
    return candidate_status if rating in allow_ratings else blocked_status


def _build_strategy_spec(row: dict[str, Any], as_of: date) -> dict[str, Any]:
    market = str(row.get("market", "JP"))
    security_id = str(row.get("security_id", "UNKNOWN"))
    return {
        "name": f"sf-{security_id.lower().replace(':', '-')}-v1",
        "asset_scope": "JP_EQ" if market == "JP" else "US_EQ",
        "as_of_date": as_of.isoformat(),
        "universe": {"security_id": security_id, "market": market},
        "signal": {
            "horizon": "5D",
            "features": ["ret_5d", "ret_20d", "vol_20d", "dollar_volume_20d", "missing_ratio", "fundamental_rating"],
            "model": {"type": "ensemble", "params": {"alpha": 0.5, "beta": 0.5}},
        },
        "risk": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
        "evaluation": {"metrics": ["sharpe", "max_dd", "cagr"], "gates": {"sharpe_min": 0.5, "max_dd_min": -0.2}},
    }


def _build_eval_metrics(row: dict[str, Any]) -> dict[str, float]:
    combined = float(row.get("combined_score") or 0.0)
    edge = float(row.get("edge_score") or 0.0)
    return {
        "sharpe": round((combined / 100.0) * 1.6 + (edge / 100.0) * 0.4, 4),
        "max_dd": round(-0.35 + (combined / 100.0) * 0.20, 4),
        "cagr": round((combined / 100.0) * 0.22, 4),
    }


def _extract_primary_validation_metrics(validation_result: dict[str, Any]) -> dict[str, Any]:
    gate = validation_result.get("gate") or {}
    summary = validation_result.get("summary") or {}
    primary_profile = str(gate.get("primary_cost_profile", "strict"))
    primary = summary.get(primary_profile) or {}
    return {
        "validation_passed": bool(gate.get("passed", False)),
        "validation_primary_profile": primary_profile,
        "validation_fold_count": int(primary.get("fold_count") or 0),
        "validation_total_trades": int(primary.get("total_trades") or 0),
        "validation_mean_sharpe": primary.get("mean_sharpe"),
        "validation_median_sharpe": primary.get("median_sharpe"),
        "validation_worst_max_dd": primary.get("worst_max_dd"),
        "validation_mean_cagr": primary.get("mean_cagr"),
        "validation_fail_reasons": list(gate.get("reasons") or []),
    }


__all__ = [
    "DEFAULT_AGENT_TASK_TYPES",
    "_build_eval_metrics",
    "_build_strategy_spec",
    "_extract_primary_validation_metrics",
    "_normalize_rating_set",
    "_strategy_status_for_rating",
    "_to_float_or_none",
]
