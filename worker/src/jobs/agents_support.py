from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
import random
import subprocess
import time
from typing import Any

OPENCLAW_SUPPORTED_TASK_TYPES = {"strategy_design"}
DEFAULT_AGENT_TASK_TYPES = [
    "strategy_design",
    "coding",
    "feature_engineering",
    "risk_evaluation",
    "orchestration",
    "idea_analysis",
]
OPENCLAW_FORBIDDEN_ENV_PREFIXES = (
    "GATEWAY_",
    "CRYPTO_GATEWAY_",
    "JP_GATEWAY_",
    "US_GATEWAY_",
    "KABU_",
    "IBKR_",
    "BINANCE_",
    "HYPERLIQUID_",
)
OPENCLAW_ENV_ALLOWLIST = {"PATH", "HOME", "LANG", "LC_ALL", "PYTHONPATH", "PYTHONUNBUFFERED", "TMPDIR"}


@dataclass(frozen=True)
class AdapterOutcome:
    provider: str
    result: dict[str, Any]
    cost_usd: float
    latency_ms: float
    retries: int = 0


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, parsed)


def _to_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _resolve_agents_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    agents_cfg = cfg.get("agents", {})
    if not isinstance(agents_cfg, dict):
        agents_cfg = {}
    adapter_cfg = agents_cfg.get("adapter", {})
    if not isinstance(adapter_cfg, dict):
        adapter_cfg = {}
    openclaw_cfg = adapter_cfg.get("openclaw_poc", {})
    if not isinstance(openclaw_cfg, dict):
        openclaw_cfg = {}
    evaluation_cfg = agents_cfg.get("evaluation", {})
    if not isinstance(evaluation_cfg, dict):
        evaluation_cfg = {}
    go_no_go_cfg = evaluation_cfg.get("go_no_go", {})
    if not isinstance(go_no_go_cfg, dict):
        go_no_go_cfg = {}
    return {
        "adapter_mode": str(adapter_cfg.get("mode", "builtin")).strip().lower() or "builtin",
        "openclaw_poc": {
            "enabled": bool(openclaw_cfg.get("enabled", False)),
            "strategy_design_only": bool(openclaw_cfg.get("strategy_design_only", True)),
            "max_retries": _to_int(openclaw_cfg.get("max_retries"), 2, minimum=0),
            "retry_backoff_sec": max(0.0, _to_float(openclaw_cfg.get("retry_backoff_sec"), 0.05)),
            "simulated_latency_ms": max(0.0, _to_float(openclaw_cfg.get("simulated_latency_ms"), 120.0)),
            "simulated_jitter_ms": max(0.0, _to_float(openclaw_cfg.get("simulated_jitter_ms"), 30.0)),
            "simulated_failure_rate": min(1.0, max(0.0, _to_float(openclaw_cfg.get("simulated_failure_rate"), 0.1))),
            "deterministic": bool(openclaw_cfg.get("deterministic", True)),
            "cost_usd_per_task": max(0.0, _to_float(openclaw_cfg.get("cost_usd_per_task"), 0.07)),
            "fallback_to_builtin_on_error": bool(openclaw_cfg.get("fallback_to_builtin_on_error", True)),
            "command": str(openclaw_cfg.get("command", "")).strip() or None,
            "timeout_sec": max(1.0, _to_float(openclaw_cfg.get("timeout_sec"), 8.0)),
        },
        "evaluation": {
            "samples": _to_int(evaluation_cfg.get("samples"), 20, minimum=1),
            "attempts_per_sample": _to_int(evaluation_cfg.get("attempts_per_sample"), 3, minimum=1),
            "output_path": str(evaluation_cfg.get("output_path", "docs/openclaw-evaluation.md")).strip()
            or "docs/openclaw-evaluation.md",
            "go_no_go": {
                "max_failure_rate": min(1.0, max(0.0, _to_float(go_no_go_cfg.get("max_failure_rate"), 0.10))),
                "min_reproducibility": min(1.0, max(0.0, _to_float(go_no_go_cfg.get("min_reproducibility"), 0.85))),
                "max_latency_ratio_vs_builtin": max(0.1, _to_float(go_no_go_cfg.get("max_latency_ratio_vs_builtin"), 2.0)),
                "max_cost_ratio_vs_builtin": max(0.1, _to_float(go_no_go_cfg.get("max_cost_ratio_vs_builtin"), 1.5)),
            },
        },
    }


def _is_forbidden_openclaw_env_key(name: str) -> bool:
    upper = str(name).strip().upper()
    return bool(upper) and any(upper.startswith(prefix) for prefix in OPENCLAW_FORBIDDEN_ENV_PREFIXES)


def build_openclaw_subprocess_env(source_env: dict[str, str] | None = None) -> dict[str, str]:
    src = source_env or dict(os.environ)
    return {
        key: value
        for key, value in src.items()
        if not _is_forbidden_openclaw_env_key(key)
        and (str(key).strip().upper() in OPENCLAW_ENV_ALLOWLIST or str(key).strip().upper().startswith("OPENCLAW_"))
    }


def evaluate_openclaw_security_boundary(source_env: dict[str, str] | None = None) -> dict[str, Any]:
    raw = source_env or dict(os.environ)
    sanitized = build_openclaw_subprocess_env(raw)
    forbidden_keys = [key for key in sanitized if _is_forbidden_openclaw_env_key(key)]
    raw_gateway_keys = [key for key in raw if _is_forbidden_openclaw_env_key(key)]
    return {
        "ok": len(forbidden_keys) == 0,
        "raw_gateway_keys_detected": sorted(raw_gateway_keys),
        "sanitized_forbidden_keys": sorted(forbidden_keys),
        "sanitized_env_size": len(sanitized),
    }


def _sanitize_openclaw_payload(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = {"strategy_name", "security_id", "combined_score", "market", "asset_scope", "agent_role", "run_id"}
    return {key: payload[key] for key in allowed if key in payload}


def _stable_result_hash(result: dict[str, Any]) -> str:
    def _sanitize(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: _sanitize(item) for key, item in sorted(value.items()) if key not in {"timestamp", "latency_ms"}}
        if isinstance(value, list):
            return [_sanitize(item) for item in value]
        return value

    encoded = json.dumps(_sanitize(result), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _invoke_openclaw_command(command: str, payload: dict[str, Any], timeout_sec: float) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        input=json.dumps(payload, ensure_ascii=False),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True,
        env=build_openclaw_subprocess_env(),
        timeout=max(1.0, timeout_sec),
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"openclaw command failed rc={completed.returncode}: {completed.stderr.strip()[:500]}")
    body = completed.stdout.strip()
    if not body:
        raise RuntimeError("openclaw command returned empty output")
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"openclaw output is not valid json: {exc}") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError("openclaw output must be a json object")
    return decoded


def _estimate_cost_usd(task_type: str) -> float:
    return {
        "strategy_design": 0.06,
        "coding": 0.08,
        "feature_engineering": 0.05,
        "risk_evaluation": 0.04,
        "orchestration": 0.02,
        "idea_analysis": 0.05,
    }.get(task_type, 0.03)


def _process_payload(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    strategy_name = str(payload.get("strategy_name", "unknown"))
    security_id = str(payload.get("security_id", "unknown"))
    combined_score = float(payload.get("combined_score") or 0.0)
    if task_type == "strategy_design":
        return {"timestamp": now, "summary": f"Designed hypothesis for {strategy_name}", "thesis": f"{security_id} score momentum follow-through", "horizon": "5D"}
    if task_type == "coding":
        return {"timestamp": now, "summary": f"Generated strategy code template for {strategy_name}", "artifact_key": f"strategies/{strategy_name}/v1.py"}
    if task_type == "feature_engineering":
        return {"timestamp": now, "summary": f"Selected feature set for {strategy_name}", "features": ["ret_5d", "ret_20d", "vol_20d", "dollar_volume_20d", "fundamental_rating"]}
    if task_type == "risk_evaluation":
        return {"timestamp": now, "summary": f"Risk review for {strategy_name}", "risk_level": "medium" if combined_score >= 60 else "high", "max_drawdown_cap": -0.03, "min_sharpe_20d": 0.0}
    if task_type == "orchestration":
        return {"timestamp": now, "summary": f"Orchestrated pipeline for {strategy_name}", "next": "manual_review"}
    if task_type == "idea_analysis":
        raw_tickers = payload.get("ticker_candidates")
        ticker_candidates = [str(item).strip().upper() for item in raw_tickers if str(item).strip()] if isinstance(raw_tickers, list) else []
        raw_edges = payload.get("extracted_edges")
        return {
            "timestamp": now,
            "summary": f"Analyzed idea {str(payload.get('idea_id', 'unknown'))}",
            "idea_id": str(payload.get("idea_id", "unknown")),
            "source_url": str(payload.get("source_url", "")).strip(),
            "ticker_candidates": ticker_candidates[:10],
            "claim_text": str(payload.get("claim_text", "")).strip(),
            "causal_hypothesis": str(payload.get("causal_hypothesis", "")).strip(),
            "edge_count": len(raw_edges) if isinstance(raw_edges, list) else 0,
            "next": "queue_experiment_design",
        }
    return {"timestamp": now, "summary": f"Processed task {task_type}"}


def _process_payload_openclaw_simulated(payload: dict[str, Any], deterministic: bool) -> dict[str, Any]:
    variant = "base" if deterministic else random.choice(["base", "alt-a", "alt-b"])
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "provider": "openclaw_poc",
        "summary": f"OpenClaw PoC designed hypothesis for {str(payload.get('strategy_name', 'unknown'))}",
        "thesis": f"{str(payload.get('security_id', 'unknown'))} short-horizon continuation (variant={variant})",
        "horizon": "5D",
        "combined_score": round(_to_float(payload.get("combined_score"), 0.0), 4),
    }


class BuiltinTaskAdapter:
    provider_name = "builtin"

    def execute(self, task_type: str, payload: dict[str, Any]) -> AdapterOutcome:
        started = time.perf_counter()
        result = _process_payload(task_type, payload)
        result["provider"] = self.provider_name
        return AdapterOutcome(
            provider=self.provider_name,
            result=result,
            cost_usd=round(_estimate_cost_usd(task_type) + random.uniform(0.0, 0.01), 4),
            latency_ms=(time.perf_counter() - started) * 1000.0,
            retries=0,
        )


class OpenClawTaskAdapter:
    provider_name = "openclaw_poc"

    def __init__(self, cfg: dict[str, Any]):
        self.max_retries = _to_int(cfg.get("max_retries"), 2, minimum=0)
        self.retry_backoff_sec = max(0.0, _to_float(cfg.get("retry_backoff_sec"), 0.05))
        self.simulated_latency_ms = max(0.0, _to_float(cfg.get("simulated_latency_ms"), 120.0))
        self.simulated_jitter_ms = max(0.0, _to_float(cfg.get("simulated_jitter_ms"), 30.0))
        self.simulated_failure_rate = min(1.0, max(0.0, _to_float(cfg.get("simulated_failure_rate"), 0.1)))
        self.deterministic = _to_bool(cfg.get("deterministic"), True)
        self.cost_usd_per_task = max(0.0, _to_float(cfg.get("cost_usd_per_task"), 0.07))
        self.command = cfg.get("command")
        self.timeout_sec = max(1.0, _to_float(cfg.get("timeout_sec"), 8.0))

    def _sample_latency(self) -> float:
        if self.deterministic:
            return self.simulated_latency_ms
        return max(0.0, self.simulated_latency_ms + random.uniform(0.0, self.simulated_jitter_ms))

    def execute(self, task_type: str, payload: dict[str, Any]) -> AdapterOutcome:
        if task_type not in OPENCLAW_SUPPORTED_TASK_TYPES:
            raise ValueError(f"openclaw_poc does not support task_type={task_type}")
        started = time.perf_counter()
        sanitized_payload = _sanitize_openclaw_payload(payload)
        retries = 0
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            latency_ms = self._sample_latency()
            if latency_ms > 0:
                time.sleep(latency_ms / 1000.0)
            should_fail = False
            if self.simulated_failure_rate > 0:
                if self.deterministic:
                    key = json.dumps(sanitized_payload, sort_keys=True, ensure_ascii=False)
                    failure_seed = int(hashlib.sha256(f"{key}:{attempt}".encode("utf-8")).hexdigest()[:8], 16)
                    should_fail = (failure_seed % 1_000_000) < int(self.simulated_failure_rate * 1_000_000)
                else:
                    should_fail = random.random() < self.simulated_failure_rate
            if should_fail:
                last_exc = RuntimeError(f"openclaw transient failure attempt={attempt + 1}")
                if attempt < self.max_retries:
                    retries += 1
                    if self.retry_backoff_sec > 0:
                        time.sleep(self.retry_backoff_sec)
                    continue
                break
            raw_result = (
                _invoke_openclaw_command(str(self.command), sanitized_payload, self.timeout_sec)
                if self.command
                else _process_payload_openclaw_simulated(sanitized_payload, self.deterministic)
            )
            raw_result["provider"] = self.provider_name
            return AdapterOutcome(
                provider=self.provider_name,
                result=raw_result,
                cost_usd=round(self.cost_usd_per_task, 4),
                latency_ms=(time.perf_counter() - started) * 1000.0,
                retries=retries,
            )
        if last_exc:
            raise last_exc
        raise RuntimeError("openclaw execution failed")


def _select_adapter_for_task(
    task_type: str,
    cfg: dict[str, Any],
    builtin_adapter: BuiltinTaskAdapter,
    openclaw_adapter: OpenClawTaskAdapter | None,
) -> tuple[str, Any]:
    mode = str(cfg.get("adapter_mode", "builtin"))
    openclaw_cfg = cfg.get("openclaw_poc", {})
    if mode != "openclaw_poc" or openclaw_adapter is None or not bool(openclaw_cfg.get("enabled", False)):
        return "builtin", builtin_adapter
    if bool(openclaw_cfg.get("strategy_design_only", True)) and task_type not in OPENCLAW_SUPPORTED_TASK_TYPES:
        return "builtin", builtin_adapter
    return "openclaw_poc", openclaw_adapter


__all__ = [
    "AdapterOutcome",
    "BuiltinTaskAdapter",
    "DEFAULT_AGENT_TASK_TYPES",
    "OPENCLAW_SUPPORTED_TASK_TYPES",
    "OpenClawTaskAdapter",
    "_estimate_cost_usd",
    "_invoke_openclaw_command",
    "_process_payload",
    "_process_payload_openclaw_simulated",
    "_resolve_agents_cfg",
    "_select_adapter_for_task",
    "_stable_result_hash",
    "_to_bool",
    "_to_float",
    "_to_int",
    "build_openclaw_subprocess_env",
    "evaluate_openclaw_security_boundary",
]
