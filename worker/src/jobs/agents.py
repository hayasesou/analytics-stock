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

from src.config import load_runtime_secrets, load_yaml_config
from src.storage.db import NeonRepository

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
    if not upper:
        return False
    return any(upper.startswith(prefix) for prefix in OPENCLAW_FORBIDDEN_ENV_PREFIXES)


def build_openclaw_subprocess_env(source_env: dict[str, str] | None = None) -> dict[str, str]:
    src = source_env or dict(os.environ)
    output: dict[str, str] = {}
    for key, value in src.items():
        if _is_forbidden_openclaw_env_key(key):
            continue
        key_upper = str(key).strip().upper()
        if key_upper in OPENCLAW_ENV_ALLOWLIST or key_upper.startswith("OPENCLAW_"):
            output[key] = value
    return output


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
    allowed = {
        "strategy_name",
        "security_id",
        "combined_score",
        "market",
        "asset_scope",
        "agent_role",
        "run_id",
    }
    output: dict[str, Any] = {}
    for key in allowed:
        if key in payload:
            output[key] = payload[key]
    return output


def _stable_result_hash(result: dict[str, Any]) -> str:
    def _sanitize(value: Any) -> Any:
        if isinstance(value, dict):
            out: dict[str, Any] = {}
            for key, item in sorted(value.items()):
                if key in {"timestamp", "latency_ms"}:
                    continue
                out[key] = _sanitize(item)
            return out
        if isinstance(value, list):
            return [_sanitize(x) for x in value]
        return value

    normalized = _sanitize(result)
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _invoke_openclaw_command(
    command: str,
    payload: dict[str, Any],
    timeout_sec: float,
) -> dict[str, Any]:
    env = build_openclaw_subprocess_env()
    completed = subprocess.run(
        command,
        input=json.dumps(payload, ensure_ascii=False),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=True,
        env=env,
        timeout=max(1.0, timeout_sec),
        check=False,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()[:500]
        raise RuntimeError(f"openclaw command failed rc={completed.returncode}: {stderr}")
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
    base = {
        "strategy_design": 0.06,
        "coding": 0.08,
        "feature_engineering": 0.05,
        "risk_evaluation": 0.04,
        "orchestration": 0.02,
        "idea_analysis": 0.05,
    }
    return base.get(task_type, 0.03)


def _process_payload(task_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    strategy_name = str(payload.get("strategy_name", "unknown"))
    security_id = str(payload.get("security_id", "unknown"))
    combined_score = float(payload.get("combined_score") or 0.0)

    if task_type == "strategy_design":
        return {
            "timestamp": now,
            "summary": f"Designed hypothesis for {strategy_name}",
            "thesis": f"{security_id} score momentum follow-through",
            "horizon": "5D",
        }
    if task_type == "coding":
        return {
            "timestamp": now,
            "summary": f"Generated strategy code template for {strategy_name}",
            "artifact_key": f"strategies/{strategy_name}/v1.py",
        }
    if task_type == "feature_engineering":
        return {
            "timestamp": now,
            "summary": f"Selected feature set for {strategy_name}",
            "features": ["ret_5d", "ret_20d", "vol_20d", "dollar_volume_20d", "fundamental_rating"],
        }
    if task_type == "risk_evaluation":
        risk_level = "medium" if combined_score >= 60 else "high"
        return {
            "timestamp": now,
            "summary": f"Risk review for {strategy_name}",
            "risk_level": risk_level,
            "max_drawdown_cap": -0.03,
            "min_sharpe_20d": 0.0,
        }
    if task_type == "orchestration":
        return {
            "timestamp": now,
            "summary": f"Orchestrated pipeline for {strategy_name}",
            "next": "manual_review",
        }
    if task_type == "idea_analysis":
        idea_id = str(payload.get("idea_id", "unknown"))
        source_url = str(payload.get("source_url", "")).strip()
        claim_text = str(payload.get("claim_text", "")).strip()
        causal_hypothesis = str(payload.get("causal_hypothesis", "")).strip()
        raw_tickers = payload.get("ticker_candidates")
        ticker_candidates = []
        if isinstance(raw_tickers, list):
            ticker_candidates = [str(item).strip().upper() for item in raw_tickers if str(item).strip()]
        raw_edges = payload.get("extracted_edges")
        edge_count = len(raw_edges) if isinstance(raw_edges, list) else 0
        return {
            "timestamp": now,
            "summary": f"Analyzed idea {idea_id}",
            "idea_id": idea_id,
            "source_url": source_url,
            "ticker_candidates": ticker_candidates[:10],
            "claim_text": claim_text,
            "causal_hypothesis": causal_hypothesis,
            "edge_count": edge_count,
            "next": "queue_experiment_design",
        }
    return {
        "timestamp": now,
        "summary": f"Processed task {task_type}",
    }


def _process_payload_openclaw_simulated(payload: dict[str, Any], deterministic: bool) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    strategy_name = str(payload.get("strategy_name", "unknown"))
    security_id = str(payload.get("security_id", "unknown"))
    combined_score = _to_float(payload.get("combined_score"), 0.0)
    variant = "base"
    if not deterministic:
        variant = random.choice(["base", "alt-a", "alt-b"])
    return {
        "timestamp": now,
        "provider": "openclaw_poc",
        "summary": f"OpenClaw PoC designed hypothesis for {strategy_name}",
        "thesis": f"{security_id} short-horizon continuation (variant={variant})",
        "horizon": "5D",
        "combined_score": round(combined_score, 4),
    }


class BuiltinTaskAdapter:
    provider_name = "builtin"

    def execute(self, task_type: str, payload: dict[str, Any]) -> AdapterOutcome:
        started = time.perf_counter()
        result = _process_payload(task_type, payload)
        latency_ms = (time.perf_counter() - started) * 1000.0
        jitter = random.uniform(0.0, 0.01)
        result["provider"] = self.provider_name
        return AdapterOutcome(
            provider=self.provider_name,
            result=result,
            cost_usd=round(_estimate_cost_usd(task_type) + jitter, 4),
            latency_ms=latency_ms,
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
                    threshold = int(self.simulated_failure_rate * 1_000_000)
                    should_fail = (failure_seed % 1_000_000) < threshold
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

            if self.command:
                raw_result = _invoke_openclaw_command(
                    command=str(self.command),
                    payload=sanitized_payload,
                    timeout_sec=self.timeout_sec,
                )
            else:
                raw_result = _process_payload_openclaw_simulated(
                    payload=sanitized_payload,
                    deterministic=self.deterministic,
                )
            raw_result["provider"] = self.provider_name
            total_latency_ms = (time.perf_counter() - started) * 1000.0
            return AdapterOutcome(
                provider=self.provider_name,
                result=raw_result,
                cost_usd=round(self.cost_usd_per_task, 4),
                latency_ms=total_latency_ms,
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
    if mode != "openclaw_poc":
        return "builtin", builtin_adapter
    if openclaw_adapter is None or not bool(openclaw_cfg.get("enabled", False)):
        return "builtin", builtin_adapter
    strategy_design_only = bool(openclaw_cfg.get("strategy_design_only", True))
    if strategy_design_only and task_type not in OPENCLAW_SUPPORTED_TASK_TYPES:
        return "builtin", builtin_adapter
    return "openclaw_poc", openclaw_adapter


def run_agents_once(limit: int = 20) -> dict[str, int]:
    cfg = load_yaml_config()
    agents_cfg = _resolve_agents_cfg(cfg)
    secrets = load_runtime_secrets()
    repo = NeonRepository(secrets.database_url)

    builtin_adapter = BuiltinTaskAdapter()
    openclaw_adapter: OpenClawTaskAdapter | None = None
    if bool(agents_cfg["openclaw_poc"].get("enabled", False)):
        openclaw_adapter = OpenClawTaskAdapter(agents_cfg["openclaw_poc"])

    tasks = repo.fetch_queued_agent_tasks(limit=limit, task_types=list(DEFAULT_AGENT_TASK_TYPES))
    stats = {
        "queued": len(tasks),
        "processed": 0,
        "success": 0,
        "failed": 0,
        "openclaw_processed": 0,
        "openclaw_failed": 0,
        "openclaw_fallback": 0,
    }
    for task in tasks:
        task_id = str(task["id"])
        task_type = str(task["task_type"])
        payload = task.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        stats["processed"] += 1
        adapter_name, adapter = _select_adapter_for_task(
            task_type=task_type,
            cfg=agents_cfg,
            builtin_adapter=builtin_adapter,
            openclaw_adapter=openclaw_adapter,
        )
        if adapter_name == "openclaw_poc":
            stats["openclaw_processed"] += 1
        try:
            repo.mark_agent_task(task_id=task_id, status="running")
            try:
                outcome = adapter.execute(task_type, payload)
            except Exception as adapter_exc:  # noqa: BLE001
                fallback_enabled = bool(agents_cfg["openclaw_poc"].get("fallback_to_builtin_on_error", True))
                if adapter_name == "openclaw_poc" and fallback_enabled:
                    stats["openclaw_fallback"] += 1
                    outcome = builtin_adapter.execute(task_type, payload)
                    outcome.result["adapter_error"] = str(adapter_exc)
                    outcome.result["fallback_from"] = "openclaw_poc"
                else:
                    raise
            repo.mark_agent_task(task_id=task_id, status="success", result=outcome.result, cost_usd=outcome.cost_usd)
            stats["success"] += 1
        except Exception as exc:  # noqa: BLE001
            if adapter_name == "openclaw_poc":
                stats["openclaw_failed"] += 1
            repo.mark_agent_task(
                task_id=task_id,
                status="failed",
                result={
                    "error": str(exc),
                    "provider": adapter_name,
                },
                cost_usd=_estimate_cost_usd(task_type),
            )
            stats["failed"] += 1
    return stats


def _evaluate_adapter_runs(
    adapter_name: str,
    adapter: Any,
    *,
    samples: int,
    attempts_per_sample: int,
) -> dict[str, Any]:
    latencies: list[float] = []
    costs: list[float] = []
    retries: list[int] = []
    failed = 0
    total = 0
    reproducibility_trials = 0
    reproducibility_matches = 0

    for idx in range(samples):
        payload = {
            "strategy_name": f"sf-eval-{idx:03d}",
            "security_id": f"JP:{1000 + idx}",
            "combined_score": 70.0 + (idx % 7),
        }
        baseline_hash: str | None = None
        for _attempt in range(attempts_per_sample):
            total += 1
            try:
                outcome = adapter.execute("strategy_design", payload)
                latencies.append(float(outcome.latency_ms))
                costs.append(float(outcome.cost_usd))
                retries.append(int(outcome.retries))
                result_hash = _stable_result_hash(outcome.result)
                if baseline_hash is None:
                    baseline_hash = result_hash
                else:
                    reproducibility_trials += 1
                    if baseline_hash == result_hash:
                        reproducibility_matches += 1
            except Exception:  # noqa: BLE001
                failed += 1
                if baseline_hash is not None:
                    reproducibility_trials += 1

    success = total - failed
    reproducibility_rate = (
        (reproducibility_matches / reproducibility_trials)
        if reproducibility_trials > 0
        else 1.0
    )
    avg_latency_ms = (sum(latencies) / len(latencies)) if latencies else None
    avg_cost_usd = (sum(costs) / len(costs)) if costs else None
    avg_retries = (sum(retries) / len(retries)) if retries else 0.0
    return {
        "adapter": adapter_name,
        "samples": samples,
        "attempts_per_sample": attempts_per_sample,
        "total_runs": total,
        "success": success,
        "failed": failed,
        "failure_rate": (failed / total) if total > 0 else 1.0,
        "avg_latency_ms": avg_latency_ms,
        "avg_cost_usd": avg_cost_usd,
        "avg_retries": avg_retries,
        "reproducibility_rate": reproducibility_rate,
    }


def _decide_openclaw_go_no_go(
    *,
    builtin_metrics: dict[str, Any],
    openclaw_metrics: dict[str, Any],
    security_boundary: dict[str, Any],
    thresholds: dict[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if not bool(security_boundary.get("ok", False)):
        reasons.append("security_boundary_failed")

    failure_rate = _to_float(openclaw_metrics.get("failure_rate"), 1.0)
    if failure_rate > _to_float(thresholds.get("max_failure_rate"), 0.10):
        reasons.append("failure_rate_too_high")

    reproducibility = _to_float(openclaw_metrics.get("reproducibility_rate"), 0.0)
    if reproducibility < _to_float(thresholds.get("min_reproducibility"), 0.85):
        reasons.append("reproducibility_too_low")

    builtin_latency = _to_float(builtin_metrics.get("avg_latency_ms"), 1.0)
    openclaw_latency = _to_float(openclaw_metrics.get("avg_latency_ms"), 9_999_999.0)
    latency_ratio = openclaw_latency / builtin_latency if builtin_latency > 0 else 9_999_999.0
    if latency_ratio > _to_float(thresholds.get("max_latency_ratio_vs_builtin"), 2.0):
        reasons.append("latency_ratio_too_high")

    builtin_cost = _to_float(builtin_metrics.get("avg_cost_usd"), 0.0001)
    openclaw_cost = _to_float(openclaw_metrics.get("avg_cost_usd"), 9_999_999.0)
    cost_ratio = openclaw_cost / builtin_cost if builtin_cost > 0 else 9_999_999.0
    if cost_ratio > _to_float(thresholds.get("max_cost_ratio_vs_builtin"), 1.5):
        reasons.append("cost_ratio_too_high")

    if reasons:
        return "NO_GO", reasons
    return "LIMITED_GO", [
        "strategy_design_only",
        "gateway_key_isolation_confirmed",
    ]


def _render_openclaw_evaluation_markdown(summary: dict[str, Any]) -> str:
    now_iso = str(summary.get("evaluated_at"))
    builtin = summary.get("builtin", {})
    openclaw = summary.get("openclaw_poc", {})
    security = summary.get("security_boundary", {})
    decision = str(summary.get("decision", "NO_GO"))
    reasons = summary.get("decision_reasons", [])
    if not isinstance(reasons, list):
        reasons = []

    def _fmt_num(value: Any, digits: int = 3) -> str:
        if value is None:
            return "-"
        try:
            return f"{float(value):.{digits}f}"
        except (TypeError, ValueError):
            return "-"

    lines = [
        "# OpenClaw Evaluation Memo",
        "",
        f"- EvaluatedAt: {now_iso}",
        f"- Decision: **{decision}**",
        f"- Reasons: {', '.join(str(x) for x in reasons) if reasons else '-'}",
        "",
        "## 1. Task Model Comparison",
        "",
        "| Item | Current agents (`agent_tasks`) | OpenClaw PoC Adapter |",
        "| --- | --- | --- |",
        "| Unit of work | DB queued task | DB queued task (compatible) |",
        "| Scope in PoC | all roles | `strategy_design` only |",
        "| Failure handling | single try | retry + optional fallback |",
        "| Result storage | `agent_tasks.result` | same (`provider=openclaw_poc`) |",
        "| Execution key access | worker env | sanitized env (gateway keys stripped) |",
        "",
        "## 2. Benchmark Results",
        "",
        "| Metric | Builtin | OpenClaw PoC |",
        "| --- | ---: | ---: |",
        f"| total_runs | {builtin.get('total_runs', '-')} | {openclaw.get('total_runs', '-')} |",
        f"| failure_rate | {_fmt_num(builtin.get('failure_rate'))} | {_fmt_num(openclaw.get('failure_rate'))} |",
        f"| avg_latency_ms | {_fmt_num(builtin.get('avg_latency_ms'))} | {_fmt_num(openclaw.get('avg_latency_ms'))} |",
        f"| avg_cost_usd | {_fmt_num(builtin.get('avg_cost_usd'), 4)} | {_fmt_num(openclaw.get('avg_cost_usd'), 4)} |",
        f"| reproducibility_rate | {_fmt_num(builtin.get('reproducibility_rate'))} | {_fmt_num(openclaw.get('reproducibility_rate'))} |",
        "",
        "## 3. Security Boundary Verification",
        "",
        f"- boundary_ok: {bool(security.get('ok', False))}",
        f"- raw_gateway_keys_detected: {len(security.get('raw_gateway_keys_detected', []))}",
        f"- sanitized_forbidden_keys: {len(security.get('sanitized_forbidden_keys', []))}",
        "",
        "## 4. Recommendation",
        "",
        "- `LIMITED_GO`: keep PoC limited to `strategy_design`, keep gateway separation strict, keep fallback on error.",
        "- `NO_GO`: do not use OpenClaw adapter in production loop; continue builtin extension.",
    ]
    return "\n".join(lines).strip() + "\n"


def run_openclaw_evaluation() -> dict[str, Any]:
    cfg = load_yaml_config()
    agents_cfg = _resolve_agents_cfg(cfg)
    eval_cfg = agents_cfg["evaluation"]
    openclaw_cfg = dict(agents_cfg["openclaw_poc"])
    openclaw_cfg["enabled"] = True
    openclaw_cfg["fallback_to_builtin_on_error"] = False

    builtin = BuiltinTaskAdapter()
    openclaw = OpenClawTaskAdapter(openclaw_cfg)

    samples = int(eval_cfg["samples"])
    attempts = int(eval_cfg["attempts_per_sample"])
    builtin_metrics = _evaluate_adapter_runs("builtin", builtin, samples=samples, attempts_per_sample=attempts)
    openclaw_metrics = _evaluate_adapter_runs("openclaw_poc", openclaw, samples=samples, attempts_per_sample=attempts)
    security_boundary = evaluate_openclaw_security_boundary()
    decision, reasons = _decide_openclaw_go_no_go(
        builtin_metrics=builtin_metrics,
        openclaw_metrics=openclaw_metrics,
        security_boundary=security_boundary,
        thresholds=dict(eval_cfg.get("go_no_go", {})),
    )

    summary = {
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "builtin": builtin_metrics,
        "openclaw_poc": openclaw_metrics,
        "security_boundary": security_boundary,
        "decision": decision,
        "decision_reasons": reasons,
    }
    output_path = str(eval_cfg.get("output_path", "docs/openclaw-evaluation.md"))
    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(_render_openclaw_evaluation_markdown(summary))
    summary["output_path"] = output_path
    return summary


def run_agents(poll_seconds: int = 20, batch_limit: int = 20) -> None:
    while True:
        stats = run_agents_once(limit=batch_limit)
        print(
            "[agents] queued=%s processed=%s success=%s failed=%s openclaw_processed=%s openclaw_failed=%s openclaw_fallback=%s"
            % (
                stats["queued"],
                stats["processed"],
                stats["success"],
                stats["failed"],
                stats.get("openclaw_processed", 0),
                stats.get("openclaw_failed", 0),
                stats.get("openclaw_fallback", 0),
            ),
            flush=True,
        )
        time.sleep(max(5, int(poll_seconds)))
