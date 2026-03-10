from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any

from src.jobs.agents_support import (
    BuiltinTaskAdapter,
    OpenClawTaskAdapter,
    _resolve_agents_cfg,
    _stable_result_hash,
    _to_float,
    evaluate_openclaw_security_boundary,
)


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
        payload = {"strategy_name": f"sf-eval-{idx:03d}", "security_id": f"JP:{1000 + idx}", "combined_score": 70.0 + (idx % 7)}
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
    return {
        "adapter": adapter_name,
        "samples": samples,
        "attempts_per_sample": attempts_per_sample,
        "total_runs": total,
        "success": success,
        "failed": failed,
        "failure_rate": (failed / total) if total > 0 else 1.0,
        "avg_latency_ms": (sum(latencies) / len(latencies)) if latencies else None,
        "avg_cost_usd": (sum(costs) / len(costs)) if costs else None,
        "avg_retries": (sum(retries) / len(retries)) if retries else 0.0,
        "reproducibility_rate": (reproducibility_matches / reproducibility_trials) if reproducibility_trials > 0 else 1.0,
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
    if _to_float(openclaw_metrics.get("failure_rate"), 1.0) > _to_float(thresholds.get("max_failure_rate"), 0.10):
        reasons.append("failure_rate_too_high")
    if _to_float(openclaw_metrics.get("reproducibility_rate"), 0.0) < _to_float(thresholds.get("min_reproducibility"), 0.85):
        reasons.append("reproducibility_too_low")
    builtin_latency = _to_float(builtin_metrics.get("avg_latency_ms"), 1.0)
    openclaw_latency = _to_float(openclaw_metrics.get("avg_latency_ms"), 9_999_999.0)
    if (openclaw_latency / builtin_latency if builtin_latency > 0 else 9_999_999.0) > _to_float(thresholds.get("max_latency_ratio_vs_builtin"), 2.0):
        reasons.append("latency_ratio_too_high")
    builtin_cost = _to_float(builtin_metrics.get("avg_cost_usd"), 0.0001)
    openclaw_cost = _to_float(openclaw_metrics.get("avg_cost_usd"), 9_999_999.0)
    if (openclaw_cost / builtin_cost if builtin_cost > 0 else 9_999_999.0) > _to_float(thresholds.get("max_cost_ratio_vs_builtin"), 1.5):
        reasons.append("cost_ratio_too_high")
    if reasons:
        return "NO_GO", reasons
    return "LIMITED_GO", ["strategy_design_only", "gateway_key_isolation_confirmed"]


def _render_openclaw_evaluation_markdown(summary: dict[str, Any]) -> str:
    builtin = summary.get("builtin", {})
    openclaw = summary.get("openclaw_poc", {})
    security = summary.get("security_boundary", {})
    reasons = summary.get("decision_reasons", [])
    if not isinstance(reasons, list):
        reasons = []

    def _fmt_num(value: Any, digits: int = 3) -> str:
        try:
            return f"{float(value):.{digits}f}"
        except (TypeError, ValueError):
            return "-"

    return "\n".join(
        [
            "# OpenClaw Evaluation Memo",
            "",
            f"- EvaluatedAt: {summary.get('evaluated_at')}",
            f"- Decision: **{summary.get('decision', 'NO_GO')}**",
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
            "",
        ]
    )


def run_openclaw_evaluation_impl(*, load_yaml_config_fn) -> dict[str, Any]:
    cfg = load_yaml_config_fn()
    agents_cfg = _resolve_agents_cfg(cfg)
    eval_cfg = agents_cfg["evaluation"]
    openclaw_cfg = dict(agents_cfg["openclaw_poc"])
    openclaw_cfg["enabled"] = True
    openclaw_cfg["fallback_to_builtin_on_error"] = False
    builtin_metrics = _evaluate_adapter_runs("builtin", BuiltinTaskAdapter(), samples=int(eval_cfg["samples"]), attempts_per_sample=int(eval_cfg["attempts_per_sample"]))
    openclaw_metrics = _evaluate_adapter_runs("openclaw_poc", OpenClawTaskAdapter(openclaw_cfg), samples=int(eval_cfg["samples"]), attempts_per_sample=int(eval_cfg["attempts_per_sample"]))
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
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(_render_openclaw_evaluation_markdown(summary))
    summary["output_path"] = output_path
    return summary


__all__ = [
    "_decide_openclaw_go_no_go",
    "_evaluate_adapter_runs",
    "_render_openclaw_evaluation_markdown",
    "run_openclaw_evaluation_impl",
]
