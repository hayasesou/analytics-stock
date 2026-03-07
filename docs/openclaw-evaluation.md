# OpenClaw Evaluation Memo

- EvaluatedAt: 2026-02-20T11:45:30.900245+00:00
- Decision: **NO_GO**
- Reasons: latency_ratio_too_high

## 1. Task Model Comparison

| Item | Current agents (`agent_tasks`) | OpenClaw PoC Adapter |
| --- | --- | --- |
| Unit of work | DB queued task | DB queued task (compatible) |
| Scope in PoC | all roles | `strategy_design` only |
| Failure handling | single try | retry + optional fallback |
| Result storage | `agent_tasks.result` | same (`provider=openclaw_poc`) |
| Execution key access | worker env | sanitized env (gateway keys stripped) |

## 2. Benchmark Results

| Metric | Builtin | OpenClaw PoC |
| --- | ---: | ---: |
| total_runs | 60 | 60 |
| failure_rate | 0.000 | 0.000 |
| avg_latency_ms | 0.002 | 120.210 |
| avg_cost_usd | 0.0652 | 0.0700 |
| reproducibility_rate | 1.000 | 1.000 |

## 3. Security Boundary Verification

- boundary_ok: True
- raw_gateway_keys_detected: 0
- sanitized_forbidden_keys: 0

## 4. Recommendation

- `LIMITED_GO`: keep PoC limited to `strategy_design`, keep gateway separation strict, keep fallback on error.
- `NO_GO`: do not use OpenClaw adapter in production loop; continue builtin extension.
