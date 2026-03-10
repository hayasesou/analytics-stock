from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.jobs.research_chat_charts import (
    _create_chart_artifacts_from_run,
    _execute_python,
    _execute_readonly_sql,
    _send_discord_chart_follow_up,
)
from src.jobs.research_chat_support import (
    _as_dict,
    _build_critic_note,
    _build_portfolio_note,
    _build_python_template,
    _build_quant_sql,
    _build_session_summary,
    _clean_text,
    _fetch_url_excerpt,
    _generate_hypotheses_via_llm,
    _label_outcome,
    _resolve_entry_and_returns,
    _select_primary_symbol,
    _send_discord_follow_up_for_session,
    _split_urls_and_text,
    _utc_now,
)
from src.types import ResearchArtifactRunSpec, ResearchArtifactSpec, ResearchHypothesisOutcomeSpec


def _process_extract_input(repo, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    inputs = repo.fetch_research_external_inputs(session_id=session_id)
    updated = fetched = failed = 0
    for item in inputs:
        if str(item.get("extraction_status", "queued")) == "success" and _clean_text(item.get("extracted_text")):
            continue
        source_type = str(item.get("source_type", "text"))
        source_url = _clean_text(item.get("source_url"))
        raw_text = _clean_text(item.get("raw_text"))
        if source_type in {"web_url", "youtube", "x"} and source_url:
            try:
                fetched_payload = _fetch_url_excerpt(source_url)
                extracted = "\n".join(part for part in [fetched_payload.get("title") or "", fetched_payload.get("excerpt") or ""] if part)
                repo.update_research_external_input(str(item["id"]), extracted_text=extracted, quality_grade="A" if fetched_payload.get("excerpt") else "B", extraction_status="success" if fetched_payload.get("excerpt") else "partial", metadata_patch={"processed_at": _utc_now().isoformat(), "processor": "research.extract_input", "fetch": fetched_payload})
                fetched += 1
            except Exception as exc:  # noqa: BLE001
                repo.update_research_external_input(str(item["id"]), extracted_text=f"Fetch failed for {source_url}", quality_grade="C", extraction_status="failed", metadata_patch={"processed_at": _utc_now().isoformat(), "processor": "research.extract_input", "error": str(exc)})
                failed += 1
        else:
            repo.update_research_external_input(str(item["id"]), extracted_text=raw_text or None, quality_grade="B" if raw_text else None, extraction_status="success" if raw_text else "partial", metadata_patch={"processed_at": _utc_now().isoformat(), "processor": "research.extract_input"})
        updated += 1
    return {"updated_inputs": updated, "fetched_urls": fetched, "failed_urls": failed}


def _process_generate_hypothesis(repo, payload: dict[str, Any], *, load_runtime_secrets_fn) -> dict[str, Any]:
    hypothesis_ids = [str(item) for item in (payload.get("hypothesis_ids") or [])]
    hypotheses = repo.fetch_research_hypotheses_by_ids(hypothesis_ids)
    session_id = str(payload.get("session_id", "")).strip()
    inputs = repo.fetch_research_external_inputs(session_id)
    urls, texts = _split_urls_and_text(inputs)
    url_summaries = [{"url": source_url, "title": _clean_text(_as_dict(_as_dict(item.get("metadata")).get("fetch")).get("title")), "excerpt": _clean_text(item.get("extracted_text"))[:2000]} for item in inputs if (source_url := _clean_text(item.get("source_url")))]
    generated = _generate_hypotheses_via_llm(question=str(payload.get("question") or ""), security_id=_clean_text(payload.get("security_id")) or None, urls=urls, texts=texts, url_summaries=url_summaries, load_runtime_secrets_fn=load_runtime_secrets_fn)
    generated_hypotheses = list(generated.get("hypotheses") or [])
    updated = 0
    for idx, hypothesis in enumerate(hypotheses):
        candidate = generated_hypotheses[idx] if idx < len(generated_hypotheses) and isinstance(generated_hypotheses[idx], dict) else {}
        repo.update_research_hypothesis(str(hypothesis["id"]), status="validate", thesis_md=str(candidate.get("thesis_md") or hypothesis.get("thesis_md") or ""), falsification_md=str(candidate.get("falsification_md") or hypothesis.get("falsification_md") or ""), confidence=float(candidate.get("confidence")) if candidate.get("confidence") is not None else hypothesis.get("confidence"), metadata_patch={"generated_at": _utc_now().isoformat(), "processor": "research.generate_hypothesis", "mode": str(generated.get("mode") or ""), "llm_summary": str(generated.get("summary") or ""), "suggested_stance": candidate.get("stance"), "suggested_horizon_days": candidate.get("horizon_days"), "validation_plan": candidate.get("validation_plan"), "key_metrics": list(candidate.get("key_metrics") or [])})
        updated += 1
    summary = str(generated.get("summary") or " / ".join(texts[:3])[:1200] or "No extracted text yet.")
    artifact_id = repo.insert_research_artifact(ResearchArtifactSpec(session_id=session_id, hypothesis_id=hypothesis_ids[0] if hypothesis_ids else None, artifact_type="report", title="Generated Hypothesis Pack", body_md=summary, metadata={"processor": "research.generate_hypothesis", "llm_used": bool(getattr(load_runtime_secrets_fn(), "openai_api_key", None)), "url_count": len(urls), "mode": str(generated.get("mode") or "")}))
    return {"updated_hypotheses": updated, "artifact_id": artifact_id, "summary": summary}


def _process_critic_review(repo, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    artifact_id = repo.insert_research_artifact(ResearchArtifactSpec(session_id=session_id, hypothesis_id=str(hypotheses[0]["id"]) if hypotheses else None, artifact_type="note", title="Critic Review", body_md=_build_critic_note(hypotheses), metadata={"processor": "research.critic_review"}))
    return {"artifact_id": artifact_id, "hypothesis_count": len(hypotheses)}


def _process_quant_plan(repo, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    symbol = _select_primary_symbol(repo.fetch_research_hypothesis_assets([str(item["id"]) for item in hypotheses]))
    artifact_id = repo.insert_research_artifact(ResearchArtifactSpec(session_id=session_id, hypothesis_id=str(hypotheses[0]["id"]) if hypotheses else None, artifact_type="sql", title="Quant Validation SQL", code_text=_build_quant_sql(symbol), language="sql", metadata={"processor": "research.quant_plan", "symbol": symbol}))
    repo.enqueue_agent_task(task_type="research.artifact_run", payload={"session_id": session_id, "artifact_id": artifact_id, "requested_by": "research.quant_plan"}, session_id=session_id, assigned_role="artifact", dedupe_key=f"{session_id}:artifact_run:{artifact_id}")
    return {"artifact_id": artifact_id, "symbol": symbol, "auto_run_enqueued": True}


def _process_code_generate(repo, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    symbol = _select_primary_symbol(repo.fetch_research_hypothesis_assets([str(item["id"]) for item in hypotheses]))
    artifact_id = repo.insert_research_artifact(ResearchArtifactSpec(session_id=session_id, hypothesis_id=str(hypotheses[0]["id"]) if hypotheses else None, artifact_type="python", title="Python Analysis Draft", code_text=_build_python_template(symbol), language="python", metadata={"processor": "research.code_generate", "symbol": symbol}))
    repo.enqueue_agent_task(task_type="research.artifact_run", payload={"session_id": session_id, "artifact_id": artifact_id, "requested_by": "research.code_generate"}, session_id=session_id, assigned_role="artifact", dedupe_key=f"{session_id}:artifact_run:{artifact_id}")
    return {"artifact_id": artifact_id, "symbol": symbol, "auto_run_enqueued": True}


def _process_portfolio_build(repo, payload: dict[str, Any], *, load_runtime_secrets_fn, send_discord_follow_up_for_session_fn=_send_discord_follow_up_for_session) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    hypotheses = repo.fetch_research_hypotheses_for_session(session_id)
    urls, _ = _split_urls_and_text(repo.fetch_research_external_inputs(session_id))
    note, weights = _build_portfolio_note(hypotheses)
    artifact_id = repo.insert_research_artifact(ResearchArtifactSpec(session_id=session_id, hypothesis_id=str(hypotheses[0]["id"]) if hypotheses else None, artifact_type="report", title="Portfolio Suggestion", body_md=note, metadata={"processor": "research.portfolio_build", "weights": weights}))
    previous = repo.fetch_latest_chat_message(session_id, role="assistant")
    summary = _build_session_summary(hypotheses, urls)
    repo.append_chat_message(session_id=session_id, role="assistant", content=summary, answer_before=str(previous.get("content")) if previous else None, answer_after=summary, change_reason="research portfolio build completed")
    send_discord_follow_up_for_session_fn(repo, payload=payload, session_id=session_id, summary=summary, load_runtime_secrets_fn=load_runtime_secrets_fn)
    return {"artifact_id": artifact_id, "weights": weights}


def _process_artifact_run(repo, payload: dict[str, Any], *, load_runtime_secrets_fn, create_chart_artifacts_from_run_fn=_create_chart_artifacts_from_run, send_discord_chart_follow_up_fn=_send_discord_chart_follow_up) -> dict[str, Any]:
    artifact_id = str(payload.get("artifact_id", "")).strip()
    artifact = repo.fetch_research_artifact(artifact_id)
    if not artifact:
        raise ValueError(f"artifact not found: {artifact_id}")
    artifact_type = str(artifact.get("artifact_type", "")).strip().lower()
    code_text = str(artifact.get("code_text") or "")
    if artifact_type in {"sql", "python"}:
        result = _execute_readonly_sql(repo, code_text) if artifact_type == "sql" else _execute_python(code_text)
        run_id = repo.insert_research_artifact_run(ResearchArtifactRunSpec(artifact_id=artifact_id, run_status="success", stdout_text=json.dumps({"row_count": result["row_count"]}, ensure_ascii=False) if artifact_type == "sql" else str(result.get("stdout", "")), result_json=result))
        chart_artifacts = create_chart_artifacts_from_run_fn(repo, artifact=artifact, result=result, run_id=run_id, chart_type=str(payload.get("chart_type") or "") or None, instruction=str(payload.get("chart_instruction") or "") or None, load_runtime_secrets_fn=load_runtime_secrets_fn)
        send_discord_chart_follow_up_fn(payload=payload, session_id=str(artifact.get("session_id") or ""), source_title=str(artifact.get("title") or artifact_id), charts=chart_artifacts, load_runtime_secrets_fn=load_runtime_secrets_fn)
        return {"run_id": run_id, "row_count": result.get("row_count"), "stdout": result.get("stdout", ""), "chart_artifact_ids": [str(item.get("id") or "") for item in chart_artifacts]}
    run_id = repo.insert_research_artifact_run(ResearchArtifactRunSpec(artifact_id=artifact_id, run_status="failed", stderr_text=f"artifact_type_not_supported: {artifact_type}", result_json={"artifact_type": artifact_type}))
    return {"run_id": run_id, "error": f"unsupported artifact type: {artifact_type}"}


def _process_chart_generate(repo, payload: dict[str, Any], *, load_runtime_secrets_fn, create_chart_artifacts_from_run_fn=_create_chart_artifacts_from_run, send_discord_chart_follow_up_fn=_send_discord_chart_follow_up) -> dict[str, Any]:
    artifact_id = str(payload.get("artifact_id", "")).strip()
    artifact = repo.fetch_research_artifact(artifact_id)
    if not artifact:
        raise ValueError(f"artifact not found: {artifact_id}")
    latest_run = repo.fetch_latest_research_artifact_run(artifact_id)
    if not latest_run or str(latest_run.get("run_status") or "").strip().lower() != "success":
        raise ValueError(f"latest artifact run is not successful: {artifact_id}")
    chart_artifacts = create_chart_artifacts_from_run_fn(repo, artifact=artifact, result=_as_dict(latest_run.get("result_json")), run_id=str(latest_run.get("id") or ""), chart_type=str(payload.get("chart_type") or "") or None, instruction=str(payload.get("chart_instruction") or "") or None, load_runtime_secrets_fn=load_runtime_secrets_fn)
    send_discord_chart_follow_up_fn(payload=payload, session_id=str(artifact.get("session_id") or ""), source_title=str(artifact.get("title") or artifact_id), charts=chart_artifacts, load_runtime_secrets_fn=load_runtime_secrets_fn)
    return {"source_artifact_id": artifact_id, "source_run_id": str(latest_run.get("id") or ""), "chart_artifact_ids": [str(item.get("id") or "") for item in chart_artifacts]}


def _process_validate_outcome(repo, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    requested_ids = [str(item) for item in (payload.get("hypothesis_ids") or [])]
    hypotheses = repo.fetch_research_hypotheses_by_ids(requested_ids) if requested_ids else repo.fetch_research_hypotheses_for_session(session_id)
    assets_by_hypothesis: dict[str, list[dict[str, Any]]] = {}
    for asset in repo.fetch_research_hypothesis_assets([str(item["id"]) for item in hypotheses]):
        assets_by_hypothesis.setdefault(str(asset["hypothesis_id"]), []).append(asset)
    created = 0
    for hypothesis in hypotheses:
        hypothesis_id = str(hypothesis["id"])
        symbol = _select_primary_symbol(assets_by_hypothesis.get(hypothesis_id, []))
        created_at = hypothesis.get("created_at")
        if not symbol or not isinstance(created_at, datetime):
            continue
        metrics = _resolve_entry_and_returns(repo, symbol, created_at)
        if not metrics:
            repo.update_research_hypothesis(hypothesis_id, status="watch", metadata_patch={"validation_status": "open", "validator": "research.validate_outcome"})
            continue
        label = _label_outcome(str(hypothesis.get("stance", "watch")), metrics.get("ret_5d"))
        repo.insert_research_hypothesis_outcome(ResearchHypothesisOutcomeSpec(hypothesis_id=hypothesis_id, checked_at=_utc_now(), ret_1d=metrics.get("ret_1d"), ret_5d=metrics.get("ret_5d"), ret_20d=metrics.get("ret_20d"), mfe=metrics.get("mfe"), mae=metrics.get("mae"), outcome_label=label, summary_md=f"Validated against {metrics.get('security_id')} from {metrics.get('entry_date')}.", metadata={"symbol": symbol, "validator": "research.validate_outcome"}))
        repo.update_research_hypothesis(hypothesis_id, status="passed" if label == "hit" else "failed" if label == "miss" else "watch", metadata_patch={"validation_status": label, "validated_symbol": symbol})
        created += 1
    return {"validated": created}


def _process_session_summarize(repo, payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id", "")).strip()
    urls, _ = _split_urls_and_text(repo.fetch_research_external_inputs(session_id))
    previous = repo.fetch_latest_chat_message(session_id, role="assistant")
    summary = _build_session_summary(repo.fetch_research_hypotheses_for_session(session_id), urls)
    message_id = repo.append_chat_message(session_id=session_id, role="assistant", content=summary, answer_before=str(previous.get("content")) if previous else None, answer_after=summary, change_reason="research session summarized")
    return {"message_id": message_id}


def process_research_task(repo, task: dict[str, Any], *, load_runtime_secrets_fn, send_discord_follow_up_for_session_fn=_send_discord_follow_up_for_session, create_chart_artifacts_from_run_fn=_create_chart_artifacts_from_run, send_discord_chart_follow_up_fn=_send_discord_chart_follow_up) -> dict[str, Any]:
    payload = _as_dict(task.get("payload"))
    task_type = str(task.get("task_type", "")).strip()
    if task_type == "research.extract_input":
        return _process_extract_input(repo, payload)
    if task_type == "research.generate_hypothesis":
        return _process_generate_hypothesis(repo, payload, load_runtime_secrets_fn=load_runtime_secrets_fn)
    if task_type == "research.critic_review":
        return _process_critic_review(repo, payload)
    if task_type == "research.quant_plan":
        return _process_quant_plan(repo, payload)
    if task_type == "research.code_generate":
        return _process_code_generate(repo, payload)
    if task_type == "research.portfolio_build":
        return _process_portfolio_build(repo, payload, load_runtime_secrets_fn=load_runtime_secrets_fn, send_discord_follow_up_for_session_fn=send_discord_follow_up_for_session_fn)
    if task_type == "research.artifact_run":
        return _process_artifact_run(repo, payload, load_runtime_secrets_fn=load_runtime_secrets_fn, create_chart_artifacts_from_run_fn=create_chart_artifacts_from_run_fn, send_discord_chart_follow_up_fn=send_discord_chart_follow_up_fn)
    if task_type == "research.chart_generate":
        return _process_chart_generate(repo, payload, load_runtime_secrets_fn=load_runtime_secrets_fn, create_chart_artifacts_from_run_fn=create_chart_artifacts_from_run_fn, send_discord_chart_follow_up_fn=send_discord_chart_follow_up_fn)
    if task_type == "research.validate_outcome":
        return _process_validate_outcome(repo, payload)
    if task_type == "research.session_summarize":
        return _process_session_summarize(repo, payload)
    return {"ignored": True, "task_type": task_type}
