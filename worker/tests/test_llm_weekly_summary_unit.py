from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from src.llm.reporting import (
    build_weekly_summary_report_prompt,
    generate_weekly_summary_report,
    generate_weekly_summary_report_with_llm,
    parse_weekly_summary_report_llm_payload,
)


def _sample_top50() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "mixed_rank": 1,
                "security_id": "JP:1301",
                "ticker": "1301",
                "name": "極洋",
                "market": "JP",
                "combined_score": 71.2,
                "confidence": "High",
            },
            {
                "mixed_rank": 2,
                "security_id": "US:119",
                "ticker": "AAPL",
                "name": "Apple Inc.",
                "market": "US",
                "combined_score": 69.8,
                "confidence": "Medium",
            },
        ]
    )


def _sample_events(with_doc: bool = True) -> list[dict[str, str]]:
    return [
        {
            "importance": "high",
            "title": "Guidance update",
            "summary": "売上見通しが維持された。",
            "doc_version_id": "11111111-1111-1111-1111-111111111111" if with_doc else "",
        }
    ]


def test_build_weekly_summary_report_prompt_contains_run_context() -> None:
    prompt = build_weekly_summary_report_prompt(
        run_id="run-123",
        as_of=datetime(2026, 2, 11, 10, 0, 0),
        top50=_sample_top50(),
        events=_sample_events(),
    )
    assert "Run ID: run-123" in prompt
    assert "Top50 count: 2" in prompt
    assert "security=1301 / 極洋 (JP:1301)" in prompt
    assert '"claim_id":"C1"' in prompt


def test_generate_weekly_summary_report_includes_company_name_in_top10_preview() -> None:
    report = generate_weekly_summary_report(
        run_id="run-123",
        as_of=datetime(2026, 2, 11, 10, 0, 0),
        top50=_sample_top50(),
        events=_sample_events(),
    )
    assert "| rank | security | market | score | confidence |" in report.body_md
    assert "1301 / 極洋 (JP:1301)" in report.body_md
    assert "AAPL / Apple Inc. (US:119)" in report.body_md


def test_parse_weekly_summary_report_llm_payload_requires_fields() -> None:
    with pytest.raises(ValueError):
        parse_weekly_summary_report_llm_payload(
            {
                "title": "Weekly",
                "body_md": "body",
                "conclusion": "conclusion",
                # falsification_conditions is missing
                "claims": [],
            }
        )


def test_generate_weekly_summary_report_with_llm_downgrades_without_event_doc() -> None:
    def fake_llm_json(prompt: str, model: str, api_key: str | None) -> dict:
        _ = (prompt, model, api_key)
        return {
            "title": "LLM Weekly Summary",
            "body_md": "## 週間サマリ\n- 監視継続",
            "conclusion": "監視継続",
            "falsification_conditions": "前提崩壊で見直し",
            "claims": [{"claim_id": "C1", "status": "supported"}],
        }

    report = generate_weekly_summary_report_with_llm(
        run_id="run-123",
        as_of=datetime(2026, 2, 11, 10, 0, 0),
        top50=_sample_top50(),
        events=_sample_events(with_doc=False),
        model="gpt-5-mini",
        llm_json_fn=fake_llm_json,
    )
    assert len(report.citations) == 0
    assert report.claims[0]["status"] == "hypothesis"


def test_generate_weekly_summary_report_with_llm_defaults_model_when_env_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_MODEL", "")

    def fake_llm_json(prompt: str, model: str, api_key: str | None) -> dict:
        assert "run-123" in prompt
        assert model == "gpt-5-mini"
        _ = api_key
        return {
            "title": "LLM Weekly Summary",
            "body_md": "本文",
            "conclusion": "結論",
            "falsification_conditions": "反証条件",
            "claims": [{"claim_id": "C1", "status": "hypothesis"}],
        }

    report = generate_weekly_summary_report_with_llm(
        run_id="run-123",
        as_of=datetime(2026, 2, 11, 10, 0, 0),
        top50=_sample_top50(),
        events=_sample_events(),
        llm_json_fn=fake_llm_json,
    )
    assert report.title == "LLM Weekly Summary"
