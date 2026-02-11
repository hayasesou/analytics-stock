from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from src.llm.reporting import (
    build_security_report_prompt,
    generate_security_report_with_llm,
    parse_security_report_llm_payload,
)
from src.types import CitationItem


def _sample_row() -> pd.Series:
    return pd.Series(
        {
            "security_id": "JP:1301",
            "quality": 72.5,
            "growth": 64.2,
            "value": 58.1,
            "momentum": 70.0,
            "catalyst": 66.3,
            "combined_score": 68.7,
            "confidence": "High",
        }
    )


def test_build_security_report_prompt_contains_key_context() -> None:
    prompt = build_security_report_prompt(
        _sample_row(),
        datetime(2026, 2, 11, 10, 0, 0),
        evidence_citations=[
            CitationItem(
                claim_id="X",
                doc_version_id="11111111-1111-1111-1111-111111111111",
                page_ref="p3",
                quote_text="売上見通しを上方修正",
            )
        ],
    )

    assert "Security: JP:1301" in prompt
    assert "Quality: 72.50" in prompt
    assert "quote=売上見通しを上方修正" in prompt
    assert '"claim_id":"C1"' in prompt


def test_parse_security_report_llm_payload_requires_fields() -> None:
    with pytest.raises(ValueError):
        parse_security_report_llm_payload(
            {
                "title": "x",
                "body_md": "y",
                # conclusion is missing
                "falsification_conditions": "z",
                "claims": [],
            }
        )


def test_generate_security_report_with_llm_downgrades_unsupported_claims_without_citation() -> None:
    def fake_llm_json(prompt: str, model: str, api_key: str | None) -> dict:
        assert "JP:1301" in prompt
        assert model == "gpt-5-mini"
        assert api_key is None
        return {
            "title": "LLM Security Report JP:1301",
            "body_md": "## 要約\n- 監視継続",
            "conclusion": "監視継続",
            "falsification_conditions": "前提崩壊で撤回",
            "claims": [
                {"claim_id": "C1", "status": "supported"},
                {"claim_id": "C2", "status": "supported"},
                {"claim_id": "C3", "status": "supported"},
            ],
        }

    report = generate_security_report_with_llm(
        _sample_row(),
        datetime(2026, 2, 11, 10, 0, 0),
        evidence_citations=[
            CitationItem(
                claim_id="ANY",
                doc_version_id="11111111-1111-1111-1111-111111111111",
                page_ref="p3",
                quote_text="売上見通しを上方修正",
            )
        ],
        model="gpt-5-mini",
        llm_json_fn=fake_llm_json,
    )

    status_by_claim = {c["claim_id"]: c["status"] for c in report.claims}
    assert status_by_claim["C1"] == "supported"
    assert status_by_claim["C2"] == "hypothesis"
    assert status_by_claim["C3"] == "hypothesis"
    assert report.citations[0].claim_id == "C1"


def test_generate_security_report_with_llm_defaults_model_when_env_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_MODEL", "")

    def fake_llm_json(prompt: str, model: str, api_key: str | None) -> dict:
        assert "JP:1301" in prompt
        assert model == "gpt-5-mini"
        _ = api_key
        return {
            "title": "LLM Security Report JP:1301",
            "body_md": "本文",
            "conclusion": "結論",
            "falsification_conditions": "反証条件",
            "claims": [
                {"claim_id": "C1", "status": "hypothesis"},
                {"claim_id": "C2", "status": "hypothesis"},
                {"claim_id": "C3", "status": "hypothesis"},
            ],
        }

    report = generate_security_report_with_llm(
        _sample_row(),
        datetime(2026, 2, 11, 10, 0, 0),
        llm_json_fn=fake_llm_json,
    )
    assert report.title == "LLM Security Report JP:1301"
