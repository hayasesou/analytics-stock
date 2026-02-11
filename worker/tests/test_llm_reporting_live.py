from __future__ import annotations

import os
from datetime import datetime

import pandas as pd
import pytest

from src.llm.openai_client import request_openai_json
from src.llm.reporting import (
    generate_security_report_with_llm,
    generate_weekly_summary_report_with_llm,
)
from src.types import CitationItem


def _require_live_settings() -> tuple[str, str]:
    api_key = os.getenv("OPENAI_API_KEY", "")
    run_live = os.getenv("RUN_LLM_LIVE", "0") == "1"
    if not run_live or not api_key:
        pytest.skip("set RUN_LLM_LIVE=1 and OPENAI_API_KEY to run live LLM tests")
    model = os.getenv("OPENAI_MODEL", "gpt-5-mini")
    return api_key, model


@pytest.mark.llm_live
def test_openai_json_live_returns_object() -> None:
    api_key, model = _require_live_settings()
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["ok", "provider"],
        "properties": {
            "ok": {"type": "boolean"},
            "provider": {"type": "string"},
        },
    }
    payload = request_openai_json(
        prompt='Return JSON only: {"ok": true, "provider": "openai"}',
        api_key=api_key,
        model=model,
        max_output_tokens=120,
        json_schema=schema,
    )
    assert isinstance(payload, dict)
    assert isinstance(payload.get("ok"), bool)
    assert isinstance(payload.get("provider"), str)
    assert payload["provider"].strip()


@pytest.mark.llm_live
def test_generate_security_report_with_llm_live_minimal() -> None:
    api_key, model = _require_live_settings()
    row = pd.Series(
        {
            "security_id": "US:119",
            "quality": 70.0,
            "growth": 60.0,
            "value": 55.0,
            "momentum": 65.0,
            "catalyst": 68.0,
            "combined_score": 64.0,
            "confidence": "Medium",
        }
    )
    report = generate_security_report_with_llm(
        row=row,
        as_of=datetime(2026, 2, 11, 10, 0, 0),
        evidence_citations=[
            CitationItem(
                claim_id="ANY",
                doc_version_id="11111111-1111-1111-1111-111111111111",
                page_ref="p1",
                quote_text="Revenue guidance has been maintained.",
            )
        ],
        model=model,
        api_key=api_key,
    )
    assert report.report_type == "security_full"
    assert report.title.strip()
    assert report.body_md.strip()
    assert report.conclusion.strip()
    assert report.falsification_conditions.strip()
    assert len(report.claims) == 3


@pytest.mark.llm_live
def test_generate_weekly_summary_report_with_llm_live_minimal() -> None:
    api_key, model = _require_live_settings()
    top50 = pd.DataFrame(
        [
            {
                "mixed_rank": 1,
                "security_id": "JP:1301",
                "market": "JP",
                "combined_score": 71.2,
                "confidence": "High",
            },
            {
                "mixed_rank": 2,
                "security_id": "US:119",
                "market": "US",
                "combined_score": 69.8,
                "confidence": "Medium",
            },
        ]
    )
    events = [
        {
            "importance": "high",
            "title": "Guidance update",
            "summary": "Revenue guidance has been maintained.",
            "doc_version_id": "11111111-1111-1111-1111-111111111111",
        }
    ]
    report = generate_weekly_summary_report_with_llm(
        run_id="live-run",
        as_of=datetime(2026, 2, 11, 10, 0, 0),
        top50=top50,
        events=events,
        model=model,
        api_key=api_key,
    )
    assert report.report_type == "weekly_summary"
    assert report.title.strip()
    assert report.body_md.strip()
    assert report.conclusion.strip()
    assert report.falsification_conditions.strip()
    assert len(report.claims) == 1
