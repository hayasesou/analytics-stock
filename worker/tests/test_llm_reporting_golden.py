from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.llm.reporting import generate_security_report_with_llm
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


def _fake_llm_json(prompt: str, model: str, api_key: str | None) -> dict:
    _ = (prompt, model, api_key)
    return {
        "title": "LLM Security Report JP:1301",
        "body_md": "## サマリ\n- 監視継続",
        "conclusion": "監視継続",
        "falsification_conditions": "ガイダンス悪化で撤回",
        "claims": [
            {"claim_id": "C1", "status": "supported"},
            {"claim_id": "C2", "status": "supported"},
            {"claim_id": "C3", "status": "hypothesis"},
        ],
    }


def test_generate_security_report_with_llm_matches_golden() -> None:
    report = generate_security_report_with_llm(
        _sample_row(),
        datetime(2026, 2, 11, 10, 0, 0),
        evidence_citations=[
            CitationItem(
                claim_id="ANY",
                doc_version_id="11111111-1111-1111-1111-111111111111",
                page_ref="p2",
                quote_text="売上見通しを上方修正",
            )
        ],
        model="gpt-5-mini",
        llm_json_fn=_fake_llm_json,
    )

    actual = asdict(report)
    fixture_path = Path(__file__).parent / "golden" / "security_report_llm_golden.json"
    expected = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert actual == expected
