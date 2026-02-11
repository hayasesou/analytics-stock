from __future__ import annotations

from datetime import datetime

import pandas as pd

from src.llm.reporting import generate_security_report
from src.types import CitationItem


def _sample_row() -> pd.Series:
    return pd.Series(
        {
            "security_id": "US:119",
            "quality": 72.5,
            "growth": 64.2,
            "value": 58.1,
            "momentum": 70.0,
            "catalyst": 66.3,
            "combined_score": 68.7,
            "confidence": "Medium",
        }
    )


def test_generate_security_report_without_citation_marks_all_claims_hypothesis() -> None:
    report = generate_security_report(
        _sample_row(),
        datetime(2026, 2, 11, 10, 0, 0),
        evidence_citations=[],
    )

    assert len(report.citations) == 0
    assert all(claim["status"] == "hypothesis" for claim in report.claims)


def test_generate_security_report_with_citation_marks_supported_claims_only() -> None:
    report = generate_security_report(
        _sample_row(),
        datetime(2026, 2, 11, 10, 0, 0),
        evidence_citations=[
            CitationItem(
                claim_id="ANY",
                doc_version_id="11111111-1111-1111-1111-111111111111",
                page_ref="p3",
                quote_text="FY2026 revenue guidance is 10.2B USD.",
            )
        ],
    )

    assert len(report.citations) == 1
    assert report.citations[0].claim_id == "C1"
    assert report.claims[0]["status"] == "supported"
    assert report.claims[1]["status"] == "hypothesis"
    assert report.claims[2]["status"] == "hypothesis"
    assert "Primary source snapshot" not in report.citations[0].quote_text
