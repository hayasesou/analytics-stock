from __future__ import annotations

from pathlib import Path

from src.research.deep_research import (
    DeepResearchInput,
    build_deep_research_snapshot,
    parse_deep_research_file_if_configured,
)


def test_build_deep_research_snapshot_fallback_without_api_key() -> None:
    snapshot = build_deep_research_snapshot(
        DeepResearchInput(
            security_id="JP:3513",
            report_text="上方修正と増益が継続。次回決算がカタリスト。リスクは円高。",
        ),
        api_key=None,
    )
    assert snapshot["rating"] in {"A", "B", "C"}
    assert isinstance(snapshot["summary"], str)
    assert isinstance(snapshot["snapshot"]["drivers"], list)
    assert isinstance(snapshot["snapshot"]["catalysts"], list)
    assert isinstance(snapshot["snapshot"]["risks"], list)


def test_parse_deep_research_file_if_configured(monkeypatch, tmp_path: Path) -> None:
    report_path = tmp_path / "deep_report.md"
    report_path.write_text("これはdeep researchレポートです。", encoding="utf-8")
    monkeypatch.setenv("DEEP_RESEARCH_REPORT_PATH", str(report_path))
    monkeypatch.setenv("DEEP_RESEARCH_SECURITY_ID", "US:AAPL")

    payload = parse_deep_research_file_if_configured()
    assert payload is not None
    assert payload.security_id == "US:AAPL"
    assert "deep research" in payload.report_text
