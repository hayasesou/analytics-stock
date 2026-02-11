from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.llm.reporting import generate_weekly_summary_report_with_llm


def _sample_top50() -> pd.DataFrame:
    return pd.DataFrame(
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


def _sample_events() -> list[dict[str, str]]:
    return [
        {
            "importance": "high",
            "title": "Guidance update",
            "summary": "売上見通しは据え置き。",
            "doc_version_id": "11111111-1111-1111-1111-111111111111",
        }
    ]


def _fake_llm_json(prompt: str, model: str, api_key: str | None) -> dict:
    _ = (prompt, model, api_key)
    return {
        "title": "LLM Weekly Summary 2026-02-11",
        "body_md": "## 市況ハイライト\n- Top50を更新\n- 重要イベントを確認",
        "conclusion": "High根拠のある銘柄を優先して監視継続",
        "falsification_conditions": "一次情報の否定があれば結論を撤回",
        "claims": [{"claim_id": "C1", "status": "supported"}],
    }


def test_generate_weekly_summary_report_with_llm_matches_golden() -> None:
    report = generate_weekly_summary_report_with_llm(
        run_id="run-123",
        as_of=datetime(2026, 2, 11, 10, 0, 0),
        top50=_sample_top50(),
        events=_sample_events(),
        model="gpt-5-mini",
        llm_json_fn=_fake_llm_json,
    )
    actual = asdict(report)
    fixture_path = Path(__file__).parent / "golden" / "weekly_summary_llm_golden.json"
    expected = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert actual == expected
