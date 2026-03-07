from __future__ import annotations

from src.llm.research_prompts import build_research_prompt, classify_research_mode


def test_classify_research_mode_routes_income_keywords() -> None:
    mode = classify_research_mode(
        question="この銘柄の配当利回りと payout ratio を見たい",
        security_id="US:KO",
        urls=[],
    )
    assert mode == "income_review"


def test_classify_research_mode_routes_event_keywords() -> None:
    mode = classify_research_mode(
        question="決算と guidance の影響を見たい",
        security_id="US:NVDA",
        urls=["https://example.com/earnings"],
    )
    assert mode == "event_analysis"


def test_build_research_prompt_includes_mode_specific_requirements() -> None:
    prompt = build_research_prompt(
        mode="risk_review",
        question="リスクを確認したい",
        security_id="US:NVDA",
        url_summaries=[{"url": "https://example.com", "title": "Example", "excerpt": "volatility is rising"}],
        text_blocks=["drawdown と beta を確認したい"],
    )

    assert "Bridgewater" in prompt
    assert "validation_plan" in prompt
    assert "key_metrics" in prompt
    assert "下方シナリオ" in prompt
    assert "市場コンテキスト" in prompt
    assert "競合比較" in prompt
