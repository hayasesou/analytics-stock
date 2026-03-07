from __future__ import annotations

from typing import Any, Literal


ResearchMode = Literal[
    "screening",
    "event_analysis",
    "risk_review",
    "technical_review",
    "income_review",
    "hypothesis_synthesis",
]


RESEARCH_HYPOTHESIS_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["mode", "summary", "hypotheses"],
    "properties": {
        "mode": {
            "type": "string",
            "enum": [
                "screening",
                "event_analysis",
                "risk_review",
                "technical_review",
                "income_review",
                "hypothesis_synthesis",
            ],
        },
        "summary": {"type": "string", "minLength": 1, "maxLength": 1200},
        "hypotheses": {
            "type": "array",
            "minItems": 1,
            "maxItems": 3,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "stance",
                    "horizon_days",
                    "thesis_md",
                    "falsification_md",
                    "confidence",
                    "validation_plan",
                    "key_metrics",
                ],
                "properties": {
                    "stance": {"type": "string", "enum": ["bullish", "bearish", "neutral", "watch"]},
                    "horizon_days": {"type": "integer", "enum": [1, 5, 20, 60, 120]},
                    "thesis_md": {"type": "string", "minLength": 1, "maxLength": 1200},
                    "falsification_md": {"type": "string", "minLength": 1, "maxLength": 800},
                    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                    "validation_plan": {"type": "string", "minLength": 1, "maxLength": 1000},
                    "key_metrics": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 6,
                        "items": {"type": "string", "minLength": 1, "maxLength": 120},
                    },
                },
            },
        },
    },
}


MODE_DESCRIPTIONS: dict[ResearchMode, str] = {
    "screening": (
        "あなたは Goldman Sachs のシニア株式スクリーニング担当です。"
        " 候補抽出、比較、財務健全性、バリュエーション整理を優先してください。"
    ),
    "event_analysis": (
        "あなたは JPMorgan のイベントドリブン・エクイティアナリストです。"
        " ニュース、開示、決算、材料が株価にどう波及するかを分析してください。"
    ),
    "risk_review": (
        "あなたは Bridgewater のシニア・リスクアナリストです。"
        " ボラティリティ、ドローダウン、相関、下方シナリオ、ヘッジ観点を優先してください。"
    ),
    "technical_review": (
        "あなたは Morgan Stanley のシニア・テクニカルストラテジストです。"
        " タイミング、サポート/レジスタンス、モメンタム、損切り水準を優先してください。"
    ),
    "income_review": (
        "あなたは BlackRock のシニア・インカムストラテジストです。"
        " 配当、安全性、持続可能性、減配リスクを優先してください。"
    ),
    "hypothesis_synthesis": (
        "あなたはバイサイドの統合リサーチ責任者です。"
        " 断片的な入力から投資仮説、反証条件、検証計画を整理してください。"
    ),
}


def classify_research_mode(*, question: str, security_id: str | None, urls: list[str]) -> ResearchMode:
    text = " ".join([question, security_id or "", *urls]).lower()
    if any(token in text for token in ["dividend", "yield", "配当", "payout", "income"]):
        return "income_review"
    if any(token in text for token in ["risk", "drawdown", "hedge", "beta", "volatility", "リスク", "ボラ", "ヘッジ"]):
        return "risk_review"
    if any(token in text for token in ["rsi", "macd", "bollinger", "support", "resistance", "テクニカル", "移動平均"]):
        return "technical_review"
    if any(token in text for token in ["earnings", "guidance", "revenue", "eps", "決算", "ガイダンス", "受注", "契約", "launch", "product", "news"]):
        return "event_analysis"
    if any(token in text for token in ["screen", "screener", "compare", "比較", "候補", "top 10", "銘柄選定"]):
        return "screening"
    return "hypothesis_synthesis"


def build_mode_specific_requirements(mode: ResearchMode) -> list[str]:
    common = [
        "- 返答は JSON のみ。コードフェンスは禁止。",
        "- 日本語で簡潔に書く。",
        "- 根拠が不足する場合は推測せず、watch または neutral を優先する。",
        "- 各仮説に falsification と validation_plan を必ず入れる。",
        "- key_metrics には後続 SQL/Python で確認すべき指標を入れる。",
        "- 仮説は要約ではなく、価格が動く条件を含む検証可能な主張にする。",
        "- 可能な範囲で市場コンテキスト、業績接続、競合比較、何が織り込み済みかを明示する。",
        "- 入力だけでは断定できない点は evidence gap として扱い、validation_plan で埋める。",
        "- thesis_md には 1) 何が起きるか 2) なぜ起きるか 3) いつまでに確認するか を含める。",
        "- falsification_md には仮説を取り下げる具体条件を入れる。",
    ]
    specifics: dict[ResearchMode, list[str]] = {
        "screening": [
            "- バリュエーション、財務健全性、成長性の比較を重視する。",
            "- 仮説は『なぜ候補として残すのか』に寄せる。",
            "- 同業他社との優位点と劣位点を最低1つずつ示す。",
        ],
        "event_analysis": [
            "- 材料が 1日/5日/20日で株価にどう波及するかを切り分ける。",
            "- 『何が織り込み済みで何が未織り込みか』を意識する。",
            "- 材料が売上・利益・受注・顧客数などのKPIにどう接続するか示す。",
        ],
        "risk_review": [
            "- 下方シナリオ、ボラティリティ、相関、ヘッジ必要性を優先する。",
            "- 強気仮説でも downside trigger を明示する。",
            "- どの指標が崩れたら即撤退かを明記する。",
        ],
        "technical_review": [
            "- 価格水準、出来高、モメンタム、支持抵抗を前提に仮説化する。",
            "- 実データが不足する場合は『テクニカル判断不可』を明記する。",
            "- エントリーよりも invalidation 水準と確認したい指標を優先する。",
        ],
        "income_review": [
            "- 配当持続性、キャッシュフロー、減配リスクを優先する。",
            "- 高配当が yield trap でないか確認する観点を含める。",
            "- 配当原資が利益・FCF・資本政策のどれに依存しているか示す。",
        ],
        "hypothesis_synthesis": [
            "- 断片的入力から最も検証価値の高い仮説を 1〜3 本に絞る。",
            "- まず『何を追加で確認すべきか』が分かる出力にする。",
            "- 不足情報が多い場合は強気/弱気を乱発せず、watch 仮説を基準にする。",
        ],
    }
    return [*common, *specifics[mode]]


def build_research_prompt(
    *,
    mode: ResearchMode,
    question: str,
    security_id: str | None,
    url_summaries: list[dict[str, Any]],
    text_blocks: list[str],
) -> str:
    lines = [
        MODE_DESCRIPTIONS[mode],
        "",
        f"分析モード: {mode}",
        f"Question: {question}",
        f"Security: {security_id or 'unknown'}",
        "",
        "URL 由来の構造化情報:",
    ]
    if url_summaries:
        for idx, item in enumerate(url_summaries[:5], start=1):
            lines.extend(
                [
                    f"[URL {idx}] {item.get('url', '-')}",
                    f"title: {item.get('title', '-')}",
                    f"excerpt: {item.get('excerpt', '-')}",
                    "",
                ]
            )
    else:
        lines.extend(["(none)", ""])

    lines.append("ユーザー補足テキスト:")
    if text_blocks:
        for idx, block in enumerate(text_blocks[:5], start=1):
            lines.append(f"[TEXT {idx}] {block[:1200]}")
    else:
        lines.append("(none)")

    lines.extend(["", "Requirements:"])
    lines.extend(build_mode_specific_requirements(mode))
    lines.extend(
        [
            "",
            "summary は 3-6 文で、入力の要点・最大の論点・不足情報を含めること。",
            "hypotheses の各 thesis_md では、価格反応または業績/KPIへの波及経路を必ず書くこと。",
            "key_metrics には価格だけでなく、業績・需給・競合比較の指標を混ぜること。",
        ]
    )
    return "\n".join(lines)
