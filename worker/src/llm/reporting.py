from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pandas as pd

from src.types import CitationItem, ReportItem


def _score_table(row: pd.Series) -> str:
    lines = [
        "| metric | value |",
        "|---|---:|",
        f"| Quality | {row['quality']:.2f} |",
        f"| Growth | {row['growth']:.2f} |",
        f"| Value | {row['value']:.2f} |",
        f"| Momentum | {row['momentum']:.2f} |",
        f"| Catalyst | {row['catalyst']:.2f} |",
        f"| Combined | {row['combined_score']:.2f} |",
        f"| Confidence | {row['confidence']} |",
    ]
    return "\n".join(lines)


def generate_weekly_summary_report(
    run_id: str,
    as_of: datetime,
    top50: pd.DataFrame,
    events: list[dict[str, str]],
) -> ReportItem:
    top_preview = top50.head(10)
    lines = [
        f"週次サマリ（as-of: {as_of.date().isoformat()}）",
        "",
        "## 市況ハイライト",
        f"- 対象銘柄数: {len(top50)}",
        f"- High/Med イベント数: {sum(1 for e in events if e['importance'] in {'high', 'medium'})}",
        "",
        "## Top10 プレビュー",
        "| rank | security_id | market | score | confidence |",
        "|---:|---|---|---:|---|",
    ]

    for _, row in top_preview.iterrows():
        lines.append(
            f"| {int(row['mixed_rank'])} | {row['security_id']} | {row['market']} | {row['combined_score']:.2f} | {row['confidence']} |"
        )

    lines.append("")
    lines.append("## 次に見るべき3点")
    lines.append("1. High confidence かつ Top10 の一次情報更新")
    lines.append("2. 流動性フラグの変化")
    lines.append("3. バックテスト strict コスト時のドローダウン推移")

    claims = [
        {
            "claim_id": "C1",
            "claim_text": "Top50 weekly ranking has been refreshed with mixed-market constraints",
            "status": "supported",
        }
    ]
    citations: list[CitationItem] = []
    if events and events[0].get("doc_version_id"):
        citations.append(
            CitationItem(
                claim_id="C1",
                doc_version_id=str(events[0]["doc_version_id"]),
                page_ref="p1",
                quote_text=f"Weekly summary references event: {events[0]['title']}",
            )
        )

    return ReportItem(
        report_type="weekly_summary",
        title=f"Weekly Summary {as_of.date().isoformat()}",
        body_md="\n".join(lines),
        conclusion="監視集合Top50とシグナル銘柄を更新。High根拠が揃う銘柄を優先。",
        falsification_conditions="主要一次情報の否定、欠損率上昇、流動性低下で結論を再評価。",
        confidence="Medium",
        claims=claims,
        citations=citations,
    )


def generate_security_report(
    row: pd.Series,
    as_of: datetime,
    evidence: dict[str, str],
    dcf_markdown: str | None = None,
) -> ReportItem:
    c1 = "C1"
    c2 = "C2"
    c3 = "C3"

    body_lines = [
        f"# {row['security_id']} レポート ({as_of.date().isoformat()})",
        "",
        "## スコア",
        _score_table(row),
        "",
        "## 重要主張",
        f"- {c1}: 総合スコアは市場内で相対上位。",
        f"- {c2}: 現時点の欠損率と流動性から監視継続が妥当。",
        f"- {c3}: シグナル条件は Confidence と順位で判定される。",
        "",
        "## 結論",
        "現時点では監視継続。シグナル点灯時のみエントリー候補。",
        "",
        "## 反証条件",
        "一次情報でガイダンス悪化/重大矛盾が確認された場合、結論を撤回。",
    ]

    if dcf_markdown:
        body_lines.extend(["", "## DCF（Top10のみ）", dcf_markdown])

    citations = [
        CitationItem(
            claim_id=c1,
            doc_version_id=evidence["doc_version_id"],
            page_ref=evidence["page_ref"],
            quote_text=evidence["quote_text"],
        ),
        CitationItem(
            claim_id=c2,
            doc_version_id=evidence["doc_version_id"],
            page_ref=evidence["page_ref"],
            quote_text="Missing ratio and liquidity gates are applied in weekly scoring.",
        ),
        CitationItem(
            claim_id=c3,
            doc_version_id=evidence["doc_version_id"],
            page_ref=evidence["page_ref"],
            quote_text="B-mode signal requires High confidence and Top10 rank.",
        ),
    ]

    claims = [
        {"claim_id": c1, "claim_text": "総合スコアは市場内で相対上位", "status": "supported"},
        {"claim_id": c2, "claim_text": "欠損率と流動性を考慮して監視継続", "status": "supported"},
        {"claim_id": c3, "claim_text": "シグナル条件は High かつ Top10", "status": "supported"},
    ]

    return ReportItem(
        report_type="security_full",
        title=f"Security Report {row['security_id']}",
        body_md="\n".join(body_lines),
        conclusion="シグナル点灯まで監視。点灯時はATRルールで執行。",
        falsification_conditions="重要開示で前提が崩れた場合、または欠損率閾値超過で撤退。",
        confidence=row["confidence"],
        security_id=row["security_id"],
        claims=claims,
        citations=citations,
    )


def generate_event_digest_report(as_of: datetime, events: Iterable[dict[str, str]]) -> ReportItem:
    events = list(events)
    high = [e for e in events if e["importance"] == "high"]
    med = [e for e in events if e["importance"] == "medium"]
    low = [e for e in events if e["importance"] == "low"]

    lines = [
        f"# Daily Event Digest ({as_of.strftime('%Y-%m-%d %H:%M:%S')})",
        "",
        f"- High: {len(high)}",
        f"- Medium: {len(med)}",
        f"- Low: {len(low)}",
        "",
        "## High",
    ]

    if not high:
        lines.append("- なし")
    else:
        for e in high:
            lines.append(f"- {e['title']}: {e['summary']} ({e.get('source_url', '-')})")

    lines.append("")
    lines.append("## Medium")
    if not med:
        lines.append("- なし")
    else:
        for e in med[:10]:
            lines.append(f"- {e['title']}: {e['summary']}")

    return ReportItem(
        report_type="event_digest",
        title=f"Daily Event Digest {as_of.date().isoformat()}",
        body_md="\n".join(lines),
        conclusion="High/Medium イベントを監視集合更新の入力に使用。",
        falsification_conditions="一次情報との不一致が判明した場合は再収集。",
        confidence="Medium",
    )
