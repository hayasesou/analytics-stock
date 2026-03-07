from __future__ import annotations

from datetime import datetime

from src.integrations.discord import DiscordNotifier


class _CaptureNotifier(DiscordNotifier):
    def __init__(self) -> None:
        super().__init__(webhook_url=None)
        self.messages: list[str] = []

    def _post(self, content: str) -> None:  # noqa: D401
        self.messages.append(content)


def test_send_risk_bulletin_formats_alert_items() -> None:
    notifier = _CaptureNotifier()
    notifier.send_risk_bulletin(
        now=datetime(2026, 2, 20, 12, 0, 0),
        items=[
            {
                "category": "partial_fill",
                "title": "crypto gateway partial fill",
                "strategy_version_id": "sv-1",
                "intent_id": "intent-1",
                "detail": "legs=2 gateway_status=partial_closed",
            }
        ],
    )

    assert len(notifier.messages) == 1
    message = notifier.messages[0]
    assert "[Risk速報]" in message
    assert "[partial_fill] crypto gateway partial fill" in message
    assert "strategy=sv-1" in message
    assert "intent=intent-1" in message


def test_send_research_kanban_formats_status_counts_and_samples() -> None:
    notifier = _CaptureNotifier()
    notifier.send_research_kanban(
        now=datetime(2026, 2, 20, 12, 0, 0),
        counts={
            "new": 2,
            "analyzing": 1,
            "rejected": 1,
            "candidate": 3,
            "paper": 2,
            "live": 1,
        },
        samples={
            "new": ["youtube claim #1"],
            "candidate": ["sf-btc-main"],
            "live": ["sf-eth-live"],
        },
    )

    assert len(notifier.messages) == 1
    message = notifier.messages[0]
    assert "[Research Kanban]" in message
    assert "- new: 2" in message
    assert "- candidate: 3" in message
    assert "new> youtube claim #1" in message
    assert "live> sf-eth-live" in message
