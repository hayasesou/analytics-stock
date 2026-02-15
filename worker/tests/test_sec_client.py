from __future__ import annotations

from typing import Any

import pytest

from src.integrations.sec import SecEdgarClient


class _FakeResponse:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return {}


def test_fetch_current_filings_parses_atom(monkeypatch: pytest.MonkeyPatch) -> None:
    atom_xml = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <title>8-K - Example Corp (0000123456) (Filer)</title>
    <updated>2026-02-15T03:00:22-05:00</updated>
    <summary type="html">&lt;b&gt;Filed&lt;/b&gt;: 2026-02-15</summary>
    <category term="8-K" />
    <link rel="alternate" href="https://www.sec.gov/Archives/edgar/data/example" />
  </entry>
</feed>
"""

    def fake_get(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        _ = args
        assert kwargs["params"]["action"] == "getcurrent"
        assert kwargs["params"]["output"] == "atom"
        return _FakeResponse(atom_xml)

    monkeypatch.setattr("src.integrations.sec.requests.get", fake_get)

    client = SecEdgarClient(user_agent="stock-analysis-test")
    rows = client.fetch_current_filings(count=10)

    assert len(rows) == 1
    row = rows[0]
    assert row["form_type"] == "8-K"
    assert row["company_name"] == "Example Corp"
    assert row["source_url"] == "https://www.sec.gov/Archives/edgar/data/example"
    assert "Filed" in row["summary"]
    assert "2026-02-15" in row["summary"]
