from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from src.jobs import research_chat as research_chat_job


class _FakeRepo:
    def __init__(self, _dsn: str):
        now = datetime(2026, 3, 7, tzinfo=timezone.utc)
        self.marked: list[dict[str, object]] = []
        self.inserted_artifacts: list[object] = []
        self.inserted_runs: list[object] = []
        self.inserted_outcomes: list[object] = []
        self.appended_messages: list[dict[str, object]] = []
        self.discord_followups: list[dict[str, object]] = []
        self.updated_hypotheses: list[dict[str, object]] = []
        self.updated_inputs: list[dict[str, object]] = []
        self.enqueued_tasks: list[dict[str, object]] = []
        self._tasks = [
            {
                "id": "task-extract",
                "task_type": "research.extract_input",
                "payload": {"session_id": "session-1", "requested_by": "discord", "discord_channel_id": "thread-1"},
            },
            {
                "id": "task-quant",
                "task_type": "research.quant_plan",
                "payload": {"session_id": "session-1", "requested_by": "discord", "discord_channel_id": "thread-1"},
            },
            {
                "id": "task-portfolio",
                "task_type": "research.portfolio_build",
                "payload": {"session_id": "session-1", "requested_by": "discord", "discord_channel_id": "thread-1"},
            },
            {
                "id": "task-validate",
                "task_type": "research.validate_outcome",
                "payload": {"session_id": "session-1", "hypothesis_ids": ["hyp-1"], "requested_by": "discord", "discord_channel_id": "thread-1"},
            },
        ]
        self._inputs = [
            {
                "id": "input-1",
                "source_type": "web_url",
                "source_url": "https://example.com/post",
                "raw_text": "https://example.com/post NVDA looks strong",
                "extracted_text": None,
                "extraction_status": "queued",
            },
            {
                "id": "input-2",
                "source_type": "text",
                "source_url": None,
                "raw_text": "NVDA looks strong",
                "extracted_text": "NVDA looks strong",
                "extraction_status": "success",
            },
        ]
        self._hypotheses = [
            {
                "id": "hyp-1",
                "session_id": "session-1",
                "stance": "bullish",
                "horizon_days": 20,
                "thesis_md": "NVDA can continue higher.",
                "falsification_md": "Demand weakens.",
                "created_at": now,
            }
        ]
        self._assets = [
            {
                "id": "asset-1",
                "hypothesis_id": "hyp-1",
                "security_id": "US:NVDA",
                "symbol_text": "US:NVDA",
                "ticker": "NVDA",
            }
        ]

    def fetch_queued_agent_tasks(self, limit: int = 20, task_types=None, assigned_role=None):  # noqa: ANN001,ARG002
        return list(self._tasks)[:limit]

    def mark_agent_task(self, task_id: str, status: str, result=None, cost_usd=None, error_text=None):  # noqa: ANN001
        self.marked.append(
            {
                "task_id": task_id,
                "status": status,
                "result": result,
                "cost_usd": cost_usd,
                "error_text": error_text,
            }
        )

    def fetch_research_external_inputs(self, session_id: str):  # noqa: ARG002
        return list(self._inputs)

    def update_research_external_input(self, input_id: str, **kwargs):  # noqa: ANN003
        self.updated_inputs.append({"input_id": input_id, **kwargs})

    def fetch_research_hypotheses_by_ids(self, hypothesis_ids: list[str]):  # noqa: ARG002
        return list(self._hypotheses)

    def fetch_research_hypotheses_for_session(self, session_id: str):  # noqa: ARG002
        return list(self._hypotheses)

    def fetch_research_hypothesis_assets(self, hypothesis_ids: list[str]):  # noqa: ARG002
        return list(self._assets)

    def insert_research_artifact(self, spec):  # noqa: ANN001
        self.inserted_artifacts.append(spec)
        return f"artifact-{len(self.inserted_artifacts)}"

    def enqueue_agent_task(self, **kwargs):  # noqa: ANN003
        self.enqueued_tasks.append(kwargs)
        return f"task-{len(self.enqueued_tasks)}"

    def fetch_research_artifact(self, artifact_id: str):  # noqa: ARG002
        return {
            "id": "artifact-1",
            "session_id": "session-1",
            "hypothesis_id": "hyp-1",
            "artifact_type": "sql",
            "title": "Quant Validation SQL",
            "code_text": "select trade_date, close_raw from prices_daily",
        }

    def fetch_research_artifacts_for_session(self, session_id: str):  # noqa: ARG002
        return [
            {"artifact_type": "report", "title": "Generated Hypothesis Pack"},
            {"artifact_type": "sql", "title": "Quant Validation SQL"},
            {"artifact_type": "python", "title": "Python Analysis Draft"},
        ]

    def fetch_latest_research_artifact_run(self, artifact_id: str):  # noqa: ARG002
        return {
            "id": "run-1",
            "run_status": "success",
            "result_json": {"rows": [["2026-03-07", 121.23], ["2026-03-08", 122.10]]},
        }

    def insert_research_artifact_run(self, spec):  # noqa: ANN001
        self.inserted_runs.append(spec)
        return f"run-{len(self.inserted_runs)}"

    def fetch_latest_price_for_symbol(self, symbol: str):  # noqa: ARG002
        return {"security_id": "US:NVDA"}

    def fetch_price_history_for_security(self, security_id: str, start_date, end_date):  # noqa: ANN001,ARG002
        import pandas as pd

        return pd.DataFrame(
            [
                {"security_id": security_id, "market": "US", "trade_date": datetime(2026, 3, 7).date(), "open_raw": 100.0, "high_raw": 101.0, "low_raw": 99.0, "close_raw": 100.0},
                {"security_id": security_id, "market": "US", "trade_date": datetime(2026, 3, 8).date(), "open_raw": 101.0, "high_raw": 102.0, "low_raw": 100.0, "close_raw": 102.0},
                {"security_id": security_id, "market": "US", "trade_date": datetime(2026, 3, 9).date(), "open_raw": 102.0, "high_raw": 103.0, "low_raw": 101.0, "close_raw": 103.0},
                {"security_id": security_id, "market": "US", "trade_date": datetime(2026, 3, 10).date(), "open_raw": 103.0, "high_raw": 104.0, "low_raw": 102.0, "close_raw": 104.0},
                {"security_id": security_id, "market": "US", "trade_date": datetime(2026, 3, 11).date(), "open_raw": 104.0, "high_raw": 105.0, "low_raw": 103.0, "close_raw": 105.0},
                {"security_id": security_id, "market": "US", "trade_date": datetime(2026, 3, 12).date(), "open_raw": 105.0, "high_raw": 107.0, "low_raw": 104.0, "close_raw": 106.0},
            ]
        )

    def insert_research_hypothesis_outcome(self, spec):  # noqa: ANN001
        self.inserted_outcomes.append(spec)
        return "outcome-1"

    def update_research_hypothesis(self, hypothesis_id: str, **kwargs):  # noqa: ANN003
        self.updated_hypotheses.append({"hypothesis_id": hypothesis_id, **kwargs})

    def fetch_latest_chat_message(self, session_id: str, role: str | None = None):  # noqa: ARG002
        return {"content": "previous summary"}

    def append_chat_message(self, **kwargs):  # noqa: ANN003
        self.appended_messages.append(kwargs)
        return "msg-1"


def test_run_research_chat_once_processes_supported_tasks(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    monkeypatch.setattr(research_chat_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(research_chat_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(
        research_chat_job,
        "send_bot_message",
        lambda bot_token, channel_id, content, timeout_sec=10: fake_repo.discord_followups.append(  # noqa: ARG005
            {"kind": "message", "bot_token": bot_token, "channel_id": channel_id, "content": content}
        ),
    )
    monkeypatch.setattr(
        research_chat_job,
        "send_bot_file",
        lambda bot_token, channel_id, **kwargs: fake_repo.discord_followups.append(  # noqa: ARG005
            {"kind": "file", "bot_token": bot_token, "channel_id": channel_id, **kwargs}
        ),
    )

    stats = research_chat_job.run_research_chat_once(limit=10)

    assert stats["queued"] == 4
    assert stats["processed"] == 4
    assert stats["success"] == 4
    assert any(item["task_id"] == "task-extract" and item["status"] == "success" for item in fake_repo.marked)
    assert any(item["task_id"] == "task-validate" and item["status"] == "success" for item in fake_repo.marked)
    assert len(fake_repo.inserted_artifacts) >= 1
    assert len(fake_repo.inserted_outcomes) == 1
    assert any(task["task_type"] == "research.artifact_run" for task in fake_repo.enqueued_tasks)
    assert fake_repo.discord_followups


def test_build_discord_follow_up_contains_url_and_artifacts(monkeypatch) -> None:
    monkeypatch.setenv("WEB_BASE_URL", "https://example.test")

    text = research_chat_job._build_discord_follow_up(  # noqa: SLF001
        session_id="session-1",
        summary="main summary",
        hypotheses=[
            {
                "stance": "bullish",
                "horizon_days": 20,
                "confidence": 0.5,
                "thesis_md": "earnings revisions can move the stock",
                "falsification_md": "revisions do not arrive",
                "metadata": {"validation_plan": "track estimates", "key_metrics": ["ret_5d", "estimate_revision"]},
            }
        ],
        artifacts=[{"artifact_type": "sql", "title": "Quant Validation SQL"}],
    )

    assert "https://example.test/research/chat?sessionId=session-1" in text
    assert "Quant Validation SQL" in text


def test_build_discord_chart_message_contains_chart_summaries(monkeypatch) -> None:
    monkeypatch.setenv("WEB_BASE_URL", "https://example.test")

    text = research_chat_job._build_discord_chart_message(  # noqa: SLF001
        session_id="session-1",
        source_title="Quant Validation SQL",
        charts=[
            {"title": "Price Trend", "kind": "line", "summary": "価格推移を確認する。"},
            {"title": "Pct Change", "kind": "bar", "summary": "変化率を確認する。"},
        ],
    )

    assert "Price Trend" in text
    assert "Pct Change" in text
    assert "https://example.test/research/chat?sessionId=session-1" in text


def test_build_chart_svg_returns_svg_markup() -> None:
    svg = research_chat_job._build_chart_svg(  # noqa: SLF001
        {
            "title": "Price Trend",
            "kind": "line",
            "summary": "価格推移を確認する。",
            "xAxisLabel": "Date",
            "yAxisLabel": "Price",
            "series": [{"name": "close", "data": [["2026-03-07", 121.23], ["2026-03-08", 122.1]]}],
        }
    )

    assert svg is not None
    assert "<svg" in svg
    assert "Price Trend" in svg


def test_build_chart_png_returns_png_bytes() -> None:
    png = research_chat_job._build_chart_png(  # noqa: SLF001
        {
            "title": "Price Trend",
            "kind": "line",
            "summary": "価格推移を確認する。",
            "xAxisLabel": "Date",
            "yAxisLabel": "Price",
            "series": [{"name": "close", "data": [["2026-03-07", 121.23], ["2026-03-08", 122.1]]}],
        }
    )

    assert png is not None
    assert png.startswith(b"\x89PNG\r\n\x1a\n")


def test_send_discord_chart_follow_up_sends_message_and_files(monkeypatch) -> None:
    sent: list[dict[str, object]] = []
    monkeypatch.setenv("WEB_BASE_URL", "https://example.test")
    monkeypatch.setattr(
        research_chat_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_bot_token="bot-token"),
    )
    monkeypatch.setattr(
        research_chat_job,
        "send_bot_message",
        lambda bot_token, channel_id, content, timeout_sec=10: sent.append(  # noqa: ARG005
            {"kind": "message", "channel_id": channel_id, "content": content}
        ),
    )
    monkeypatch.setattr(
        research_chat_job,
        "send_bot_file",
        lambda bot_token, channel_id, **kwargs: sent.append(  # noqa: ARG005
            {"kind": "file", "channel_id": channel_id, **kwargs}
        ),
    )

    research_chat_job._send_discord_chart_follow_up(  # noqa: SLF001
        payload={"requested_by": "discord", "discord_channel_id": "thread-1"},
        session_id="session-1",
        source_title="Quant Validation SQL",
        charts=[
            {
                "title": "Price Trend",
                "kind": "line",
                "summary": "価格推移を確認する。",
                "xAxisLabel": "Date",
                "yAxisLabel": "Price",
                "series": [{"name": "close", "data": [["2026-03-07", 121.23], ["2026-03-08", 122.1]]}],
            }
        ],
    )

    assert any(item["kind"] == "message" for item in sent)
    assert any(item["kind"] == "file" and str(item["filename"]).endswith(".png") for item in sent)


def test_process_chart_generate_uses_latest_run(monkeypatch) -> None:
    fake_repo = _FakeRepo("postgresql://unused")
    sent: list[dict[str, object]] = []
    monkeypatch.setattr(
        research_chat_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_bot_token="bot-token"),
    )
    monkeypatch.setattr(
        research_chat_job,
        "send_bot_message",
        lambda bot_token, channel_id, content, timeout_sec=10: sent.append({"kind": "message", "content": content}),  # noqa: ARG005
    )
    monkeypatch.setattr(
        research_chat_job,
        "send_bot_file",
        lambda bot_token, channel_id, **kwargs: sent.append({"kind": "file", **kwargs}),  # noqa: ARG005
    )

    result = research_chat_job._process_chart_generate(  # noqa: SLF001
        fake_repo,
        {"artifact_id": "artifact-1", "requested_by": "discord", "discord_channel_id": "thread-1"},
    )

    assert result["chart_artifact_ids"]
    assert any(getattr(spec, "artifact_type", "") == "chart" for spec in fake_repo.inserted_artifacts)


def test_execute_python_returns_stdout():
    result = research_chat_job._execute_python("print({'value': 1})")  # noqa: SLF001

    assert "value" in result["stdout"]


def test_execute_python_allows_safe_imports():
    result = research_chat_job._execute_python(  # noqa: SLF001
        "import math\nreturns = [0.01, -0.005, 0.012, 0.004]\nmean_ret = sum(returns) / len(returns)\nvariance = sum((x - mean_ret) ** 2 for x in returns) / len(returns)\nprint(round(math.sqrt(variance), 6))"
    )

    assert result["stdout"].strip() == "0.00661"


def test_fallback_chart_specs_from_python_stdout_dict() -> None:
    specs = research_chat_job._fallback_chart_specs_from_python_result(  # noqa: SLF001
        {"stdout": "{'symbol': 'US:NVDA', 'mean_return': 0.01, 'volatility': 0.02}"},
        "Python Analysis Draft",
    )

    assert specs
    assert specs[0]["kind"] == "bar"
    assert len(specs[0]["series"][0]["data"]) == 2


def test_fallback_chart_specs_from_sql_result_returns_multiple_specs() -> None:
    specs = research_chat_job._fallback_chart_specs_from_sql_result(  # noqa: SLF001
        {"rows": [["2026-03-07", 121.23], ["2026-03-08", 122.1]]},
        "Quant Validation SQL",
    )

    assert len(specs) >= 1
    assert specs[0]["kind"] in {"line", "bar"}
    assert len(specs[0]["series"][0]["data"]) == 2


def test_fallback_chart_specs_respects_preferred_chart_type() -> None:
    specs = research_chat_job._fallback_chart_specs_from_sql_result(  # noqa: SLF001
        {"rows": [["2026-03-07", 121.23], ["2026-03-08", 122.1]]},
        "Quant Validation SQL",
        preferred_chart_type="scatter",
        instruction="散布で見たい",
    )

    assert specs
    assert specs[0]["kind"] == "scatter"


def test_create_chart_artifacts_from_run_inserts_chart() -> None:
    fake_repo = _FakeRepo("postgresql://unused")

    chart_rows = research_chat_job._create_chart_artifacts_from_run(  # noqa: SLF001
        fake_repo,
        artifact={
            "id": "artifact-sql-1",
            "session_id": "session-1",
            "hypothesis_id": "hyp-1",
            "artifact_type": "sql",
            "title": "Quant Validation SQL",
        },
        result={"rows": [["2026-03-07", 121.23], ["2026-03-08", 122.1]]},
        run_id="run-1",
    )

    assert chart_rows
    assert any(getattr(spec, "artifact_type", "") == "chart" for spec in fake_repo.inserted_artifacts)


def test_is_sql_safe_rejects_mutation():
    assert research_chat_job._is_sql_safe("select 1") is True  # noqa: SLF001
    assert research_chat_job._is_sql_safe("delete from prices_daily") is False  # noqa: SLF001


def test_fallback_hypotheses_returns_watch_bias():
    result = research_chat_job._fallback_hypotheses(  # noqa: SLF001
        question="NVDA looks strong",
        urls=["https://example.com/a"],
        texts=["NVDA demand remains strong."],
        security_id="US:NVDA",
    )

    assert "summary" in result
    assert len(result["hypotheses"]) >= 1
    assert result["hypotheses"][0]["stance"] in {"watch", "neutral", "bullish"}


def test_fetch_url_excerpt_parses_title_and_excerpt(monkeypatch):
    class _Resp:
        status_code = 200
        headers = {"content-type": "text/html; charset=utf-8"}
        text = "<html><head><title>Test Page</title></head><body><h1>Hello</h1><p>World content</p></body></html>"

        def raise_for_status(self):
            return None

    monkeypatch.setattr(research_chat_job.requests, "get", lambda *args, **kwargs: _Resp())

    result = research_chat_job._fetch_url_excerpt("https://example.com")  # noqa: SLF001

    assert result["title"] == "Test Page"
    assert "Hello World content" in result["excerpt"]
