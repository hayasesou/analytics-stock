from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
import json
from typing import Any

import pandas as pd

from src.storage.db import NeonRepository
from src.types import (
    CitationItem,
    CryptoDataQualitySnapshot,
    CryptoMarketSnapshot,
    EdgeRisk,
    EdgeState,
    ExperimentSpec,
    FundamentalSnapshot,
    IdeaEvidenceSpec,
    IdeaSpec,
    LessonSpec,
    OrderIntent,
    PortfolioSpec,
    ReportItem,
    StrategyLifecycleReview,
    StrategyRiskEvent,
    StrategyRiskSnapshot,
    StrategyVersionSpec,
)


class _FakeCopy:
    def __init__(self, cursor: "_FakeCursor", query: str):
        self._cursor = cursor
        self.query = query
        self.rows: list[tuple[Any, ...]] = []

    def __enter__(self) -> "_FakeCopy":
        self._cursor.copy_queries.append(self.query)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        self._cursor.copy_rows.extend(self.rows)
        return False

    def write_row(self, row: tuple[Any, ...]) -> None:
        self.rows.append(row)


class _FakeCursor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, Any]] = []
        self.executemany_calls: list[tuple[str, list[tuple[Any, ...]]]] = []
        self.copy_queries: list[str] = []
        self.copy_rows: list[tuple[Any, ...]] = []
        self.fetchone_value: dict[str, Any] | None = None
        self.fetchall_rows: list[dict[str, Any]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        return False

    def execute(self, query: str, params: Any = None) -> None:
        self.execute_calls.append((query, params))

    def executemany(self, query: str, params: list[tuple[Any, ...]], **kwargs: Any) -> None:  # noqa: ARG002
        self.executemany_calls.append((query, list(params)))

    def copy(self, query: str) -> _FakeCopy:
        return _FakeCopy(self, query)

    def fetchone(self) -> dict[str, Any] | None:
        return self.fetchone_value

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self.fetchall_rows)


class _FakeConnection:
    def __init__(self) -> None:
        self._cursor = _FakeCursor()
        self.commit_count = 0
        self.rollback_count = 0

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1


def _repo_with_fake_conn(fake_conn: _FakeConnection) -> NeonRepository:
    repo = NeonRepository("postgresql://unused")

    @contextmanager
    def _fake_conn_ctx():
        yield fake_conn

    repo._conn = _fake_conn_ctx  # type: ignore[method-assign]
    return repo


def test_upsert_prices_uses_copy_and_upsert() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    prices = pd.DataFrame(
        [
            {
                "security_id": "JP:1111",
                "trade_date": date(2026, 2, 7),
                "open_raw": 100.0,
                "high_raw": 101.0,
                "low_raw": 99.0,
                "close_raw": 100.5,
                "volume": 12345,
                "adjusted_close": 100.4,
                "adjustment_factor": 1.0,
                "source": "mock",
            },
            {
                "security_id": "JP:9999",
                "trade_date": date(2026, 2, 7),
                "open_raw": 10.0,
                "high_raw": 11.0,
                "low_raw": 9.0,
                "close_raw": 10.5,
                "volume": 99,
                "source": "mock",
            },
        ]
    )

    repo.upsert_prices(prices, {"JP:1111": "11111111-1111-1111-1111-111111111111"})

    assert fake_conn.commit_count == 1
    assert fake_conn.rollback_count == 0
    assert len(fake_conn._cursor.copy_queries) == 1
    assert len(fake_conn._cursor.copy_rows) == 1

    copied = fake_conn._cursor.copy_rows[0]
    assert copied[0] == "11111111-1111-1111-1111-111111111111"
    assert copied[1] == date(2026, 2, 7)
    assert copied[6] == 12345

    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "CREATE TEMP TABLE tmp_prices_daily_stage" in executed_sql
    assert "INSERT INTO prices_daily" in executed_sql


def test_upsert_document_with_version_inserts_document_and_version() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchone_value = {"id": "11111111-1111-1111-1111-111111111111"}
    repo = _repo_with_fake_conn(fake_conn)

    doc_version_id = repo.upsert_document_with_version(
        external_doc_id="JP:1111:abc",
        source_system="deep_research",
        source_url="file:///tmp/deep-report.txt",
        title="Deep Research JP:1111",
        published_at=datetime(2026, 2, 18, 9, 0, 0),
        retrieved_at=datetime(2026, 2, 18, 9, 1, 0),
        sha256="a" * 64,
        mime_type="text/plain",
        r2_object_key="research/deep_research/2026-02-18/JP_1111/aaa.txt",
        r2_text_key="research/deep_research/2026-02-18/JP_1111/aaa.txt",
        page_count=1,
    )

    assert doc_version_id == "11111111-1111-1111-1111-111111111111"
    assert fake_conn.commit_count == 1
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "INSERT INTO documents" in executed_sql
    assert "INSERT INTO document_versions" in executed_sql


def test_upsert_document_with_version_rejects_invalid_sha() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    try:
        repo.upsert_document_with_version(
            external_doc_id="JP:1111:bad",
            source_system="deep_research",
            source_url="file:///tmp/deep-report.txt",
            title="x",
            published_at=None,
            retrieved_at=None,
            sha256="12345",
            mime_type="text/plain",
            r2_object_key="k",
        )
        raise AssertionError("expected ValueError")
    except ValueError:
        pass


def test_upsert_prices_rolls_back_when_no_mapped_rows() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    prices = pd.DataFrame(
        [
            {
                "security_id": "JP:9999",
                "trade_date": date(2026, 2, 7),
                "open_raw": 10.0,
                "high_raw": 11.0,
                "low_raw": 9.0,
                "close_raw": 10.5,
                "volume": 99,
            }
        ]
    )

    repo.upsert_prices(prices, {"JP:1111": "11111111-1111-1111-1111-111111111111"})

    assert fake_conn.commit_count == 0
    assert fake_conn.rollback_count == 1
    assert len(fake_conn._cursor.copy_rows) == 0

    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "CREATE TEMP TABLE tmp_prices_daily_stage" in executed_sql
    assert "INSERT INTO prices_daily" not in executed_sql


def test_insert_reports_bulk_batches_and_persists_related_rows() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)
    ensured_docs: list[str] = []

    def _fake_ensure_document_version(cur, doc_version_id: str, source_url: str | None = None) -> None:  # noqa: ANN001, ARG001
        ensured_docs.append(doc_version_id)

    repo._ensure_document_version = _fake_ensure_document_version  # type: ignore[method-assign]

    reports = [
        ReportItem(
            report_type="security_full",
            title=f"Report {i}",
            body_md="body",
            conclusion="ok",
            falsification_conditions="none",
            confidence="Medium",
            security_id=f"JP:{1000 + i}",
            claims=[{"claim_id": "C1", "claim_text": "text", "status": "supported"}],
            citations=[
                CitationItem(
                    claim_id="C1",
                    doc_version_id=f"00000000-0000-0000-0000-00000000000{i}",
                    page_ref="p1",
                    quote_text="q",
                )
            ],
        )
        for i in range(1, 4)
    ]

    security_map = {
        "JP:1001": "11111111-1111-1111-1111-111111111111",
        "JP:1002": "22222222-2222-2222-2222-222222222222",
        "JP:1003": "33333333-3333-3333-3333-333333333333",
    }

    report_ids = repo.insert_reports_bulk(
        run_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        reports=reports,
        security_uuid_map=security_map,
        batch_size=2,
    )

    assert len(report_ids) == 3
    assert len(set(report_ids)) == 3
    assert fake_conn.commit_count == 2
    assert fake_conn.rollback_count == 0
    assert sorted(ensured_docs) == [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000003",
    ]

    report_rows = [
        rows
        for sql, rows in fake_conn._cursor.executemany_calls
        if "INSERT INTO reports" in sql
    ]
    claim_rows = [
        rows
        for sql, rows in fake_conn._cursor.executemany_calls
        if "INSERT INTO report_claims" in sql
    ]
    citation_rows = [
        rows
        for sql, rows in fake_conn._cursor.executemany_calls
        if "INSERT INTO citations" in sql
    ]

    assert sum(len(rows) for rows in report_rows) == 3
    assert sum(len(rows) for rows in claim_rows) == 3
    assert sum(len(rows) for rows in citation_rows) == 3


def test_get_evidence_stats_uses_db_aggregates() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {
            "security_id": "JP:1111",
            "primary_source_count": 2,
            "has_key_numbers": True,
            "has_major_contradiction": False,
            "catalyst_bonus": 0.2,
        },
        {
            "security_id": "US:119",
            "primary_source_count": 1,
            "has_key_numbers": False,
            "has_major_contradiction": True,
            "catalyst_bonus": 0.05,
        },
    ]
    repo = _repo_with_fake_conn(fake_conn)

    df = repo.get_evidence_stats(["JP:1111", "US:119"], lookback_days=30)

    assert list(df["security_id"]) == ["JP:1111", "US:119"]
    assert list(df["primary_source_count"]) == [2, 1]
    assert list(df["has_key_numbers"]) == [True, False]
    assert list(df["has_major_contradiction"]) == [False, True]
    assert list(df["catalyst_bonus"]) == [0.2, 0.05]

    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "COUNT(DISTINCT c.doc_version_id)" in executed_sql
    assert "report_claims" in executed_sql
    assert "events" in executed_sql


def test_get_recent_citations_by_security_groups_rows() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {
            "security_id": "JP:1111",
            "claim_id": "C1",
            "doc_version_id": "11111111-1111-1111-1111-111111111111",
            "page_ref": "p2",
            "quote_text": "売上高は前年比10%増。",
        },
        {
            "security_id": "JP:1111",
            "claim_id": "C2",
            "doc_version_id": "22222222-2222-2222-2222-222222222222",
            "page_ref": "p5",
            "quote_text": "営業利益率は12.4%。",
        },
    ]
    repo = _repo_with_fake_conn(fake_conn)

    citations = repo.get_recent_citations_by_security(["JP:1111"], lookback_days=30, per_security_limit=3)

    assert set(citations.keys()) == {"JP:1111"}
    assert len(citations["JP:1111"]) == 2
    assert citations["JP:1111"][0].doc_version_id == "11111111-1111-1111-1111-111111111111"
    assert citations["JP:1111"][0].quote_text == "売上高は前年比10%増。"

    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "ROW_NUMBER() OVER" in executed_sql
    assert "WHERE row_num <= %s" in executed_sql


def test_fetch_signals_for_diagnostics_reads_signal_history() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {"security_id": "JP:1111", "as_of_date": date(2026, 1, 10)},
        {"security_id": "US:119", "as_of_date": date(2026, 1, 17)},
    ]
    repo = _repo_with_fake_conn(fake_conn)

    df = repo.fetch_signals_for_diagnostics(as_of_date=date(2026, 2, 1), lookback_days=365)

    assert list(df["security_id"]) == ["JP:1111", "US:119"]
    assert list(df["as_of_date"]) == [date(2026, 1, 10), date(2026, 1, 17)]
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "FROM signals s" in executed_sql
    assert "s.is_signal = TRUE" in executed_sql


def test_upsert_signal_diagnostics_weekly_persists_three_horizons() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    repo.upsert_signal_diagnostics_weekly(
        run_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        diagnostics=[
            {
                "horizon_days": 5,
                "hit_rate": 0.6,
                "median_return": 0.015,
                "p10_return": -0.03,
                "p90_return": 0.06,
                "sample_size": 40,
            },
            {
                "horizon_days": 20,
                "hit_rate": 0.55,
                "median_return": 0.025,
                "p10_return": -0.08,
                "p90_return": 0.12,
                "sample_size": 40,
            },
            {
                "horizon_days": 60,
                "hit_rate": 0.52,
                "median_return": 0.045,
                "p10_return": -0.12,
                "p90_return": 0.18,
                "sample_size": 40,
            },
        ],
    )

    assert fake_conn.commit_count == 1
    rows = [
        params
        for sql, params in fake_conn._cursor.executemany_calls
        if "INSERT INTO signal_diagnostics_weekly" in sql
    ]
    assert len(rows) == 1
    assert len(rows[0]) == 3


def test_upsert_strategy_version_and_activate() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchone_value = {"id": "11111111-1111-1111-1111-111111111111"}
    repo = _repo_with_fake_conn(fake_conn)

    repo.upsert_strategy_version(
        StrategyVersionSpec(
            strategy_name="mean-reversion-jp",
            version=1,
            spec={"asset_scope": "JP_EQ", "signal": {"type": "rule"}},
            created_by="agent-coder",
            is_active=True,
        )
    )

    assert fake_conn.commit_count == 1
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "INSERT INTO strategies" in executed_sql
    assert "INSERT INTO strategy_versions" in executed_sql
    assert "UPDATE strategy_versions" in executed_sql


def test_fetch_strategies_for_lifecycle_reads_status_filter() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {
            "strategy_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "strategy_name": "sf-btc",
            "asset_scope": "CRYPTO",
            "status": "paper",
            "live_candidate": False,
            "strategy_version_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "version": 2,
            "updated_at": datetime(2026, 2, 20, 12, 0, 0),
        }
    ]
    repo = _repo_with_fake_conn(fake_conn)

    rows = repo.fetch_strategies_for_lifecycle(statuses=["candidate", "paper"], limit=25)

    assert len(rows) == 1
    assert rows[0]["status"] == "paper"
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "FROM strategies s" in executed_sql
    assert "WHERE s.status = ANY(%s::text[])" in executed_sql


def test_fetch_strategy_paper_metrics_uses_intents_and_risk() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchone_value = {
        "paper_days": 18,
        "round_trips": 52,
        "first_intent_at": datetime(2026, 1, 1, 0, 0, 0),
        "last_intent_at": datetime(2026, 2, 20, 0, 0, 0),
        "max_drawdown": -0.02,
        "sharpe_20d": 0.35,
    }
    repo = _repo_with_fake_conn(fake_conn)

    row = repo.fetch_strategy_paper_metrics(
        strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        lookback_days=365,
    )

    assert row["paper_days"] == 18
    assert row["round_trips"] == 52
    query = fake_conn._cursor.execute_calls[0][0]
    assert "FROM order_intents oi" in query
    assert "FROM strategy_risk_snapshots srs" in query


def test_update_strategy_lifecycle_state_updates_status_and_live_candidate() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    repo.update_strategy_lifecycle_state(
        strategy_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        status="paper",
        live_candidate=True,
    )

    assert fake_conn.commit_count == 1
    query, params = fake_conn._cursor.execute_calls[0]
    assert "UPDATE strategies" in query
    assert params == ("paper", True, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def test_insert_strategy_lifecycle_review_persists_action_log() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchone_value = {"id": "11111111-1111-1111-1111-111111111111"}
    repo = _repo_with_fake_conn(fake_conn)

    review_id = repo.insert_strategy_lifecycle_review(
        StrategyLifecycleReview(
            strategy_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            action="approve_live",
            from_status="paper",
            to_status="live",
            live_candidate=False,
            reason="manual approval",
            acted_by="ui-user",
            metadata={"ticket": "TICKET-011"},
        )
    )

    assert review_id == "11111111-1111-1111-1111-111111111111"
    assert fake_conn.commit_count == 1
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "INSERT INTO strategy_lifecycle_reviews" in executed_sql


def test_upsert_portfolio_and_order_intent() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    fake_conn._cursor.fetchone_value = {"id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
    portfolio_id = repo.upsert_portfolio(PortfolioSpec(name="core", base_currency="JPY", broker_map={"JP": "kabu"}))
    assert portfolio_id == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"

    fake_conn._cursor.fetchone_value = {"id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"}
    intent_id = repo.insert_order_intent(
        OrderIntent(
            portfolio_id=portfolio_id,
            strategy_version_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
            as_of=datetime(2026, 2, 15, 7, 0, 0),
            target_positions=[{"symbol": "JP:1111", "target_qty": 100}],
            status="proposed",
            reason="weekly rebalance",
            risk_checks={"max_drawdown": -0.03},
        )
    )
    assert intent_id == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    assert fake_conn.commit_count == 2

    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "INSERT INTO portfolios" in executed_sql
    assert "INSERT INTO order_intents" in executed_sql


def test_upsert_fundamental_snapshot_raises_for_unknown_security() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    snapshot = FundamentalSnapshot(
        security_id="JP:1111",
        as_of_date=date(2026, 2, 15),
        rating="A",
        summary="業績上方修正が継続。",
        snapshot={"drivers": ["earnings_revision"]},
    )

    try:
        repo.upsert_fundamental_snapshot(snapshot, security_uuid_map={})
    except KeyError as exc:
        assert "security not found" in str(exc)
    else:
        raise AssertionError("expected KeyError")


def test_fetch_approved_order_intents_returns_rows() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {
            "intent_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "portfolio_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "strategy_version_id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "as_of": datetime(2026, 2, 15, 7, 0, 0),
            "target_positions": [{"symbol": "JP:1111", "target_qty": 100}],
            "reason": "rebalance",
            "risk_checks": {"max_drawdown": -0.03},
            "status": "approved",
            "portfolio_name": "core",
            "base_currency": "JPY",
            "broker_map": {"JP": "kabu"},
        }
    ]
    repo = _repo_with_fake_conn(fake_conn)

    rows = repo.fetch_approved_order_intents(limit=10)

    assert len(rows) == 1
    assert rows[0]["intent_id"] == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "FROM order_intents oi" in executed_sql
    assert "WHERE oi.status = 'approved'" in executed_sql


def test_has_recent_open_intent_for_strategy_checks_recent_nonterminal_statuses() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchone_value = {"exists_flag": True}
    repo = _repo_with_fake_conn(fake_conn)

    exists = repo.has_recent_open_intent_for_strategy(
        strategy_version_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        lookback_minutes=180,
    )

    assert exists is True
    assert len(fake_conn._cursor.execute_calls) == 1
    query, params = fake_conn._cursor.execute_calls[0]
    assert "FROM order_intents oi" in query
    assert "oi.status IN ('proposed', 'approved', 'sent', 'executing')" in query
    assert "make_interval(mins => %s)" in query
    assert params == ("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", 180)


def test_update_order_intent_status_updates_row() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    repo.update_order_intent_status(
        intent_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        status="done",
    )

    assert fake_conn.commit_count == 1
    assert len(fake_conn._cursor.execute_calls) == 1
    query, params = fake_conn._cursor.execute_calls[0]
    assert "UPDATE order_intents" in query
    assert params == ("done", "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def test_fetch_latest_price_for_symbol_uses_security_or_ticker() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchone_value = {
        "security_id": "JP:1111",
        "market": "JP",
        "ticker": "1111",
        "trade_date": date(2026, 2, 15),
        "close_raw": 1234.5,
    }
    repo = _repo_with_fake_conn(fake_conn)

    row = repo.fetch_latest_price_for_symbol("JP:1111")

    assert row is not None
    assert row["close_raw"] == 1234.5
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "JOIN LATERAL" in executed_sql
    assert "UPPER(s.ticker) = UPPER(%s)" in executed_sql


def test_insert_edge_states_writes_ticket2_columns() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    inserted = repo.insert_edge_states(
        [
            EdgeState(
                strategy_name="sf-jp-1111",
                market_scope="JP_EQ",
                symbol="JP:1111",
                observed_at=datetime(2026, 2, 20, 9, 0, 0),
                edge_score=72.5,
                strategy_version_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                expected_net_edge=0.0125,
                distance_to_entry=0.004,
                confidence=0.82,
                risk_json={"drawdown": 0.02},
                explain="spread after costs is positive",
                market_regime="risk_on",
                meta={"source": "unit-test"},
            )
        ]
    )

    assert inserted == 1
    assert fake_conn.commit_count == 1

    edge_rows = [
        rows
        for sql, rows in fake_conn._cursor.executemany_calls
        if "INSERT INTO edge_states" in sql
    ]
    assert len(edge_rows) == 1
    assert len(edge_rows[0]) == 1

    row = edge_rows[0][0]
    assert row[0] == "sf-jp-1111"
    assert row[6] == 0.0125
    assert row[7] == 0.004
    assert row[8] == 0.0125
    assert row[9] == 0.004
    assert row[14] == "risk_on"

    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.executemany_calls)
    assert "expected_net_edge" in executed_sql
    assert "distance_to_entry" in executed_sql
    assert "risk_json" in executed_sql
    assert "market_regime" in executed_sql


def test_fetch_latest_edge_state_for_strategy_uses_time_filter() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchone_value = {"id": "edge-id", "edge_score": 71.2}
    repo = _repo_with_fake_conn(fake_conn)

    row = repo.fetch_latest_edge_state_for_strategy(
        strategy_version_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        at_or_before=datetime(2026, 2, 20, 12, 0, 0),
    )

    assert row is not None
    assert row["id"] == "edge-id"
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "FROM edge_states" in executed_sql
    assert "strategy_version_id = %s::uuid" in executed_sql
    assert "observed_at <= %s" in executed_sql
    assert "LIMIT 1" in executed_sql


def test_fetch_edge_states_for_period_uses_strategy_and_window() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [{"id": "edge-1"}, {"id": "edge-2"}]
    repo = _repo_with_fake_conn(fake_conn)

    rows = repo.fetch_edge_states_for_period(
        strategy_version_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        start_at=datetime(2026, 2, 1, 0, 0, 0),
        end_at=datetime(2026, 2, 20, 0, 0, 0),
        limit=500,
    )

    assert [r["id"] for r in rows] == ["edge-1", "edge-2"]
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "FROM edge_states" in executed_sql
    assert "observed_at >= %s" in executed_sql
    assert "observed_at <= %s" in executed_sql
    assert "ORDER BY observed_at ASC" in executed_sql


def test_create_idea_chain_and_evidence_persists_linked_rows() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    fake_conn._cursor.fetchone_value = {"id": "11111111-1111-1111-1111-111111111111"}
    idea_id = repo.create_idea(
        IdeaSpec(
            source_type="youtube",
            source_url="https://www.youtube.com/watch?v=abc",
            title="半導体設備需要",
            raw_text="2026年の投資見通し",
            status="new",
            priority=80,
            created_by="discord-user",
            metadata={"channel": "example"},
        )
    )
    assert idea_id == "11111111-1111-1111-1111-111111111111"

    fake_conn._cursor.fetchone_value = {"id": "22222222-2222-2222-2222-222222222222"}
    evidence_id = repo.insert_idea_evidence(
        IdeaEvidenceSpec(
            idea_id=idea_id,
            doc_version_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            excerpt="設備投資は前年比で回復。",
            locator={"offset": 123},
        )
    )
    assert evidence_id == "22222222-2222-2222-2222-222222222222"

    fake_conn._cursor.fetchone_value = {"id": "33333333-3333-3333-3333-333333333333"}
    experiment_id = repo.create_experiment(
        ExperimentSpec(
            idea_id=idea_id,
            strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            hypothesis="半導体設備株のモメンタム継続",
            eval_status="queued",
            metrics={"sharpe": 1.2},
            artifacts={"notebook": "s3://bucket/a.ipynb"},
        )
    )
    assert experiment_id == "33333333-3333-3333-3333-333333333333"

    fake_conn._cursor.fetchone_value = {"id": "44444444-4444-4444-4444-444444444444"}
    lesson_id = repo.create_lesson(
        LessonSpec(
            idea_id=idea_id,
            experiment_id=experiment_id,
            lesson_type="negative",
            summary="イベント前後で逆張りは機能しない局面がある。",
            reusable_checklist={"avoid_pre_earnings": True},
        )
    )
    assert lesson_id == "44444444-4444-4444-4444-444444444444"

    assert fake_conn.commit_count == 4
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "INSERT INTO ideas" in executed_sql
    assert "INSERT INTO idea_evidence" in executed_sql
    assert "INSERT INTO experiments" in executed_sql
    assert "INSERT INTO lessons" in executed_sql


def test_fetch_idea_claim_hashes_by_source_url_returns_normalized_set() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {"claim_hash": "abc123"},
        {"claim_hash": "ABC123"},
        {"claim_hash": ""},
        {"claim_hash": None},
    ]
    repo = _repo_with_fake_conn(fake_conn)

    output = repo.fetch_idea_claim_hashes_by_source_url(
        source_type="youtube",
        source_url="https://www.youtube.com/watch?v=abc",
        limit=20,
    )

    assert output == {"abc123"}
    assert len(fake_conn._cursor.execute_calls) == 1
    query, params = fake_conn._cursor.execute_calls[0]
    assert "metadata->>'claim_hash'" in query
    assert "metadata ? 'claim_hash'" in query
    assert params == ("youtube", "https://www.youtube.com/watch?v=abc", 20)


def test_fetch_research_kanban_counts_merges_idea_and_strategy_statuses() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {"lane": "new", "cnt": 3},
        {"lane": "candidate", "cnt": 4},
        {"lane": "live", "cnt": 1},
    ]
    repo = _repo_with_fake_conn(fake_conn)

    output = repo.fetch_research_kanban_counts()

    assert output["new"] == 3
    assert output["candidate"] == 4
    assert output["live"] == 1
    assert output["analyzing"] == 0
    query, params = fake_conn._cursor.execute_calls[0]
    assert "FROM ideas" in query
    assert "FROM strategies" in query
    assert "GROUP BY lane" in query
    assert len(params) == 2


def test_fetch_research_kanban_samples_returns_per_lane_titles() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {"lane": "new", "item_title": "youtube claim"},
        {"lane": "candidate", "item_title": "sf-btc-main"},
        {"lane": "candidate", "item_title": "sf-sol-main"},
    ]
    repo = _repo_with_fake_conn(fake_conn)

    output = repo.fetch_research_kanban_samples(limit_per_lane=2)

    assert output["new"] == ["youtube claim"]
    assert output["candidate"] == ["sf-btc-main", "sf-sol-main"]
    assert output["paper"] == []
    query, params = fake_conn._cursor.execute_calls[0]
    assert "idea_ranked" in query
    assert "strategy_ranked" in query
    assert "WHERE rn <= %s" in query
    assert params[2] == 2


def test_insert_and_fetch_strategy_risk_events() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    fake_conn._cursor.fetchone_value = {"id": "55555555-5555-5555-5555-555555555555"}
    risk_event_id = repo.insert_strategy_risk_event(
        StrategyRiskEvent(
            strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            event_type="dd_limit_breach",
            payload={"drawdown": -0.035, "threshold": -0.03},
            triggered_at=datetime(2026, 2, 20, 10, 30, 0),
        )
    )
    assert risk_event_id == "55555555-5555-5555-5555-555555555555"

    fake_conn._cursor.fetchall_rows = [
        {
            "id": "55555555-5555-5555-5555-555555555555",
            "strategy_version_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "event_type": "dd_limit_breach",
            "payload": {"drawdown": -0.035, "threshold": -0.03},
            "triggered_at": datetime(2026, 2, 20, 10, 30, 0),
            "created_at": datetime(2026, 2, 20, 10, 30, 0),
        }
    ]
    rows = repo.fetch_strategy_risk_events(
        strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        limit=20,
    )

    assert len(rows) == 1
    assert rows[0]["event_type"] == "dd_limit_breach"
    assert fake_conn.commit_count == 1
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "INSERT INTO strategy_risk_events" in executed_sql
    assert "FROM strategy_risk_events" in executed_sql


def test_insert_edge_states_bulk_upserts_rows() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)
    inserted = repo.insert_edge_states(
        [
            EdgeState(
                strategy_name="edge-radar-equities",
                strategy_version_id=None,
                market_scope="JP_EQ",
                symbol="JP:1111",
                observed_at=datetime(2026, 2, 20, 11, 10, 0),
                edge_score=72.5,
                expected_net_edge_bps=4.5,
                distance_to_entry_bps=0.0,
                confidence=0.87,
                risk={"missing_ratio": 0.1},
                explain="JP:1111 edge_est=+4.50bps",
                meta={"source": "unit-test"},
            )
        ]
    )

    assert inserted == 1
    assert fake_conn.commit_count == 1
    assert len(fake_conn._cursor.executemany_calls) == 1
    query, rows = fake_conn._cursor.executemany_calls[0]
    assert "INSERT INTO edge_states" in query
    assert "ON CONFLICT (strategy_name, market_scope, symbol, observed_at)" in query
    assert len(rows) == 1
    assert rows[0][0] == "edge-radar-equities"
    assert rows[0][3] == "JP:1111"


def test_insert_edge_states_normalizes_edge_risk_schema_and_aliases() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    inserted = repo.insert_edge_states(
        [
            EdgeState(
                strategy_name="edge-risk-normalize",
                strategy_version_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                market_scope="CRYPTO",
                symbol="CRYPTO:BTCUSDT.PERP.BINANCE",
                observed_at=datetime(2026, 2, 20, 9, 15, 0),
                edge_score=66.0,
                expected_net_edge_bps=4.2,
                distance_to_entry_bps=0.0,
                confidence=0.73,
                risk_json={
                    "neutral_ok": True,
                    "liquidity_score": 0.91,
                    "custom_flag": "from-risk-json",
                },
                risk=EdgeRisk(
                    missing_ratio=0.12,
                    extra={"fallback_flag": "from-risk"},
                ),
                explain="normalized risk payload",
                meta={},
            )
        ]
    )

    assert inserted == 1
    query, rows = fake_conn._cursor.executemany_calls[0]
    assert "INSERT INTO edge_states" in query
    risk_payload = json.loads(rows[0][11])
    assert risk_payload["delta_neutral_ok"] is True
    assert risk_payload["liquidity_score"] == 0.91
    assert risk_payload["missing_ratio"] == 0.12
    assert risk_payload["extra"]["custom_flag"] == "from-risk-json"
    assert risk_payload["extra"]["fallback_flag"] == "from-risk"


def test_insert_crypto_market_snapshots_upserts_rows() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    inserted = repo.insert_crypto_market_snapshots(
        [
            CryptoMarketSnapshot(
                exchange="binance",
                symbol="BTCUSDT",
                market_type="perp",
                observed_at=datetime(2026, 2, 20, 12, 0, 0),
                best_bid=100.0,
                best_ask=100.2,
                mid=100.1,
                spread_bps=19.98,
                funding_rate=0.0001,
                open_interest=1000.0,
                mark_price=100.15,
                index_price=100.0,
                basis_bps=15.0,
                source_mode="rest",
                latency_ms=120.0,
                data_quality={"ws_failed": True},
                raw_payload={"book": {"bidPrice": "100.0"}},
            )
        ]
    )

    assert inserted == 1
    assert fake_conn.commit_count == 1
    assert len(fake_conn._cursor.executemany_calls) == 1
    query, rows = fake_conn._cursor.executemany_calls[0]
    assert "INSERT INTO crypto_market_snapshots" in query
    assert "ON CONFLICT (exchange, symbol, market_type, observed_at)" in query
    assert len(rows) == 1
    assert rows[0][0] == "binance"
    assert rows[0][1] == "BTCUSDT"
    assert rows[0][2] == "perp"


def test_insert_crypto_data_quality_snapshots_upserts_rows() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    inserted = repo.insert_crypto_data_quality_snapshots(
        [
            CryptoDataQualitySnapshot(
                exchange="binance",
                symbol="BTCUSDT",
                market_type="perp",
                window_start=datetime(2026, 2, 20, 11, 59, 0),
                window_end=datetime(2026, 2, 20, 12, 0, 0),
                sample_count=3,
                missing_count=0,
                missing_ratio=0.0,
                latency_p95_ms=120.0,
                ws_failover_count=1,
                eligible_for_edge=True,
                details={"source_mode": "rest"},
            )
        ]
    )

    assert inserted == 1
    assert fake_conn.commit_count == 1
    assert len(fake_conn._cursor.executemany_calls) == 1
    query, rows = fake_conn._cursor.executemany_calls[0]
    assert "INSERT INTO crypto_data_quality" in query
    assert "ON CONFLICT (exchange, symbol, market_type, window_start, window_end)" in query
    assert len(rows) == 1
    assert rows[0][0] == "binance"
    assert rows[0][1] == "BTCUSDT"
    assert rows[0][2] == "perp"


def test_fetch_crypto_market_inputs_for_edge_uses_quality_filters() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {"exchange": "binance", "symbol": "BTCUSDT", "market_type": "perp"},
        {"exchange": "hyperliquid", "symbol": "BTC", "market_type": "perp"},
    ]
    repo = _repo_with_fake_conn(fake_conn)

    rows = repo.fetch_crypto_market_inputs_for_edge(
        max_missing_ratio=0.1,
        max_latency_ms=500.0,
        lookback_minutes=30,
        limit=10,
    )

    assert len(rows) == 2
    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "latest_snapshots" in executed_sql
    assert "latest_quality" in executed_sql
    assert "COALESCE(lq.missing_ratio, 0.0) <= %s" in executed_sql
    assert "COALESCE(lq.latency_p95_ms, 0.0) <= %s" in executed_sql


def test_fetch_positions_for_portfolio_returns_rows() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {
            "symbol": "JP:1111",
            "instrument_type": "JP_EQ",
            "qty": 10.0,
            "avg_price": 1000.0,
            "last_price": 1010.0,
            "market_value": 10100.0,
            "unrealized_pnl": 100.0,
            "realized_pnl": 0.0,
            "updated_at": datetime(2026, 2, 20, 10, 0, 0),
        }
    ]
    repo = _repo_with_fake_conn(fake_conn)

    rows = repo.fetch_positions_for_portfolio(
        portfolio_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        symbols=["JP:1111"],
    )

    assert len(rows) == 1
    assert rows[0]["symbol"] == "JP:1111"
    query = fake_conn._cursor.execute_calls[0][0]
    assert "FROM positions" in query
    assert "portfolio_id = %s::uuid" in query


def test_fetch_open_orders_for_portfolio_returns_rows() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {
            "order_id": "ord-1",
            "intent_id": "intent-1",
            "broker": "gateway_jp",
            "symbol": "JP:7203",
            "instrument_type": "JP_EQ",
            "side": "BUY",
            "qty": 100.0,
            "status": "ack",
            "submitted_at": datetime(2026, 2, 20, 10, 0, 0),
            "updated_at": datetime(2026, 2, 20, 10, 1, 0),
            "meta": {},
        }
    ]
    repo = _repo_with_fake_conn(fake_conn)

    rows = repo.fetch_open_orders_for_portfolio(
        portfolio_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        symbols=["JP:7203"],
        limit=20,
    )

    assert len(rows) == 1
    assert rows[0]["status"] == "ack"
    query = fake_conn._cursor.execute_calls[0][0]
    assert "FROM orders o" in query
    assert "JOIN order_intents oi" in query
    assert "o.status IN ('new', 'sent', 'ack', 'partially_filled')" in query


def test_upsert_and_fetch_strategy_risk_snapshots() -> None:
    fake_conn = _FakeConnection()
    repo = _repo_with_fake_conn(fake_conn)

    fake_conn._cursor.fetchone_value = {"id": "99999999-9999-9999-9999-999999999999"}
    snapshot_id = repo.upsert_strategy_risk_snapshot(
        StrategyRiskSnapshot(
            strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            as_of=datetime(2026, 2, 20, 12, 0, 0),
            drawdown=-0.031,
            sharpe_20d=-0.05,
            state="halted",
            trigger_flags={"drawdown_breach": True},
            cooldown_until=datetime(2026, 2, 21, 12, 0, 0),
        )
    )
    assert snapshot_id == "99999999-9999-9999-9999-999999999999"

    fake_conn._cursor.fetchone_value = {
        "id": "99999999-9999-9999-9999-999999999999",
        "strategy_version_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        "state": "halted",
    }
    latest = repo.fetch_latest_strategy_risk_snapshot(
        strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    )
    assert latest is not None
    assert latest["state"] == "halted"

    fake_conn._cursor.fetchall_rows = [
        {
            "id": "99999999-9999-9999-9999-999999999999",
            "strategy_version_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "as_of": datetime(2026, 2, 20, 12, 0, 0),
            "as_of_date": date(2026, 2, 20),
            "drawdown": -0.031,
            "sharpe_20d": -0.05,
            "state": "halted",
            "trigger_flags": {"drawdown_breach": True},
            "cooldown_until": datetime(2026, 2, 21, 12, 0, 0),
            "created_at": datetime(2026, 2, 20, 12, 0, 0),
        }
    ]
    rows = repo.fetch_recent_strategy_risk_snapshots(
        strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        limit=20,
    )
    assert len(rows) == 1
    assert rows[0]["state"] == "halted"

    executed_sql = "\n".join(call[0] for call in fake_conn._cursor.execute_calls)
    assert "INSERT INTO strategy_risk_snapshots" in executed_sql
    assert "FROM strategy_risk_snapshots" in executed_sql


def test_fetch_strategy_symbols_for_portfolio_uses_jsonb_targets() -> None:
    fake_conn = _FakeConnection()
    fake_conn._cursor.fetchall_rows = [
        {"symbol": "JP:7203", "instrument_type": "JP_EQ"},
        {"symbol": "US:AAPL", "instrument_type": "US_EQ"},
    ]
    repo = _repo_with_fake_conn(fake_conn)

    rows = repo.fetch_strategy_symbols_for_portfolio(
        strategy_version_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        portfolio_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        lookback_days=14,
    )

    assert len(rows) == 2
    query = fake_conn._cursor.execute_calls[0][0]
    assert "jsonb_array_elements(oi.target_positions)" in query
    assert "oi.strategy_version_id = %s::uuid" in query
