from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from typing import Any

import pandas as pd

from src.storage.db import NeonRepository
from src.types import (
    CitationItem,
    FundamentalSnapshot,
    OrderIntent,
    PortfolioSpec,
    ReportItem,
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
