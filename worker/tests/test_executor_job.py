from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from src.jobs import executor as executor_job


class _FakeRepo:
    def __init__(self, _dsn: str, intents: list[dict] | None = None, latest_price: dict | None = None):
        self.intents = intents or []
        self.latest_price = latest_price
        self.status_updates: list[tuple[str, str]] = []
        self.inserted_risk_states: list[str] = []
        self.orders_inserted = 0
        self.fills_inserted = 0
        self.positions_upserted = 0

    def fetch_approved_order_intents(self, limit: int = 20):  # noqa: ARG002
        return list(self.intents)

    def update_order_intent_status(self, intent_id: str, status: str):
        self.status_updates.append((intent_id, status))

    def fetch_latest_risk_snapshot(self, portfolio_id: str):  # noqa: ARG002
        return None

    def fetch_recent_risk_snapshots(self, portfolio_id: str, limit: int = 40):  # noqa: ARG002
        return []

    def insert_risk_snapshot(self, snapshot):
        self.inserted_risk_states.append(snapshot.state)
        return "risk-id"

    def fetch_latest_fundamental_ratings_by_symbols(self, symbols):  # noqa: ANN001
        return {str(symbol): "A" for symbol in symbols}

    def fetch_latest_price_for_symbol(self, symbol: str):  # noqa: ARG002
        return self.latest_price

    def insert_orders_bulk(self, orders):
        self.orders_inserted += len(orders)
        return [f"order-{i}" for i, _ in enumerate(orders)]

    def insert_order_fills(self, fills):
        self.fills_inserted += len(fills)

    def upsert_positions(self, positions):
        self.positions_upserted += len(positions)


def test_run_executor_once_rejects_intent_when_drawdown_breach(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-1",
                "portfolio_id": "portfolio-1",
                "strategy_version_id": None,
                "target_positions": [{"symbol": "JP:1111", "target_qty": 100}],
                "risk_checks": {"drawdown": -0.05, "sharpe_20d": 0.2},
                "broker_map": {"JP": "kabu"},
                "portfolio_name": "core",
            }
        ],
    )

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {"execution": {"risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0}}},
    )
    monkeypatch.setattr(executor_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["rejected"] == 1
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates == [("intent-1", "executing"), ("intent-1", "rejected")]
    assert fake_repo.inserted_risk_states == ["halted"]


def test_run_executor_once_processes_paper_fill(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-2",
                "portfolio_id": "portfolio-2",
                "strategy_version_id": "sv-1",
                "target_positions": [{"symbol": "JP:2222", "target_qty": 50}],
                "risk_checks": {"drawdown": -0.01, "sharpe_20d": 0.4},
                "broker_map": {"JP": "kabu"},
                "portfolio_name": "core",
            }
        ],
        latest_price={"close_raw": 1234.5},
    )

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {"execution": {"risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0}}},
    )
    monkeypatch.setattr(executor_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["done"] == 1
    assert fake_repo.orders_inserted == 1
    assert fake_repo.fills_inserted == 1
    assert fake_repo.positions_upserted == 1
    assert fake_repo.status_updates[-1] == ("intent-2", "done")
    assert fake_repo.inserted_risk_states == ["normal"]


def test_run_executor_once_rejects_by_fundamental_overlay(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-3",
                "portfolio_id": "portfolio-3",
                "strategy_version_id": "sv-2",
                "target_positions": [{"symbol": "JP:3333", "target_qty": 20}],
                "risk_checks": {"drawdown": -0.005, "sharpe_20d": 0.3},
                "broker_map": {"JP": "kabu"},
                "portfolio_name": "core",
            }
        ],
        latest_price={"close_raw": 321.0},
    )

    def _ratings(symbols):  # noqa: ANN001
        return {str(symbol): "C" for symbol in symbols}

    fake_repo.fetch_latest_fundamental_ratings_by_symbols = _ratings  # type: ignore[method-assign]
    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "fundamental_overlay": {
                    "enabled": True,
                    "allow_if_missing": False,
                    "trade_allow_ratings": ["A", "B"],
                    "size_multiplier_by_rating": {"A": 1.0, "B": 0.6, "C": 0.0},
                },
            }
        },
    )
    monkeypatch.setattr(executor_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["rejected"] == 1
    assert stats["skipped_by_fundamental"] == 1
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates[-1] == ("intent-3", "rejected")


def test_run_executor_once_rejects_by_data_quality_staleness(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-4",
                "portfolio_id": "portfolio-4",
                "strategy_version_id": "sv-3",
                "target_positions": [{"symbol": "JP:4444", "target_qty": 10}],
                "risk_checks": {"drawdown": -0.005, "sharpe_20d": 0.3},
                "broker_map": {"JP": "kabu"},
                "portfolio_name": "core",
            }
        ],
        latest_price={"close_raw": 111.0, "trade_date": date(2020, 1, 1)},
    )

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "data_quality": {
                    "enabled": True,
                    "reject_on_missing_price": True,
                    "max_price_staleness_days": {"JP": 1, "US": 1, "CRYPTO": 1},
                },
            }
        },
    )
    monkeypatch.setattr(executor_job, "load_runtime_secrets", lambda: SimpleNamespace(database_url="postgresql://unused"))
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["rejected"] == 1
    assert stats["skipped_by_data_quality"] == 1
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates[-1] == ("intent-4", "rejected")
