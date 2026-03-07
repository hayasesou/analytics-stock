from __future__ import annotations

from datetime import date, datetime, timezone
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
        self.inserted_orders = []
        self.inserted_risk_events = []
        self.positions_rows: list[dict] = []
        self.open_orders_rows: list[dict] = []
        self.strategy_risk_snapshots: list[dict] = []
        self.strategy_symbols_rows: list[dict] = []

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
        self.inserted_orders.extend(list(orders))
        return [f"order-{i}" for i, _ in enumerate(orders)]

    def insert_order_fills(self, fills):
        self.fills_inserted += len(fills)

    def upsert_positions(self, positions):
        self.positions_upserted += len(positions)

    def insert_strategy_risk_event(self, event):  # noqa: ANN001
        self.inserted_risk_events.append(event)
        return f"risk-event-{len(self.inserted_risk_events)}"

    def fetch_positions_for_portfolio(self, portfolio_id: str, symbols: list[str] | None = None):  # noqa: ARG002
        return list(self.positions_rows)

    def fetch_open_orders_for_portfolio(self, portfolio_id: str, symbols: list[str] | None = None, limit: int = 500):  # noqa: ARG002
        return list(self.open_orders_rows)

    def fetch_latest_strategy_risk_snapshot(self, strategy_version_id: str):  # noqa: ARG002
        if not self.strategy_risk_snapshots:
            return None
        return dict(self.strategy_risk_snapshots[-1])

    def fetch_recent_strategy_risk_snapshots(self, strategy_version_id: str, limit: int = 40):  # noqa: ARG002
        rows = list(self.strategy_risk_snapshots)

        def _sort_key(row: dict) -> tuple[int, int]:
            as_of = row.get("as_of")
            if isinstance(as_of, datetime):
                return (int(as_of.timestamp()), 0)
            as_of_date = row.get("as_of_date")
            if hasattr(as_of_date, "toordinal"):
                return (0, int(as_of_date.toordinal()))
            return (0, 0)

        rows.sort(key=lambda row: _sort_key(row if isinstance(row, dict) else {}), reverse=True)
        return rows[: max(1, int(limit))]

    def upsert_strategy_risk_snapshot(self, snapshot):  # noqa: ANN001
        self.strategy_risk_snapshots.append(
            {
                "strategy_version_id": snapshot.strategy_version_id,
                "as_of": snapshot.as_of,
                "as_of_date": snapshot.as_of.date(),
                "drawdown": snapshot.drawdown,
                "sharpe_20d": snapshot.sharpe_20d,
                "state": snapshot.state,
                "trigger_flags": snapshot.trigger_flags,
                "cooldown_until": snapshot.cooldown_until,
            }
        )
        return f"strategy-risk-{len(self.strategy_risk_snapshots)}"

    def fetch_strategy_symbols_for_portfolio(self, strategy_version_id: str, portfolio_id: str, lookback_days: int = 30):  # noqa: ARG002
        return list(self.strategy_symbols_rows)


class _FakeGatewayClient:
    next_response: dict = {}
    calls: list[dict] = []

    def __init__(self, base_url: str, auth_token: str | None = None, timeout_sec: float = 8.0):  # noqa: ARG002
        self.base_url = base_url
        self.auth_token = auth_token
        self.timeout_sec = timeout_sec

    def execute_intent(self, payload: dict):
        _FakeGatewayClient.calls.append(payload)
        return dict(_FakeGatewayClient.next_response)


class _FakeJpGatewayClient:
    next_response: dict = {}
    calls: list[dict] = []

    def __init__(self, base_url: str, auth_token: str | None = None, timeout_sec: float = 8.0):  # noqa: ARG002
        self.base_url = base_url
        self.auth_token = auth_token
        self.timeout_sec = timeout_sec

    def execute_intent(self, payload: dict):
        _FakeJpGatewayClient.calls.append(payload)
        return dict(_FakeJpGatewayClient.next_response)


class _FakeUsGatewayClient:
    next_response: dict = {}
    calls: list[dict] = []

    def __init__(self, base_url: str, auth_token: str | None = None, timeout_sec: float = 8.0):  # noqa: ARG002
        self.base_url = base_url
        self.auth_token = auth_token
        self.timeout_sec = timeout_sec

    def execute_intent(self, payload: dict):
        _FakeUsGatewayClient.calls.append(payload)
        return dict(_FakeUsGatewayClient.next_response)


class _FakeNotifier:
    alerts: list[tuple[str, dict | None]] = []
    risk_bulletins: list[dict] = []

    def __init__(self, webhook_url: str | None, timeout_sec: int = 10):  # noqa: ARG002
        self.webhook_url = webhook_url

    def send_executor_alert(self, title: str, details: dict | None = None):
        _FakeNotifier.alerts.append((title, details))

    def send_risk_bulletin(self, now: datetime, items: list[dict], top_n: int = 8):  # noqa: ARG002
        _FakeNotifier.risk_bulletins.append({"now": now, "items": list(items), "top_n": top_n})


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
    _FakeNotifier.alerts = []
    _FakeNotifier.risk_bulletins = []
    _FakeNotifier.risk_bulletins = []
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["rejected"] == 1
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates == [("intent-1", "executing"), ("intent-1", "rejected")]
    assert fake_repo.inserted_risk_states == ["halted"]
    assert len(_FakeNotifier.risk_bulletins) == 1
    assert _FakeNotifier.risk_bulletins[0]["items"][0]["category"] == "dd_sharpe_gate"


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
    _FakeNotifier.alerts = []
    _FakeNotifier.risk_bulletins = []
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["rejected"] == 1
    assert stats["skipped_by_data_quality"] == 1
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates[-1] == ("intent-4", "rejected")
    assert len(_FakeNotifier.risk_bulletins) == 1
    assert _FakeNotifier.risk_bulletins[0]["items"][0]["category"] == "data_freshness_ng"


def test_run_executor_once_executes_crypto_intent_via_gateway(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-5",
                "portfolio_id": "portfolio-5",
                "strategy_version_id": "sv-5",
                "target_positions": [
                    {
                        "symbol": "CRYPTO:BTCUSDT.PERP.BINANCE",
                        "target_qty": 1.0,
                        "delta_qty": 1.0,
                        "instrument_type": "CRYPTO",
                        "venue": "binance_perp",
                        "price_hint": 100000.0,
                        "timeout_sec": 30,
                    },
                    {
                        "symbol": "CRYPTO:BTCUSDT.PERP.HYPER",
                        "target_qty": -1.0,
                        "delta_qty": -1.0,
                        "instrument_type": "CRYPTO",
                        "venue": "hyperliquid_perp",
                        "price_hint": 100010.0,
                        "timeout_sec": 30,
                    },
                ],
                "risk_checks": {"drawdown": -0.005, "sharpe_20d": 0.8},
                "broker_map": {"CRYPTO": "crypto_gateway"},
                "portfolio_name": "crypto_arb",
            }
        ],
    )

    _FakeGatewayClient.calls = []
    _FakeGatewayClient.next_response = {
        "status": "filled",
        "legs": [
            {
                "leg_id": "leg-1",
                "symbol": "CRYPTO:BTCUSDT.PERP.BINANCE",
                "venue": "binance_perp",
                "side": "BUY",
                "qty": 1.0,
                "filled_qty": 1.0,
                "avg_price": 100001.0,
                "fee": 0.1,
                "status": "filled",
                "broker_order_id": "bn-1",
            },
            {
                "leg_id": "leg-2",
                "symbol": "CRYPTO:BTCUSDT.PERP.HYPER",
                "venue": "hyperliquid_perp",
                "side": "SELL",
                "qty": 1.0,
                "filled_qty": 1.0,
                "avg_price": 100009.0,
                "fee": 0.1,
                "status": "filled",
                "broker_order_id": "hl-1",
            },
        ],
        "panic_close": {"triggered": False, "legs": []},
    }
    _FakeNotifier.alerts = []
    _FakeNotifier.risk_bulletins = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "gateway_crypto": {
                    "enabled": True,
                    "broker_name": "crypto_gateway",
                    "base_url": "http://gateway-crypto:8080",
                    "request_timeout_sec": 8,
                    "default_leg_timeout_sec": 30,
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "CryptoGatewayClient", _FakeGatewayClient)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["done"] == 1
    assert stats["executed_via_crypto_gateway"] == 1
    assert fake_repo.orders_inserted == 2
    assert fake_repo.fills_inserted == 2
    assert fake_repo.positions_upserted == 2
    assert fake_repo.inserted_risk_events == []
    assert fake_repo.status_updates[-1] == ("intent-5", "done")
    assert len(_FakeGatewayClient.calls) == 1
    assert _FakeNotifier.alerts == []


def test_run_executor_once_records_risk_event_on_crypto_partial_close(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-6",
                "portfolio_id": "portfolio-6",
                "strategy_version_id": "sv-6",
                "target_positions": [
                    {
                        "symbol": "CRYPTO:BTCUSDT.PERP.BINANCE",
                        "target_qty": 1.0,
                        "delta_qty": 1.0,
                        "instrument_type": "CRYPTO",
                        "venue": "binance_perp",
                        "price_hint": 100000.0,
                        "timeout_sec": 30,
                    },
                    {
                        "symbol": "CRYPTO:BTCUSDT.PERP.HYPER",
                        "target_qty": -1.0,
                        "delta_qty": -1.0,
                        "instrument_type": "CRYPTO",
                        "venue": "hyperliquid_perp",
                        "price_hint": 100010.0,
                        "timeout_sec": 30,
                    },
                ],
                "risk_checks": {"drawdown": -0.005, "sharpe_20d": 0.8},
                "broker_map": {"CRYPTO": "crypto_gateway"},
                "portfolio_name": "crypto_arb",
            }
        ],
    )

    _FakeGatewayClient.calls = []
    _FakeGatewayClient.next_response = {
        "status": "partial_closed",
        "legs": [
            {
                "leg_id": "leg-1",
                "symbol": "CRYPTO:BTCUSDT.PERP.BINANCE",
                "venue": "binance_perp",
                "side": "BUY",
                "qty": 1.0,
                "filled_qty": 1.0,
                "avg_price": 100001.0,
                "fee": 0.1,
                "status": "filled",
                "broker_order_id": "bn-2",
            },
            {
                "leg_id": "leg-2",
                "symbol": "CRYPTO:BTCUSDT.PERP.HYPER",
                "venue": "hyperliquid_perp",
                "side": "SELL",
                "qty": 1.0,
                "filled_qty": 0.0,
                "avg_price": 0.0,
                "fee": 0.0,
                "status": "rejected",
                "reject_reason": "insufficient_liquidity",
                "broker_order_id": "hl-2",
            },
        ],
        "panic_close": {
            "triggered": True,
            "reason": "partial_fill_forced_flat",
            "legs": [
                {
                    "leg_id": "leg-1:close",
                    "symbol": "CRYPTO:BTCUSDT.PERP.BINANCE",
                    "venue": "binance_perp",
                    "side": "SELL",
                    "qty": 1.0,
                    "filled_qty": 1.0,
                    "avg_price": 100000.0,
                    "fee": 0.1,
                    "status": "filled",
                    "broker_order_id": "bn-2-close",
                }
            ],
        },
        "risk_event": {
            "event_type": "crypto_partial_fill_forced_flat",
            "payload": {"panic_reason": "partial_fill_forced_flat"},
        },
    }
    _FakeNotifier.alerts = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "gateway_crypto": {
                    "enabled": True,
                    "broker_name": "crypto_gateway",
                    "base_url": "http://gateway-crypto:8080",
                    "request_timeout_sec": 8,
                    "default_leg_timeout_sec": 30,
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "CryptoGatewayClient", _FakeGatewayClient)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["failed"] == 1
    assert stats["executed_via_crypto_gateway"] == 1
    assert fake_repo.orders_inserted == 3
    assert fake_repo.fills_inserted == 2
    assert fake_repo.positions_upserted == 2
    assert len(fake_repo.inserted_risk_events) == 1
    assert fake_repo.inserted_risk_events[0].event_type == "crypto_partial_fill_forced_flat"
    assert fake_repo.status_updates[-1] == ("intent-6", "failed")
    assert len(_FakeNotifier.alerts) == 1
    assert len(_FakeNotifier.risk_bulletins) == 1
    assert _FakeNotifier.risk_bulletins[0]["items"][0]["category"] == "partial_fill"


def test_run_executor_once_executes_jp_intent_via_gateway_ack(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-7",
                "portfolio_id": "portfolio-7",
                "strategy_version_id": "sv-7",
                "target_positions": [
                    {
                        "symbol": "JP:7203",
                        "target_qty": 100,
                        "delta_qty": 100,
                        "instrument_type": "JP_EQ",
                        "order_type": "MKT",
                        "margin_type": "cash",
                    }
                ],
                "risk_checks": {"drawdown": -0.002, "sharpe_20d": 1.0},
                "broker_map": {"JP": "gateway_jp"},
                "portfolio_name": "jp_arb",
            }
        ],
    )

    _FakeJpGatewayClient.calls = []
    _FakeJpGatewayClient.next_response = {
        "status": "ack",
        "legs": [
            {
                "leg_id": "jp-leg-1",
                "symbol": "JP:7203",
                "side": "BUY",
                "qty": 100,
                "order_type": "MKT",
                "status": "ack",
                "filled_qty": 0.0,
                "broker_order_id": "jp-order-1",
            }
        ],
    }
    _FakeNotifier.alerts = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "gateway_jp": {
                    "enabled": True,
                    "broker_name": "gateway_jp",
                    "base_url": "http://gateway-jp:8081",
                    "request_timeout_sec": 8,
                    "wait_timeout_sec": 2,
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "JpGatewayClient", _FakeJpGatewayClient)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["sent"] == 1
    assert stats["executed_via_jp_gateway"] == 1
    assert fake_repo.orders_inserted == 1
    assert fake_repo.fills_inserted == 0
    assert fake_repo.status_updates[-1] == ("intent-7", "sent")
    assert len(_FakeJpGatewayClient.calls) == 1
    assert _FakeNotifier.alerts == []


def test_run_executor_once_records_risk_event_on_jp_gateway_failure(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-8",
                "portfolio_id": "portfolio-8",
                "strategy_version_id": "sv-8",
                "target_positions": [
                    {
                        "symbol": "JP:9984",
                        "target_qty": 50,
                        "delta_qty": 50,
                        "instrument_type": "JP_EQ",
                        "order_type": "MKT",
                    }
                ],
                "risk_checks": {"drawdown": -0.002, "sharpe_20d": 1.0},
                "broker_map": {"JP": "gateway_jp"},
                "portfolio_name": "jp_arb",
            }
        ],
    )

    _FakeJpGatewayClient.calls = []
    _FakeJpGatewayClient.next_response = {
        "status": "failed",
        "legs": [
            {
                "leg_id": "jp-leg-1",
                "symbol": "JP:9984",
                "side": "BUY",
                "qty": 50,
                "order_type": "MKT",
                "status": "error",
                "filled_qty": 0.0,
                "reject_reason": "rate_limit_timeout",
            }
        ],
        "risk_event": {
            "event_type": "jp_gateway_execution_failed",
            "payload": {"reason": "rate_limit_timeout"},
        },
    }
    _FakeNotifier.alerts = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "gateway_jp": {
                    "enabled": True,
                    "broker_name": "gateway_jp",
                    "base_url": "http://gateway-jp:8081",
                    "request_timeout_sec": 8,
                    "wait_timeout_sec": 2,
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "JpGatewayClient", _FakeJpGatewayClient)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["failed"] == 1
    assert stats["executed_via_jp_gateway"] == 1
    assert len(fake_repo.inserted_risk_events) == 1
    assert fake_repo.inserted_risk_events[0].event_type == "jp_gateway_execution_failed"
    assert fake_repo.status_updates[-1] == ("intent-8", "failed")
    assert len(_FakeNotifier.alerts) == 1


def test_run_executor_once_finishes_done_when_reconcile_results_in_no_change(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-9",
                "portfolio_id": "portfolio-9",
                "strategy_version_id": "sv-9",
                "target_positions": [{"symbol": "JP:1111", "target_qty": 50, "instrument_type": "JP_EQ"}],
                "risk_checks": {"drawdown": -0.001, "sharpe_20d": 0.9},
                "broker_map": {"JP": "kabu"},
                "portfolio_name": "core",
            }
        ],
        latest_price={"close_raw": 1234.5},
    )
    fake_repo.positions_rows = [{"symbol": "JP:1111", "instrument_type": "JP_EQ", "qty": 50.0}]
    fake_repo.open_orders_rows = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "order_reconcile": {
                    "enabled": True,
                    "min_abs_delta_qty": 0.0,
                    "min_abs_delta_notional": 0.0,
                    "open_order_policy": "skip",
                    "net_notional_epsilon": 10.0,
                    "neutrality_strategy_types": ["perp_perp", "cash_carry"],
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["done"] == 1
    assert stats["skipped_by_reconcile"] == 1
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates[-1] == ("intent-9", "done")


def test_run_executor_once_rejects_on_reconcile_net_notional_violation(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-10",
                "portfolio_id": "portfolio-10",
                "strategy_version_id": "sv-10",
                "target_positions": [
                    {
                        "symbol": "CRYPTO:AAA",
                        "target_qty": 1.0,
                        "instrument_type": "CRYPTO",
                        "price_hint": 100.0,
                    },
                    {
                        "symbol": "CRYPTO:BBB",
                        "target_qty": -0.7,
                        "instrument_type": "CRYPTO",
                        "price_hint": 100.0,
                    },
                ],
                "risk_checks": {
                    "drawdown": -0.001,
                    "sharpe_20d": 0.9,
                    "strategy_type": "perp_perp",
                },
                "broker_map": {"CRYPTO": "crypto_gateway"},
                "portfolio_name": "crypto_arb",
            }
        ],
    )
    fake_repo.positions_rows = []
    fake_repo.open_orders_rows = []
    _FakeNotifier.alerts = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "order_reconcile": {
                    "enabled": True,
                    "min_abs_delta_qty": 0.0,
                    "min_abs_delta_notional": 0.0,
                    "open_order_policy": "skip",
                    "net_notional_epsilon": 10.0,
                    "neutrality_strategy_types": ["perp_perp"],
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["rejected"] == 1
    assert len(fake_repo.inserted_risk_events) == 1
    assert fake_repo.inserted_risk_events[0].event_type == "reconcile_rejected"
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates[-1] == ("intent-10", "rejected")
    assert len(_FakeNotifier.alerts) == 1


def test_run_executor_once_marks_sent_when_reconcile_skips_for_open_order_conflict(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-11",
                "portfolio_id": "portfolio-11",
                "strategy_version_id": "sv-11",
                "target_positions": [{"symbol": "JP:2222", "target_qty": 100, "instrument_type": "JP_EQ"}],
                "risk_checks": {"drawdown": -0.001, "sharpe_20d": 0.9},
                "broker_map": {"JP": "kabu"},
                "portfolio_name": "core",
            }
        ],
    )
    fake_repo.positions_rows = [{"symbol": "JP:2222", "instrument_type": "JP_EQ", "qty": 0.0}]
    fake_repo.open_orders_rows = [{"order_id": "open-1", "symbol": "JP:2222", "side": "BUY", "qty": 100.0, "status": "ack"}]

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "order_reconcile": {
                    "enabled": True,
                    "open_order_policy": "skip",
                    "min_abs_delta_qty": 0.0,
                    "min_abs_delta_notional": 0.0,
                    "net_notional_epsilon": 10.0,
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["sent"] == 1
    assert stats["skipped_by_reconcile"] == 1
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates[-1] == ("intent-11", "sent")


def test_run_executor_once_emits_strategy_warning_but_executes(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-12",
                "portfolio_id": "portfolio-12",
                "strategy_version_id": "sv-12",
                "target_positions": [{"symbol": "JP:1301", "target_qty": 10, "instrument_type": "JP_EQ"}],
                "risk_checks": {"drawdown": -0.005, "sharpe_20d": 0.2, "strategy_sharpe_20d": 0.2},
                "broker_map": {"JP": "kabu"},
                "portfolio_name": "core",
            }
        ],
        latest_price={"close_raw": 200.0},
    )
    fake_repo.strategy_risk_snapshots = [
        {"as_of_date": date(2026, 2, 19), "sharpe_20d": 0.2, "cooldown_until": None},
        {"as_of_date": date(2026, 2, 18), "sharpe_20d": 0.25, "cooldown_until": None},
    ]
    _FakeNotifier.alerts = []
    _FakeNotifier.risk_bulletins = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.10, "min_sharpe_20d": -1.0},
                "strategy_risk_gate": {
                    "enabled": True,
                    "max_drawdown_breach": -0.03,
                    "warning_sharpe_threshold": 0.3,
                    "warning_consecutive_days": 3,
                    "halt_sharpe_threshold": 0.0,
                    "halt_consecutive_days": 2,
                    "cooldown_hours": 24,
                    "panic_close_on_halt": True,
                },
                "order_reconcile": {"enabled": False},
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["strategy_warning"] == 1
    assert stats["done"] == 1
    assert fake_repo.status_updates[-1] == ("intent-12", "done")
    assert any(ev.event_type == "strategy_warning" for ev in fake_repo.inserted_risk_events)
    assert any(
        item.get("category") == "dd_sharpe_gate"
        for bulletin in _FakeNotifier.risk_bulletins
        for item in bulletin["items"]
    )


def test_run_executor_once_halts_strategy_and_triggers_panic_close(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-13",
                "portfolio_id": "portfolio-13",
                "strategy_version_id": "sv-13",
                "target_positions": [{"symbol": "JP:7203", "target_qty": 10, "instrument_type": "JP_EQ"}],
                "risk_checks": {"drawdown": -0.04, "sharpe_20d": 0.2},
                "broker_map": {"JP": "kabu"},
                "portfolio_name": "core",
            }
        ],
        latest_price={"close_raw": 1000.0},
    )
    fake_repo.strategy_symbols_rows = [{"symbol": "JP:7203", "instrument_type": "JP_EQ"}]
    fake_repo.positions_rows = [{"symbol": "JP:7203", "instrument_type": "JP_EQ", "qty": 5.0}]
    _FakeNotifier.alerts = []
    _FakeNotifier.risk_bulletins = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.10, "min_sharpe_20d": -1.0},
                "strategy_risk_gate": {
                    "enabled": True,
                    "max_drawdown_breach": -0.03,
                    "warning_sharpe_threshold": 0.3,
                    "warning_consecutive_days": 3,
                    "halt_sharpe_threshold": 0.0,
                    "halt_consecutive_days": 2,
                    "cooldown_hours": 24,
                    "panic_close_on_halt": True,
                    "symbol_lookback_days": 30,
                },
                "order_reconcile": {"enabled": False},
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["strategy_halt"] == 1
    assert stats["strategy_panic_close"] == 1
    assert stats["rejected"] == 1
    assert fake_repo.positions_upserted >= 1
    assert fake_repo.status_updates[-1] == ("intent-13", "rejected")
    assert any(ev.event_type == "strategy_halt" for ev in fake_repo.inserted_risk_events)
    assert any(
        item.get("title") == "strategy risk halt"
        for bulletin in _FakeNotifier.risk_bulletins
        for item in bulletin["items"]
    )


def test_run_executor_once_rejects_when_strategy_in_cooldown(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-14",
                "portfolio_id": "portfolio-14",
                "strategy_version_id": "sv-14",
                "target_positions": [{"symbol": "US:AAPL", "target_qty": 10, "instrument_type": "US_EQ"}],
                "risk_checks": {"drawdown": -0.001, "sharpe_20d": 1.0},
                "broker_map": {"US": "paper"},
                "portfolio_name": "core",
            }
        ],
        latest_price={"close_raw": 200.0},
    )
    fake_repo.strategy_risk_snapshots = [
        {
            "as_of_date": date(2026, 2, 20),
            "sharpe_20d": 0.8,
            "cooldown_until": datetime(2099, 1, 1, 0, 0, tzinfo=timezone.utc),
        }
    ]
    _FakeNotifier.alerts = []
    _FakeNotifier.risk_bulletins = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.10, "min_sharpe_20d": -1.0},
                "strategy_risk_gate": {"enabled": True},
                "order_reconcile": {"enabled": False},
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["strategy_cooldown_reject"] == 1
    assert stats["rejected"] == 1
    assert fake_repo.orders_inserted == 0
    assert fake_repo.status_updates[-1] == ("intent-14", "rejected")
    assert any(ev.event_type == "strategy_cooldown_reject" for ev in fake_repo.inserted_risk_events)
    assert any(
        item.get("title") == "strategy cooldown reject"
        for bulletin in _FakeNotifier.risk_bulletins
        for item in bulletin["items"]
    )


def test_run_executor_once_executes_us_intent_via_gateway(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-9",
                "portfolio_id": "portfolio-9",
                "strategy_version_id": "sv-9",
                "target_positions": [
                    {
                        "symbol": "US:AAPL",
                        "target_qty": 15,
                        "delta_qty": 15,
                        "instrument_type": "US_EQ",
                        "order_type": "MKT",
                        "time_in_force": "DAY",
                        "price_hint": 210.5,
                    }
                ],
                "risk_checks": {"drawdown": -0.002, "sharpe_20d": 1.0},
                "broker_map": {"US": "gateway_us"},
                "portfolio_name": "us_core",
            }
        ],
    )

    _FakeUsGatewayClient.calls = []
    _FakeUsGatewayClient.next_response = {
        "status": "filled",
        "orders": [
            {
                "order_id": "us-ord-1",
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 15,
                "order_type": "MKT",
                "status": "filled",
                "filled_qty": 15,
                "avg_price": 211.0,
                "broker_order_id": "us-order-1",
            }
        ],
        "fills": [
            {
                "broker_order_id": "us-order-1",
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 15,
                "price": 211.0,
                "fee": 0.0,
            }
        ],
    }
    _FakeNotifier.alerts = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "gateway_us": {
                    "enabled": True,
                    "broker_name": "gateway_us",
                    "base_url": "http://gateway-us:8090",
                    "request_timeout_sec": 8,
                    "default_order_timeout_sec": 20,
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "USGatewayClient", _FakeUsGatewayClient)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["done"] == 1
    assert stats["executed_via_us_gateway"] == 1
    assert fake_repo.orders_inserted == 1
    assert fake_repo.fills_inserted == 1
    assert fake_repo.positions_upserted == 1
    assert fake_repo.inserted_risk_events == []
    assert fake_repo.status_updates[-1] == ("intent-9", "done")
    assert len(_FakeUsGatewayClient.calls) == 1
    assert _FakeNotifier.alerts == []


def test_run_executor_once_records_risk_event_on_us_gateway_failure(monkeypatch):
    fake_repo = _FakeRepo(
        "postgresql://unused",
        intents=[
            {
                "intent_id": "intent-10",
                "portfolio_id": "portfolio-10",
                "strategy_version_id": "sv-10",
                "target_positions": [
                    {
                        "symbol": "US:TSLA",
                        "target_qty": 5,
                        "delta_qty": 5,
                        "instrument_type": "US_EQ",
                        "order_type": "MKT",
                    }
                ],
                "risk_checks": {"drawdown": -0.002, "sharpe_20d": 1.0},
                "broker_map": {"US": "gateway_us"},
                "portfolio_name": "us_core",
            }
        ],
    )

    _FakeUsGatewayClient.calls = []
    _FakeUsGatewayClient.next_response = {
        "status": "failed",
        "orders": [
            {
                "order_id": "us-ord-1",
                "symbol": "TSLA",
                "side": "BUY",
                "qty": 5,
                "order_type": "MKT",
                "status": "rejected",
                "filled_qty": 0,
                "avg_price": 0,
                "reject_reason": "outside_rth_rejected",
                "broker_order_id": "us-order-2",
            }
        ],
        "risk_event": {
            "event_type": "us_execution_failed",
            "payload": {"reject_reasons": ["outside_rth_rejected"]},
        },
    }
    _FakeNotifier.alerts = []

    monkeypatch.setattr(
        executor_job,
        "load_yaml_config",
        lambda: {
            "execution": {
                "risk_gate": {"max_drawdown_breach": -0.03, "min_sharpe_20d": 0.0},
                "gateway_us": {
                    "enabled": True,
                    "broker_name": "gateway_us",
                    "base_url": "http://gateway-us:8090",
                    "request_timeout_sec": 8,
                    "default_order_timeout_sec": 20,
                },
            }
        },
    )
    monkeypatch.setattr(
        executor_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(executor_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(executor_job, "USGatewayClient", _FakeUsGatewayClient)
    monkeypatch.setattr(executor_job, "DiscordNotifier", _FakeNotifier)

    stats = executor_job.run_executor_once(limit=10)

    assert stats["processed"] == 1
    assert stats["failed"] == 1
    assert stats["executed_via_us_gateway"] == 1
    assert len(fake_repo.inserted_risk_events) == 1
    assert fake_repo.inserted_risk_events[0].event_type == "us_execution_failed"
    assert fake_repo.status_updates[-1] == ("intent-10", "failed")
    assert len(_FakeUsGatewayClient.calls) == 1
    assert len(_FakeNotifier.alerts) == 1
