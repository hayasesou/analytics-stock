from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from src.jobs import edge_radar as edge_radar_job


class _FakeRepo:
    def __init__(self, _dsn: str):
        self.edge_states = []
        self.order_intents = []
        self.portfolios = []

    def fetch_latest_weekly_candidates(self, limit: int = 50):  # noqa: ARG002
        return [
            {
                "security_id": "JP:1111",
                "market": "JP",
                "combined_score": 82.0,
                "edge_score": 74.0,
                "confidence": "High",
                "missing_ratio": 0.05,
                "primary_source_count": 3,
                "has_major_contradiction": False,
            }
        ]

    def fetch_latest_strategy_edge_inputs(self, asset_scope: str, statuses: list[str] | None = None, limit: int = 30):  # noqa: ARG002
        assert asset_scope == "CRYPTO"
        return [
            {
                "strategy_id": "s-1",
                "strategy_name": "arb-crypto-main",
                "asset_scope": "CRYPTO",
                "status": "paper",
                "strategy_version_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "version": 1,
                "eval_type": "robust_backtest",
                "sharpe": 1.2,
                "max_dd": -0.08,
                "cagr": 0.25,
                "strategy_spec": {
                    "crypto_arb": {
                        "type": "perp_perp",
                        "params": {
                            "z_entry": 0.3,
                            "entry_min_edge_bps": 0.5,
                            "target_notional_usd": 1500,
                            "epsilon_notional_usd": 5.0,
                        },
                    }
                },
                "artifacts": {
                    "market_snapshot": {
                        "venue_a": "binance_perp",
                        "venue_b": "hyperliquid_perp",
                        "symbol_a": "CRYPTO:BTCUSDT.PERP.BINANCE",
                        "symbol_b": "CRYPTO:BTCUSDT.PERP.HYPER",
                        "pair_symbol": "BTC-PERP-SPREAD",
                        "price_a": 101.4,
                        "price_b": 100.0,
                        "spread_history_bps": [3.0, 8.0, 15.0, 22.0],
                        "funding_a_bps": 2.0,
                        "funding_b_bps": 3.5,
                        "basis_bps": 1.0,
                        "fee_bps": 1.2,
                        "slippage_bps": 0.8,
                        "borrow_bps": 0.0,
                        "liquidity_score": 0.9,
                        "liquidation_distance_pct": 0.25,
                        "net_notional_usd": 1.0,
                    }
                },
            }
        ]

    def insert_edge_states(self, states):  # noqa: ANN001
        self.edge_states.extend(states)
        return len(states)

    def upsert_portfolio(self, portfolio):  # noqa: ANN001
        self.portfolios.append(portfolio)
        return "portfolio-1"

    def has_recent_open_intent_for_strategy(self, strategy_version_id: str, lookback_minutes: int = 180):  # noqa: ARG002
        return False

    def insert_order_intent(self, intent):  # noqa: ANN001
        self.order_intents.append(intent)
        return f"intent-{len(self.order_intents)}"


class _FakeNotifier:
    def __init__(self, webhook_url: str | None, timeout_sec: int = 10):  # noqa: ARG002
        self.webhook_url = webhook_url
        self.calls: list[tuple[str, int]] = []

    def send_edge_radar(self, now: datetime, scope: str, rows: list[dict], top_n: int = 10):  # noqa: ARG002
        self.calls.append((scope, len(rows)))


BASE_CFG = {
    "timezone": "Asia/Tokyo",
    "edge_radar": {
        "enabled": True,
        "notify": {"top_n": 5},
        "equities": {"run_hour_jst": 20, "run_minute_jst": 10, "max_candidates": 50, "entry_threshold_bps": 2.0},
        "crypto": {
            "run_minute_jst": 5,
            "max_candidates": 20,
            "entry_threshold_bps": 1.5,
            "expected_cost_bps": 12.0,
            "strategy_defaults": {
                "perp_perp": {
                    "ewma_alpha": 0.2,
                    "z_entry": 2.0,
                    "z_exit": 0.6,
                    "z_signal_scale_bps": 2.5,
                    "entry_min_edge_bps": 0.8,
                    "fee_bps": 4.0,
                    "slippage_bps": 3.0,
                    "borrow_bps": 0.0,
                    "min_liquidity_score": 0.35,
                    "min_liquidation_distance_pct": 0.15,
                    "epsilon_notional_usd": 10.0,
                    "target_notional_usd": 1000.0,
                    "timeout_sec": 30,
                    "score_per_bps": 3.0,
                },
                "cash_carry": {
                    "ewma_alpha": 0.2,
                    "z_entry": 1.8,
                    "z_exit": 0.5,
                    "z_signal_scale_bps": 2.0,
                    "basis_entry_bps": 8.0,
                    "basis_exit_bps": 3.0,
                    "entry_min_edge_bps": 0.8,
                    "fee_bps": 4.0,
                    "slippage_bps": 3.0,
                    "borrow_bps": 0.0,
                    "min_liquidity_score": 0.35,
                    "min_liquidation_distance_pct": 0.15,
                    "epsilon_notional_usd": 10.0,
                    "target_notional_usd": 1000.0,
                    "timeout_sec": 30,
                    "score_per_bps": 3.0,
                },
            },
            "intent_generation": {
                "enabled": True,
                "status": "proposed",
                "portfolio_name": "crypto_arb",
                "base_currency": "USD",
                "max_new_intents_per_run": 5,
                "lookback_minutes": 180,
                "min_edge_score": 50.0,
                "min_confidence": 0.55,
                "require_positive_expected_edge": True,
                "broker_map": {"CRYPTO": "crypto_gateway"},
            },
        },
        "formula": {
            "combined_weight": 0.55,
            "legacy_edge_weight": 0.45,
            "signal_scale_bps": 0.30,
            "confidence_multiplier": {"High": 1.0, "Medium": 0.75, "Low": 0.5},
            "source_boost_bps_per_primary": 0.8,
            "max_source_boost_bps": 4.0,
            "contradiction_penalty_bps": 5.0,
            "missing_penalty_bps_per_10pct": 1.0,
            "score_per_bps": 3.0,
            "crypto_sharpe_bps_factor": 10.0,
            "crypto_cagr_bps_factor": 80.0,
            "crypto_drawdown_penalty_bps_factor": 25.0,
            "crypto_status_bonus_bps": {"candidate": 0.0, "paper": 1.0, "live": 2.0},
        },
    },
}


def test_run_edge_radar_all_persists_notifies_and_generates_intent(monkeypatch):
    fake_repo = _FakeRepo("postgresql://unused")
    fake_notifier = _FakeNotifier(None)

    monkeypatch.setattr(edge_radar_job, "load_yaml_config", lambda: BASE_CFG)
    monkeypatch.setattr(
        edge_radar_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(edge_radar_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(edge_radar_job, "DiscordNotifier", lambda *args, **kwargs: fake_notifier)

    result = edge_radar_job.run_edge_radar(
        scope="all",
        now=datetime(2026, 2, 20, 11, 25, tzinfo=timezone.utc),
        send_notification=True,
    )

    assert result["enabled"] is True
    assert result["equities"] == 1
    assert result["crypto"] == 1
    assert result["inserted"] == 2
    assert result["intents_created"] == 1
    assert len(fake_repo.edge_states) == 2
    assert any(state.market_scope == "JP_EQ" for state in fake_repo.edge_states)
    assert any(state.market_scope == "CRYPTO" for state in fake_repo.edge_states)
    assert ("equities", 1) in fake_notifier.calls
    assert ("crypto", 1) in fake_notifier.calls
    assert len(fake_repo.order_intents) == 1
    intent = fake_repo.order_intents[0]
    assert intent.status == "proposed"
    assert len(intent.target_positions) == 2


def test_run_edge_radar_keeps_expected_edge_null_when_metrics_missing(monkeypatch):
    class _MissingMetricsRepo(_FakeRepo):
        def fetch_latest_strategy_edge_inputs(self, asset_scope: str, statuses: list[str] | None = None, limit: int = 30):  # noqa: ARG002
            return [
                {
                    "strategy_id": "s-2",
                    "strategy_name": "arb-crypto-missing",
                    "asset_scope": "CRYPTO",
                    "status": "candidate",
                    "strategy_version_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "version": 1,
                    "eval_type": "quick_backtest",
                    "sharpe": None,
                    "max_dd": -0.10,
                    "cagr": None,
                    "strategy_spec": {"crypto_arb": {"type": "cash_carry"}},
                    "artifacts": {},
                }
            ]

    fake_repo = _MissingMetricsRepo("postgresql://unused")
    cfg = {
        "timezone": "Asia/Tokyo",
        "edge_radar": {
            "enabled": True,
            "crypto": {
                "run_minute_jst": 5,
                "intent_generation": {"enabled": True},
            },
        },
    }

    monkeypatch.setattr(edge_radar_job, "load_yaml_config", lambda: cfg)
    monkeypatch.setattr(
        edge_radar_job,
        "load_runtime_secrets",
        lambda: SimpleNamespace(database_url="postgresql://unused", discord_webhook_url=None),
    )
    monkeypatch.setattr(edge_radar_job, "NeonRepository", lambda dsn: fake_repo)
    monkeypatch.setattr(edge_radar_job, "DiscordNotifier", lambda *args, **kwargs: _FakeNotifier(None))

    result = edge_radar_job.run_edge_radar(
        scope="crypto",
        now=datetime(2026, 2, 20, 11, 25, tzinfo=timezone.utc),
        send_notification=False,
    )

    assert result["crypto"] == 1
    assert result["inserted"] == 1
    assert result["intents_created"] == 0
    saved = fake_repo.edge_states[0]
    assert saved.expected_net_edge_bps is None
    assert saved.distance_to_entry_bps is None
    assert saved.edge_score == 0.0
