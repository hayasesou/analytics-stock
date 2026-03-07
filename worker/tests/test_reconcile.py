from __future__ import annotations

from src.execution.reconcile import ReconcileSettings, reconcile_target_positions


def test_reconcile_computes_delta_increase_decrease_and_flip() -> None:
    result = reconcile_target_positions(
        target_positions=[
            {"symbol": "JP:1111", "target_qty": 100.0},
            {"symbol": "JP:2222", "target_qty": 20.0},
            {"symbol": "JP:3333", "target_qty": -10.0},
            {"symbol": "JP:4444", "target_qty": 0.0},
        ],
        current_position_qty_by_symbol={
            "JP:1111": 60.0,
            "JP:2222": 50.0,
            "JP:3333": 10.0,
            "JP:4444": 0.0,
        },
        open_orders_by_symbol={},
        price_by_symbol={"JP:1111": 100.0, "JP:2222": 100.0, "JP:3333": 100.0, "JP:4444": 100.0},
        settings=ReconcileSettings(min_abs_delta_qty=1e-9, min_abs_delta_notional=0.0),
    )

    assert result.reject_reason is None
    assert len(result.target_positions) == 3
    by_symbol = {row["symbol"]: row for row in result.target_positions}
    assert by_symbol["JP:1111"]["delta_qty"] == 40.0
    assert by_symbol["JP:2222"]["delta_qty"] == -30.0
    assert by_symbol["JP:3333"]["delta_qty"] == -20.0
    assert any(row["symbol"] == "JP:4444" for row in result.skipped)


def test_reconcile_skips_when_open_order_conflict_policy_skip() -> None:
    result = reconcile_target_positions(
        target_positions=[{"symbol": "US:AAPL", "target_qty": 10.0}],
        current_position_qty_by_symbol={"US:AAPL": 0.0},
        open_orders_by_symbol={
            "US:AAPL": [
                {
                    "order_id": "ord-1",
                    "side": "BUY",
                    "qty": 10.0,
                    "status": "ack",
                }
            ]
        },
        price_by_symbol={"US:AAPL": 100.0},
        settings=ReconcileSettings(open_order_policy="skip"),
    )

    assert result.target_positions == []
    assert len(result.skipped) == 1
    assert result.skipped[0]["reason"] == "open_order_conflict"


def test_reconcile_replace_policy_keeps_leg_and_marks_cancel_replace() -> None:
    result = reconcile_target_positions(
        target_positions=[{"symbol": "US:MSFT", "target_qty": 10.0}],
        current_position_qty_by_symbol={"US:MSFT": 0.0},
        open_orders_by_symbol={
            "US:MSFT": [
                {
                    "order_id": "ord-9",
                    "side": "BUY",
                    "qty": 9.0,
                    "status": "ack",
                }
            ]
        },
        price_by_symbol={"US:MSFT": 100.0},
        settings=ReconcileSettings(open_order_policy="replace"),
    )

    assert len(result.target_positions) == 1
    row = result.target_positions[0]
    assert row["cancel_replace"] is True
    assert row["reconcile_open_order_ids"] == ["ord-9"]
    assert row["delta_qty"] == 10.0


def test_reconcile_skips_small_delta_by_notional_threshold() -> None:
    result = reconcile_target_positions(
        target_positions=[{"symbol": "CRYPTO:BTCUSDT.PERP.BINANCE", "target_qty": 1.001}],
        current_position_qty_by_symbol={"CRYPTO:BTCUSDT.PERP.BINANCE": 1.0},
        open_orders_by_symbol={},
        price_by_symbol={"CRYPTO:BTCUSDT.PERP.BINANCE": 10000.0},
        settings=ReconcileSettings(min_abs_delta_qty=0.0, min_abs_delta_notional=20.0),
    )

    assert result.target_positions == []
    assert len(result.skipped) == 1
    assert result.skipped[0]["reason"] == "delta_below_notional_threshold"


def test_reconcile_rejects_when_net_notional_violates_epsilon() -> None:
    result = reconcile_target_positions(
        target_positions=[
            {"symbol": "CRYPTO:AAA", "target_qty": 1.0},
            {"symbol": "CRYPTO:BBB", "target_qty": -0.7},
        ],
        current_position_qty_by_symbol={"CRYPTO:AAA": 0.0, "CRYPTO:BBB": 0.0},
        open_orders_by_symbol={},
        price_by_symbol={"CRYPTO:AAA": 100.0, "CRYPTO:BBB": 100.0},
        settings=ReconcileSettings(net_notional_epsilon=10.0),
        enforce_net_neutral=True,
    )

    assert result.reject_reason == "net_notional_violation"
    assert result.target_positions == []
    assert result.net_target_notional == 30.0
