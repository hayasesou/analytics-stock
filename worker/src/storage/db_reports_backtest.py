from __future__ import annotations

from datetime import date
import json

from src.types import BacktestResult


class NeonRepositoryReportBacktestMixin:
    def create_backtest_run(
        self,
        run_id: str,
        as_of_date: date,
        period_start: date,
        period_end: date,
        common_period_start: date,
        common_period_end: date,
    ) -> str:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO backtest_runs (
                    run_id, as_of_date, period_start, period_end, common_period_start, common_period_end
                )
                VALUES (%s::uuid, %s, %s, %s, %s, %s)
                RETURNING id::text
                """,
                (
                    run_id,
                    as_of_date,
                    period_start,
                    period_end,
                    common_period_start,
                    common_period_end,
                ),
            )
            backtest_run_id = cur.fetchone()["id"]
            conn.commit()
        return backtest_run_id

    def insert_backtest_results(
        self,
        backtest_run_id: str,
        results: list[BacktestResult],
        security_uuid_map: dict[str, str],
    ) -> None:
        if not results:
            return

        with self._conn() as conn, conn.cursor() as cur:
            for result in results:
                cur.execute(
                    """
                    INSERT INTO backtest_metrics (
                        backtest_run_id, cost_profile, market_scope,
                        cagr, max_dd, sharpe, sortino, volatility,
                        win_rate, avg_win, avg_loss, alpha_simple, information_ratio_simple
                    )
                    VALUES (
                        %s::uuid, %s, 'MIXED',
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (backtest_run_id, cost_profile, market_scope)
                    DO UPDATE SET cagr = EXCLUDED.cagr,
                                  max_dd = EXCLUDED.max_dd,
                                  sharpe = EXCLUDED.sharpe,
                                  sortino = EXCLUDED.sortino,
                                  volatility = EXCLUDED.volatility,
                                  win_rate = EXCLUDED.win_rate,
                                  avg_win = EXCLUDED.avg_win,
                                  avg_loss = EXCLUDED.avg_loss,
                                  alpha_simple = EXCLUDED.alpha_simple,
                                  information_ratio_simple = EXCLUDED.information_ratio_simple
                    """,
                    (
                        backtest_run_id,
                        result.cost_profile,
                        result.metrics.get("cagr"),
                        result.metrics.get("max_dd"),
                        result.metrics.get("sharpe"),
                        result.metrics.get("sortino"),
                        result.metrics.get("volatility"),
                        result.metrics.get("win_rate"),
                        result.metrics.get("avg_win"),
                        result.metrics.get("avg_loss"),
                        result.metrics.get("alpha_simple"),
                        result.metrics.get("information_ratio_simple"),
                    ),
                )

                curve_rows = [
                    (
                        backtest_run_id,
                        result.cost_profile,
                        point["trade_date"],
                        point["equity"],
                        point["benchmark_equity"],
                    )
                    for point in result.equity_curve
                ]
                if curve_rows:
                    cur.executemany(
                        """
                        INSERT INTO backtest_equity_curve (
                            backtest_run_id, cost_profile, trade_date, equity, benchmark_equity
                        )
                        VALUES (%s::uuid, %s, %s, %s, %s)
                        ON CONFLICT (backtest_run_id, cost_profile, trade_date)
                        DO UPDATE SET equity = EXCLUDED.equity,
                                      benchmark_equity = EXCLUDED.benchmark_equity
                        """,
                        curve_rows,
                    )

                trade_rows = []
                for trade in result.trades:
                    sec_uuid = security_uuid_map.get(trade.security_id)
                    trade_rows.append(
                        (
                            backtest_run_id,
                            result.cost_profile,
                            sec_uuid,
                            trade.market,
                            trade.entry_date,
                            trade.entry_price,
                            trade.exit_date,
                            trade.exit_price,
                            trade.quantity,
                            trade.gross_pnl,
                            trade.net_pnl,
                            trade.cost,
                            trade.exit_reason,
                            json.dumps({}),
                        )
                    )
                if trade_rows:
                    cur.executemany(
                        """
                        INSERT INTO backtest_trades (
                            backtest_run_id, cost_profile, security_id, market,
                            entry_date, entry_price, exit_date, exit_price,
                            quantity, gross_pnl, net_pnl, cost, exit_reason, meta
                        )
                        VALUES (
                            %s::uuid, %s, %s::uuid, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s::jsonb
                        )
                        """,
                        trade_rows,
                    )

            conn.commit()
