from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _discounted_value(fcff0: float, growth: float, wacc: float, years: int = 5) -> float:
    if wacc <= growth:
        wacc = growth + 0.005
    value = 0.0
    fcff = fcff0
    for t in range(1, years + 1):
        fcff *= 1.0 + growth
        value += fcff / ((1.0 + wacc) ** t)
    terminal = (fcff * (1.0 + growth)) / (wacc - growth)
    value += terminal / ((1.0 + wacc) ** years)
    return value


def run_dcf_top10(
    top50: pd.DataFrame,
    latest_prices: pd.DataFrame,
    fx_usdjpy: float,
    config: dict[str, Any],
) -> pd.DataFrame:
    if top50.empty or latest_prices.empty:
        return pd.DataFrame()

    merged = top50.merge(
        latest_prices[["security_id", "close_raw"]],
        on="security_id",
        how="left",
    )
    top10 = merged.sort_values("mixed_rank").head(10).copy()

    rows: list[dict[str, object]] = []
    for _, row in top10.iterrows():
        market = row["market"]
        market_cfg = config["markets"]["jp" if market == "JP" else "us"]["dcf"]
        wacc = float(market_cfg["wacc"])
        g = float(market_cfg["perpetual_growth"])
        price = float(row.get("close_raw", 0.0) or 0.0)

        # MVP baseline proxy: FCFF0 を価格連動で近似
        fcff0 = max(price * (0.05 + (float(row.get("growth", 50.0)) / 1000.0)), 0.1)
        base_value = _discounted_value(fcff0, g, wacc)

        wacc_grid = [wacc - 0.01, wacc, wacc + 0.01]
        g_grid = [max(g - 0.005, 0.0), g, g + 0.005]
        for wacc_i in wacc_grid:
            for g_i in g_grid:
                sens_value = _discounted_value(fcff0, g_i, max(wacc_i, g_i + 0.005))
                rows.append(
                    {
                        "security_id": row["security_id"],
                        "market": market,
                        "ticker": row.get("ticker", row["security_id"]),
                        "mixed_rank": int(row["mixed_rank"]),
                        "price_native": price,
                        "base_intrinsic_native": base_value,
                        "base_intrinsic_jpy": base_value if market == "JP" else base_value * fx_usdjpy,
                        "wacc": wacc_i,
                        "g": g_i,
                        "sensitivity_intrinsic_native": sens_value,
                        "sensitivity_intrinsic_jpy": sens_value if market == "JP" else sens_value * fx_usdjpy,
                    }
                )

    return pd.DataFrame(rows)


def render_dcf_markdown(dcf_df: pd.DataFrame, security_id: str) -> str:
    target = dcf_df[dcf_df["security_id"] == security_id].copy()
    if target.empty:
        return "DCF data unavailable"

    base = target.iloc[4] if len(target) >= 5 else target.iloc[0]
    lines = [
        "| parameter | value |",
        "|---|---:|",
        f"| Base intrinsic (native) | {base['base_intrinsic_native']:.2f} |",
        f"| Base intrinsic (JPY) | {base['base_intrinsic_jpy']:.2f} |",
    ]

    lines.append("\nSensitivity (WACC x g):")
    lines.append("| WACC | g | intrinsic(native) | intrinsic(JPY) |")
    lines.append("|---:|---:|---:|---:|")
    for _, row in target.sort_values(["wacc", "g"]).iterrows():
        lines.append(
            f"| {row['wacc']:.3f} | {row['g']:.3f} | {row['sensitivity_intrinsic_native']:.2f} | {row['sensitivity_intrinsic_jpy']:.2f} |"
        )

    return "\n".join(lines)
