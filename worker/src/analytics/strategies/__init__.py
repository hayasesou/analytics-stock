"""Crypto arbitrage strategy evaluators."""

from src.analytics.strategies.crypto_cash_carry import evaluate_cash_carry_edge
from src.analytics.strategies.crypto_perp_perp import evaluate_perp_perp_edge

__all__ = ["evaluate_perp_perp_edge", "evaluate_cash_carry_edge"]
