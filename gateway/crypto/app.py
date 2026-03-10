from __future__ import annotations

import os

from gateway.crypto.adapter import BinanceTradeAdapter, HyperliquidTradeAdapter
from gateway.crypto.common import env_flag as _env_flag
from gateway.crypto.common import normalize_side as _normalize_side
from gateway.crypto.common import normalize_symbol as _normalize_symbol
from gateway.crypto.common import opposite_side as _opposite_side
from gateway.crypto.common import to_float as _to_float
from gateway.crypto.common import utc_now_iso as _utc_now_iso
from gateway.crypto.execution import ExecutionCoordinator
from gateway.crypto.server import create_app
from gateway.crypto.store import IdempotencyStore


if __name__ == "__main__":
    host = os.getenv("CRYPTO_GATEWAY_HOST", "0.0.0.0")
    port = int(_to_float(os.getenv("CRYPTO_GATEWAY_PORT"), 8080))
    app = create_app()
    app.run(host=host, port=port, debug=False, threaded=True)
