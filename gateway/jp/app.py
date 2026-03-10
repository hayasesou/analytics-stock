from __future__ import annotations

import os

from gateway.jp.adapter import KabuStationAdapter
from gateway.jp.common import env_flag as _env_flag
from gateway.jp.common import normalize_margin_type as _normalize_margin_type
from gateway.jp.common import normalize_side as _normalize_side
from gateway.jp.common import normalize_symbol as _normalize_symbol
from gateway.jp.common import to_float as _to_float
from gateway.jp.common import to_int as _to_int
from gateway.jp.common import utc_now_iso as _utc_now_iso
from gateway.jp.execution import ExecutionCoordinator
from gateway.jp.rate_limit import RateLimiter
from gateway.jp.server import create_app
from gateway.jp.store import IdempotencyStore


if __name__ == "__main__":
    host = os.getenv("JP_GATEWAY_HOST", "0.0.0.0")
    port = int(_to_float(os.getenv("JP_GATEWAY_PORT"), 8081))
    app = create_app()
    app.run(host=host, port=port, debug=False, threaded=True)
