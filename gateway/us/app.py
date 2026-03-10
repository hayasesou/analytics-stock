from __future__ import annotations

import os

from gateway.us.adapter import IbkrTradeAdapter
from gateway.us.common import PENDING_STATUSES, TERMINAL_STATUSES
from gateway.us.common import env_flag as _env_flag
from gateway.us.common import is_terminal as _is_terminal
from gateway.us.common import normalize_side as _normalize_side
from gateway.us.common import normalize_symbol as _normalize_symbol
from gateway.us.common import standardize_status as _standardize_status
from gateway.us.common import to_float as _to_float
from gateway.us.common import utc_now_iso as _utc_now_iso
from gateway.us.execution import ExecutionCoordinator
from gateway.us.server import create_app
from gateway.us.store import IdempotencyStore, OrderStateStore


if __name__ == "__main__":
    host = os.getenv("US_GATEWAY_HOST", "0.0.0.0")
    port = int(_to_float(os.getenv("US_GATEWAY_PORT"), 8090))
    app = create_app()
    app.run(host=host, port=port, debug=False, threaded=True)
