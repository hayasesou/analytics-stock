from __future__ import annotations

from collections import defaultdict, deque
import threading
import time

from gateway.jp.common import normalize_symbol


class RateLimiter:
    def __init__(self, *, global_limit_per_sec: int, per_symbol_limit_per_sec: int) -> None:
        self.global_limit_per_sec = max(1, int(global_limit_per_sec))
        self.per_symbol_limit_per_sec = max(1, int(per_symbol_limit_per_sec))
        self._global_events: deque[float] = deque()
        self._symbol_events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def _prune(self, now_mono: float) -> None:
        threshold = now_mono - 1.0
        while self._global_events and self._global_events[0] < threshold:
            self._global_events.popleft()
        for symbol in list(self._symbol_events):
            events = self._symbol_events[symbol]
            while events and events[0] < threshold:
                events.popleft()
            if not events:
                self._symbol_events.pop(symbol, None)

    def acquire(self, symbol: str, timeout_sec: float, sleep_sec: float = 0.05) -> bool:
        deadline = time.monotonic() + max(0.1, float(timeout_sec))
        normalized_symbol = normalize_symbol(symbol)
        while True:
            with self._lock:
                now_mono = time.monotonic()
                self._prune(now_mono)
                global_ok = len(self._global_events) < self.global_limit_per_sec
                symbol_ok = len(self._symbol_events[normalized_symbol]) < self.per_symbol_limit_per_sec
                if global_ok and symbol_ok:
                    self._global_events.append(now_mono)
                    self._symbol_events[normalized_symbol].append(now_mono)
                    return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(max(0.01, float(sleep_sec)))
