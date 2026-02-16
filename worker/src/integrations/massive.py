from __future__ import annotations

import time
import requests


class MassiveClient:
    def __init__(self, api_key: str | None):
        self.api_key = api_key

    def available(self) -> bool:
        return bool(self.api_key)

    def fetch(self, endpoint: str, params: dict | None = None) -> dict:
        if not self.available():
            raise RuntimeError("Massive API key is not set")
        headers = {"Authorization": f"Bearer {self.api_key}"}
        last_error: Exception | None = None
        for attempt in range(4):
            resp = requests.get(endpoint, headers=headers, params=params or {}, timeout=20)
            if resp.status_code != 429:
                resp.raise_for_status()
                return resp.json()

            retry_after_raw = (resp.headers.get("Retry-After") or "").strip()
            try:
                retry_after = max(1.0, float(retry_after_raw))
            except ValueError:
                retry_after = 8.0 + (attempt * 4.0)
            time.sleep(retry_after)
            last_error = requests.HTTPError(
                f"Massive rate limited status=429 endpoint={endpoint}",
                response=resp,
            )

        if last_error:
            raise last_error
        raise RuntimeError("Massive fetch failed without response")
