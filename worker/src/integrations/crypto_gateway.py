from __future__ import annotations

from typing import Any

import requests


class CryptoGatewayClient:
    def __init__(
        self,
        base_url: str,
        auth_token: str | None = None,
        timeout_sec: float = 8.0,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = str(base_url).rstrip("/")
        self.auth_token = auth_token
        self.timeout_sec = max(1.0, float(timeout_sec))
        self._session = session or requests.Session()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = (self.auth_token or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def execute_intent(self, payload: dict[str, Any]) -> dict[str, Any]:
        resp = self._session.post(
            f"{self.base_url}/v1/intents/execute",
            json=payload,
            headers=self._headers(),
            timeout=self.timeout_sec,
        )
        resp.raise_for_status()
        body = resp.json()
        if not isinstance(body, dict):
            raise RuntimeError("crypto gateway returned non-object response")
        return body

    def panic_close(self, payload: dict[str, Any]) -> dict[str, Any]:
        resp = self._session.post(
            f"{self.base_url}/v1/panic-close",
            json=payload,
            headers=self._headers(),
            timeout=self.timeout_sec,
        )
        resp.raise_for_status()
        body = resp.json()
        if not isinstance(body, dict):
            raise RuntimeError("crypto gateway returned non-object response")
        return body
