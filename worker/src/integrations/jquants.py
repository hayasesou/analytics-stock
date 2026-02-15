from __future__ import annotations

import requests


class JQuantsClient:
    V2_EQ_MASTER_URL = "https://api.jquants.com/v2/equities/master"
    V1_AUTH_USER_URL = "https://api.jquants.com/v1/token/auth_user"
    V1_AUTH_REFRESH_URL = "https://api.jquants.com/v1/token/auth_refresh"
    V1_LISTED_INFO_URL = "https://api.jquants.com/v1/listed/info"

    def __init__(
        self,
        api_key: str | None = None,
        email: str | None = None,
        password: str | None = None,
    ):
        self.api_key = api_key
        self.email = email
        self.password = password

    def available(self) -> bool:
        return self.v2_available() or self.v1_available()

    def v2_available(self) -> bool:
        return bool(self.api_key)

    def v1_available(self) -> bool:
        return bool(self.email and self.password)

    @staticmethod
    def _raise_for_status(resp: requests.Response, context: str) -> None:
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            body = ""
            try:
                body = str(resp.text or "").strip()
            except Exception:
                body = ""
            if body:
                raise RuntimeError(f"{context} failed: status={resp.status_code} body={body}") from exc
            raise RuntimeError(f"{context} failed: status={resp.status_code}") from exc

    def fetch(self, endpoint: str, params: dict | None = None) -> dict:
        if not self.available():
            raise RuntimeError("J-Quants credentials are not set (JQUANTS_API_KEY or JQUANTS_EMAIL/JQUANTS_PASSWORD)")

        headers: dict[str, str] = {}
        if self.v2_available():
            headers["x-api-key"] = str(self.api_key)
        resp = requests.get(endpoint, params=params or {}, headers=headers, timeout=10)
        self._raise_for_status(resp, f"J-Quants GET {endpoint}")
        return resp.json()

    def _fetch_v1_refresh_token(self) -> str:
        if not self.v1_available():
            raise RuntimeError("J-Quants v1 credentials are not set")
        resp = requests.post(
            self.V1_AUTH_USER_URL,
            json={
                "mailaddress": self.email,
                "password": self.password,
            },
            timeout=10,
        )
        self._raise_for_status(resp, "J-Quants v1 auth_user")
        payload = resp.json()
        token = str(payload.get("refreshToken") or "").strip()
        if not token:
            raise RuntimeError("J-Quants refreshToken was empty")
        return token

    def _fetch_v1_id_token(self, refresh_token: str) -> str:
        resp = requests.get(
            self.V1_AUTH_REFRESH_URL,
            params={"refreshtoken": refresh_token},
            timeout=10,
        )
        self._raise_for_status(resp, "J-Quants v1 auth_refresh")
        payload = resp.json()
        token = str(payload.get("idToken") or "").strip()
        if not token:
            raise RuntimeError("J-Quants idToken was empty")
        return token

    def _fetch_listed_info_v2(self, code: str | None = None, date: str | None = None) -> list[dict]:
        if not self.v2_available():
            raise RuntimeError("J-Quants API key is not set")
        params: dict[str, str] = {}
        if code:
            params["code"] = code
        if date:
            params["date"] = date
        resp = requests.get(
            self.V2_EQ_MASTER_URL,
            params=params,
            headers={"x-api-key": str(self.api_key)},
            timeout=20,
        )
        self._raise_for_status(resp, "J-Quants v2 equities/master")
        payload = resp.json()
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise RuntimeError("J-Quants v2 equities/master response missing data[]")
        return [r for r in rows if isinstance(r, dict)]

    def _fetch_listed_info_v1(self) -> list[dict]:
        refresh_token = self._fetch_v1_refresh_token()
        id_token = self._fetch_v1_id_token(refresh_token)
        resp = requests.get(
            self.V1_LISTED_INFO_URL,
            headers={"Authorization": f"Bearer {id_token}"},
            timeout=20,
        )
        self._raise_for_status(resp, "J-Quants v1 listed/info")
        payload = resp.json()
        rows = payload.get("info")
        if not isinstance(rows, list):
            raise RuntimeError("J-Quants v1 listed/info response missing info[]")
        return [r for r in rows if isinstance(r, dict)]

    def fetch_listed_info(self, code: str | None = None, date: str | None = None) -> list[dict]:
        """
        Return listed master from J-Quants.
        Prefer v2 API key authentication, and fall back to v1 mail/password auth.
        """
        if self.v2_available():
            return self._fetch_listed_info_v2(code=code, date=date)
        if self.v1_available():
            return self._fetch_listed_info_v1()
        raise RuntimeError("J-Quants credentials are not set (JQUANTS_API_KEY or JQUANTS_EMAIL/JQUANTS_PASSWORD)")
