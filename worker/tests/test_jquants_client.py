from __future__ import annotations

from typing import Any

import pytest
import requests

from src.integrations.jquants import JQuantsClient


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any], text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")


def test_fetch_listed_info_v2_uses_x_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_get(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        captured["url"] = args[0]
        captured["params"] = kwargs.get("params", {})
        captured["headers"] = kwargs.get("headers", {})
        return _FakeResponse(200, payload={"data": [{"Code": "13010", "CompanyName": "Test Corp"}]})

    monkeypatch.setattr("src.integrations.jquants.requests.get", fake_get)

    client = JQuantsClient(api_key="test-api-key")
    rows = client.fetch_listed_info(code="13010", date="20260105")

    assert rows == [{"Code": "13010", "CompanyName": "Test Corp"}]
    assert captured["url"] == "https://api.jquants.com/v2/equities/master"
    assert captured["params"] == {"code": "13010", "date": "20260105"}
    assert captured["headers"]["x-api-key"] == "test-api-key"


def test_fetch_listed_info_v1_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {"auth_headers": None}

    def fake_post(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        assert args[0] == "https://api.jquants.com/v1/token/auth_user"
        assert kwargs["json"]["mailaddress"] == "user@example.com"
        return _FakeResponse(200, payload={"refreshToken": "refresh-token"})

    def fake_get(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        url = args[0]
        if url == "https://api.jquants.com/v1/token/auth_refresh":
            return _FakeResponse(200, payload={"idToken": "id-token"})
        if url == "https://api.jquants.com/v1/listed/info":
            captured["auth_headers"] = kwargs.get("headers", {})
            return _FakeResponse(200, payload={"info": [{"Code": "72030", "CompanyName": "Toyota"}]})
        raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr("src.integrations.jquants.requests.post", fake_post)
    monkeypatch.setattr("src.integrations.jquants.requests.get", fake_get)

    client = JQuantsClient(email="user@example.com", password="password")
    rows = client.fetch_listed_info()

    assert rows == [{"Code": "72030", "CompanyName": "Toyota"}]
    assert captured["auth_headers"] == {"Authorization": "Bearer id-token"}


def test_fetch_listed_info_v2_raises_with_response_body(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        _ = (args, kwargs)
        return _FakeResponse(401, payload={"message": "invalid api key"}, text='{"message":"invalid api key"}')

    monkeypatch.setattr("src.integrations.jquants.requests.get", fake_get)

    client = JQuantsClient(api_key="bad-key")
    with pytest.raises(RuntimeError, match="v2 equities/master failed: status=401"):
        client.fetch_listed_info()
