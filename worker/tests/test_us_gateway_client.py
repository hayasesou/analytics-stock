from __future__ import annotations

from typing import Any

import pytest

from src.integrations.us_gateway import USGatewayClient


class _FakeResponse:
    def __init__(self, body: Any, status_code: int = 200) -> None:
        self._body = body
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}")

    def json(self) -> Any:
        return self._body


class _FakeSession:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, json: dict[str, Any], headers: dict[str, str], timeout: float):  # noqa: A002
        self.calls.append(
            {
                "url": url,
                "json": json,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return self.response


def test_execute_intent_posts_with_auth_header() -> None:
    session = _FakeSession(_FakeResponse({"status": "filled"}))
    client = USGatewayClient(
        base_url="http://gateway-us:8090/",
        auth_token="secret-token",
        timeout_sec=7,
        session=session,  # type: ignore[arg-type]
    )

    result = client.execute_intent({"intent_id": "intent-1", "orders": []})

    assert result["status"] == "filled"
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == "http://gateway-us:8090/v1/intents/execute"
    assert call["headers"]["Authorization"] == "Bearer secret-token"
    assert call["timeout"] == 7


def test_resync_orders_posts_expected_endpoint() -> None:
    session = _FakeSession(_FakeResponse({"resynced": 2}))
    client = USGatewayClient(
        base_url="http://gateway-us:8090",
        session=session,  # type: ignore[arg-type]
    )

    result = client.resync_orders()

    assert result["resynced"] == 2
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == "http://gateway-us:8090/v1/orders/resync"


def test_execute_intent_raises_when_response_is_not_object() -> None:
    session = _FakeSession(_FakeResponse(["invalid"]))
    client = USGatewayClient(
        base_url="http://gateway-us:8090",
        session=session,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="non-object"):
        client.execute_intent({"intent_id": "intent-2", "orders": []})
