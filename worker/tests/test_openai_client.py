from __future__ import annotations

from typing import Any

import pytest

from src.llm.openai_client import OpenAIClientError, request_openai_json


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any], text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._payload


def test_request_openai_json_parses_output_text(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        return _FakeResponse(
            200,
            payload={
                "output_text": '{"title":"A","body_md":"B","conclusion":"C","falsification_conditions":"D","claims":[]}'
            },
        )

    monkeypatch.setattr("src.llm.openai_client.requests.post", fake_post)
    out = request_openai_json(
        prompt="test",
        api_key="test-key",
        model="gpt-5-mini",
    )
    assert out["title"] == "A"
    assert out["conclusion"] == "C"


def test_request_openai_json_handles_response_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        return _FakeResponse(401, payload={}, text="unauthorized")

    monkeypatch.setattr("src.llm.openai_client.requests.post", fake_post)
    with pytest.raises(OpenAIClientError):
        request_openai_json(
            prompt="test",
            api_key="bad-key",
            model="gpt-5-mini",
        )


def test_request_openai_json_raises_when_output_has_no_json(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        return _FakeResponse(200, payload={"output_text": "not-json"})

    monkeypatch.setattr("src.llm.openai_client.requests.post", fake_post)
    with pytest.raises(OpenAIClientError):
        request_openai_json(
            prompt="test",
            api_key="test-key",
            model="gpt-5-mini",
        )


def test_request_openai_json_falls_back_default_model_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_payload: dict[str, Any] = {}

    def fake_post(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        nonlocal captured_payload
        captured_payload = kwargs.get("json", {})
        return _FakeResponse(200, payload={"output_text": '{"title":"A"}'})

    monkeypatch.setattr("src.llm.openai_client.requests.post", fake_post)
    out = request_openai_json(
        prompt="test",
        api_key="test-key",
        model="",
        max_output_tokens=1,
    )
    assert out["title"] == "A"
    assert captured_payload["model"] == "gpt-5-mini"
    assert captured_payload["max_output_tokens"] == 100


def test_request_openai_json_attaches_json_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_payload: dict[str, Any] = {}

    def fake_post(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        nonlocal captured_payload
        captured_payload = kwargs.get("json", {})
        return _FakeResponse(200, payload={"output_text": '{"title":"A"}'})

    monkeypatch.setattr("src.llm.openai_client.requests.post", fake_post)
    out = request_openai_json(
        prompt="test",
        api_key="test-key",
        model="gpt-5-mini",
        json_schema={"type": "object", "properties": {"title": {"type": "string"}}},
    )
    assert out["title"] == "A"
    assert captured_payload["text"]["format"]["type"] == "json_schema"
    assert captured_payload["text"]["format"]["strict"] is True


def test_request_openai_json_retries_when_incomplete_due_to_max_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payloads: list[dict[str, Any]] = []

    def fake_post(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        captured_payloads.append(kwargs.get("json", {}))
        if len(captured_payloads) == 1:
            return _FakeResponse(
                200,
                payload={
                    "status": "incomplete",
                    "incomplete_details": {"reason": "max_output_tokens"},
                    "output": [],
                },
            )
        return _FakeResponse(200, payload={"status": "completed", "output_text": '{"title":"A"}'})

    monkeypatch.setattr("src.llm.openai_client.requests.post", fake_post)
    out = request_openai_json(
        prompt="test",
        api_key="test-key",
        model="gpt-5-mini",
        max_output_tokens=200,
    )
    assert out["title"] == "A"
    assert len(captured_payloads) == 2
    assert captured_payloads[0]["max_output_tokens"] == 200
    assert captured_payloads[1]["max_output_tokens"] == 400


def test_request_openai_json_raises_when_incomplete_for_non_retry_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_post(*args: Any, **kwargs: Any) -> _FakeResponse:  # noqa: ANN401
        return _FakeResponse(
            200,
            payload={
                "status": "incomplete",
                "incomplete_details": {"reason": "content_filter"},
                "output": [],
            },
        )

    monkeypatch.setattr("src.llm.openai_client.requests.post", fake_post)
    with pytest.raises(OpenAIClientError, match="incomplete"):
        request_openai_json(
            prompt="test",
            api_key="test-key",
            model="gpt-5-mini",
            max_output_tokens=200,
        )
