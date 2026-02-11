from __future__ import annotations

import json
from typing import Any

import requests

DEFAULT_OPENAI_MODEL = "gpt-5-mini"
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com/v1"
DEFAULT_OPENAI_TIMEOUT_SEC = 20.0
DEFAULT_OPENAI_MAX_OUTPUT_TOKENS = 900
DEFAULT_OPENAI_MAX_OUTPUT_TOKENS_RETRY_CAP = 4000


class OpenAIClientError(RuntimeError):
    """Raised when OpenAI request/response handling fails."""


def _extract_text_from_payload(payload: dict[str, Any]) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output = payload.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for part in content:
                if not isinstance(part, dict):
                    continue
                text = part.get("text")
                if isinstance(text, str) and text.strip():
                    return text

    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first, dict) else None
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                return content

    raise OpenAIClientError("OpenAI response text is missing")


def _extract_json_block(text: str) -> dict[str, Any]:
    raw = text.strip()
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise OpenAIClientError("OpenAI JSON output is not an object")
        return parsed
    except json.JSONDecodeError:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise OpenAIClientError("OpenAI output does not contain JSON object")

    snippet = raw[start : end + 1]
    try:
        parsed = json.loads(snippet)
    except json.JSONDecodeError as exc:
        raise OpenAIClientError(f"OpenAI output JSON parse failed: {exc}") from exc
    if not isinstance(parsed, dict):
        raise OpenAIClientError("OpenAI JSON output is not an object")
    return parsed


def request_openai_json(
    prompt: str,
    api_key: str,
    model: str = DEFAULT_OPENAI_MODEL,
    base_url: str = DEFAULT_OPENAI_BASE_URL,
    timeout_sec: float = DEFAULT_OPENAI_TIMEOUT_SEC,
    max_output_tokens: int = DEFAULT_OPENAI_MAX_OUTPUT_TOKENS,
    json_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not api_key:
        raise OpenAIClientError("OPENAI_API_KEY is required")
    selected_model = (model or "").strip() or DEFAULT_OPENAI_MODEL

    url = f"{base_url.rstrip('/')}/responses"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    attempt_tokens = max(int(max_output_tokens), 100)
    body: dict[str, Any] | None = None
    for _ in range(2):
        payload: dict[str, Any] = {
            "model": selected_model,
            "input": prompt,
            "max_output_tokens": attempt_tokens,
        }
        if selected_model.startswith("gpt-5"):
            # Keep reasoning overhead low so JSON output is less likely to truncate.
            payload["reasoning"] = {"effort": "minimal"}
        if json_schema:
            payload["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": "report_payload",
                    "schema": json_schema,
                    "strict": True,
                }
            }

        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
        if resp.status_code >= 400:
            raise OpenAIClientError(f"OpenAI API error {resp.status_code}: {resp.text[:500]}")
        body = resp.json()
        if body.get("status") != "incomplete":
            break

        reason = ""
        incomplete_details = body.get("incomplete_details")
        if isinstance(incomplete_details, dict):
            reason = str(incomplete_details.get("reason") or "")
        if reason != "max_output_tokens" or attempt_tokens >= DEFAULT_OPENAI_MAX_OUTPUT_TOKENS_RETRY_CAP:
            raise OpenAIClientError(f"OpenAI response incomplete: reason={reason or 'unknown'}")
        attempt_tokens = min(attempt_tokens * 2, DEFAULT_OPENAI_MAX_OUTPUT_TOKENS_RETRY_CAP)

    if body is None:
        raise OpenAIClientError("OpenAI response is missing")
    text = _extract_text_from_payload(body)
    return _extract_json_block(text)
