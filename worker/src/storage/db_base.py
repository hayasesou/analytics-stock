from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from src.types import EdgeRisk


def _chunks(seq: list[Any], size: int = 1000) -> Iterator[list[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _normalize_edge_risk_payload(value: Any) -> dict[str, Any]:
    return EdgeRisk.from_mapping(value).to_dict()


def _merge_edge_risk_payload(
    primary: dict[str, Any],
    fallback: dict[str, Any],
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    keys = set(primary.keys()) | set(fallback.keys())
    for key in keys:
        if key == "extra":
            merged_extra: dict[str, Any] = {}
            if isinstance(fallback.get("extra"), dict):
                merged_extra.update(dict(fallback["extra"]))
            if isinstance(primary.get("extra"), dict):
                merged_extra.update(dict(primary["extra"]))
            output[key] = merged_extra
            continue
        primary_value = primary.get(key)
        fallback_value = fallback.get(key)
        output[key] = primary_value if primary_value is not None else fallback_value
    return output


class NeonRepositoryBase:
    def __init__(self, dsn: str):
        self.dsn = dsn

    @contextmanager
    def _conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            yield conn
