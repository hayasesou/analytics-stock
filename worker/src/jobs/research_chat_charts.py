from __future__ import annotations

from contextlib import redirect_stdout
import ast
import io
import json
import re
import struct
from typing import Any
import zlib

from src.integrations.discord import build_web_session_url, send_bot_file, send_bot_message
from src.llm.openai_client import OpenAIClientError, request_openai_json
from src.jobs.research_chat_support import (
    FORBIDDEN_SQL_PATTERNS,
    RESEARCH_CHART_PLAN_JSON_SCHEMA,
    _as_dict,
    _clean_text,
    _json_safe,
    _resolve_openai_model,
    _trim_block,
)
from src.storage.db import NeonRepository
from src.types import ResearchArtifactRunSpec, ResearchArtifactSpec
import requests


def _build_discord_chart_message(*, session_id: str, source_title: str, charts: list[dict[str, Any]]) -> str:
    session_url = build_web_session_url(session_id)
    lines = ["research charts", f"session={session_id}", f"source={source_title}"]
    if session_url:
        lines.append(f"url={session_url}")
    for idx, chart in enumerate(charts[:3], start=1):
        lines.extend(["", f"{idx}. {chart.get('title', '-')}", f"kind: {chart.get('kind', '-')}", f"summary: {_trim_block(str(chart.get('summary') or ''), 220)}"])
    return "\n".join(lines)[:1900]


def _escape_svg(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;").replace("'", "&apos;")


def _chart_color(idx: int) -> str:
    return ["#235789", "#c1292e", "#f1a208", "#3f7d20"][idx % 4]


def _hex_to_rgb(color: str) -> tuple[int, int, int]:
    raw = str(color or "").strip().lstrip("#")
    if len(raw) != 6:
        return (0, 0, 0)
    try:
        return (int(raw[0:2], 16), int(raw[2:4], 16), int(raw[4:6], 16))
    except ValueError:
        return (0, 0, 0)


def _set_pixel(buffer: bytearray, width: int, height: int, x: int, y: int, color: tuple[int, int, int]) -> None:
    if 0 <= x < width and 0 <= y < height:
        idx = (y * width + x) * 4
        buffer[idx : idx + 4] = bytes((color[0], color[1], color[2], 255))


def _draw_line(buffer: bytearray, width: int, height: int, x0: int, y0: int, x1: int, y1: int, color: tuple[int, int, int]) -> None:
    dx = abs(x1 - x0)
    sx = 1 if x0 < x1 else -1
    dy = -abs(y1 - y0)
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    while True:
        _set_pixel(buffer, width, height, x0, y0, color)
        if x0 == x1 and y0 == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x0 += sx
        if e2 <= dx:
            err += dx
            y0 += sy


def _draw_rect(buffer: bytearray, width: int, height: int, x: int, y: int, rect_width: int, rect_height: int, color: tuple[int, int, int]) -> None:
    for yy in range(y, y + rect_height):
        for xx in range(x, x + rect_width):
            _set_pixel(buffer, width, height, xx, yy, color)


def _draw_circle(buffer: bytearray, width: int, height: int, cx: int, cy: int, radius: int, color: tuple[int, int, int]) -> None:
    for y in range(cy - radius, cy + radius + 1):
        for x in range(cx - radius, cx + radius + 1):
            if (x - cx) ** 2 + (y - cy) ** 2 <= radius ** 2:
                _set_pixel(buffer, width, height, x, y, color)


def _blend_pixel(buffer: bytearray, width: int, height: int, x: int, y: int, color: tuple[int, int, int], alpha: float) -> None:
    if 0 <= x < width and 0 <= y < height:
        idx = (y * width + x) * 4
        existing = buffer[idx : idx + 4]
        buffer[idx : idx + 4] = bytes(tuple(int(existing[i] * (1.0 - alpha) + color[i] * alpha) for i in range(3)) + (255,))


def _build_png_bytes(width: int, height: int, rgba: bytearray) -> bytes:
    header = b"\x89PNG\r\n\x1a\n"
    def _chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack("!I", len(data)) + tag + data + struct.pack("!I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    raw = bytearray()
    stride = width * 4
    for y in range(height):
        raw.append(0)
        raw.extend(rgba[y * stride : y * stride + stride])
    return header + _chunk(b"IHDR", struct.pack("!IIBBBBB", width, height, 8, 6, 0, 0, 0)) + _chunk(b"IDAT", zlib.compress(bytes(raw), level=9)) + _chunk(b"IEND", b"")


def _coerce_float(value: Any) -> float | None:
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


def _normalize_chart_spec(raw: dict[str, Any]) -> dict[str, Any] | None:
    kind = str(raw.get("kind") or "").strip().lower()
    if kind not in {"line", "bar", "scatter", "area"}:
        return None
    series_payload = raw.get("series")
    if not isinstance(series_payload, list) or not series_payload:
        return None
    normalized_series = []
    for entry in series_payload[:4]:
        if not isinstance(entry, dict) or not isinstance(entry.get("data"), list):
            continue
        points = []
        for point in entry["data"][:200]:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            y_value = _coerce_float(point[1])
            x_value = _clean_text(point[0])
            if y_value is None or not x_value:
                continue
            points.append([x_value, y_value])
        if len(points) >= 2:
            normalized_series.append({"name": _clean_text(entry.get("name")) or "series", "data": points})
    if not normalized_series:
        return None
    return {"title": _clean_text(raw.get("title")) or "Generated Chart", "kind": kind, "xAxisLabel": _clean_text(raw.get("x_axis_label") or raw.get("xAxisLabel")) or "X", "yAxisLabel": _clean_text(raw.get("y_axis_label") or raw.get("yAxisLabel")) or "Value", "summary": _clean_text(raw.get("summary")) or "", "series": normalized_series}


def _build_chart_svg(chart: dict[str, Any], width: int = 960, height: int = 540) -> str | None:
    spec = _normalize_chart_spec(chart)
    if not spec:
        return None
    series = list(spec.get("series") or [])
    all_y = [float(point[1]) for item in series for point in list(item.get("data") or []) if _coerce_float(point[1]) is not None]
    if len(all_y) < 2:
        return None
    plot_left, plot_top, plot_right, plot_bottom = 84, 68, width - 36, height - 92
    plot_width, plot_height = plot_right - plot_left, plot_bottom - plot_top
    min_y, max_y = min(all_y), max(all_y)
    if min_y == max_y:
        max_y += 1.0
    def _scale_x(index: int, total: int) -> float:
        return float(plot_left) if total <= 1 else plot_left + (plot_width * index / (total - 1))
    def _scale_y(value: float) -> float:
        return plot_bottom - ((value - min_y) / (max_y - min_y)) * plot_height
    x_labels = [str(point[0]) for point in list(series[0].get("data") or [])]
    grid_lines, axis_labels, series_paths, legend_items = [], [], [], []
    for idx in range(5):
        y_val = min_y + ((max_y - min_y) * idx / 4)
        y = _scale_y(y_val)
        grid_lines.append(f'<line x1="{plot_left}" y1="{y:.1f}" x2="{plot_right}" y2="{y:.1f}" stroke="#d7dde5" stroke-width="1" />')
        axis_labels.append(f'<text x="{plot_left - 12}" y="{y + 4:.1f}" text-anchor="end" font-size="12" fill="#425466">{y_val:.2f}</text>')
    for idx in range(min(6, len(x_labels))):
        source_idx = round(idx * (len(x_labels) - 1) / max(1, min(6, len(x_labels)) - 1))
        x = _scale_x(source_idx, len(x_labels))
        grid_lines.append(f'<line x1="{x:.1f}" y1="{plot_top}" x2="{x:.1f}" y2="{plot_bottom}" stroke="#eef2f6" stroke-width="1" />')
        axis_labels.append(f'<text x="{x:.1f}" y="{plot_bottom + 22}" text-anchor="middle" font-size="12" fill="#425466">{_escape_svg(x_labels[source_idx])}</text>')
    for idx, item in enumerate(series):
        points = list(item.get("data") or [])
        coords = [(_scale_x(point_idx, len(points)), _scale_y(float(point[1]))) for point_idx, point in enumerate(points) if _coerce_float(point[1]) is not None]
        if len(coords) < 2:
            continue
        color = _chart_color(idx)
        if spec.get("kind") == "scatter":
            series_paths.extend([f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4" fill="{color}" opacity="0.9" />' for x, y in coords])
        else:
            path = " ".join([f"M {coords[0][0]:.1f} {coords[0][1]:.1f}"] + [f"L {x:.1f} {y:.1f}" for x, y in coords[1:]])
            if spec.get("kind") == "area":
                area_path = f"{path} L {coords[-1][0]:.1f} {plot_bottom:.1f} L {coords[0][0]:.1f} {plot_bottom:.1f} Z"
                series_paths.append(f'<path d="{area_path}" fill="{color}" opacity="0.18" />')
            series_paths.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round" />')
        legend_items.append(f'<g transform="translate({plot_left + idx * 160},{height - 38})"><rect x="0" y="-10" width="18" height="4" fill="{color}" rx="2" /><text x="26" y="-5" font-size="12" fill="#23313f">{_escape_svg(str(item.get("name") or "series"))}</text></g>')
    return "\n".join([f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">', '<rect width="100%" height="100%" fill="#f8fbff" />', f'<text x="{plot_left}" y="34" font-size="22" font-weight="700" fill="#1d2a38">{_escape_svg(str(spec.get("title") or "Chart"))}</text>', f'<text x="{plot_left}" y="54" font-size="13" fill="#5b6b7b">{_escape_svg(str(spec.get("summary") or ""))}</text>', f'<rect x="{plot_left}" y="{plot_top}" width="{plot_width}" height="{plot_height}" fill="#ffffff" stroke="#d7dde5" />', *grid_lines, *axis_labels, f'<text x="{(plot_left + plot_right) / 2:.1f}" y="{height - 12}" text-anchor="middle" font-size="13" fill="#425466">{_escape_svg(str(spec.get("xAxisLabel") or "X"))}</text>', f'<text x="24" y="{(plot_top + plot_bottom) / 2:.1f}" text-anchor="middle" font-size="13" fill="#425466" transform="rotate(-90 24 {(plot_top + plot_bottom) / 2:.1f})">{_escape_svg(str(spec.get("yAxisLabel") or "Value"))}</text>', *series_paths, *legend_items, "</svg>"])


def _build_chart_png(chart: dict[str, Any], width: int = 960, height: int = 540) -> bytes | None:
    spec = _normalize_chart_spec(chart)
    if not spec:
        return None
    series = list(spec.get("series") or [])
    all_y = [float(point[1]) for item in series for point in list(item.get("data") or []) if _coerce_float(point[1]) is not None]
    if len(all_y) < 2:
        return None
    rgba = bytearray([248, 251, 255, 255] * width * height)
    plot_left, plot_top, plot_right, plot_bottom = 84, 68, width - 36, height - 92
    plot_width, plot_height = plot_right - plot_left, plot_bottom - plot_top
    min_y, max_y = min(all_y), max(all_y)
    if min_y == max_y:
        max_y += 1.0
    def _scale_x(index: int, total: int) -> int:
        return plot_left if total <= 1 else int(round(plot_left + (plot_width * index / (total - 1))))
    def _scale_y(value: float) -> int:
        return int(round(plot_bottom - ((value - min_y) / (max_y - min_y)) * plot_height))
    _draw_rect(rgba, width, height, plot_left, plot_top, plot_width, plot_height, (255, 255, 255))
    border = (215, 221, 229)
    _draw_line(rgba, width, height, plot_left, plot_top, plot_right, plot_top, border)
    _draw_line(rgba, width, height, plot_left, plot_bottom, plot_right, plot_bottom, border)
    _draw_line(rgba, width, height, plot_left, plot_top, plot_left, plot_bottom, border)
    _draw_line(rgba, width, height, plot_right, plot_top, plot_right, plot_bottom, border)
    for idx in range(5):
        _draw_line(rgba, width, height, plot_left, _scale_y(min_y + ((max_y - min_y) * idx / 4)), plot_right, _scale_y(min_y + ((max_y - min_y) * idx / 4)), (225, 232, 238))
    first_series_points = list(series[0].get("data") or [])
    for idx in range(min(6, len(first_series_points))):
        source_idx = round(idx * (len(first_series_points) - 1) / max(1, min(6, len(first_series_points)) - 1))
        x = _scale_x(source_idx, len(first_series_points))
        _draw_line(rgba, width, height, x, plot_top, x, plot_bottom, (238, 242, 246))
    for idx, item in enumerate(series):
        points = list(item.get("data") or [])
        coords = [(_scale_x(point_idx, len(points)), _scale_y(float(point[1]))) for point_idx, point in enumerate(points) if _coerce_float(point[1]) is not None]
        if len(coords) < 2:
            continue
        color = _hex_to_rgb(_chart_color(idx))
        kind = str(spec.get("kind") or "line")
        if kind == "bar":
            baseline = _scale_y(0.0 if min_y <= 0.0 <= max_y else min_y)
            bar_half = max(3, int(plot_width / max(20, len(coords) * 4)))
            for x, y in coords:
                _draw_rect(rgba, width, height, x - bar_half, min(y, baseline), bar_half * 2, max(1, abs(baseline - y)), color)
        elif kind == "scatter":
            for x, y in coords:
                _draw_circle(rgba, width, height, x, y, 4, color)
        else:
            if kind == "area":
                for x, y in coords:
                    for yy in range(y, plot_bottom):
                        _blend_pixel(rgba, width, height, x, yy, color, 0.12)
            for start, end in zip(coords, coords[1:]):
                _draw_line(rgba, width, height, start[0], start[1], end[0], end[1], color)
    return _build_png_bytes(width, height, rgba)


def _is_sql_safe(sql_text: str) -> bool:
    normalized = sql_text.strip().lower()
    return normalized.startswith(("select", "with")) and not any(re.search(pattern, normalized) for pattern in FORBIDDEN_SQL_PATTERNS)


def _execute_readonly_sql(repo: NeonRepository, sql_text: str) -> dict[str, Any]:
    if not _is_sql_safe(sql_text):
        raise ValueError("only read-only SELECT/WITH SQL is allowed")
    with repo._conn() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute(sql_text)
        rows = cur.fetchmany(200)
        columns = [str(col.name) for col in list(cur.description or [])]
    return {"columns": columns, "row_count": len(rows), "rows": _json_safe(rows)}


def _execute_python(code_text: str) -> dict[str, Any]:
    allowed_modules = {"math", "statistics", "json", "datetime"}
    def _safe_import(name: str, globals=None, locals=None, fromlist=(), level=0):  # noqa: ANN001
        root_name = str(name or "").split(".", 1)[0]
        if root_name not in allowed_modules:
            raise ImportError(f"import not allowed: {name}")
        return __import__(name, globals, locals, fromlist, level)
    stdout_buffer = io.StringIO()
    globals_dict = {"__builtins__": {"print": print, "len": len, "range": range, "min": min, "max": max, "sum": sum, "sorted": sorted, "abs": abs, "round": round, "__import__": _safe_import}}
    with redirect_stdout(stdout_buffer):
        exec(compile(code_text, "<research-artifact>", "exec"), globals_dict, globals_dict)
    return {"stdout": stdout_buffer.getvalue(), "globals": sorted(key for key in globals_dict.keys() if not key.startswith("__"))}


def _build_chart_planning_prompt(*, artifact: dict[str, Any], result: dict[str, Any], chart_type: str | None = None, instruction: str | None = None) -> str:
    result_preview = {"columns": result.get("columns"), "row_count": result.get("row_count"), "rows": list(result.get("rows") or [])[:20]}
    return "\n".join(["あなたは buy-side のデータ可視化リサーチャーです。", "与えられた artifact 実行結果から、検証価値の高い chart を 1-3 個だけ提案してください。", "見た目よりも、投資判断に効く比較・変化・異常値・トレンドを優先してください。", "chart は固定ではなく、データの列構造を見て最適な種類を選んでください。", "kind は line/bar/scatter/area のみ使えます。", "返答は JSON のみ。コードフェンスは禁止。", "", f"artifact_type: {artifact.get('artifact_type', '-')}", f"title: {artifact.get('title', '-')}", f"body: {_trim_block(str(artifact.get('body_md') or ''), 400)}", f"code: {_trim_block(str(artifact.get('code_text') or ''), 800)}", f"preferred_chart_type: {_clean_text(chart_type) or 'auto'}", f"user_instruction: {_trim_block(str(instruction or ''), 300) or '(none)'}", "", "result preview:", json.dumps(result_preview, ensure_ascii=False), "", "要件:", "- x 軸と y 軸の意味が自然になるように選ぶこと。", "- 同じデータから別観点の chart を作ってよい。", "- summary にはその chart で何を確認したいかを1-2文で書くこと。", "- series.data は [x, y] 形式にすること。", "- preferred_chart_type が auto でなければ、その意図を優先して構成すること。", "- user_instruction があれば、その意図を尊重して chart の観点を選ぶこと。"])


def _fallback_chart_specs_from_sql_result(result: dict[str, Any], title: str, *, preferred_chart_type: str | None = None, instruction: str | None = None) -> list[dict[str, Any]]:
    rows = result.get("rows")
    if not isinstance(rows, list) or not rows:
        return []
    points = [[str(row[0]), float(row[1])] for row in rows if isinstance(row, (list, tuple)) and len(row) >= 2 and _coerce_float(row[1]) is not None]
    if len(points) < 2:
        return []
    pct_points = []
    if _coerce_float(points[0][1]) not in {None, 0.0}:
        first = float(points[0][1])
        pct_points = [[x_value, ((float(y_value) / first) - 1.0) * 100.0] for x_value, y_value in points]
    charts = [{"title": title, "kind": "line", "xAxisLabel": str((result.get("columns") or ["X"])[0] if isinstance(result.get("columns"), list) and result.get("columns") else "X"), "yAxisLabel": str((result.get("columns") or ["X", "Value"])[1] if isinstance(result.get("columns"), list) and len(result.get("columns") or []) > 1 else "Value"), "summary": "原系列の水準推移を確認する。", "series": [{"name": "series_1", "data": points}]}]
    if len(pct_points) >= 2:
        charts.append({"title": f"{title} (% change)", "kind": "bar", "xAxisLabel": charts[0]["xAxisLabel"], "yAxisLabel": "% change from first point", "summary": "初期点からの変化率でトレンドの強さを確認する。", "series": [{"name": "pct_change", "data": pct_points}]})
    preferred = _clean_text(preferred_chart_type).lower()
    instruction_text = _clean_text(instruction).lower()
    if preferred == "scatter":
        charts.insert(0, {"title": f"{title} (scatter)", "kind": "scatter", "xAxisLabel": charts[0]["xAxisLabel"], "yAxisLabel": charts[0]["yAxisLabel"], "summary": "点の散らばりと外れ値を確認する。", "series": [{"name": "scatter", "data": points}]})
    elif preferred in {"bar_compare", "volume"} or "出来高" in instruction_text:
        charts[0]["kind"] = "bar"
        charts[0]["summary"] = "棒グラフで水準差を確認する。"
    elif preferred in {"cumulative_return", "relative_return"} and len(pct_points) >= 2:
        charts.insert(0, charts.pop(1))
    return charts


def _fallback_chart_specs_from_python_result(result: dict[str, Any], title: str, *, preferred_chart_type: str | None = None, instruction: str | None = None) -> list[dict[str, Any]]:
    payload = result.get("chart")
    if not isinstance(payload, dict):
        stdout_text = str(result.get("stdout") or "").strip()
        last_line = stdout_text.splitlines()[-1] if stdout_text else ""
        parsed = None
        if last_line:
            try:
                candidate = ast.literal_eval(last_line)
            except (ValueError, SyntaxError):
                candidate = None
            if isinstance(candidate, dict):
                parsed = candidate
        if not parsed:
            return []
        numeric_points = [[str(key), numeric] for key, value in parsed.items() if (numeric := _coerce_float(value)) is not None]
        if len(numeric_points) < 2:
            return []
        kind = "scatter" if _clean_text(preferred_chart_type).lower() == "scatter" else "area" if (_clean_text(preferred_chart_type).lower() in {"cumulative_return"} or "エリア" in _clean_text(instruction).lower()) else "bar"
        return [{"title": f"{title} Metrics", "kind": kind, "xAxisLabel": "Metric", "yAxisLabel": "Value", "summary": "Python artifact の数値出力を指標別に比較する。", "series": [{"name": "metrics", "data": numeric_points}]}]
    normalized = _normalize_chart_spec({"title": str(payload.get("title") or title), "kind": str(payload.get("kind") or "line"), "xAxisLabel": str(payload.get("xAxisLabel") or "X"), "yAxisLabel": str(payload.get("yAxisLabel") or "Value"), "summary": str(payload.get("summary") or "Python artifact が返した可視化データ。"), "series": payload.get("series")})
    if not normalized:
        return []
    preferred = _clean_text(preferred_chart_type).lower()
    if preferred in {"scatter", "bar_compare"}:
        normalized["kind"] = "scatter" if preferred == "scatter" else "bar"
    elif "エリア" in _clean_text(instruction).lower() or preferred == "cumulative_return":
        normalized["kind"] = "area"
    return [normalized]


def _plan_chart_specs_via_llm(artifact: dict[str, Any], result: dict[str, Any], *, chart_type: str | None = None, instruction: str | None = None, load_runtime_secrets_fn) -> list[dict[str, Any]]:
    try:
        secrets = load_runtime_secrets_fn()
    except RuntimeError:
        return []
    api_key = str(getattr(secrets, "openai_api_key", "") or "").strip()
    if not api_key:
        return []
    prompt = _build_chart_planning_prompt(artifact=artifact, result=result, chart_type=chart_type, instruction=instruction)
    try:
        payload = request_openai_json(prompt=prompt, api_key=api_key, model=_resolve_openai_model(), json_schema=RESEARCH_CHART_PLAN_JSON_SCHEMA, max_output_tokens=1600)
    except (OpenAIClientError, requests.RequestException, RuntimeError):
        return []
    charts = payload.get("charts")
    if not isinstance(charts, list):
        return []
    return [normalized for chart in charts if isinstance(chart, dict) and (normalized := _normalize_chart_spec(chart))]


def _create_chart_artifacts_from_run(repo: NeonRepository, *, artifact: dict[str, Any], result: dict[str, Any], run_id: str, chart_type: str | None = None, instruction: str | None = None, load_runtime_secrets_fn) -> list[dict[str, Any]]:
    artifact_type = str(artifact.get("artifact_type") or "").strip().lower()
    source_title = str(artifact.get("title") or "Artifact").strip() or "Artifact"
    chart_specs = _plan_chart_specs_via_llm(artifact, result, chart_type=chart_type, instruction=instruction, load_runtime_secrets_fn=load_runtime_secrets_fn)
    if not chart_specs:
        if artifact_type == "sql":
            chart_specs = _fallback_chart_specs_from_sql_result(result, source_title, preferred_chart_type=chart_type, instruction=instruction)
        elif artifact_type == "python":
            chart_specs = _fallback_chart_specs_from_python_result(result, source_title, preferred_chart_type=chart_type, instruction=instruction)
    created = []
    for chart_spec in chart_specs[:3]:
        artifact_id = repo.insert_research_artifact(ResearchArtifactSpec(session_id=str(artifact.get("session_id") or ""), hypothesis_id=str(artifact.get("hypothesis_id") or "") or None, artifact_type="chart", title=f"Chart: {chart_spec.get('title') or source_title}", body_md=str(chart_spec.get("summary") or f"Auto-generated chart from {artifact_type} artifact run."), metadata={"processor": "research.chart_generate", "source_artifact_id": str(artifact.get("id") or ""), "source_run_id": run_id, "chart_spec": chart_spec}))
        created.append({"id": artifact_id, "title": str(chart_spec.get("title") or source_title), "summary": str(chart_spec.get("summary") or ""), "kind": str(chart_spec.get("kind") or "")})
    return created


def _send_discord_chart_follow_up(*, payload: dict[str, Any], session_id: str, source_title: str, charts: list[dict[str, Any]], load_runtime_secrets_fn, send_bot_message_fn=send_bot_message, send_bot_file_fn=send_bot_file) -> None:
    if str(payload.get("requested_by") or "").strip().lower() != "discord":
        return
    channel_id = _clean_text(payload.get("discord_channel_id"))
    if not channel_id or not charts:
        return
    secrets = load_runtime_secrets_fn()
    token = getattr(secrets, "discord_bot_token", None)
    send_bot_message_fn(token, channel_id, _build_discord_chart_message(session_id=session_id, source_title=source_title, charts=charts))
    for idx, chart in enumerate(charts[:3], start=1):
        png = _build_chart_png(chart)
        if png:
            send_bot_file_fn(token, channel_id, filename=f"research-chart-{idx}.png", content=png, message=f"{chart.get('title', 'chart')} ({chart.get('kind', '-')})", content_type="image/png")
