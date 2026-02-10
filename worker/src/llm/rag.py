from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RagConfig:
    chunk_chars: int = 1200
    overlap_chars: int = 150
    top_k: int = 8


def chunk_text(text: str, cfg: RagConfig) -> list[str]:
    if not text:
        return []
    chunks: list[str] = []
    step = max(cfg.chunk_chars - cfg.overlap_chars, 1)
    for i in range(0, len(text), step):
        chunk = text[i : i + cfg.chunk_chars]
        if chunk:
            chunks.append(chunk)
    return chunks


def lexical_retrieve(query: str, chunks: list[str], top_k: int = 8) -> list[str]:
    tokens = {t.lower() for t in query.split() if len(t) > 1}
    scored: list[tuple[int, str]] = []
    for ch in chunks:
        score = sum(1 for t in tokens if t in ch.lower())
        if score > 0:
            scored.append((score, ch))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored[:top_k]]
