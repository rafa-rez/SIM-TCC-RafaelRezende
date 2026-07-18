"""Métricas de recuperação documental (RAG)."""

from __future__ import annotations

import re
import unicodedata
from typing import Any


def _normalize(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _doc_key(text: str) -> str:
    """Extrai núcleo numérico de referências como 'Decreto Municipal nº 247/2024'."""
    norm = _normalize(text)
    m = re.search(r"(\d+)\s*/\s*(\d{4})", norm)
    if m:
        return f"{int(m.group(1))}/{m.group(2)}"
    m = re.search(r"(\d{4})", norm)
    if m and len(m.group(1)) == 4:
        nums = re.findall(r"\d+", norm)
        if len(nums) >= 2:
            return f"{int(nums[0])}/{nums[-1]}"
    return norm


def hit_expected(expected: str, candidate: str) -> bool:
    """Verifica se o documento recuperado corresponde ao gabarito."""
    if not expected or not candidate:
        return False
    exp = _normalize(expected)
    cand = _normalize(candidate)
    if exp in cand or cand in exp:
        return True
    return _doc_key(expected) == _doc_key(candidate) and _doc_key(expected) != ""


def extract_match_text(hit: dict[str, Any]) -> str:
    """Texto usado para comparar com o doc_id_esperado do golden."""
    payload = hit.get("payload") or hit.get("metadata") or {}
    content = payload.get("page_content") or payload.get("content") or ""
    if content:
        return str(content)
    return extract_doc_label(hit)


def extract_doc_label(hit: dict[str, Any]) -> str:
    """Obtém rótulo do documento a partir de um hit Qdrant ou checkpoint."""
    payload = hit.get("payload") or hit.get("metadata") or {}
    for key in (
        "titulo",
        "title",
        "documento",
        "doc_title",
        "nome",
        "source",
        "doc_id",
        "page_content",
    ):
        val = payload.get(key) or hit.get(key)
        if val:
            text = str(val)
            if key == "page_content" and len(text) > 120:
                return text[:120] + "..."
            return text
    return str(hit.get("id", ""))


def recall_at_k(expected: str, retrieved: list[str], k: int) -> bool:
    top = retrieved[:k]
    return any(hit_expected(expected, doc) for doc in top)


def reciprocal_rank(expected: str, retrieved: list[str]) -> float:
    for i, doc in enumerate(retrieved, start=1):
        if hit_expected(expected, doc):
            return 1.0 / i
    return 0.0


def aggregate_recall(recalls: list[bool]) -> float:
    if not recalls:
        return 0.0
    return sum(1 for r in recalls if r) / len(recalls)


def aggregate_mrr(rrs: list[float]) -> float:
    if not rrs:
        return 0.0
    return sum(rrs) / len(rrs)
