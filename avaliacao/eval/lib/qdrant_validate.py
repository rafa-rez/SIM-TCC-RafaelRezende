"""Validação de itens golden RAG contra o índice Qdrant."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

import requests

from eval.lib.rag_compare import _doc_key, extract_match_text, hit_expected

FEDERAL_LEI_NUMBERS = {
    "8666", "14133", "13019", "10520", "13204", "12527", "101", "195",
    "8666", "8429", "9784", "11445", "123", "8142",
}


@dataclass
class ActRef:
    tipo: str
    numero: str
    ano: str

    @property
    def doc_key(self) -> str:
        return f"{int(self.numero)}/{self.ano}"

    def canonical_title(self) -> str:
        tipo_label = "Decreto Municipal" if self.tipo == "DECRETO" else "Lei Municipal"
        return f"{tipo_label} nº {self.numero}/{self.ano}"


@dataclass
class ValidationResult:
    indexed: bool
    chunk_count: int
    best_chunk: str
    rank_at_20: int | None
    act: ActRef | None = None


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text.lower()


def parse_act_from_expected(doc_id_esperado: str) -> ActRef | None:
    norm = _normalize(doc_id_esperado)
    m = re.search(r"(decreto|lei)\s+municipal\s+n[ºo°]?\s*([\d\.]+)\s*/\s*(\d{4})", norm)
    if not m:
        return None
    tipo = "DECRETO" if m.group(1) == "decreto" else "LEI"
    return ActRef(tipo=tipo, numero=m.group(2).replace(".", ""), ano=m.group(3))


def _format_num_variants(numero: str) -> list[str]:
    n = int(numero)
    variants = [numero, str(n), f"{n:04d}", f"{n:03d}"]
    if n >= 1000:
        thousands = n // 1000
        rest = n % 1000
        variants.append(f"{thousands}.{rest:03d}")
    return list(dict.fromkeys(variants))


def _header_pattern(act: ActRef) -> re.Pattern[str]:
    nums = "|".join(re.escape(v) for v in _format_num_variants(act.numero))
    year = act.ano
    tipo = act.tipo
    return re.compile(rf"{tipo}\s+N[ºo°\.]?\s*(?:{nums})\s*/\s*{year}", re.IGNORECASE)


def chunk_matches_act(content: str, act: ActRef) -> bool:
    if not content:
        return False
    if _header_pattern(act).search(content):
        return True
    if act.tipo == "DECRETO":
        nums = "|".join(re.escape(v) for v in _format_num_variants(act.numero))
        return bool(
            re.search(
                rf"DOCUMENTO:\s*DECRETO\s+N[ºo°\.]?\s*(?:{nums})\s*/\s*{act.ano}",
                content,
                re.I,
            )
        )
    nums = "|".join(re.escape(v) for v in _format_num_variants(act.numero))
    return bool(
        re.search(
            rf"DOCUMENTO:\s*LEI\s+N[ºo°\.]?\s*(?:{nums})\s*/\s*{act.ano}",
            content,
            re.I,
        )
    )


def find_chunks_for_act(
    act: ActRef,
    *,
    url: str,
    collection: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    search_terms: list[str] = []
    for num_var in _format_num_variants(act.numero):
        search_terms.append(f"{num_var}/{act.ano}")
    search_terms.append(act.doc_key)
    seen: set[str] = set()
    hits: list[dict[str, Any]] = []

    for term in search_terms:
        offset = None
        for _ in range(5):
            body: dict[str, Any] = {
                "limit": 100,
                "with_payload": True,
                "filter": {"must": [{"key": "page_content", "match": {"text": term}}]},
            }
            if offset:
                body["offset"] = offset
            resp = requests.post(
                f"{url.rstrip('/')}/collections/{collection}/points/scroll",
                json=body,
                timeout=60,
            )
            resp.raise_for_status()
            result = resp.json().get("result") or {}
            for point in result.get("points") or []:
                content = (point.get("payload") or {}).get("page_content", "")
                if not chunk_matches_act(content, act):
                    continue
                key = content[:200]
                if key in seen:
                    continue
                seen.add(key)
                hits.append(point)
                if len(hits) >= limit:
                    return hits
            offset = result.get("next_page_offset")
            if offset is None:
                break
    return hits


def search_rank(
    question: str,
    expected: str,
    *,
    url: str,
    collection: str,
    vector: list[float],
    top_k: int = 20,
) -> int | None:
    resp = requests.post(
        f"{url.rstrip('/')}/collections/{collection}/points/search",
        json={"vector": vector, "limit": top_k, "with_payload": True},
        timeout=60,
    )
    resp.raise_for_status()
    for i, hit in enumerate(resp.json().get("result") or [], start=1):
        if hit_expected(expected, extract_match_text(hit)):
            return i
    return None


def validate_item(
    doc_id_esperado: str,
    pergunta: str,
    *,
    url: str,
    collection: str,
    embed_fn,
    top_k: int = 20,
) -> ValidationResult:
    act = parse_act_from_expected(doc_id_esperado)
    if not act:
        return ValidationResult(False, 0, "", None, None)

    chunks = find_chunks_for_act(act, url=url, collection=collection)
    best_chunk = ""
    if chunks:
        best_chunk = (chunks[0].get("payload") or {}).get("page_content", "")[:500]

    rank = None
    if chunks:
        vec = embed_fn(pergunta)
        rank = search_rank(
            pergunta,
            doc_id_esperado,
            url=url,
            collection=collection,
            vector=vec,
            top_k=top_k,
        )

    return ValidationResult(
        indexed=bool(chunks),
        chunk_count=len(chunks),
        best_chunk=best_chunk,
        rank_at_20=rank,
        act=act,
    )


def is_municipal_act(act: ActRef) -> bool:
    if act.tipo == "LEI" and act.numero in FEDERAL_LEI_NUMBERS:
        return False
    if act.tipo == "LEI" and len(act.numero) <= 2 and act.ano in {"2000", "2011", "2014", "2015"}:
        return False
    year = int(act.ano)
    return 2000 <= year <= 2030 and int(act.numero) < 20000


def excerpt_from_chunk(content: str, max_len: int = 280) -> str:
    text = re.sub(r"\s+", " ", content).strip()
    text = re.sub(r"^DOCUMENTO:\s*", "", text, flags=re.I)
    text = re.sub(r"\[TIPO:[^\]]+\]", "", text, flags=re.I)
    text = re.sub(r"\[PARTE \d+/\d+\]", "", text, flags=re.I)
    text = text.replace("...", " ").strip()
    if len(text) > max_len:
        return text[: max_len - 3].rstrip() + "..."
    return text
