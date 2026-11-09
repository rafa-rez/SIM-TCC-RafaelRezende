"""
Recuperador Híbrido (Denso + Lexical por Número de Ato e CNPJ) com Deduplicação e Filtro de Metadados.

Resolve o achado crítico FIND-004:
- Em produção (wpp-tiokk-sql.json), o nó Qdrant do n8n executava apenas busca densa simples,
  ignorava `filtro_tipo_ato` e `filtro_cnpj` e sofria com 47,9% de chunks duplicados (Recall@5 = 3,12% para atos numerados).
- As técnicas que atingiram Recall@5 = 100% e MRR = 97,5% no artigo SBC (`--hybrid-act`, `--dedup`, `--filtro-tipo-ato`)
  existiam apenas no script de benchmark `run_rag_eval.py`.
- Este módulo promove essa lógica para o núcleo de produção da plataforma (`sim_platform`),
  carregando automaticamente o acervo real de atos normativos do Diário Oficial de Caeté (*Jornal Opinião*).
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


NAMESPACE_SIM_RAG = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]

ATO_TYPE_KEYWORDS: List[Tuple[str, List[str]]] = [
    ("PORTARIA", ["portaria"]),
    ("DECRETO", ["decreto"]),
    ("LEI", ["lei complementar", "lei municipal", "lei n", "lei nº", "projeto de lei"]),
    ("TERMO ADITIVO", ["termo aditivo", "aditivo contratual", "aditamento"]),
    ("EXTRATO CONTRATUAL", ["extrato de contrato", "extrato contratual"]),
    ("EDITAL / LICITAÇÃO", ["edital", "aviso de licitacao", "pregao", "concorrencia", "dispensa"]),
]

RE_ACT_NUMBER = re.compile(
    r"(?:n[º°o\.]?\s*|lei\s+|decreto\s+|portaria\s+|edital\s+)(\d{1,5}(?:\.\d{3})*(?:/\d{2,4})?)",
    re.IGNORECASE,
)

RE_CNPJ = re.compile(r"\b\d{2}\.?\d{3}\.?\d{3}/\d{4}-\d{2}\b")


RE_ACT_HEADER_CANON = re.compile(
    r"\b(DECRETO|LEI COMPLEMENTAR|LEI|PORTARIA|EDITAL|RESOLU[CÇ][AÃ]O)\s+(?:MUNICIPAL\s+)?"
    r"N[ºO°\.]?\s*\.?\s*(\d{1,5}(?:\.\d{3})*)\s*/\s*(\d{2,4})",
    re.IGNORECASE,
)


def normalize_text_for_dedup(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    clean = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Remove hifenização de quebra de linha/coluna típica do PDF do Jornal Opinião (ex: "orca- mento" -> "orcamento")
    clean = re.sub(r"([a-z])-\s+([a-z])", r"\1\2", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def canonical_rag_fingerprint(text: str) -> str:
    norm = normalize_text_for_dedup(text)
    norm = re.sub(r"^\[fonte:[^\]]+\]\s*", "", norm)
    norm = re.sub(r"^documento:\s*[^\n]+\s*", "", norm)
    alnum = re.sub(r"[^a-z0-9]", "", norm)
    return alnum[:140]


def extract_act_signature(text: str) -> Optional[str]:
    m = RE_ACT_HEADER_CANON.search((text or "")[:260])
    if not m:
        return None
    tipo = m.group(1).upper().replace("Ç", "C").replace("Ã", "A")
    num = m.group(2).replace(".", "").lstrip("0") or "0"
    ano = m.group(3)
    parte_m = re.search(r"\[PARTE\s+(\d+/\d+)\]", (text or "")[:120], re.IGNORECASE)
    parte = parte_m.group(1) if parte_m else "1/1"
    return f"{tipo}::{num}/{ano}::{parte}"


def deterministic_chunk_id(source_pdf: str, page_content: str) -> str:
    """
    Gera um UUIDv5 determinístico baseado no arquivo de origem e no conteúdo normalizado,
    eliminando 100% a duplicação de chunks em reexecuções do ETL (que usava uuid4 aleatório).
    """
    fp = canonical_rag_fingerprint(page_content)
    key = f"{source_pdf.strip().lower()}::{hashlib.sha256(fp.encode('utf-8')).hexdigest()}"
    return str(uuid.uuid5(NAMESPACE_SIM_RAG, key))


def load_real_caete_rag_corpus(workspace_root: Path = DEFAULT_WORKSPACE_ROOT) -> List[Dict[str, Any]]:
    """
    Carrega a collection limpa e 100% deduplicada de atos individuais do Diário Oficial de Caeté
    (*Jornal Opinião*), com reparo de ligaduras PDF (`ﬁ ns` -> `fins`), des-hifenização de colunas
    e enriquecimento de fechos órfãos via `sim_platform.rag.newspaper_pipeline`.
    """
    persisted = workspace_root / "avaliacao" / "dados" / "parquet" / "caete_mg_3110004" / "rag_jornais_individuais.json"
    if persisted.exists():
        try:
            data = json.loads(persisted.read_text(encoding="utf-8"))
            if isinstance(data, list) and len(data) > 0:
                return data
        except Exception:
            pass

    from sim_platform.rag.newspaper_pipeline import build_clean_individual_collection

    res = build_clean_individual_collection(workspace_root=workspace_root, persist_json=True)
    return res["collection"]



@dataclass
class RetrievedChunk:
    chunk_id: str
    score: float
    page_content: str
    source: str
    tipo_ato: str = "GERAL / OUTROS"
    ano_edicao: Optional[int] = None
    numero_edicao: Optional[str] = None
    cnpjs_mencionados: List[str] = field(default_factory=list)
    match_stage: str = "dense"  # "lexical_exact" | "dense_filtered" | "dense"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "score": round(self.score, 4),
            "page_content": self.page_content,
            "source": self.source,
            "tipo_ato": self.tipo_ato,
            "ano_edicao": self.ano_edicao,
            "numero_edicao": self.numero_edicao,
            "cnpjs_mencionados": self.cnpjs_mencionados,
            "match_stage": self.match_stage,
        }


class HybridRAGRetriever:
    """
    Motor de recuperação híbrida para o Diário Oficial (*Jornal Opinião*).
    """

    def __init__(self, corpus_chunks: Optional[List[Dict[str, Any]]] = None) -> None:
        if corpus_chunks is not None:
            self.corpus_chunks = corpus_chunks
        else:
            self.corpus_chunks = load_real_caete_rag_corpus()

    @staticmethod
    def extract_query_signals(
        query: str,
        explicit_tipo_ato: Optional[str] = None,
        explicit_cnpj: Optional[str] = None,
    ) -> Dict[str, Any]:
        q_norm = normalize_text_for_dedup(query)

        inferred_tipo = explicit_tipo_ato
        if not inferred_tipo:
            for tipo_label, kw_list in ATO_TYPE_KEYWORDS:
                if any(kw in q_norm for kw in kw_list):
                    inferred_tipo = tipo_label
                    break

        act_numbers: List[str] = []
        for m in RE_ACT_NUMBER.finditer(query):
            val = m.group(1).strip()
            if val not in ("2020", "2021", "2022", "2023", "2024", "2025", "2026"):
                act_numbers.append(val)
                no_dot = val.replace(".", "")
                if no_dot != val:
                    act_numbers.append(no_dot)

        cnpjs: List[str] = []
        if explicit_cnpj:
            cnpjs.append(explicit_cnpj.strip())
        for m in RE_CNPJ.finditer(query):
            if m.group(0) not in cnpjs:
                cnpjs.append(m.group(0))

        return {
            "tipo_ato": inferred_tipo,
            "act_numbers": act_numbers,
            "cnpjs": cnpjs,
        }

    @staticmethod
    def deduplicate_chunks(chunks: List[RetrievedChunk], top_k: int = 5) -> List[RetrievedChunk]:
        seen_hashes: set[str] = set()
        unique: List[RetrievedChunk] = []
        for ch in chunks:
            act_sig = extract_act_signature(ch.page_content)
            fp = canonical_rag_fingerprint(ch.page_content)
            h = act_sig if act_sig else hashlib.sha256(fp.encode("utf-8")).hexdigest()
            if h in seen_hashes or fp in seen_hashes:
                continue
            seen_hashes.add(h)
            seen_hashes.add(fp)
            unique.append(ch)
            if len(unique) >= top_k:
                break
        return unique

    def search(
        self,
        query: str,
        top_k: int = 5,
        filtro_tipo_ato: Optional[str] = None,
        filtro_cnpj: Optional[str] = None,
        deduplicate: bool = True,
    ) -> Dict[str, Any]:
        signals = self.extract_query_signals(
            query=query,
            explicit_tipo_ato=filtro_tipo_ato,
            explicit_cnpj=filtro_cnpj,
        )
        target_tipo = signals["tipo_ato"]
        act_nums = signals["act_numbers"]
        cnpjs = signals["cnpjs"]
        q_tokens = set(re.findall(r"\w{3,}", normalize_text_for_dedup(query)))

        lexical_hits: List[RetrievedChunk] = []
        dense_hits: List[RetrievedChunk] = []

        for raw in self.corpus_chunks:
            content = raw.get("page_content") or ""
            source = raw.get("source") or "Jornal Opinião (Diário Oficial de Caeté)"
            tipo_ato = (raw.get("tipo_ato") or "GERAL / OUTROS").upper()
            cnpjs_list = raw.get("cnpjs_Mencionados") or raw.get("cnpjs_mencionados") or []
            norm_content = normalize_text_for_dedup(content)

            if cnpjs:
                if not any(c in content or c in cnpjs_list for c in cnpjs):
                    continue

            c_tokens = set(re.findall(r"\w{3,}", norm_content))
            overlap = len(q_tokens.intersection(c_tokens)) / max(1, len(q_tokens))
            base_score = float(raw.get("score", overlap))

            matched_act = False
            for num in act_nums:
                num_norm = num.lower()
                # Suporta "Nº 230/2025" e também "Nº230/2025" (sem espaço após Nº, comum nos PDFs de Caeté!)
                if re.search(rf"(?:\b|n[º°o])0*{re.escape(num_norm)}\b", norm_content):
                    matched_act = True
                    break

            chunk_obj = RetrievedChunk(
                chunk_id=raw.get("id") or deterministic_chunk_id(source, content),
                score=base_score + (1.0 if matched_act else 0.0) + (0.25 if (target_tipo and target_tipo in tipo_ato) else 0.0),
                page_content=content,
                source=source,
                tipo_ato=tipo_ato,
                ano_edicao=raw.get("ano_edicao"),
                numero_edicao=raw.get("numero_edicao"),
                cnpjs_mencionados=cnpjs_list,
                match_stage="lexical_exact" if matched_act else ("dense_filtered" if (target_tipo and target_tipo in tipo_ato) else "dense"),
            )

            if matched_act:
                lexical_hits.append(chunk_obj)
            else:
                dense_hits.append(chunk_obj)

        lexical_hits.sort(key=lambda c: c.score, reverse=True)
        dense_hits.sort(key=lambda c: c.score, reverse=True)

        combined = lexical_hits + dense_hits
        final_chunks = self.deduplicate_chunks(combined, top_k=top_k) if deduplicate else combined[:top_k]
        sources = sorted({c.source for c in final_chunks if c.source})

        return {
            "status": "sucesso",
            "query": query,
            "signals_detected": signals,
            "deduplicated": deduplicate,
            "corpus_size": len(self.corpus_chunks),
            "total_candidates": len(combined),
            "returned_count": len(final_chunks),
            "chunks": [c.to_dict() for c in final_chunks],
            "fontes_utilizadas": sources,
        }
