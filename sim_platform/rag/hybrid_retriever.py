"""
Recuperador Híbrido (BM25 Okapi com IDF + Lexical por Número de Ato e CNPJ + Cross-Linker SQL↔RAG)
com Deduplicação Determinística e Filtro de Metadados.

Resolve o achado crítico FIND-004 e as evoluções de IA/RAG v3.0:
- Em produção (wpp-tiokk-sql.json), o nó Qdrant do n8n executava apenas busca densa simples,
  ignorava `filtro_tipo_ato` e `filtro_cnpj` e sofria com 47,9% de chunks duplicados (Recall@5 = 3,12% para atos numerados).
- Promove a recuperação híbrida (`--hybrid-act`, `--dedup`, `--filtro-tipo-ato`) para o núcleo da plataforma,
  substituindo a sobreposição rasa de palavras por **Okapi BM25 ($k_1=1.5, b=0.75$) com IDF** e remoção de stopwords,
  além de oferecer `search_by_entities()` para cruzamento automático SQL ↔ Diário Oficial (*Jornal Opinião*).
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
import uuid
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


NAMESPACE_SIM_RAG = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]

PT_STOPWORDS: Set[str] = {
    "que", "para", "com", "por", "uma", "dos", "das", "nos", "nas", "pelo", "pela",
    "pelos", "pelas", "aos", "esta", "este", "esse", "essa", "isso", "isto", "como",
    "mais", "mas", "foi", "ser", "sao", "sua", "seu", "suas", "seus", "qual", "quais",
    "sobre", "entre", "ate", "apos", "sem", "sob", "municipio", "caete", "prefeitura",
    "municipal", "estado", "minas", "gerais", "art", "artigo", "data", "publicacao",
}

ATO_TYPE_KEYWORDS: List[Tuple[str, List[str]]] = [
    ("PORTARIA", ["portaria"]),
    ("DECRETO", ["decreto"]),
    ("LEI", ["lei complementar", "lei municipal", "lei n", "lei nº", "projeto de lei"]),
    ("TERMO ADITIVO", ["termo aditivo", "aditivo contratual", "aditamento"]),
    ("EXTRATO CONTRATUAL", ["extrato de contrato", "extrato contratual", "termo de fomento", "termo de colaboracao"]),
    ("EDITAL / LICITAÇÃO", ["edital", "aviso de licitacao", "pregao", "concorrencia", "dispensa", "inexigibilidade"]),
]

RE_ACT_NUMBER = re.compile(
    r"(?:n[º°o\.]\s*|(?:lei|decreto|portaria|edital|resolu[cç][aã]o|aditivo|contrato|dispensa|inexigibilidade)\s+(?:municipal\s+)?(?:n[º°o\.]?\s*)?)(\d{1,5}(?:\.\d{3})*(?:/\d{2,4})?)",
    re.IGNORECASE,
)

RE_CNPJ = re.compile(r"\b\d{2}\.?\d{3}\.?\d{3}/\d{4}-\d{2}\b")

RE_ACT_HEADER_CANON = re.compile(
    r"\b(DECRETO|LEI COMPLEMENTAR|LEI|PORTARIA|EDITAL|RESOLU[CÇ][AÃ]O|EXTRATO DE CONTRATO|TERMO ADITIVO)\s+(?:MUNICIPAL\s+)?"
    r"(?:N[ºO°\.]?\s*\.?\s*)?(\d{1,5}(?:\.\d{3})*)\s*/\s*(\d{2,4})",
    re.IGNORECASE,
)


def normalize_text_for_dedup(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    clean = "".join(c for c in nfkd if not unicodedata.combining(c))
    clean = re.sub(r"([a-z]{2,})-\s+([a-z]{2,})", r"\1\2", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def tokenize_bm25(text: str) -> List[str]:
    norm = normalize_text_for_dedup(text)
    tokens = re.findall(r"[a-z0-9]{3,}", norm)
    return [t for t in tokens if t not in PT_STOPWORDS]


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
    parte_m = re.search(r"\[[^\]]*PARTE\s+(\d+/\d+)\]", (text or "")[:140], re.IGNORECASE)
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
    e separação de atos colados via `sim_platform.rag.newspaper_pipeline`.
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
    match_stage: str = "bm25_hybrid"  # "lexical_exact" | "bm25_filtered" | "bm25_hybrid"

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
    Motor de recuperação híbrida para o Diário Oficial (*Jornal Opinião*) com BM25 Okapi + IDF,
    boosting determinístico de número de ato/CNPJ e Cross-Linker SQL↔RAG.
    """

    def __init__(self, corpus_chunks: Optional[List[Dict[str, Any]]] = None, k1: float = 1.5, b: float = 0.75) -> None:
        if corpus_chunks is not None:
            self.corpus_chunks = corpus_chunks
        else:
            self.corpus_chunks = load_real_caete_rag_corpus()
        self.k1 = k1
        self.b = b
        self._doc_freqs: Dict[str, int] = {}
        self._doc_lens: List[int] = []
        self._doc_tfs: List[Counter] = []
        self._doc_norms: List[str] = []
        self._avgdl: float = 1.0
        self._build_bm25_index()

    def _build_bm25_index(self) -> None:
        self._doc_freqs.clear()
        self._doc_lens.clear()
        self._doc_tfs.clear()
        self._doc_norms.clear()

        for raw in self.corpus_chunks:
            content = raw.get("page_content") or ""
            norm = normalize_text_for_dedup(content)
            self._doc_norms.append(norm)
            tokens = tokenize_bm25(content)
            tf = Counter(tokens)
            self._doc_tfs.append(tf)
            self._doc_lens.append(len(tokens))
            for term in tf.keys():
                self._doc_freqs[term] = self._doc_freqs.get(term, 0) + 1

        self._avgdl = (sum(self._doc_lens) / len(self._doc_lens)) if self._doc_lens else 1.0

    def _bm25_score(self, query_tokens: List[str], doc_idx: int) -> float:
        if not query_tokens or doc_idx >= len(self._doc_tfs):
            return 0.0
        n_docs = len(self._doc_tfs)
        tf_map = self._doc_tfs[doc_idx]
        dl = max(1, self._doc_lens[doc_idx])
        score = 0.0
        for term in query_tokens:
            f = tf_map.get(term, 0)
            if f == 0:
                continue
            df = self._doc_freqs.get(term, 0)
            idf = math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))
            denom = f + self.k1 * (1.0 - self.b + self.b * (dl / self._avgdl))
            score += idf * ((f * (self.k1 + 1.0)) / denom)
        # Normaliza para faixa [0.0, 1.0] aproximada para combinar com boosts de ato/CNPJ
        return round(min(1.0, score / max(3.0, len(query_tokens) * 1.8)), 4)

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
            if val not in ("2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026"):
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
        q_tokens = tokenize_bm25(query)

        lexical_hits: List[RetrievedChunk] = []
        dense_hits: List[RetrievedChunk] = []

        for idx, raw in enumerate(self.corpus_chunks):
            content = raw.get("page_content") or ""
            source = raw.get("source") or "Jornal Opinião (Diário Oficial de Caeté)"
            tipo_ato = (raw.get("tipo_ato") or "GERAL / OUTROS").upper()
            cnpjs_list = raw.get("cnpjs_Mencionados") or raw.get("cnpjs_mencionados") or []
            norm_content = self._doc_norms[idx] if idx < len(self._doc_norms) else normalize_text_for_dedup(content)

            if cnpjs:
                cnpjs_digits = [re.sub(r"\D", "", c) for c in cnpjs]
                content_digits = re.sub(r"\D", "", content)
                if not any(c in content or c in cnpjs_list or (cd and cd in content_digits) for c, cd in zip(cnpjs, cnpjs_digits)):
                    continue

            bm25_val = self._bm25_score(q_tokens, idx)
            base_score = float(raw.get("score", bm25_val)) if "score" in raw else bm25_val

            matched_act = False
            for num in act_nums:
                num_norm = num.lower()
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
                match_stage="lexical_exact" if matched_act else ("bm25_filtered" if (target_tipo and target_tipo in tipo_ato) else "bm25_hybrid"),
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

    def search_by_entities(
        self,
        entity_names: Optional[List[str]] = None,
        cnpjs: Optional[List[str]] = None,
        act_numbers: Optional[List[str]] = None,
        top_k: int = 3,
    ) -> List[Dict[str, Any]]:
        """
        Cross-Linker SQL -> RAG: busca atos normativos no *Jornal Opinião* que mencionem
        os CNPJs, razões sociais ou números de contrato/decreto retornados pelo resultado SQL.
        """
        entity_names = [e.strip() for e in (entity_names or []) if e and len(e.strip()) >= 4]
        cnpjs_clean = [re.sub(r"\D", "", c) for c in (cnpjs or []) if c and len(re.sub(r"\D", "", c)) >= 11]
        act_numbers = [str(a).strip() for a in (act_numbers or []) if a]

        if not entity_names and not cnpjs_clean and not act_numbers:
            return []

        hits: List[RetrievedChunk] = []
        for idx, raw in enumerate(self.corpus_chunks):
            content = raw.get("page_content") or ""
            norm_content = self._doc_norms[idx] if idx < len(self._doc_norms) else normalize_text_for_dedup(content)
            content_digits = re.sub(r"\D", "", content)
            score = 0.0

            # 1. Match por CNPJ (altíssima precisão)
            for cd in cnpjs_clean:
                if cd in content_digits:
                    score += 1.5

            # 2. Match por número de ato/contrato/decreto
            for anum in act_numbers:
                if re.search(rf"(?:\b|n[º°o])0*{re.escape(anum.lower())}\b", norm_content):
                    score += 1.2

            # 3. Match por razão social / nome de credor (tokens distintivos >= 4 letras)
            for ename in entity_names[:3]:
                e_tokens = [
                    t for t in tokenize_bm25(ename)
                    if len(t) >= 4 and t not in ("ltda", "eireli", "me", "epp", "servicos", "comercio", "construtora", "engenharia", "empresa", "associacao")
                ]
                if e_tokens:
                    matched_toks = sum(1 for t in e_tokens if t in norm_content)
                    if matched_toks == len(e_tokens) or (len(e_tokens) >= 2 and matched_toks >= 2):
                        score += 0.9 * (matched_toks / len(e_tokens))

            if score >= 0.6:
                source = raw.get("source") or "Jornal Opinião (Diário Oficial de Caeté)"
                hits.append(
                    RetrievedChunk(
                        chunk_id=raw.get("id") or deterministic_chunk_id(source, content),
                        score=score,
                        page_content=content,
                        source=source,
                        tipo_ato=(raw.get("tipo_ato") or "GERAL / OUTROS").upper(),
                        ano_edicao=raw.get("ano_edicao"),
                        numero_edicao=raw.get("numero_edicao"),
                        cnpjs_mencionados=raw.get("cnpjs_Mencionados") or [],
                        match_stage="sql_rag_crosslink",
                    )
                )

        hits.sort(key=lambda c: c.score, reverse=True)
        deduped = self.deduplicate_chunks(hits, top_k=top_k)
        return [c.to_dict() for c in deduped]
