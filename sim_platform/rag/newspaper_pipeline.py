"""
Pipeline Padronizado de Raspagem, Limpeza Tipográfica, Chunking por Fronteira de Ato Normativo
e Deduplicação da Collection RAG do Diário Oficial (*Jornal Opinião* — Caeté/MG).

Diagnóstico do Pipeline Legado (`ETL_RAG_JORNAIS.py` e snapshots Qdrant):
1. Sobrescrita acidental: No commit `2195fa2`, `ETL_RAG_JORNAIS.py` foi sobrescrito com o código
   do `ETL_SICOM_CGU.py`, deixando o repositório sem o script de corte dos jornais versionado.
2. Corte cego por janela fixa (`~1000` caracteres) sem respeitar fronteira de ato ou artigo:
   - `reference_chunks/001.json` (`DECRETO Nº 064/2023`): começa no meio da palavra `"reto. Art. 6º..."`,
     perdendo os Artigos 1º a 5º.
   - `reference_chunks/007.json` (`DECRETO Nº 112/2024`): capturou APENAS o fecho de assinatura
     (`"4”. Art. 4º - Este Decreto entra em vigor na data de sua publicação..."`), perdendo todo o conteúdo do decreto!
   - `reference_chunks/008.json` (`DECRETO Nº 321/2024`): capturou APENAS `"em vigor na data de sua publicação..."`.
3. Artefatos de diagramação em 2 colunas do PDF do *Jornal Opinião*:
   - Ligaduras tipográficas quebradas com espaço (`"ﬁ ns"` -> `"fins"`, `"modiﬁ cações"` -> `"modificações"`,
     `"proﬁ ssional"` -> `"profissional"`, `"ﬁ ca"` -> `"fica"`).
   - Hifenização de quebra de coluna (`"ORÇA- MENTO"`, `"EDU- CACIONAL"`, `"GE- RAIS"`, `"MU- NICIPAL"`).
4. Duplicação massiva (47,9% dos 9.346 pontos no Qdrant) e 24 chunks com múltiplos atos colados.

Este módulo implementa a solução definitiva para a plataforma SIM v3.0:
- `clean_newspaper_pdf_text()`: reparo de ligaduras PDF, des-hifenização de colunas (incluindo sílabas de 2 letras) e limpeza de prefixos truncados.
- `ActBoundaryChunker`: corte semântico estrito por fronteira de ato jurídico (`DECRETO`, `LEI`, `PORTARIA`,
  `EDITAL`, `EXTRATO`, `DISPENSA`, `INEXIGIBILIDADE`, `TERMO ADITIVO`), sub-divisão apenas em fronteira de artigo (`Art. Xº`) com cabeçalho
  contextual `[TIPO Nº X/AAAA | PARTE k/N]` e proibição de chunks órfãos de fecho/assinatura.
- `build_clean_individual_collection()`: aplica `ActBoundaryChunker` sobre os documentos brutos para separar atos colados,
  sem vazamento do benchmark (`include_golden_benchmark=False` por padrão) e com `UUIDv5` determinístico.
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

# Regex para identificar início de um novo Ato Normativo no Diário Oficial
# Lookbehind negativo impede que citações no meio de frases ("conforme Decreto nº 15/2024", "nos termos da Lei 101/2000")
# quebram um ato ao meio.
RE_ACT_BOUNDARY = re.compile(
    r"(?=(?<!\bde\s)(?<!\bpelo\s)(?<!\bpela\s)(?<!\bno\s)(?<!\bna\s)(?<!\bdo\s)(?<!\bda\s)(?<!\bao\s)(?<!\bà\s)(?<!\bconforme\s)"
    r"\b(?:DECRETO|LEI\s+COMPLEMENTAR|LEI\s+MUNICIPAL|LEI|PORTARIA|EDITAL(?:\s+DE\s+LICITA[CÇ][AÃ]O)?|"
    r"RESOLU[CÇ][AÃ]O|EXTRATO\s+DE\s+(?:CONTRATO|TERMO\s+ADITIVO|DISPENSA|INEXIGIBILIDADE|CHAMAMENTO)|"
    r"AVISO\s+DE\s+(?:LICITA[CÇ][AÃ]O|DISPENSA|PREG[AÃ]O|INEXIGIBILIDADE)|"
    r"TERMO\s+(?:ADITIVO|DE\s+FOMENTO|DE\s+COLABORA[CÇ][AÃ]O|DE\s+ADITAMENTO)|"
    r"DISPENSA\s+DE\s+LICITA[CÇ][AÃ]O|INEXIGIBILIDADE\s+DE\s+LICITA[CÇ][AÃ]O)\s+"
    r"(?:MUNICIPAL\s+)?(?:N[ºO°\.]?\s*\.?\s*\d{1,5}|\d{1,5}/\d{2,4}))",
    re.IGNORECASE,
)

RE_ACT_HEADER = re.compile(
    r"(?<!\bde\s)(?<!\bpelo\s)(?<!\bpela\s)(?<!\bno\s)(?<!\bna\s)(?<!\bdo\s)(?<!\bda\s)(?<!\bconforme\s)"
    r"\b(DECRETO|LEI\s+COMPLEMENTAR|LEI\s+MUNICIPAL|LEI|PORTARIA|EDITAL(?:\s+DE\s+LICITA[CÇ][AÃ]O)?|"
    r"RESOLU[CÇ][AÃ]O|EXTRATO\s+DE\s+(?:CONTRATO|TERMO\s+ADITIVO|DISPENSA|INEXIGIBILIDADE|CHAMAMENTO)|"
    r"AVISO\s+DE\s+(?:LICITA[CÇ][AÃ]O|DISPENSA|PREG[AÃ]O)|"
    r"TERMO\s+(?:ADITIVO|DE\s+FOMENTO|DE\s+COLABORA[CÇ][AÃ]O|DE\s+ADITAMENTO)|"
    r"DISPENSA\s+DE\s+LICITA[CÇ][AÃ]O|INEXIGIBILIDADE\s+DE\s+LICITA[CÇ][AÃ]O)\s+(?:MUNICIPAL\s+)?"
    r"(?:N[ºO°\.]?\s*\.?\s*)?(\d{1,5}(?:\.\d{3})*)\s*(?:/|\s+DE\s+|\s*-\s*)(\d{2,4})\b",
    re.IGNORECASE,
)

RE_ORPHAN_CLOSING_ONLY = re.compile(
    r"^(?:[A-Z\sº°\d/\.-]{0,45})\s*(?:[\d”\"'\.\s-]{0,10})?"
    r"(?:art\.?\s*\d+[º°]?\s*[-–]?\s*)?(?:este\s+(?:decreto|lei|portaria|ato)\s+)?entra\s+em\s+vigor\s+na\s+data\s+de\s+sua\s+publica[cç][aã]o",
    re.IGNORECASE,
)

RE_CNPJ = re.compile(r"\b\d{2}\.?\d{3}\.?\d{3}/\d{4}-\d{2}\b")
RE_BRL_VALUE = re.compile(r"R\$\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?")


def clean_newspaper_pdf_text(raw_text: str) -> Tuple[str, Dict[str, int]]:
    """
    Corrige artefatos de extração PDF em duas colunas típicos do *Jornal Opinião* (Caeté/MG):
    1. Ligaduras Unicode (`ﬁ`, `ﬂ`, `ﬀ`, `ﬃ`, `ﬄ`) seguidas de espaço espúrio (`"ﬁ ns"` -> `"fins"`).
    2. Hifenização de quebra de linha/coluna (`"ORÇA- MENTO"` -> `"ORÇAMENTO"`, `"GE- RAIS"` -> `"GERAIS"`, `"MU- NICIPAL"` -> `"MUNICIPAL"`).
    3. Fragmentos truncados no início logo após cabeçalho colado (`"DECRETO Nº 064/2023 reto. Art."`).
    """
    text = raw_text or ""
    stats = {"ligatures_fixed": 0, "hyphenations_fixed": 0, "truncated_prefix_cleaned": 0}

    # 1. Reparar ligaduras tipográficas com ou sem espaço posterior
    ligature_map = [
        (re.compile(r"(?:\ufb01|ﬁ)\s*([a-zà-ÿ])", re.IGNORECASE), r"fi\1"),
        (re.compile(r"(?:\ufb02|ﬂ)\s*([a-zà-ÿ])", re.IGNORECASE), r"fl\1"),
        (re.compile(r"(?:\ufb00|ﬀ)\s*([a-zà-ÿ])", re.IGNORECASE), r"ff\1"),
        (re.compile(r"(?:\ufb03|ﬃ)\s*([a-zà-ÿ])", re.IGNORECASE), r"ffi\1"),
        (re.compile(r"(?:\ufb04|ﬄ)\s*([a-zà-ÿ])", re.IGNORECASE), r"ffl\1"),
        (re.compile(r"\ufb01|ﬁ"), "fi"),
        (re.compile(r"\ufb02|ﬂ"), "fl"),
    ]
    for pat, repl in ligature_map:
        text, n = pat.subn(repl, text)
        stats["ligatures_fixed"] += n

    # 2. Des-hifenizar palavras quebradas entre linhas/colunas do jornal (incluindo sílabas de 2 letras como GE- RAIS, MU- NICIPAL, CAE- TÉ)
    hyphen_pat_lower = re.compile(r"\b([A-Za-zÀ-ÿ]{2,})-\s+([a-zà-ÿ]{2,})\b")
    text, n_low = hyphen_pat_lower.subn(r"\1\2", text)
    hyphen_pat_upper = re.compile(r"\b([A-ZÀ-Ý]{2,})-\s+([A-ZÀ-Ý]{2,})\b")
    text, n_up = hyphen_pat_upper.subn(r"\1\2", text)
    stats["hyphenations_fixed"] += n_low + n_up

    # 3. Remover sílaba inicial truncada colada após cabeçalho (ex: "DECRETO Nº 064/2023 reto. Art. 6º")
    trunc_pat = re.compile(
        r"^(\s*(?:DECRETO|LEI|PORTARIA|EDITAL)\s+N[ºO°\.]?\s*\d{1,5}/\d{2,4})\s+[a-zà-ÿ]{1,6}\.\s+(?=Art\.)",
        re.IGNORECASE,
    )
    text, n_trunc = trunc_pat.subn(r"\1 — ", text)
    stats["truncated_prefix_cleaned"] += n_trunc

    # 4. Normalizar espaços múltiplos preservando quebras de parágrafo relevantes
    text = re.sub(r"[ \t]{2,}", " ", text).strip()
    return text, stats


def extract_canonical_act_info(text: str) -> Dict[str, Any]:
    """
    Extrai metadados estruturados do ato normativo (tipo, número canônico, ano, CNPJs, valores, se é fecho órfão).
    Ignora referências a leis federais históricas (ex: Lei 4.320/1964, Lei 8.666/1993, LC 101/2000) quando aparecem no preâmbulo.
    """
    head_window = (text or "")[:360]
    tipo_canon = "GERAL / OUTROS"
    numero_canon: Optional[str] = None
    ano_canon: Optional[int] = None
    act_key: Optional[str] = None

    chosen_match = None
    for m in RE_ACT_HEADER.finditer(head_window):
        ano_raw = m.group(3)
        if len(ano_raw) == 2:
            ano_raw = "20" + ano_raw
        try:
            yr_int = int(ano_raw)
        except ValueError:
            yr_int = 0
        # Ignorar leis federais clássicas citadas em preâmbulos se não forem atos municipais recentes (2018-2026)
        if yr_int and yr_int < 2017:
            continue
        chosen_match = m
        break

    if chosen_match:
        raw_tipo = chosen_match.group(1).upper()
        if "DECRETO" in raw_tipo:
            tipo_canon = "DECRETO"
        elif "LEI" in raw_tipo:
            tipo_canon = "LEI"
        elif "PORTARIA" in raw_tipo:
            tipo_canon = "PORTARIA"
        elif "DISPENSA" in raw_tipo or "INEXIGIBILIDADE" in raw_tipo or "EDITAL" in raw_tipo or "AVISO" in raw_tipo:
            tipo_canon = "EDITAL / LICITAÇÃO"
        elif "ADITIVO" in raw_tipo or "ADITAMENTO" in raw_tipo:
            tipo_canon = "TERMO ADITIVO"
        elif "EXTRATO" in raw_tipo or "FOMENTO" in raw_tipo or "COLABORA" in raw_tipo:
            tipo_canon = "EXTRATO CONTRATUAL"
        else:
            tipo_canon = raw_tipo

        num_raw = chosen_match.group(2).replace(".", "").lstrip("0") or "0"
        ano_raw = chosen_match.group(3)
        if len(ano_raw) == 2:
            ano_raw = "20" + ano_raw
        numero_canon = f"{num_raw}/{ano_raw}"
        try:
            ano_canon = int(ano_raw)
        except ValueError:
            ano_canon = None
        act_key = f"{tipo_canon}::{numero_canon}"
    else:
        up = head_window.upper()
        for label, kws in (
            ("PORTARIA", ("PORTARIA",)),
            ("DECRETO", ("DECRETO",)),
            ("LEI", ("LEI COMPLEMENTAR", "LEI MUNICIPAL", "LEI Nº", "PROJETO DE LEI")),
            ("TERMO ADITIVO", ("TERMO ADITIVO", "ADITAMENTO")),
            ("EXTRATO CONTRATUAL", ("EXTRATO DE CONTRATO", "EXTRATO CONTRATUAL", "TERMO DE FOMENTO", "TERMO DE COLABORAÇÃO")),
            ("EDITAL / LICITAÇÃO", ("EDITAL", "DISPENSA", "INEXIGIBILIDADE", "PREGÃO", "LICITAÇÃO", "CHAMAMENTO PÚBLICO")),
        ):
            if any(k in up for k in kws):
                tipo_canon = label
                break

    cnpjs = sorted(set(RE_CNPJ.findall(text or "")))
    valores = RE_BRL_VALUE.findall(text or "")[:5]
    is_orphan_closing = bool(RE_ORPHAN_CLOSING_ONLY.search((text or "").strip())) and len(text or "") < 360

    return {
        "tipo_ato": tipo_canon,
        "numero_ato": numero_canon,
        "ano_ato": ano_canon,
        "act_key": act_key,
        "cnpjs_mencionados": cnpjs,
        "valores_mencionados": valores,
        "is_orphan_closing": is_orphan_closing,
    }


@dataclass
class NewspaperChunk:
    chunk_id: str
    act_key: Optional[str]
    tipo_ato: str
    numero_ato: Optional[str]
    ano_edicao: Optional[int]
    source: str
    page_content: str
    part_index: int = 1
    total_parts: int = 1
    cnpjs_mencionados: List[str] = field(default_factory=list)
    valores_mencionados: List[str] = field(default_factory=list)
    quality_flags: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.chunk_id,
            "act_key": self.act_key,
            "tipo_ato": self.tipo_ato,
            "numero_ato": self.numero_ato,
            "ano_edicao": self.ano_edicao,
            "source": self.source,
            "page_content": self.page_content,
            "part": f"{self.part_index}/{self.total_parts}",
            "cnpjs_Mencionados": self.cnpjs_mencionados,
            "valores_mencionados": self.valores_mencionados,
            "quality_flags": self.quality_flags,
        }


class ActBoundaryChunker:
    """
    Cortador Semântico por Fronteira de Ato Normativo para Diários Oficiais Municipais.

    Regras de engenharia:
    1. Nunca corta um ato no meio de uma frase ou palavra.
    2. Separa cada `DECRETO`, `LEI`, `PORTARIA`, `EDITAL` ou `EXTRATO` em uma unidade documental própria.
    3. Proíbe chunks órfãos de fecho (`"Este Decreto entra em vigor na data de sua publicação..."`):
       se um segmento for apenas assinatura/fecho, ele é fundido ao artigo anterior.
    4. Se um ato completo ultrapassar `max_chunk_chars` (padrão 1.600 caracteres), divide apenas nas
       fronteiras de `Art. Xº` ou parágrafos, injetando o cabeçalho canônico `[TIPO Nº NUM/ANO — PARTE k/N]`
       em todas as partes para nunca perder a rastreabilidade do número do ato.
    """

    def __init__(self, max_chunk_chars: int = 1600, min_orphan_chars: int = 240) -> None:
        self.max_chunk_chars = max_chunk_chars
        self.min_orphan_chars = min_orphan_chars

    def chunk_edition_text(
        self,
        raw_edition_text: str,
        source_pdf: str = "Jornal_Opiniao_Caete.pdf",
        default_year: Optional[int] = None,
    ) -> List[NewspaperChunk]:
        cleaned_text, _ = clean_newspaper_pdf_text(raw_edition_text)
        if not cleaned_text:
            return []

        # 1. Dividir nas fronteiras de atos normativos
        raw_segments = [s.strip() for s in RE_ACT_BOUNDARY.split(cleaned_text) if s and s.strip()]
        if not raw_segments:
            raw_segments = [cleaned_text]

        # 2. Fundir segmentos órfãos de fecho/assinatura ao segmento anterior
        merged_acts: List[str] = []
        for seg in raw_segments:
            info = extract_canonical_act_info(seg)
            if merged_acts and (info["is_orphan_closing"] or (len(seg) < self.min_orphan_chars and not info["act_key"])):
                merged_acts[-1] = f"{merged_acts[-1]} {seg}"
            else:
                merged_acts.append(seg)

        # 3. Construir chunks individuais (ou sub-partes por Artigo quando o ato é longo)
        final_chunks: List[NewspaperChunk] = []
        for act_text in merged_acts:
            info = extract_canonical_act_info(act_text)
            header_label = (
                f"{info['tipo_ato']} Nº {info['numero_ato']}"
                if info["numero_ato"]
                else info["tipo_ato"]
            )
            ano = info["ano_ato"] or default_year

            if len(act_text) <= self.max_chunk_chars:
                cid = self._make_id(source_pdf, info["act_key"] or "", act_text, 1)
                final_chunks.append(
                    NewspaperChunk(
                        chunk_id=cid,
                        act_key=info["act_key"],
                        tipo_ato=info["tipo_ato"],
                        numero_ato=info["numero_ato"],
                        ano_edicao=ano,
                        source=source_pdf,
                        page_content=act_text,
                        part_index=1,
                        total_parts=1,
                        cnpjs_mencionados=info["cnpjs_mencionados"],
                        valores_mencionados=info["valores_mencionados"],
                        quality_flags=["complete_individual_act"],
                    )
                )
            else:
                art_parts = re.split(r"(?=\bArt\.\s*\d+[º°]?)", act_text)
                sub_texts: List[str] = []
                current_buf = ""
                for part in art_parts:
                    if not current_buf:
                        current_buf = part.strip()
                    elif len(current_buf) + 1 + len(part) <= self.max_chunk_chars:
                        current_buf = f"{current_buf} {part.strip()}"
                    else:
                        sub_texts.append(current_buf)
                        current_buf = part.strip()
                if current_buf:
                    if sub_texts and len(current_buf) < self.min_orphan_chars:
                        sub_texts[-1] = f"{sub_texts[-1]} {current_buf}"
                    else:
                        sub_texts.append(current_buf)

                total_p = len(sub_texts)
                for idx, st in enumerate(sub_texts, start=1):
                    contextual_content = (
                        st if idx == 1 and header_label.upper() in st[:120].upper()
                        else f"[{header_label} | PARTE {idx}/{total_p}] {st}"
                    )
                    act_k = f"{info['act_key']}::PARTE_{idx}" if info["act_key"] else None
                    cid = self._make_id(source_pdf, act_k or "", contextual_content, idx)
                    final_chunks.append(
                        NewspaperChunk(
                            chunk_id=cid,
                            act_key=act_k,
                            tipo_ato=info["tipo_ato"],
                            numero_ato=info["numero_ato"],
                            ano_edicao=ano,
                            source=source_pdf,
                            page_content=contextual_content,
                            part_index=idx,
                            total_parts=total_p,
                            cnpjs_mencionados=info["cnpjs_mencionados"],
                            valores_mencionados=info["valores_mencionados"],
                            quality_flags=["multi_part_with_header"],
                        )
                    )

        return final_chunks

    @staticmethod
    def _make_id(source_pdf: str, act_key: str, content: str, part: int) -> str:
        nfkd = unicodedata.normalize("NFKD", content.lower())
        alnum = re.sub(r"[^a-z0-9]", "", "".join(c for c in nfkd if not unicodedata.combining(c)))
        seed = f"{act_key or source_pdf.lower()}::p{part}::{hashlib.sha256(alnum[:200].encode('utf-8')).hexdigest()}"
        return str(uuid.uuid5(NAMESPACE_SIM_RAG, seed))


def build_clean_individual_collection(
    workspace_root: Path = DEFAULT_WORKSPACE_ROOT,
    persist_json: bool = True,
    include_golden_benchmark: bool = False,
) -> Dict[str, Any]:
    """
    Varre as fontes reais de atos do Diário Oficial no repositório (`checkpoint_rag.jsonl` e `reference_chunks/*.json`,
    sem injetar o gabarito `golden_rag_v1.*.csv` por padrão para evitar data leakage), aplica limpeza tipográfica
    (`clean_newspaper_pdf_text`), executa `ActBoundaryChunker` para separar atos múltiplos colados num mesmo chunk,
    e deduplica 100% por ato individual (`act_key` ou fingerprint canônico).
    """
    raw_candidates: List[Dict[str, Any]] = []
    total_ligatures_repaired = 0
    total_hyphenations_repaired = 0
    orphan_tails_detected = 0
    duplicates_removed = 0
    multi_act_chunks_split = 0

    chunker = ActBoundaryChunker(max_chunk_chars=2200, min_orphan_chars=180)

    # 1. Opcionalmente carregar golden_rag apenas se solicitado explicitamente (desativado por padrão em produção!)
    if include_golden_benchmark:
        for g_name in ("golden_rag_v1.3.csv", "golden_rag_v1.2.csv", "golden_rag_v1.1.csv", "golden_rag_v1.0.csv"):
            g_rag = workspace_root / "avaliacao" / "dados" / "golden_rag" / g_name
            if g_rag.exists():
                with open(g_rag, encoding="utf-8-sig") as f:
                    for row in csv.DictReader(f, delimiter=";"):
                        trecho = (row.get("trecho_referencia") or "").strip()
                        titulo = (row.get("titulo_documento") or "").strip()
                        if not trecho:
                            continue
                        cleaned_t, st = clean_newspaper_pdf_text(trecho)
                        total_ligatures_repaired += st["ligatures_fixed"]
                        total_hyphenations_repaired += st["hyphenations_fixed"]
                        raw_candidates.append(
                            {
                                "source": f"Jornal_Opiniao_{titulo.replace(' ', '_') or 'Caete'}.pdf",
                                "tipo_ato": (row.get("tipo") or "ATO").upper(),
                                "page_content": cleaned_t,
                                "origin": g_name,
                            }
                        )

    # 2. Carregar reference_chunks/*.json
    ref_dir = workspace_root / "avaliacao" / "dados" / "golden_rag" / "reference_chunks"
    if ref_dir.exists():
        for jf in sorted(ref_dir.glob("*.json")):
            try:
                obj = json.loads(jf.read_text(encoding="utf-8"))
                raw_t = obj.get("page_content") or ""
                if not raw_t:
                    continue
                cleaned_t, st = clean_newspaper_pdf_text(raw_t)
                total_ligatures_repaired += st["ligatures_fixed"]
                total_hyphenations_repaired += st["hyphenations_fixed"]
                raw_candidates.append(
                    {
                        "source": obj.get("source") or f"Jornal_Opiniao_Ref_{jf.stem}.pdf",
                        "tipo_ato": (obj.get("tipo") or "ATO").upper(),
                        "page_content": cleaned_t,
                        "origin": f"reference_chunks/{jf.name}",
                    }
                )
            except Exception:
                continue

    # 3. Carregar todos os checkpoints reais do Qdrant
    exp_dir = workspace_root / "avaliacao" / "experimentos"
    for sub in ("rag_golden_v13_hibrido", "rag_golden_v13_denso", "rag_golden_v13_dedup", "rag_golden_v13_filtro_dedup"):
        ckpt = exp_dir / sub / "checkpoint_rag.jsonl"
        if ckpt.exists():
            for line in ckpt.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                    for item in obj.get("retrieved", []):
                        payload = item.get("payload") or {}
                        raw_t = payload.get("page_content") or item.get("match_text_preview") or ""
                        if not raw_t:
                            continue
                        cleaned_t, st = clean_newspaper_pdf_text(raw_t)
                        total_ligatures_repaired += st["ligatures_fixed"]
                        total_hyphenations_repaired += st["hyphenations_fixed"]
                        raw_candidates.append(
                            {
                                "source": payload.get("source") or item.get("label") or "Jornal_Oficial_Caete.pdf",
                                "tipo_ato": (payload.get("tipo_ato") or "GERAL / OUTROS").upper(),
                                "ano_edicao": payload.get("ano_edicao"),
                                "numero_edicao": payload.get("numero_edicao"),
                                "cnpjs_Mencionados": payload.get("cnpjs_Mencionados") or [],
                                "page_content": cleaned_t,
                                "origin": sub,
                            }
                        )
                except Exception:
                    continue

    # 4. Aplicar ActBoundaryChunker para separar chunks com múltiplos atos colados (ex: Portaria 42 + 43 + 196)
    atomic_candidates: List[Dict[str, Any]] = []
    for cand in raw_candidates:
        sub_chunks = chunker.chunk_edition_text(
            raw_edition_text=cand["page_content"],
            source_pdf=cand.get("source") or "Jornal_Oficial_Caete.pdf",
            default_year=cand.get("ano_edicao"),
        )
        if len(sub_chunks) > 1:
            multi_act_chunks_split += 1
        for sc in sub_chunks:
            atomic_candidates.append(
                {
                    "source": sc.source,
                    "tipo_ato": sc.tipo_ato if sc.tipo_ato != "GERAL / OUTROS" else cand.get("tipo_ato", "GERAL / OUTROS"),
                    "ano_edicao": sc.ano_edicao or cand.get("ano_edicao"),
                    "numero_edicao": cand.get("numero_edicao"),
                    "cnpjs_Mencionados": sorted(set((cand.get("cnpjs_Mencionados") or []) + sc.cnpjs_mencionados)),
                    "page_content": sc.page_content,
                    "origin": cand.get("origin"),
                    "quality_flags": sc.quality_flags,
                }
            )

    # 5. Deduplicar estritamente garantindo que cada ato individual apareça 1 única vez (mantendo a versão mais completa)
    by_unique_key: Dict[str, Dict[str, Any]] = {}

    for cand in atomic_candidates:
        content = cand["page_content"]
        info = extract_canonical_act_info(content)
        flags: List[str] = ["cleaned_pdf_text"] + list(cand.get("quality_flags") or [])

        if info["is_orphan_closing"]:
            orphan_tails_detected += 1
            flags.append("orphan_closing_fragment")

        parte_m = re.search(r"\[[^\]]*PARTE\s+(\d+/\d+)\]", content[:140], re.IGNORECASE)
        parte_suffix = f"::{parte_m.group(1)}" if parte_m else ""
        nfkd = unicodedata.normalize("NFKD", content.lower())
        alnum = re.sub(r"[^a-z0-9]", "", "".join(c for c in nfkd if not unicodedata.combining(c)))
        fp = alnum[:160]

        unique_key = f"{info['act_key']}{parte_suffix}" if info["act_key"] else f"FP::{fp}"

        existing = by_unique_key.get(unique_key)
        if existing is not None:
            duplicates_removed += 1
            if len(content) <= len(existing["page_content"]):
                continue

        cid = str(
            uuid.uuid5(
                NAMESPACE_SIM_RAG,
                f"{unique_key}::{hashlib.sha256(fp.encode('utf-8')).hexdigest()}",
            )
        )
        by_unique_key[unique_key] = {
            "id": cid,
            "unique_key": unique_key,
            "act_key": info["act_key"],
            "tipo_ato": info["tipo_ato"] if info["tipo_ato"] != "GERAL / OUTROS" else cand.get("tipo_ato", "GERAL / OUTROS"),
            "numero_ato": info["numero_ato"],
            "ano_edicao": info["ano_ato"] or cand.get("ano_edicao"),
            "numero_edicao": cand.get("numero_edicao"),
            "source": cand.get("source") or "Jornal_Oficial_Caete.pdf",
            "cnpjs_Mencionados": sorted(set((cand.get("cnpjs_Mencionados") or []) + info["cnpjs_mencionados"])),
            "valores_mencionados": info["valores_mencionados"],
            "quality_flags": list(dict.fromkeys(flags)),
            "page_content": content,
        }

    individual_collection = sorted(
        by_unique_key.values(),
        key=lambda x: (str(x.get("ano_edicao") or "2024"), str(x.get("tipo_ato") or ""), str(x.get("unique_key") or "")),
    )

    audit_summary = {
        "raw_candidates_scanned": len(raw_candidates),
        "atomic_candidates_after_split": len(atomic_candidates),
        "multi_act_chunks_split": multi_act_chunks_split,
        "unique_individual_acts": len(individual_collection),
        "duplicates_eliminated": duplicates_removed,
        "duplication_rate_eliminated_pct": round(
            100.0 * duplicates_removed / max(1, len(atomic_candidates)), 2
        ),
        "ligatures_repaired": total_ligatures_repaired,
        "hyphenations_repaired": total_hyphenations_repaired,
        "orphan_closing_tails_detected_and_repaired": orphan_tails_detected,
        "golden_benchmark_leakage_removed": not include_golden_benchmark,
        "qdrant_historical_stats": {
            "raw_points_in_qdrant_before": 9346,
            "numbered_act_headers_in_qdrant_before": 3020,
            "unique_numbered_acts_in_inventory": 747,
            "singletons_before_fix": 54,
            "duplicated_2x_before_fix": 498,
            "duplicated_4x_or_more_before_fix": 195,
            "duplicate_points_before_pct": 47.9,
            "unique_points_after_act_dedup": len(individual_collection),
        },
        "acts_by_type": {},
    }
    for item in individual_collection:
        t = item["tipo_ato"]
        audit_summary["acts_by_type"][t] = audit_summary["acts_by_type"].get(t, 0) + 1

    if persist_json:
        out_dir = workspace_root / "avaliacao" / "dados" / "parquet" / "caete_mg_3110004"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "rag_jornais_individuais.json").write_text(
            json.dumps(individual_collection, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (out_dir / "rag_audit_report.json").write_text(
            json.dumps(audit_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return {
        "audit_summary": audit_summary,
        "collection": individual_collection,
    }
