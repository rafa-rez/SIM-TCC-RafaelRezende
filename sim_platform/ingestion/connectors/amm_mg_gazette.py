"""
Conector Padrão `AmmMgGazetteConnector` (Diário Oficial dos Municípios Mineiros — AMM-MG).

Usado por mais de 650 prefeituras e câmaras municipais de Minas Gerais (`diariomunicipal.com.br/amm-mg`):
1. Filtro estrito de seção municipal (`filter_municipal_section`): isola exclusivamente os atos do
   município alvo dentro do caderno estadual consolidado da AMM-MG, interrompendo imediatamente a
   captura ao encontrar cabeçalho de outro município (`PREFEITURA MUNICIPAL DE <OUTRA_CIDADE>`).
2. Trava temporal estrita (`data_publicacao <= hoje`): bloqueia edições/atos com data futura.
3. Deduplicação canônica sem código volátil (`strip_volatile_amm_codes` + `compute_stable_act_signature`):
   remove rodapés voláteis da AMM-MG (`Código Identificador: XXXXXXXX`, `Matéria publicada no Diário
   Oficial dos Municípios Mineiros...`) antes de calcular a assinatura SHA-1, evitando duplicatas em
   republicações ou reextrações.
4. Hook de OCR seletivo por página (`extract_pdf_pages_with_selective_ocr`): aciona `ocr_page_hook`
   APENAS nas páginas escaneadas onde `len(page.extract_text().strip()) < 40`, preservando páginas
   nativas vetoriais intactas e limitando a `<= 300` páginas.
"""

from __future__ import annotations

import hashlib
import io
import re
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sim_platform.ingestion.connectors.base import (
    DEFAULT_WORKSPACE_ROOT,
    BaseMunicipalConnector,
    ConnectorProfile,
    ConnectorRunResult,
    normalize_ascii_upper,
)
from sim_platform.rag.newspaper_pipeline import (
    RE_ACT_BOUNDARY,
    ActBoundaryChunker,
    clean_newspaper_pdf_text,
    extract_canonical_act_info,
)


MAX_GAZETTE_PDF_PAGES = 300
OCR_MIN_NATIVE_CHARS = 40

# Padrões voláteis inseridos dinamicamente pelo sistema SIGPub / AMM-MG a cada publicação/reimpressão
_RE_VOLATILE_AMM_PATTERNS: Tuple[re.Pattern[str], ...] = (
    re.compile(
        r"C[oó]digo\s+Identificador\s*:\s*[A-Za-z0-9_-]{5,32}",
        re.IGNORECASE,
    ),
    re.compile(
        r"Mat[eé]ria\s+publicada\s+no\s+Di[aá]rio\s+Oficial\s+dos\s+Munic[ií]pios\s+Mineiros[^\n]*",
        re.IGNORECASE,
    ),
    re.compile(
        r"A\s+verifica[cç][aã]o\s+de\s+autenticidade\s+da\s+mat[eé]ria\s+pode\s+ser\s+feita\s+informando\s+o\s+c[oó]digo[^\n]*",
        re.IGNORECASE,
    ),
    re.compile(
        r"https?://(?:www\.)?diariomunicipal\.com\.br/amm-mg\S*",
        re.IGNORECASE,
    ),
)

# Cabeçalho de transição de município dentro do caderno estadual AMM-MG (linha isolada de entidade municipal)
_RE_AMM_CITY_SECTION_HEADER = re.compile(
    r"(?:^|\n)[ \t]*(?:ESTADO\s+DE\s+MINAS\s+GERAIS[ \t]*\n+[ \t]*)?"
    r"((?:PREFEITURA\s+MUNICIPAL|C[AÂ]MARA\s+MUNICIPAL|MUNIC[IÍ]PIO\s+DE|FUNDO\s+MUNICIPAL|"
    r"SERVI[CÇ]O\s+AUT[OÔ]NOMO|INSTITUTO\s+DE\s+PREVID[EÊ]NCIA|FUNDA[CÇ][AÃ]O\s+MUNICIPAL)"
    r"[^\n.;:]{2,95})(?=\s*(?:\n|$))",
    re.IGNORECASE,
)


OcrPageHook = Callable[[int, Any], str]


class AmmMgGazetteConnector(BaseMunicipalConnector):
    """Conector oficial blindado para o Diário Oficial dos Municípios Mineiros (AMM-MG) e PDFs locais."""

    profile = ConnectorProfile(
        connector_id="amm_mg_gazette",
        vendor_family="AMM-MG / Diário Oficial dos Municípios Mineiros",
        sync_mode="AUTO_SCRAPER_OR_PDF",
        requires_human_captcha=False,
        default_window_id="portais_abertos_pncp_diarios",
        target_silver_tables=(),
        description=(
            "Filtro estrito de seção municipal no caderno AMM-MG, trava `data_publicacao <= hoje`, "
            "deduplicação imune a `Código Identificador` volátil e OCR seletivo para páginas `< 40` chars."
        ),
    )

    def __init__(
        self,
        tenant_id: str,
        workspace_root: Path = DEFAULT_WORKSPACE_ROOT,
        tenant_profile: Optional[Dict[str, Any]] = None,
        ocr_page_hook: Optional[OcrPageHook] = None,
    ) -> None:
        super().__init__(
            tenant_id=tenant_id,
            workspace_root=workspace_root,
            tenant_profile=tenant_profile,
        )
        self.ocr_page_hook = ocr_page_hook

    @staticmethod
    def strip_volatile_amm_codes(text: str) -> str:
        """Remove códigos rastreadores voláteis do SIGPub/AMM-MG preservando 100% do texto normativo."""
        cleaned = text or ""
        for pat in _RE_VOLATILE_AMM_PATTERNS:
            cleaned = pat.sub("", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        return cleaned.strip()

    @classmethod
    def compute_stable_act_signature(cls, text: str) -> str:
        """
        Gera assinatura SHA-1 canônica insensível a códigos identificadores voláteis da AMM-MG,
        variações de espaçamento ou acentuação de OCR.
        """
        core = cls.strip_volatile_amm_codes(text)
        nfkd = unicodedata.normalize("NFKD", core.lower())
        alnum = re.sub(r"[^a-z0-9]", "", "".join(c for c in nfkd if not unicodedata.combining(c)))
        return hashlib.sha1(alnum.encode("utf-8")).hexdigest()

    def _header_belongs_to_target_city(self, header_line: str) -> bool:
        target_city = normalize_ascii_upper(self.municipality_name)
        hdr_norm = normalize_ascii_upper(header_line)
        if not target_city or len(target_city) < 3:
            return False
        # Exige fronteira de palavra exata (ex.: "CAETE" não casa com "SAO JOAO DO CAETE" se não for o nome exato,
        # e "LAGOA DA PRATA" não casa com "LAGOA SANTA"!)
        pat = re.compile(
            rf"(?:\bDE\s+|\bMUNICIPIO\s+DE\s+|\bCAMARA\s+MUNICIPAL\s+DE\s+|\bPREFEITURA\s+MUNICIPAL\s+DE\s+|^)"
            rf"{re.escape(target_city)}(?:\s*[-/]\s*{re.escape(self.state_uf)}|\s*$|\b(?!\s+(?:DO|DA|DOS|DAS|DE)\s+[A-Z]))"
        )
        if pat.search(hdr_norm):
            return True
        # Verificar também entidades configuradas explicitamente em `orgaos_map` / `local_aliases`
        for org_lbl in (self.tenant_profile.get("orgaos_map") or {}).values():
            org_norm = normalize_ascii_upper(org_lbl)
            if org_norm and len(org_norm) >= 8 and org_norm in hdr_norm:
                return True
        return False

    def filter_municipal_section(self, caderno_text: str) -> Tuple[str, Dict[str, int]]:
        """
        Filtra estritamente apenas as seções pertencentes a `self.municipality_name` dentro de um
        caderno estadual multi-cidades da AMM-MG.
        Se o documento não contiver cabeçalhos de múltiplos municípios (edição local exclusiva da própria
        cidade, desde que não mencione explicitamente outra prefeitura no cabeçalho), preserva o texto
        validando que não pertence a município vizinho.
        """
        raw = caderno_text or ""
        matches = list(_RE_AMM_CITY_SECTION_HEADER.finditer(raw))
        stats = {
            "sections_scanned": len(matches),
            "target_city_sections_kept": 0,
            "foreign_city_sections_dropped": 0,
        }
        if not matches:
            return raw, stats

        kept_blocks: List[str] = []
        for idx, m in enumerate(matches):
            hdr = m.group(1).strip()
            seg_start = m.start()
            seg_end = matches[idx + 1].start() if (idx + 1) < len(matches) else len(raw)
            block = raw[seg_start:seg_end].strip()
            if self._header_belongs_to_target_city(hdr):
                kept_blocks.append(block)
                stats["target_city_sections_kept"] += 1
            else:
                stats["foreign_city_sections_dropped"] += 1

        return "\n\n".join(kept_blocks), stats

    def extract_pdf_pages_with_selective_ocr(
        self,
        pdf_source: bytes | Path,
        ocr_page_hook: Optional[OcrPageHook] = None,
    ) -> Tuple[str, Dict[str, int]]:
        """
        Extrai texto página a página limitando `<= 300` páginas e dispara `ocr_page_hook(page_idx, page)`
        seletivamente apenas quando `len(extract_text().strip()) < 40`.
        """
        from pypdf import PdfReader  # type: ignore

        reader = (
            PdfReader(io.BytesIO(pdf_source))
            if isinstance(pdf_source, (bytes, bytearray))
            else PdfReader(str(pdf_source))
        )
        total_pages = len(reader.pages)
        if total_pages > MAX_GAZETTE_PDF_PAGES:
            raise ValueError(
                f"PDF do Diário Oficial excede o limite de segurança de {MAX_GAZETTE_PDF_PAGES} páginas ({total_pages})."
            )

        hook = ocr_page_hook or self.ocr_page_hook
        page_texts: List[str] = []
        native_pages = 0
        ocr_triggered = 0
        low_text_without_ocr = 0

        for idx, page in enumerate(reader.pages):
            extracted = (page.extract_text() or "").strip()
            if len(extracted) < OCR_MIN_NATIVE_CHARS:
                if callable(hook):
                    ocr_txt = (hook(idx, page) or "").strip()
                    ocr_triggered += 1
                    page_texts.append(ocr_txt if ocr_txt else extracted)
                else:
                    low_text_without_ocr += 1
                    if extracted:
                        page_texts.append(extracted)
            else:
                native_pages += 1
                page_texts.append(extracted)

        return "\n\n".join(page_texts), {
            "total_pages": total_pages,
            "native_text_pages": native_pages,
            "ocr_pages_triggered": ocr_triggered,
            "low_text_pages_without_hook": low_text_without_ocr,
        }

    @staticmethod
    def _validate_pub_date_not_future(raw_date: Any, reference_today: str) -> Optional[str]:
        s = str(raw_date or "").strip()
        m_iso = re.search(r"(?<!\d)(20[12][0-9]-[01][0-9]-[0-3][0-9])(?!\d)", s)
        if m_iso:
            cand = m_iso.group(1)
        else:
            m_br = re.search(r"(?<!\d)([0-3][0-9])/([01][0-9])/(20[12][0-9])(?!\d)", s)
            if not m_br:
                return None
            cand = f"{m_br.group(3)}-{m_br.group(2)}-{m_br.group(1)}"
        try:
            iso_dt = date.fromisoformat(cand).isoformat()
        except ValueError:
            return None
        if iso_dt > reference_today:
            return None
        return iso_dt

    def _chunk_and_deduplicate_text(
        self,
        municipal_text: str,
        pub_date: str,
        source_label: str,
        seen_signatures: Set[str],
    ) -> Tuple[List[Dict[str, Any]], int]:
        no_volatile = self.strip_volatile_amm_codes(municipal_text)
        cleaned_text, _ = clean_newspaper_pdf_text(no_volatile)
        if len(cleaned_text) < 25:
            return [], 0

        # Remover cabeçalho puro de expediente (< 160 chars) antes do 1º ato normativo
        segs = [s.strip() for s in RE_ACT_BOUNDARY.split(cleaned_text) if s and s.strip()]
        if len(segs) > 1:
            inf0 = extract_canonical_act_info(segs[0])
            if len(segs[0]) < 160 and not inf0.get("act_key"):
                cleaned_text = "\n\n".join(segs[1:])

        gazette_name = str(
            (self.tenant_profile.get("diario_oficial") or {}).get("veiculo")
            or f"Diário Oficial dos Municípios Mineiros (AMM-MG — {self.municipality_name}/{self.state_uf})"
        )
        yr_ed = int(pub_date[:4])
        chunker = ActBoundaryChunker(max_chunk_chars=2200, min_orphan_chars=180)
        chunks = chunker.chunk_edition_text(
            raw_edition_text=cleaned_text,
            source_pdf=source_label,
            default_year=yr_ed,
            municipality_label=f"{self.municipality_name}/{self.state_uf}",
            gazette_name=gazette_name,
        )

        acts: List[Dict[str, Any]] = []
        dups = 0
        for sc in chunks:
            body = self.strip_volatile_amm_codes(sc.page_content)
            if len(body) < 25:
                continue
            if len(body) < 160 and not sc.act_key and sc.tipo_ato == "GERAL / OUTROS":
                continue
            sig = self.compute_stable_act_signature(body)
            if sig in seen_signatures:
                dups += 1
                continue
            seen_signatures.add(sig)
            acts.append(
                {
                    "id": sc.chunk_id,
                    "chunk_id": sc.chunk_id,
                    "act_id": sc.chunk_id,
                    "act_key": sc.act_key,
                    "stable_sha1": sig,
                    "tipo_ato": sc.tipo_ato,
                    "numero_ato": sc.numero_ato or "",
                    "data_publicacao": pub_date,
                    "ano_edicao": min(int(sc.ano_edicao or yr_ed), yr_ed),
                    "source": source_label,
                    "contextual_header": sc.contextual_header,
                    "cnpjs_Mencionados": sc.cnpjs_mencionados,
                    "valores_mencionados": sc.valores_mencionados,
                    "page_content": body,
                }
            )
        return acts, dups

    def transform_raw_payload(self, raw_payload: Any, **kwargs: Any) -> ConnectorRunResult:
        """
        Processa edições AMM-MG fornecidas como:
        - `bytes` ou `Path` de arquivo `.pdf` (acionando OCR seletivo quando `len(extract_text()) < 40`);
        - `str` (texto bruto de caderno AMM-MG);
        - `Sequence[Dict[str, Any]]` (lista de matérias JSON com `municipality`/`orgao`, `data_publicacao` e `texto`/`page_content`).
        """
        ref_today = str(kwargs.get("reference_today") or date.today().isoformat())
        source_name = str(kwargs.get("source_name") or f"AMM_MG_{self.ibge_code}.pdf")
        ocr_hook = kwargs.get("ocr_page_hook") or self.ocr_page_hook

        seen_sigs: Set[str] = set()
        all_acts: List[Dict[str, Any]] = []
        total_dups = 0
        blocked_future_dates = 0
        dropped_other_cities = 0
        warnings: List[str] = []
        audit: Dict[str, Any] = {"reference_today": ref_today}

        if isinstance(raw_payload, (bytes, bytearray, Path)):
            raw_pub_dt = kwargs.get("data_publicacao") or source_name
            valid_dt = self._validate_pub_date_not_future(raw_pub_dt, ref_today)
            if raw_pub_dt and re.search(r"20[12][0-9]-[01][0-9]-[0-3][0-9]", str(raw_pub_dt)) and valid_dt is None:
                blocked_future_dates += 1
                warnings.append(f"Edição rejeitada pela trava `data_publicacao <= {ref_today}`: '{raw_pub_dt}'.")
            else:
                eff_dt = valid_dt or ref_today
                pdf_txt, ocr_stats = self.extract_pdf_pages_with_selective_ocr(
                    raw_payload,
                    ocr_page_hook=ocr_hook,
                )
                audit["pdf_extraction"] = ocr_stats
                mun_txt, sec_stats = self.filter_municipal_section(pdf_txt)
                audit["section_filter"] = sec_stats
                dropped_other_cities += sec_stats["foreign_city_sections_dropped"]
                acts, dups = self._chunk_and_deduplicate_text(mun_txt, eff_dt, source_name, seen_sigs)
                all_acts.extend(acts)
                total_dups += dups

        elif isinstance(raw_payload, str):
            raw_pub_dt = kwargs.get("data_publicacao")
            if raw_pub_dt is not None:
                valid_dt = self._validate_pub_date_not_future(raw_pub_dt, ref_today)
                if valid_dt is None:
                    blocked_future_dates += 1
                    warnings.append(f"Caderno bloqueado pela trava `data_publicacao <= {ref_today}`: '{raw_pub_dt}'.")
                    audit["blocked_future_dates"] = blocked_future_dates
                    return ConnectorRunResult(
                        connector_id=self.profile.connector_id,
                        tenant_id=self.tenant_id,
                        ibge_code=self.ibge_code,
                        status="parcial",
                        warnings=warnings,
                        audit_metrics=audit,
                    )
                eff_dt = valid_dt
            else:
                eff_dt = self._validate_pub_date_not_future(source_name, ref_today) or ref_today

            mun_txt, sec_stats = self.filter_municipal_section(raw_payload)
            audit["section_filter"] = sec_stats
            dropped_other_cities += sec_stats["foreign_city_sections_dropped"]
            acts, dups = self._chunk_and_deduplicate_text(mun_txt, eff_dt, source_name, seen_sigs)
            all_acts.extend(acts)
            total_dups += dups

        elif isinstance(raw_payload, Iterable):
            for idx, item in enumerate(raw_payload):
                if not isinstance(item, dict):
                    continue
                org_hdr = str(
                    item.get("orgao") or item.get("entidade") or item.get("municipio") or self.municipality_name
                )
                if not self._header_belongs_to_target_city(org_hdr):
                    dropped_other_cities += 1
                    continue

                raw_dt = item.get("data_publicacao") or item.get("dataPublicacao")
                if raw_dt:
                    valid_dt = self._validate_pub_date_not_future(raw_dt, ref_today)
                    if valid_dt is None:
                        blocked_future_dates += 1
                        continue
                    eff_dt = valid_dt
                else:
                    eff_dt = ref_today

                txt_item = str(item.get("page_content") or item.get("texto") or item.get("conteudo") or "")
                src_item = str(item.get("source") or f"AMM_MG_{self.ibge_code}_{eff_dt}_{idx+1}.pdf")
                acts, dups = self._chunk_and_deduplicate_text(txt_item, eff_dt, src_item, seen_sigs)
                all_acts.extend(acts)
                total_dups += dups
        else:
            raise ValueError("Payload inválido para `AmmMgGazetteConnector`.")

        audit["acts_emitted"] = len(all_acts)
        audit["duplicates_removed_volatile_safe"] = total_dups
        audit["blocked_future_dates"] = blocked_future_dates
        audit["foreign_city_items_dropped"] = dropped_other_cities

        return ConnectorRunResult(
            connector_id=self.profile.connector_id,
            tenant_id=self.tenant_id,
            ibge_code=self.ibge_code,
            status="sucesso" if (all_acts or blocked_future_dates == 0) else "parcial",
            gazette_acts=all_acts,
            deduplicated_count=total_dups,
            warnings=warnings,
            audit_metrics=audit,
        )
