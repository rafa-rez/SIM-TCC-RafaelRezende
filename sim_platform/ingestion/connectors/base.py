"""
 Primitivas Base, Contratos Tipados e Síntese Determinística de Chaves (`deterministic_seq_id`)
 para Conectores de Portais Municipais de Minas Gerais (SIM v4.5).
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import unicodedata
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

from sim_platform.ingestion.city_onboarding import is_valid_ibge7, load_tenant_profile


DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
MAX_SAFE_52BIT_MASK = (1 << 52) - 1


def deterministic_seq_id(*parts: Any) -> int:
    """
    Gera um identificador inteiro positivo determinístico de 52 bits (`1 .. 4_503_599_627_370_495`)
    a partir de `*parts` via SHA-256.

    Propriedades:
    - 100% estável entre execuções, máquinas e versões do Python (sem `hash()` aleatorizado).
    - Cabe com folga em `BIGINT` do DuckDB e em `Number.MAX_SAFE_INTEGER` (`2^53 - 1`) do JSON/JavaScript.
    - Nunca retorna `0` ou `-1` (reservado pelo SICOM para Restos a Pagar).
    """
    norm_parts: List[str] = []
    for p in parts:
        if p is None:
            norm_parts.append("")
        else:
            norm_parts.append(re.sub(r"\s+", " ", str(p).strip()))
    canonical_seed = "::".join(norm_parts).encode("utf-8")
    digest = hashlib.sha256(canonical_seed).digest()
    val52 = int.from_bytes(digest[:8], byteorder="big", signed=False) & MAX_SAFE_52BIT_MASK
    return val52 if val52 >= 2 else (val52 + 2)


def digits_only(val: Any) -> str:
    """Extrai exclusivamente dígitos ASCII `0-9`."""
    return re.sub(r"[^0-9]", "", str(val or ""))


def normalize_ascii_upper(val: Any) -> str:
    """Normaliza string para comparação canônica (sem acentos, maiúscula e espaços simples)."""
    raw = str(val or "").strip()
    nfkd = unicodedata.normalize("NFKD", raw)
    no_acc = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", no_acc).upper().strip()


def parse_ptbr_money(val: Any, default: float = 0.0) -> float:
    """
    Converte com segurança valores monetários PT-BR (`'R$ 1.234,56'`, `'1.234,56'`, `'-450,20'`)
    ou ISO (`1234.56`, `Decimal('1234.56')`) em `float` arredondado a 2 casas decimais.
    """
    if val is None or isinstance(val, bool):
        return default
    if isinstance(val, (int, float)):
        return round(float(val), 2)
    if isinstance(val, Decimal):
        return round(float(val), 2)

    s = str(val).strip()
    if not s:
        return default
    s = re.sub(r"^[Rr]\$\s*", "", s).strip()
    # Tratar parênteses contábeis negativos: "(1.234,50)" -> "-1.234,50"
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1].strip()
    s = s.replace(" ", "")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif re.match(r"^[+-]?\d{1,3}(\.\d{3}){2,}$", s):
        s = s.replace(".", "")
    try:
        return round(float(Decimal(s)), 2)
    except (InvalidOperation, ValueError):
        return default


def parse_date_int_yyyymmdd(val: Any) -> Optional[int]:
    """Converte datas `'YYYY-MM-DD'` ou `'DD/MM/YYYY'` no inteiro `YYYYMMDD` padrão SICOM."""
    s = str(val or "").strip()
    if not s:
        return None
    m_iso = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m_iso:
        return int(f"{m_iso.group(1)}{m_iso.group(2)}{m_iso.group(3)}")
    m_br = re.match(r"^(\d{2})/(\d{2})/(\d{4})", s)
    if m_br:
        return int(f"{m_br.group(3)}{m_br.group(2)}{m_br.group(1)}")
    d_only = digits_only(s)
    if len(d_only) == 8 and d_only[:4].isdigit() and 1990 <= int(d_only[:4]) <= 2100:
        return int(d_only)
    return None


@dataclass(frozen=True)
class ConnectorProfile:
    """Metadados arquiteturais e operacionais de um conector padrão da frota MG."""

    connector_id: str
    vendor_family: str
    sync_mode: str
    requires_human_captcha: bool
    default_window_id: str
    target_silver_tables: Tuple[str, ...]
    description: str = ""


@dataclass
class ConnectorRunResult:
    """Resultado auditável da execução/transformação de um conector municipal."""

    connector_id: str
    tenant_id: str
    ibge_code: str
    status: str = "sucesso"
    tables_normalized: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)
    gazette_acts: List[Dict[str, Any]] = field(default_factory=list)
    deduplicated_count: int = 0
    warnings: List[str] = field(default_factory=list)
    audit_metrics: Dict[str, Any] = field(default_factory=dict)
    release_report: Optional[Dict[str, Any]] = None
    executed_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    @property
    def total_rows_emitted(self) -> int:
        return sum(len(rows) for rows in self.tables_normalized.values())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "connector_id": self.connector_id,
            "tenant_id": self.tenant_id,
            "ibge_code": self.ibge_code,
            "status": self.status,
            "tables_row_counts": {k: len(v) for k, v in self.tables_normalized.items()},
            "total_rows_emitted": self.total_rows_emitted,
            "gazette_acts_count": len(self.gazette_acts),
            "deduplicated_count": self.deduplicated_count,
            "warnings": list(self.warnings),
            "audit_metrics": dict(self.audit_metrics),
            "release_report": self.release_report,
            "executed_at": self.executed_at,
        }


class BaseMunicipalConnector(ABC):
    """
    Classe base tipada para todos os conectores municipais de Minas Gerais.
    Padroniza validação de IBGE, conversão de tabelas Silver em Parquet ZSTD em memória
    e publicação via `DataSyncManager.ingest_uploaded_package` (releases imutáveis `CURRENT.json`).
    """

    profile: ConnectorProfile

    def __init__(
        self,
        tenant_id: str,
        workspace_root: Path = DEFAULT_WORKSPACE_ROOT,
        tenant_profile: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.workspace_root = workspace_root
        self.tenant_id = str(tenant_id).strip()
        self.tenant_profile = tenant_profile or load_tenant_profile(
            tenant_id=self.tenant_id,
            workspace_root=workspace_root,
        )
        self.ibge_code = str(self.tenant_profile.get("ibge_code") or "").strip()
        if not is_valid_ibge7(self.ibge_code):
            raise ValueError(
                f"Conector '{self.profile.connector_id}' exige código IBGE oficial de 7 dígitos; "
                f"recebido '{self.ibge_code}' para '{self.tenant_id}'."
            )
        self.municipality_name = str(self.tenant_profile.get("municipality_name") or self.tenant_id).strip()
        self.state_uf = str(self.tenant_profile.get("state_uf") or "MG").upper().strip()

    @abstractmethod
    def transform_raw_payload(self, raw_payload: Any, **kwargs: Any) -> ConnectorRunResult:
        """Transforma dados brutos do portal/pacote em tabelas Silver normalizadas e/ou atos RAG."""

    def build_upload_zip_bytes(self, run_result: ConnectorRunResult) -> bytes:
        """
        Empacota `run_result.tables_normalized` (em Parquet ZSTD via DuckDB) e `run_result.gazette_acts`
        em um `.zip` em memória prefixado com `{ibge_code}` para ingestão blindada no `DataSyncManager`.
        """
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            if run_result.tables_normalized:
                import tempfile

                with tempfile.TemporaryDirectory(prefix="sim_conn_pq_") as tmp_d:
                    tmp_dir = Path(tmp_d)
                    con = duckdb.connect(":memory:")
                    try:
                        con.execute("SET memory_limit = '256MB';")
                        con.execute("SET threads = 2;")
                        for tbl_name, rows in sorted(run_result.tables_normalized.items()):
                            if not rows:
                                continue
                            json_tmp = tmp_dir / f"{tbl_name}.json"
                            pq_tmp = tmp_dir / f"{tbl_name}.parquet"
                            json_tmp.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
                            con.execute(
                                f"COPY (SELECT * FROM read_json_auto('{json_tmp.as_posix()}')) "
                                f"TO '{pq_tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                            )
                            zf.write(
                                pq_tmp,
                                arcname=f"{tbl_name}.parquet",
                                compress_type=zipfile.ZIP_STORED,
                            )
                    finally:
                        con.close()

            if run_result.gazette_acts:
                acts_bytes = json.dumps(run_result.gazette_acts, ensure_ascii=False, indent=2).encode("utf-8")
                zf.writestr(
                    f"{self.ibge_code}__diario_oficial_{self.profile.connector_id}.json",
                    acts_bytes,
                )
        return buf.getvalue()

    def stage_and_ingest(
        self,
        run_result: ConnectorRunResult,
        sync_manager: Optional[Any] = None,
        db_manager: Optional[Any] = None,
        rag_retriever: Optional[Any] = None,
    ) -> ConnectorRunResult:
        """Publica o `ConnectorRunResult` num novo release imutável via `DataSyncManager`."""
        if not run_result.tables_normalized and not run_result.gazette_acts:
            return run_result

        if sync_manager is None:
            from sim_platform.ingestion.sync_manager import DataSyncManager

            sync_manager = DataSyncManager(
                workspace_root=self.workspace_root,
                tenant_id=self.tenant_id,
            )

        zip_bytes = self.build_upload_zip_bytes(run_result)
        pkg_name = f"{self.ibge_code}__{self.profile.connector_id}_bundle.zip"
        rep = sync_manager.ingest_uploaded_package(
            filename=pkg_name,
            content_bytes=zip_bytes,
            db_manager=db_manager,
            rag_retriever=rag_retriever,
        )
        run_result.release_report = rep
        return run_result
