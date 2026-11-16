"""
Conector Padrão `SicomTceMgConnector` (Pacotes ZIP Oficiais do SICOM / TCE-MG).

Cobre universalmente todas as 853 prefeituras mineiras — especialmente aquelas cujos portais locais
usam ERPs fechados ou heterogêneos (`Pronim / GovBR`, `Memory`, `Fiorilli`, `Sonner`, `Pública`, `E&L`),
pois todas são obrigadas por Instrução Normativa do TCE-MG a remeter mensalmente os 65 arquivos
padronizados do SICOM (`YYYY.<ibge7>.<modulo>.<submodulo>.csv`).

Diretrizes estritas:
- JAMAIS tenta burlar ou resolver o CAPTCHA de `sicomconsulta.tce.mg.gov.br`; expõe instruções de
  download assistido para `avaliacao/dados/inbox/` e processa o `.zip` oficial com segurança anti-Zip-Bomb,
  anti-Zip-Slip, trava de código IBGE e vacina `repair_sicom_remuneracao_csv_lines`.
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from sim_platform.ingestion.city_onboarding import (
    is_valid_ibge7,
    repair_sicom_remuneracao_csv_lines,
)
from sim_platform.ingestion.connectors.base import (
    DEFAULT_WORKSPACE_ROOT,
    BaseMunicipalConnector,
    ConnectorProfile,
    ConnectorRunResult,
)
from sim_platform.ingestion.sync_manager import (
    MAX_ZIP_ENTRIES,
    MAX_ZIP_UNCOMPRESSED_BYTES,
    _decode_text_smart,
    _sanitize_table_stem,
)


SUPPORTED_ERP_FAMILIES: Tuple[str, ...] = (
    "Pronim / GovBR",
    "Memory",
    "Fiorilli",
    "Sonner",
    "Pública",
    "E&L Produções de Software",
    "Betha Sistemas",
    "IPM / Ábaco / Próprio",
)


class SicomTceMgConnector(BaseMunicipalConnector):
    """Conector universal para pacotes `.zip` oficiais do Portal de Dados Abertos SICOM/TCE-MG."""

    profile = ConnectorProfile(
        connector_id="sicom_tcemg",
        vendor_family="TCE-MG SICOM Oficial (Pronim, Memory, Fiorilli, Sonner)",
        sync_mode="HYBRID_ASSISTED_ZIP",
        requires_human_captcha=True,
        default_window_id="lote_assistido_sicom_tcemg",
        target_silver_tables=(
            "orgao_orgao",
            "orgao_municipio",
            "pessoa_municipio",
            "remuneracao_municipio",
            "receita_receita",
            "despesa_despesa",
            "empenho_empenho",
            "empenho_credorEmpenho",
            "despesa_liquidacao",
            "despesa_pagamento",
            "contrato_contratos",
            "contrato_contContrato",
            "contrato_termoContrato",
            "licitacao_habLicitacao",
            "licitacao_quadroSocLicitacao",
            "frota_frota",
            "frota_gastoFrota",
        ),
        description=(
            "Ingestão universal de pacotes ZIP oficiais do SICOM/TCE-MG (65 tabelas padronizadas), "
            "atendendo prefeituras Pronim, Memory, Fiorilli e Sonner sem depender de scraper customizado."
        ),
    )

    def __init__(
        self,
        tenant_id: str,
        workspace_root: Path = DEFAULT_WORKSPACE_ROOT,
        tenant_profile: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(
            tenant_id=tenant_id,
            workspace_root=workspace_root,
            tenant_profile=tenant_profile,
        )

    def build_assisted_download_instruction(self, ano_exercicio: int = 2025) -> Dict[str, Any]:
        """Gera instrução operacional auditável para a Fila Assistida Humana (sem automação de CAPTCHA)."""
        return {
            "connector_id": self.profile.connector_id,
            "tenant_id": self.tenant_id,
            "municipality": f"{self.municipality_name}/{self.state_uf}",
            "ibge_code": self.ibge_code,
            "ano_exercicio": int(ano_exercicio),
            "portal_url": "https://sicomconsulta.tce.mg.gov.br/",
            "supported_erps": list(SUPPORTED_ERP_FAMILIES),
            "target_inbox_filename": f"{self.ibge_code}__sicom_{int(ano_exercicio)}.zip",
            "human_step": (
                f"Acesse https://sicomconsulta.tce.mg.gov.br/, selecione '{self.municipality_name}' "
                f"(IBGE {self.ibge_code}, exercício {int(ano_exercicio)}), conclua o CAPTCHA manualmente "
                f"e salve o `.zip` em `avaliacao/dados/inbox/{self.ibge_code}__sicom_{int(ano_exercicio)}.zip`."
            ),
        }

    def inspect_and_sanitize_sicom_zip(self, zip_bytes: bytes) -> Tuple[bytes, Dict[str, Any]]:
        """
        Valida o pacote `.zip` oficial do TCE-MG:
        - Bloqueia Zip-Slip, Zip-Bomb e códigos IBGE pertencentes a outros municípios;
        - Aplica `repair_sicom_remuneracao_csv_lines` automaticamente em `remuneracao_municipio`;
        - Retorna `(sanitized_zip_bytes, audit_metrics)`.
        """
        if not zip_bytes:
            raise ValueError("Pacote ZIP SICOM está vazio.")

        try:
            src_zf = zipfile.ZipFile(io.BytesIO(zip_bytes), "r")
        except zipfile.BadZipFile as exc:
            raise ValueError("Arquivo enviado não é um pacote .zip válido do SICOM/TCE-MG.") from exc

        tables_detected: List[str] = []
        years_detected: Set[int] = set()
        repaired_remuneracao_rows = 0
        out_buf = io.BytesIO()

        with src_zf, zipfile.ZipFile(out_buf, "w", compression=zipfile.ZIP_DEFLATED) as dst_zf:
            infos = [i for i in src_zf.infolist() if not i.is_dir()]
            if not infos:
                raise ValueError("Pacote .zip do SICOM não contém arquivos.")
            if len(infos) > MAX_ZIP_ENTRIES:
                raise ValueError(f"Pacote .zip excede o limite de {MAX_ZIP_ENTRIES} entradas.")

            declared_total = sum(max(0, int(i.file_size)) for i in infos)
            if declared_total > MAX_ZIP_UNCOMPRESSED_BYTES:
                raise ValueError("Pacote .zip do SICOM excede o limite anti-Zip-Bomb (300 MB).")

            streamed_bytes = 0
            for info in infos:
                raw_member = info.filename.replace("\\", "/")
                mp = Path(raw_member)
                if mp.is_absolute() or ".." in mp.parts or raw_member.startswith("/") or ":" in raw_member:
                    raise ValueError(f"Caminho inseguro bloqueado no pacote SICOM (Zip-Slip): '{info.filename}'.")
                if any(part.startswith(".") or part == "__MACOSX" for part in mp.parts):
                    continue
                if mp.suffix.lower() not in (".csv", ".parquet"):
                    continue

                for code7 in re.findall(r"(?<!\d)(\d{7})(?!\d)", raw_member):
                    if is_valid_ibge7(code7) and code7 != self.ibge_code:
                        raise ValueError(
                            f"Bloqueio inter-municipal SICOM: membro '{raw_member}' pertence ao IBGE '{code7}', "
                            f"divergente de '{self.ibge_code}' ({self.municipality_name})."
                        )

                canon_table, yr = _sanitize_table_stem(mp.stem)
                tables_detected.append(canon_table)
                if yr is not None:
                    years_detected.add(yr)

                chunks: List[bytes] = []
                with src_zf.open(info, "r") as fh:
                    while True:
                        b = fh.read(65536)
                        if not b:
                            break
                        streamed_bytes += len(b)
                        if streamed_bytes > MAX_ZIP_UNCOMPRESSED_BYTES:
                            raise ValueError("Pacote .zip excedeu 300 MB durante descompressão (anti-Zip-Bomb).")
                        chunks.append(b)
                member_bytes = b"".join(chunks)

                if mp.suffix.lower() == ".csv" and "remuneracao_municipio" in canon_table.lower():
                    decoded = _decode_text_smart(member_bytes)
                    if ";" in "\n".join(decoded.splitlines()[:3]):
                        repaired_txt, n_rep = repair_sicom_remuneracao_csv_lines(decoded)
                        repaired_remuneracao_rows += int(n_rep)
                        member_bytes = str(repaired_txt).encode("utf-8")

                dst_zf.writestr(mp.name, member_bytes)

        if not tables_detected:
            raise ValueError("Nenhuma tabela CSV/Parquet válida encontrada no pacote SICOM.")

        metrics = {
            "tables_detected_count": len(set(t.lower() for t in tables_detected)),
            "tables_detected": sorted(set(tables_detected)),
            "years_detected": sorted(years_detected),
            "remuneracao_shifted_rows_repaired": repaired_remuneracao_rows,
            "erp_agnostic_standard": "SICOM/TCE-MG Layout Oficial",
        }
        return out_buf.getvalue(), metrics

    def transform_raw_payload(self, raw_payload: Any, **kwargs: Any) -> ConnectorRunResult:
        """
        Aceita `bytes` ou `Path` de um pacote `.zip` oficial do SICOM/TCE-MG (ou `None` para retornar
        a instrução da Fila Assistida pós-CAPTCHA).
        """
        if raw_payload is None:
            ano_ex = int(kwargs.get("ano_referencia") or 2025)
            return ConnectorRunResult(
                connector_id=self.profile.connector_id,
                tenant_id=self.tenant_id,
                ibge_code=self.ibge_code,
                status="pendente_humano",
                audit_metrics={"assisted_queue_item": self.build_assisted_download_instruction(ano_ex)},
            )

        if isinstance(raw_payload, (str, Path)):
            zip_bytes = Path(raw_payload).read_bytes()
        elif isinstance(raw_payload, (bytes, bytearray)):
            zip_bytes = bytes(raw_payload)
        else:
            raise ValueError("`SicomTceMgConnector.transform_raw_payload` espera `bytes`, `Path` ou `None`.")

        sanitized_zip, metrics = self.inspect_and_sanitize_sicom_zip(zip_bytes)
        res = ConnectorRunResult(
            connector_id=self.profile.connector_id,
            tenant_id=self.tenant_id,
            ibge_code=self.ibge_code,
            status="sucesso",
            audit_metrics=metrics,
        )
        res.audit_metrics["_sanitized_zip_bytes_len"] = len(sanitized_zip)
        self._last_sanitized_zip: bytes = sanitized_zip
        return res

    def ingest_sicom_zip(
        self,
        zip_payload: bytes | Path,
        filename: Optional[str] = None,
        sync_manager: Optional[Any] = None,
        db_manager: Optional[Any] = None,
        rag_retriever: Optional[Any] = None,
    ) -> ConnectorRunResult:
        """Valida, higieniza e ingere um pacote `.zip` do SICOM/TCE-MG publicando novo release imutável."""
        res = self.transform_raw_payload(zip_payload)
        if sync_manager is None:
            from sim_platform.ingestion.sync_manager import DataSyncManager

            sync_manager = DataSyncManager(
                workspace_root=self.workspace_root,
                tenant_id=self.tenant_id,
            )
        eff_name = filename or f"{self.ibge_code}__sicom_tcemg_oficial.zip"
        rep = sync_manager.ingest_uploaded_package(
            filename=eff_name,
            content_bytes=self._last_sanitized_zip,
            db_manager=db_manager,
            rag_retriever=rag_retriever,
        )
        res.release_report = rep
        return res
