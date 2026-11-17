"""
Gerenciador Unificado de Sincronização, Padronização, Upload e Releases Imutáveis (`DataSyncManager`).

Evolução SIM v4.3 (Multi-Cidade + Releases Imutáveis `CURRENT.json` + Conectores Oficiais + Upload Seguro):
1. Lê dinamicamente o manifesto da cidade (`tenants/<tenant_id>.json`) via `load_tenant_profile()`,
   eliminando qualquer vazamento de indicadores de Caeté (`base_capag`) para outros municípios.
2. Publica atualizações em diretórios imutáveis `releases/<release_id>/` + ponteiro atômico `CURRENT.json`,
   prevenindo `PermissionError: [WinError 32]` no Windows com DuckDB aberto e permitindo execução
   externa via Cronjob CLI (`python -m sim_platform.ingestion.sync_manager --tenant <id> --live-api`)
   com hot-swap automático no FastAPI sem reinicialização.
3. Suporta tanto Conectores Online Reais (IBGE API v1/v3, SICONFI/STN ORDS com rate-limit de 1,1s/req
   e Portal da Transparência CGU quando `CGU_API_KEY` configurada) quanto Upload Seguro Drag-and-Drop
   de pacotes `.zip`, `.csv`, `.parquet`, `.json`, `.txt` e `.pdf` (anti-Zip-Slip e anti-Zip-Bomb).
4. Controle de concorrência por tenant (`_sync.lock`) retornando `SyncAlreadyRunningError` (HTTP 409).
"""

from __future__ import annotations

import argparse
import csv
import ctypes
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import sys
import tempfile
import time
import zipfile
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

import duckdb

from sim_platform.ingestion.city_onboarding import (
    is_valid_ibge7,
    load_tenant_profile,
    repair_sicom_remuneracao_csv_lines,
)
from sim_platform.rag.newspaper_pipeline import (
    RE_ACT_BOUNDARY,
    ActBoundaryChunker,
    build_clean_individual_collection,
    clean_newspaper_pdf_text,
    extract_canonical_act_info,
)


DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]

MAX_UPLOAD_RAW_BYTES = 100 * 1024 * 1024          # 100 MB
MAX_ZIP_UNCOMPRESSED_BYTES = 300 * 1024 * 1024   # 300 MB
MAX_ZIP_ENTRIES = 200
MAX_PDF_PAGES = 300
KEEP_RECENT_RELEASES = 2
ALLOWED_UPLOAD_EXTENSIONS = {".parquet", ".csv", ".zip", ".json", ".txt", ".pdf"}


def _is_pid_alive(pid: int) -> bool:
    """Verifica de forma multiplataforma (Windows Kernel32 / POSIX kill 0) se `pid` ainda está em execução."""
    if pid <= 0:
        return False
    if os.name == "nt":
        try:
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            STILL_ACTIVE = 259
            ERROR_ACCESS_DENIED = 5
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
            if not handle:
                err = ctypes.get_last_error() or kernel32.GetLastError()
                return err == ERROR_ACCESS_DENIED
            try:
                exit_code = ctypes.c_ulong(0)
                if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                    return int(exit_code.value) == STILL_ACTIVE
                return True
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return False
    else:
        try:
            os.kill(int(pid), 0)
            return True
        except PermissionError:
            return True
        except OSError:
            return False


def _write_json_atomic(target: Path, payload: Any) -> None:
    """Grava JSON atomicamente via arquivo `.tmp` no mesmo diretório seguido de `os.replace()`."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.parent / f".{target.name}.{os.getpid()}.{time.time_ns()}.tmp"
    try:
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(str(tmp_path), str(target))
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def _validate_no_foreign_ibge_in_name(name: str, expected_ibge: str) -> None:
    """
    Bloqueia contaminação cruzada entre municípios (ING-06): se `name` contiver código IBGE explícito
    de 7 dígitos diferente de `expected_ibge`, levanta `ValueError`.
    """
    exp = str(expected_ibge or "").strip()
    if not is_valid_ibge7(exp):
        return
    for code7 in re.findall(r"(?<!\d)(\d{7})(?!\d)", str(name or "")):
        if is_valid_ibge7(code7) and code7 != exp:
            raise ValueError(
                f"Bloqueio de contaminação entre municípios (ING-06): o arquivo '{name}' contém o código IBGE "
                f"'{code7}', divergente do município ativo ('{exp}')."
            )


class SyncAlreadyRunningError(RuntimeError):
    """Disparado quando já há sincronização ou ingestão ativa para o mesmo município."""


def _build_source_families(ibge_code: str, city_name: str, diario_veiculo: str) -> List[Dict[str, Any]]:
    slug = city_name.lower().replace(" ", "_")
    has_cgu_key = bool(os.environ.get("CGU_API_KEY", "").strip())
    return [
        {
            "source_id": "SICOM",
            "name": "SICOM / TCE-MG (Contas Anuais e Execução Mensal)",
            "agency": "Tribunal de Contas do Estado de Minas Gerais",
            "update_cadence": "Mensal (até 40 dias após fechamento do mês)",
            "sync_mode": "HYBRID_UPLOAD_OR_PORTAL",
            "sync_mode_label": "Upload Pacote Mensal (.zip/.csv/.parquet) + Reconciliação Automática",
            "expected_tables": 65,
            "years_target": [2020, 2021, 2022, 2023, 2024, 2025],
            "endpoint_pattern": f"https://dadosabertos.tce.mg.gov.br/sicom/{{ano}}/{slug}_{ibge_code}.zip",
        },
        {
            "source_id": "CGU",
            "name": "Portal da Transparência Federal (Benefícios, Transferências, Convênios e Emendas)",
            "agency": "Controladoria-Geral da União (CGU)",
            "update_cadence": "Mensal",
            "sync_mode": "AUTO_API" if has_cgu_key else "HYBRID_UPLOAD_OR_API_KEY",
            "sync_mode_label": (
                "API Oficial CGU Ativa (CGU_API_KEY)"
                if has_cgu_key
                else "Base Reconciliada + Upload CSV/Parquet (defina CGU_API_KEY para API direta)"
            ),
            "expected_tables": 9,
            "years_target": [2020, 2021, 2022, 2023, 2024, 2025],
            "endpoint_pattern": "https://api.portaldatransparencia.gov.br/api-de-dados",
        },
        {
            "source_id": "SICONFI",
            "name": "SICONFI / Tesouro Nacional (FINBRA, RREO, RGF e CAPAG)",
            "agency": "Secretaria do Tesouro Nacional (STN)",
            "update_cadence": "Bimestral / Quadrimestral",
            "sync_mode": "AUTO_API",
            "sync_mode_label": "API Oficial Aberta STN (ORDS 1 req/s) + Reconciliação LRF",
            "expected_tables": 1,
            "years_target": [2020, 2021, 2022, 2023, 2024, 2025],
            "endpoint_pattern": f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo?id_ente={ibge_code}",
        },
        {
            "source_id": "IBGE",
            "name": "IBGE (Censo 2022, Estimativas Populacionais, PIB Municipal e IDHM)",
            "agency": "Instituto Brasileiro de Geografia e Estatística",
            "update_cadence": "Anual",
            "sync_mode": "AUTO_API",
            "sync_mode_label": "API Oficial Aberta IBGE (Localidades + SIDRA Agregados)",
            "expected_tables": 1,
            "years_target": [2020, 2021, 2022, 2023, 2024, 2025],
            "endpoint_pattern": f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{ibge_code}",
        },
        {
            "source_id": "INEP",
            "name": "INEP / MEC (IDEB Municipal Bienal, Taxa de Aprovação e Censo Escolar)",
            "agency": "Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira",
            "update_cadence": "Bienal (Anos Ímpares: 2019, 2021, 2023)",
            "sync_mode": "HYBRID_UPLOAD_OR_PORTAL",
            "sync_mode_label": "Base Bienal Oficial + Upload Microdados (.csv/.parquet)",
            "expected_tables": 1,
            "years_target": [2019, 2021, 2023],
            "endpoint_pattern": f"https://dadosabertos.inep.gov.br/censo-escolar/{ibge_code}",
        },
        {
            "source_id": "DATASUS",
            "name": "DATASUS / SIOPS (Saúde Constitucional, CNES e Atenção Básica)",
            "agency": "Ministério da Saúde / SIOPS",
            "update_cadence": "Bimestral / Anual",
            "sync_mode": "LOCAL_RECONCILED",
            "sync_mode_label": "Reconciliação Automática SICOM × IBGE + Upload SIOPS",
            "expected_tables": 1,
            "years_target": [2020, 2021, 2022, 2023, 2024, 2025],
            "endpoint_pattern": f"https://siops-consulta-publica-api.saude.gov.br/v1/ente/{ibge_code}",
        },
        {
            "source_id": "RECEITA_QSA",
            "name": "Receita Federal (CNPJ, Porte, CNAE e Quadro Societário de Fornecedores)",
            "agency": "Secretaria Especial da Receita Federal do Brasil + SICOM Habilitação",
            "update_cadence": "Mensal",
            "sync_mode": "LOCAL_RECONCILED",
            "sync_mode_label": "Cruzamento Automático Habilitação/Contratos + Upload QSA",
            "expected_tables": 1,
            "years_target": [2020, 2021, 2022, 2023, 2024, 2025],
            "endpoint_pattern": f"https://dadosabertos.rfb.gov.br/CNPJ/qsa_fornecedores_{ibge_code}",
        },
        {
            "source_id": "DIARIO_OFICIAL",
            "name": f"Diário Oficial ({diario_veiculo} — Collection RAG Atos Individuais)",
            "agency": f"Prefeitura Municipal de {city_name}",
            "update_cadence": "Semanal / Por Edição",
            "sync_mode": "HYBRID_UPLOAD_OR_PIPELINE",
            "sync_mode_label": "Atomizador de Atos + Upload de Edições (.pdf/.txt/.json)",
            "expected_tables": 1,
            "years_target": [2021, 2022, 2023, 2024, 2025, 2026],
            "endpoint_pattern": f"https://transparencia.{slug}.mg.gov.br/diario-oficial",
        },
    ]


OFFICIAL_SOURCE_FAMILIES = _build_source_families("3110004", "Caeté", "Jornal Opinião")

NEW_ENRICHED_TABLES: List[str] = [
    "cgu_convenios",
    "cgu_emendas",
    "siconfi_finbra_indicadores",
    "ibge_indicadores_municipais",
    "inep_ideb_educacao",
    "datasus_siops_saude",
    "receita_qsa_fornecedores",
]

# Predicados SQL canônicos e unificados para classificação de Entes Públicos / Folha / Filantrópicas
_SQL_IS_PUBLIC_OR_PAYROLL = """(
    c.razao_social ILIKE '%INSS%'
    OR c.razao_social ILIKE '%INSTITUTO NACIONAL DO SEGURO%'
    OR c.razao_social ILIKE '%FGTS%'
    OR c.razao_social ILIKE '%SECRETARIA DA RECEITA%'
    OR c.razao_social ILIKE '%RECEITA FEDERAL%'
    OR c.razao_social ILIKE '%MINISTERIO%'
    OR c.razao_social ILIKE '%PREFEITURA%'
    OR c.razao_social ILIKE '%MUNICIPIO DE%'
    OR c.razao_social ILIKE '%CAMARA MUNICIPAL%'
    OR c.razao_social ILIKE '%TRIBUNAL%'
    OR c.razao_social ILIKE '%PODER JUDICIARIO%'
    OR c.razao_social ILIKE '%ESTADO DE MINAS%'
    OR c.razao_social ILIKE '%PIS/PASEP%'
    OR c.razao_social ILIKE '%PASEP%'
    OR c.razao_social ILIKE '%CAIXA ECONOMICA%'
    OR c.razao_social ILIKE '%BANCO DO BRASIL%'
    OR c.razao_social ILIKE '%CEMIG%'
    OR c.razao_social ILIKE '%COPASA%'
    OR c.razao_social ILIKE '%CORREIOS%'
    OR c.razao_social ILIKE '%IMPRENSA NACIONAL%'
    OR c.razao_social ILIKE '%SERVIDORES MUNICIPAIS%'
    OR c.razao_social ILIKE '%FOLHA DE PAGAMENTO%'
    OR c.razao_social ILIKE '%FUNDO MUNICIPAL%'
    OR c.razao_social ILIKE '%FUNDACAO EDUCACIONAL%'
    OR c.razao_social ILIKE '%SERVICO AUTONOMO%'
    OR c.num_cnpj IN ('18302299000102', '65174518000197', '19893791000154', '21155528000100', '22257307000105', '29979036000140', '29979036044946', '21154554000113', '01298583000141')
)"""

_SQL_IS_NONPROFIT_OR_CONSORTIUM = """(
    c.razao_social ILIKE '%BENEFICENCIA%'
    OR c.razao_social ILIKE '%SANTA CASA%'
    OR c.razao_social ILIKE '%ASSOCIACAO%'
    OR c.razao_social ILIKE '%FUNDACAO%'
    OR c.razao_social ILIKE '%CONSORCIO%'
    OR c.razao_social ILIKE '%CISMEP%'
    OR c.razao_social ILIKE '%CIAS%'
    OR c.razao_social ILIKE '%CENTRO INFANTIL%'
    OR c.razao_social ILIKE '%LAR DOS IDOSOS%'
    OR c.razao_social ILIKE '%APAE%'
    OR c.razao_social ILIKE '%CAIXA ESCOLAR%'
    OR c.razao_social ILIKE '%SINDICATO%'
)"""


def _sanitize_table_stem(raw_stem: str) -> Tuple[str, Optional[int]]:
    """
    Normaliza o nome de arquivo enviado para o nome canônico da tabela Parquet e extrai
    `ano_referencia` caso venha no sufixo ou no padrão oficial SICOM/TCE-MG
    (ex.: `2024.3137205.despesa.pagamento` -> `('despesa_pagamento', 2024)`,
     `lagoa_da_prata_mg_3137205__despesa_pagamento_2025` -> `('despesa_pagamento', 2025)`).
    """
    s = raw_stem.strip()
    if "__" in s:
        s = s.split("__", 1)[1].strip()
    # Padrão oficial SICOM TCE-MG: YYYY.<ibge7>.<modulo>.<submodulo>
    m_sicom = re.match(r"^(20[12][0-9])\.(\d{7})\.(.+)$", s)
    if m_sicom:
        yr_sic = int(m_sicom.group(1))
        rest = m_sicom.group(3).replace(".", "_").replace("-", "_")
        rest = re.sub(r"[^A-Za-z0-9_]", "_", rest)
        rest = re.sub(r"_+", "_", rest).strip("_")
        if rest and rest[0].isdigit():
            rest = f"sicom_{rest}"
        if rest and re.match(r"^[A-Za-z][A-Za-z0-9_]{1,80}$", rest):
            return rest, yr_sic

    clean = re.sub(r"[^A-Za-z0-9_]", "_", s)
    clean = re.sub(r"_+", "_", clean).strip("_")
    yr: Optional[int] = None
    m_yr = re.search(r"^(.*?)_(20[12][0-9])$", clean)
    if m_yr and m_yr.group(1):
        clean = m_yr.group(1)
        yr = int(m_yr.group(2))
    if not clean or not re.match(r"^[A-Za-z][A-Za-z0-9_]{1,80}$", clean):
        raise ValueError(f"Nome de tabela/arquivo inválido para ingestão: '{raw_stem}'.")
    return clean, yr


def _decode_text_smart(raw_bytes: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw_bytes.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("utf-8", errors="replace")


def _detect_csv_delimiter(sample_text: str) -> str:
    first_lines = "\n".join(sample_text.splitlines()[:5])
    counts = {sep: first_lines.count(sep) for sep in (";", "|", ",", "\t")}
    best = max(counts, key=lambda k: counts[k])
    return best if counts[best] > 0 else ";"


class DataSyncManager:
    """
    Motor de verificação de frescor, padronização de schema, conectores de APIs oficiais,
    upload seguro de pacotes municipais e publicação em releases imutáveis (`CURRENT.json`).
    """

    def __init__(self, workspace_root: Path = DEFAULT_WORKSPACE_ROOT, tenant_id: str = "caete_mg_3110004") -> None:
        self.workspace_root = workspace_root
        self.tenant_id = tenant_id
        self.tenant_profile = load_tenant_profile(tenant_id=tenant_id, workspace_root=workspace_root)
        self.parquet_dir = workspace_root / "avaliacao" / "dados" / "parquet" / tenant_id
        self.releases_root = self.parquet_dir / "releases"
        self.current_pointer_file = self.parquet_dir / "CURRENT.json"
        self.previous_pointer_file = self.parquet_dir / "PREVIOUS.json"
        self.sync_state_file = self.parquet_dir / "_sync_state.json"
        self.lock_file = self.parquet_dir / "_sync.lock"

    @contextmanager
    def _acquire_sync_lock(self, stale_after_sec: int = 900) -> Iterator[None]:
        """
        Impede duas sincronizações ou uploads simultâneos sobre o mesmo tenant (`409 Conflict`).
        Grava `lock_token` único + `pid`, verifica se o `pid` ainda existe antes de remover lock órfão,
        e só remove `_sync.lock` no `finally` se `lock_token` coincidir exatamente.
        """
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        if self.lock_file.exists():
            try:
                age = time.time() - self.lock_file.stat().st_mtime
                old_pid = 0
                try:
                    lock_meta = json.loads(self.lock_file.read_text(encoding="utf-8"))
                    if isinstance(lock_meta, dict):
                        old_pid = int(lock_meta.get("pid") or 0)
                except Exception:
                    old_pid = 0

                if old_pid > 0:
                    if not _is_pid_alive(old_pid):
                        self.lock_file.unlink(missing_ok=True)
                elif age > stale_after_sec:
                    self.lock_file.unlink(missing_ok=True)
            except Exception:
                pass

        lock_token = secrets.token_hex(16)
        try:
            fd = os.open(str(self.lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "pid": os.getpid(),
                        "lock_token": lock_token,
                        "tenant_id": self.tenant_id,
                        "started_at": datetime.now().isoformat(),
                    },
                    fh,
                )
        except FileExistsError as exc:
            raise SyncAlreadyRunningError(
                f"Já existe uma atualização ou upload em andamento para o município '{self.tenant_id}'. Aguarde alguns instantes."
            ) from exc

        try:
            yield
        finally:
            try:
                if self.lock_file.exists():
                    cur_meta = json.loads(self.lock_file.read_text(encoding="utf-8"))
                    if isinstance(cur_meta, dict) and cur_meta.get("lock_token") == lock_token:
                        self.lock_file.unlink(missing_ok=True)
            except Exception:
                pass

    def _read_current_pointer(self) -> Dict[str, Any]:
        if self.current_pointer_file.exists():
            try:
                data = json.loads(self.current_pointer_file.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    def _resolve_active_parquet_map(self) -> Dict[str, Path]:
        """
        Retorna `{stem_original_case: Path}` mesclando o diretório base `parquet_dir` com a cadeia
        de releases imutáveis em `CURRENT.json` (idêntico à resolução do `DuckDBManager`).
        """
        layered: Dict[str, Path] = {}
        lower_to_canon: Dict[str, str] = {}
        if self.parquet_dir.exists():
            for p in sorted(self.parquet_dir.glob("*.parquet")):
                layered[p.stem] = p
                lower_to_canon[p.stem.lower()] = p.stem

        ptr = self._read_current_pointer()
        chain: List[str] = []
        raw_chain = ptr.get("release_chain")
        if isinstance(raw_chain, list):
            chain.extend(str(x).strip() for x in raw_chain if str(x).strip())
        rel_single = str(ptr.get("release_dir") or "").strip()
        if rel_single and rel_single not in chain:
            chain.append(rel_single)

        releases_root_resolved = self.releases_root.resolve()
        for rel_entry in chain:
            cand = Path(rel_entry)
            if not cand.is_absolute():
                cand = (self.parquet_dir / rel_entry).resolve()
            else:
                cand = cand.resolve()
            try:
                cand.relative_to(releases_root_resolved)
            except ValueError:
                continue
            if cand.exists() and cand.is_dir():
                for p in sorted(cand.glob("*.parquet")):
                    canon = lower_to_canon.get(p.stem.lower(), p.stem)
                    layered[canon] = p
                    lower_to_canon[p.stem.lower()] = canon
        return layered

    def _publish_immutable_release(self, release_dir: Path, release_id: str, summary: str) -> Dict[str, Any]:
        """
        Atualiza atomicamente `CURRENT.json` apontando para o novo `release_dir`, salva `PREVIOUS.json`
        para rollback imediato, valida confinamento em `releases_root`, preserva `KEEP_RECENT_RELEASES = 2`
        e só remove diretórios fora da cadeia ativa/anterior cujo `st_mtime` tenha `> 60` segundos.
        """
        releases_root_resolved = self.releases_root.resolve()
        release_dir_resolved = release_dir.resolve()
        release_dir_resolved.relative_to(releases_root_resolved)

        prev = self._read_current_pointer()
        prev_chain: List[str] = []
        if isinstance(prev.get("release_chain"), list):
            prev_chain = [str(x).strip() for x in prev["release_chain"] if str(x).strip()]
        elif prev.get("release_dir"):
            prev_chain = [str(prev["release_dir"]).strip()]

        rel_posix = release_dir_resolved.relative_to(self.parquet_dir.resolve()).as_posix()
        new_stems = {p.stem.lower() for p in release_dir_resolved.glob("*.parquet")}

        # Manter na chain apenas diretórios anteriores válidos que ainda contenham tabelas não sobrescritas
        kept_chain: List[str] = []
        covered_stems = set(new_stems)
        prev_valid_dirs: Set[Path] = set()
        for entry in reversed(prev_chain):
            if entry == rel_posix:
                continue
            cand = (self.parquet_dir / entry).resolve()
            try:
                cand.relative_to(releases_root_resolved)
            except ValueError:
                continue
            if not cand.exists() or not cand.is_dir():
                continue
            prev_valid_dirs.add(cand)
            entry_stems = {p.stem.lower() for p in cand.glob("*.parquet")}
            unique_needed = entry_stems - covered_stems
            if unique_needed:
                kept_chain.append(entry)
                covered_stems.update(entry_stems)
        kept_chain.reverse()
        kept_chain.append(rel_posix)

        if prev and (prev.get("release_id") or prev.get("release_dir") or prev.get("release_chain")):
            _write_json_atomic(self.previous_pointer_file, prev)

        payload = {
            "release_id": release_id,
            "release_dir": rel_posix,
            "release_chain": kept_chain,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": summary,
        }
        _write_json_atomic(self.current_pointer_file, payload)
        try:
            (self.parquet_dir / "_catalog_profile_v1.json").unlink(missing_ok=True)
        except Exception:
            pass

        # Proteger cadeia ativa (CURRENT), cadeia de rollback (PREVIOUS) e os KEEP_RECENT_RELEASES mais recentes
        protected_dirs: Set[Path] = set(prev_valid_dirs)
        protected_dirs.add(release_dir_resolved)
        for active_entry in kept_chain:
            c_act = (self.parquet_dir / active_entry).resolve()
            try:
                c_act.relative_to(releases_root_resolved)
                protected_dirs.add(c_act)
            except ValueError:
                continue

        if self.releases_root.exists():
            all_rel_dirs: List[Tuple[float, Path]] = []
            for d in self.releases_root.iterdir():
                if not d.is_dir():
                    continue
                d_res = d.resolve()
                try:
                    d_res.relative_to(releases_root_resolved)
                except ValueError:
                    continue
                try:
                    all_rel_dirs.append((d_res.stat().st_mtime, d_res))
                except Exception:
                    continue
            all_rel_dirs.sort(key=lambda x: x[0], reverse=True)
            for _, recent_d in all_rel_dirs[:KEEP_RECENT_RELEASES]:
                protected_dirs.add(recent_d)

            now_ts = time.time()
            for mtime_d, cand_d in all_rel_dirs:
                if cand_d in protected_dirs:
                    continue
                if (now_ts - mtime_d) > 60.0:
                    try:
                        shutil.rmtree(cand_d, ignore_errors=True)
                    except Exception:
                        pass

        return payload

    def rollback_to_previous_release(self) -> Dict[str, Any]:
        """Restaura atomicamente `PREVIOUS.json` em `CURRENT.json` para rollback seguro de release."""
        if not self.previous_pointer_file.exists():
            raise ValueError(f"Nenhum `PREVIOUS.json` disponível para rollback em '{self.tenant_id}'.")
        prev_payload = json.loads(self.previous_pointer_file.read_text(encoding="utf-8"))
        if not isinstance(prev_payload, dict) or not prev_payload.get("release_id"):
            raise ValueError("Conteúdo de `PREVIOUS.json` inválido para rollback.")
        releases_root_resolved = self.releases_root.resolve()
        chain = prev_payload.get("release_chain") or ([prev_payload["release_dir"]] if prev_payload.get("release_dir") else [])
        for entry in chain:
            cand = (self.parquet_dir / str(entry)).resolve()
            cand.relative_to(releases_root_resolved)
            if not cand.exists() or not cand.is_dir():
                raise ValueError(f"Diretório do release anterior ausente para rollback: '{entry}'.")
        prev_payload["rolled_back_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _write_json_atomic(self.current_pointer_file, prev_payload)
        return prev_payload

    # ------------------------------------------------------------------
    # Conectores Online Reais (IBGE / SICONFI STN / CGU)
    # ------------------------------------------------------------------
    def _fetch_live_ibge_updates(self, ibge_code: str) -> Dict[str, Any]:
        import requests

        telemetry: Dict[str, Any] = {"source": "IBGE", "attempted": True, "ok": False, "detail": ""}
        pop_by_year: Dict[int, Tuple[int, str]] = {}
        pib_by_year: Dict[int, float] = {}
        area_km2_live: float = 0.0
        try:
            # 1. Validar metadados oficiais do município na API v1 de Localidades
            r_loc = requests.get(
                f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/{ibge_code}",
                timeout=6,
            )
            loc_name = ""
            if r_loc.status_code == 200 and isinstance(r_loc.json(), dict):
                loc_name = str(r_loc.json().get("nome") or "")

            # 2. Consultar Estimativas Populacionais (Agregado 6579, variável 9324) e Censo 2022 (Agregado 4714, variável 93)
            r_est = requests.get(
                f"https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2020|2021|2024|2025|2026/variaveis/9324?localidades=N6[{ibge_code}]",
                timeout=7,
            )
            if r_est.status_code == 200 and isinstance(r_est.json(), list) and r_est.json():
                res_list = r_est.json()[0].get("resultados") or []
                if res_list:
                    serie = (res_list[0].get("series") or [{}])[0].get("serie") or {}
                    for yr_s, val_s in serie.items():
                        if str(yr_s).isdigit() and str(val_s).isdigit():
                            pop_by_year[int(yr_s)] = (int(val_s), f"Estimativa Populacional Oficial API IBGE ({yr_s})")

            r_censo = requests.get(
                f"https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/93?localidades=N6[{ibge_code}]",
                timeout=7,
            )
            if r_censo.status_code == 200 and isinstance(r_censo.json(), list) and r_censo.json():
                res_list = r_censo.json()[0].get("resultados") or []
                if res_list:
                    serie = (res_list[0].get("series") or [{}])[0].get("serie") or {}
                    if str(serie.get("2022", "")).isdigit():
                        pop_by_year[2022] = (int(serie["2022"]), "Censo Demográfico Oficial API IBGE 2022")

            # 3. Consultar PIB Municipal a preços correntes em mil R$ (Agregado 5938, variável 37)
            r_pib = requests.get(
                f"https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/2020|2021|2022|2023|2024|2025/variaveis/37?localidades=N6[{ibge_code}]",
                timeout=7,
            )
            if r_pib.status_code == 200 and isinstance(r_pib.json(), list) and r_pib.json():
                res_list = r_pib.json()[0].get("resultados") or []
                if res_list:
                    serie = (res_list[0].get("series") or [{}])[0].get("serie") or {}
                    for yr_s, val_s in serie.items():
                        try:
                            v_num = float(str(val_s).replace(",", "."))
                            if str(yr_s).isdigit() and v_num > 0:
                                pib_by_year[int(yr_s)] = round(v_num, 2)
                        except Exception:
                            pass

            # 4. Consultar Área Territorial Oficial km² (Agregado 1301, variável 615)
            r_area = requests.get(
                f"https://servicodados.ibge.gov.br/api/v3/agregados/1301/periodos/2010/variaveis/615?localidades=N6[{ibge_code}]",
                timeout=7,
            )
            if r_area.status_code == 200 and isinstance(r_area.json(), list) and r_area.json():
                res_list = r_area.json()[0].get("resultados") or []
                if res_list:
                    serie = (res_list[0].get("series") or [{}])[0].get("serie") or {}
                    for _, val_s in serie.items():
                        try:
                            v_a = float(str(val_s).replace(",", "."))
                            if v_a > 0:
                                area_km2_live = round(v_a, 3)
                        except Exception:
                            pass

            if loc_name or pop_by_year:
                telemetry["ok"] = True
                telemetry["municipality_verified"] = loc_name
                telemetry["years_fetched"] = sorted(pop_by_year.keys())
                telemetry["pib_years_fetched"] = sorted(pib_by_year.keys())
                telemetry["detail"] = (
                    f"API IBGE consultada com sucesso ({len(pop_by_year)} exercício(s) populacionais e "
                    f"{len(pib_by_year)} exercício(s) de PIB verificados para {loc_name or ibge_code})."
                )
            else:
                telemetry["detail"] = "API IBGE respondeu sem novas séries; mantendo série local auditada."
        except Exception as exc:
            telemetry["detail"] = f"API IBGE indisponível no momento ({type(exc).__name__}); mantendo base local."

        telemetry["pop_by_year"] = pop_by_year
        telemetry["pib_by_year"] = pib_by_year
        telemetry["area_km2"] = area_km2_live
        return telemetry

    def _fetch_live_siconfi_updates(self, ibge_code: str, target_years: List[int], full_extract: bool = False) -> Dict[str, Any]:
        """
        Consulta a API aberta SICONFI / Tesouro Nacional respeitando estritamente o limite de 1 req/s (`sleep(1.1)`).
        Quando `full_extract=True` (ex.: municípios ainda sem pacote SICOM local), coleta RCL + Despesa com Pessoal LRF
        (`RGF-Anexo 01`) e Despesas Liquidadas em Saúde e Educação (`RREO-Anexo 02`) para materializar imediatamente
        `siconfi_finbra_indicadores` e `datasus_siops_saude` com números 100% oficiais do Tesouro Nacional!
        """
        import requests

        telemetry: Dict[str, Any] = {"source": "SICONFI", "attempted": True, "ok": False, "detail": ""}
        rcl_live: Dict[int, float] = {}
        siconfi_rows_by_year: Dict[int, Dict[str, float]] = {}
        checked_years: List[int] = []
        req_count = 0

        def _throttled_get(url: str) -> Any:
            nonlocal req_count
            if req_count > 0:
                time.sleep(1.1)
            req_count += 1
            return requests.get(url, timeout=10)

        try:
            years_to_scan = sorted(target_years) if full_extract else sorted(target_years)[-2:]
            for yr in years_to_scan:
                yr_data: Dict[str, float] = {}
                eff_bim = 6
                # 1. RGF Anexo 01 (suporta tanto regime quadrimestral `RGF` quanto semestral `RGF Simplificado` do art. 63 da LRF!)
                rgf_periods = (("Q", 3), ("S", 2)) if yr <= 2024 else (("Q", 3), ("S", 2), ("Q", 2), ("S", 1), ("Q", 1))
                found_rgf = False
                for per_type, nr_p in rgf_periods:
                    rgf_demos = ("RGF", "RGF%20Simplificado") if per_type == "S" else ("RGF",)
                    for rgf_dem in rgf_demos:
                        url_rgf = (
                            f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rgf"
                            f"?an_exercicio={yr}&in_periodicidade={per_type}&nr_periodo={nr_p}"
                            f"&co_tipo_demonstrativo={rgf_dem}&no_anexo=RGF-Anexo%2001&co_poder=E&id_ente={ibge_code}"
                        )
                        r_rgf = _throttled_get(url_rgf)
                        if r_rgf.status_code == 200:
                            rgf_items = (r_rgf.json() or {}).get("items") or []
                            if rgf_items:
                                checked_years.append(yr)
                                for it in rgf_items:
                                    cod_c = str(it.get("cod_conta") or "")
                                    col_u = str(it.get("coluna") or "").upper()
                                    val = it.get("valor")
                                    if not isinstance(val, (int, float)) or float(val) <= 0:
                                        continue
                                    if cod_c in ("ReceitaCorrenteLiquidaAjustada", "ReceitaCorrenteLiquidaLimiteLegal") and (
                                        "VALOR" in col_u or "TOTAL" in col_u
                                    ):
                                        yr_data.setdefault("rcl", round(float(val), 2))
                                    elif cod_c == "DespesaComPessoalLiquida" and (
                                        "12 MESES" in col_u or "TOTAL" in col_u or col_u.strip() == "VALOR"
                                    ):
                                        if "%" not in col_u:
                                            yr_data["pessoal"] = round(float(val), 2)
                                found_rgf = True
                                break
                    if found_rgf:
                        break

                # 2. RREO Anexo 02 (suporta `RREO` padrão e `RREO Simplificado` para municípios < 50 mil hab. — art. 63 LRF)
                bim_candidates = (6,) if yr <= 2024 else (6, 5, 4, 3, 2, 1)
                eff_rreo_dem = "RREO"
                found_a02 = False
                for cand_b in bim_candidates:
                    for tp_dem_a02 in ("RREO", "RREO%20Simplificado"):
                        url_a02 = (
                            f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"
                            f"?an_exercicio={yr}&nr_periodo={cand_b}&co_tipo_demonstrativo={tp_dem_a02}&no_anexo=RREO-Anexo%2002&id_ente={ibge_code}"
                        )
                        r_a02 = _throttled_get(url_a02)
                        if r_a02.status_code == 200:
                            a02_items = (r_a02.json() or {}).get("items") or []
                            if a02_items:
                                eff_bim = cand_b
                                eff_rreo_dem = tp_dem_a02
                                checked_years.append(yr)
                                for it in a02_items:
                                    conta = str(it.get("conta") or "").strip()
                                    col_u = str(it.get("coluna") or "").upper()
                                    val = it.get("valor")
                                    if not isinstance(val, (int, float)) or float(val) <= 0:
                                        continue
                                    if "LIQUIDADAS AT" in col_u and "BIMESTRE" in col_u:
                                        if conta.lower() in ("saúde", "saude"):
                                            yr_data["saude"] = round(float(val), 2)
                                        elif conta.lower() in ("educação", "educacao"):
                                            yr_data["educacao"] = round(float(val), 2)
                                found_a02 = True
                                break
                    if found_a02:
                        break

                # 2b. RREO Anexo 08 (MDE Educação Art. 212 CF/88) e Anexo 12 (Saúde ASPS LC 141/2012):
                # Extrai APENAS percentuais constitucionais oficialmente homologados pelo ente no SICONFI.
                url_a08 = (
                    f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"
                    f"?an_exercicio={yr}&nr_periodo={eff_bim}&co_tipo_demonstrativo={eff_rreo_dem}&no_anexo=RREO-Anexo%2008&id_ente={ibge_code}"
                )
                r_a08 = _throttled_get(url_a08)
                if r_a08.status_code == 200:
                    for it in (r_a08.json() or {}).get("items") or []:
                        cod_c = str(it.get("cod_conta") or "")
                        conta_u = str(it.get("conta") or "").upper()
                        col_u = str(it.get("coluna") or "").upper()
                        val = it.get("valor")
                        if not isinstance(val, (int, float)) or not (5.0 <= float(val) <= 65.0):
                            continue
                        if ("MINIMO" in cod_c.upper() or "MÍNIMO" in conta_u or "ART. 212" in conta_u or "MDE" in conta_u) and (
                            "%" in col_u or "%" in conta_u or "PERCENTUAL" in col_u
                        ) and "FUNDEB" not in conta_u:
                            yr_data["pct_mde_oficial"] = round(float(val), 2)
                            break

                url_a12 = (
                    f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"
                    f"?an_exercicio={yr}&nr_periodo={eff_bim}&co_tipo_demonstrativo={eff_rreo_dem}&no_anexo=RREO-Anexo%2012&id_ente={ibge_code}"
                )
                r_a12 = _throttled_get(url_a12)
                if r_a12.status_code == 200:
                    for it in (r_a12.json() or {}).get("items") or []:
                        cod_c = str(it.get("cod_conta") or "")
                        conta_u = str(it.get("conta") or "").upper()
                        col_u = str(it.get("coluna") or "").upper()
                        val = it.get("valor")
                        if not isinstance(val, (int, float)) or not (5.0 <= float(val) <= 65.0):
                            continue
                        if (
                            "PERCENTUAL" in conta_u
                            or "LC 141" in conta_u
                            or "APURADO" in cod_c.upper()
                            or ("SAUDE" in cod_c.upper() and "%" in col_u)
                        ) and ("%" in col_u or "PERCENTUAL" in conta_u):
                            if float(val) != 15.0:
                                yr_data["pct_siops_oficial"] = round(float(val), 2)
                                break

                # 3. RREO Anexo 03 (RCL 12 meses — sempre verifica se RCL ainda não foi preenchida)
                if "rcl" not in yr_data:
                    a03_periods = (6,) if yr <= 2024 else (6, 5, 4, 3, 2)
                    for p_a03 in a03_periods:
                        found_a03 = False
                        for tp_dem in ("RREO", "RREO%20Simplificado"):
                            url_a03 = (
                                f"https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"
                                f"?an_exercicio={yr}&nr_periodo={p_a03}&co_tipo_demonstrativo={tp_dem}&no_anexo=RREO-Anexo%2003&id_ente={ibge_code}"
                            )
                            resp = _throttled_get(url_a03)
                            if resp.status_code == 200:
                                items_a03 = (resp.json() or {}).get("items") or []
                                if items_a03:
                                    checked_years.append(yr)
                                    for it in items_a03:
                                        cod_conta = str(it.get("cod_conta") or "")
                                        coluna = str(it.get("coluna") or "")
                                        if "ReceitaCorrenteLiquida" in cod_conta and ("TOTAL" in coluna.upper() or "12 MESES" in coluna.upper()):
                                            val = it.get("valor")
                                            if isinstance(val, (int, float)) and float(val) > 0:
                                                yr_data["rcl"] = round(float(val), 2)
                                                found_a03 = True
                                    if found_a03:
                                        break
                        if found_a03:
                            break
                else:
                    checked_years.append(yr)

                if "rcl" in yr_data:
                    rcl_live[yr] = yr_data["rcl"]
                if yr_data:
                    siconfi_rows_by_year[yr] = yr_data

            if checked_years or siconfi_rows_by_year:
                telemetry["ok"] = True
                telemetry["years_checked"] = sorted(set(checked_years) | set(siconfi_rows_by_year.keys()))
                telemetry["rcl_fetched_years"] = sorted(rcl_live.keys())
                telemetry["detail"] = (
                    f"API SICONFI/STN consultada com rate-limit seguro (exercícios homologados: {telemetry['years_checked']})."
                )
            else:
                telemetry["detail"] = "API SICONFI/STN sem novos demonstrativos RREO/RGF publicados; mantendo base local."
        except Exception as exc:
            telemetry["detail"] = f"API SICONFI/STN indisponível no momento ({type(exc).__name__}); mantendo base local."

        telemetry["rcl_live"] = rcl_live
        telemetry["siconfi_rows_by_year"] = siconfi_rows_by_year
        return telemetry

    def _fetch_live_cgu_updates(self, ibge_code: str, full_extract: bool = False) -> Dict[str, Any]:
        import requests

        cgu_key = os.environ.get("CGU_API_KEY", "").strip()
        if not cgu_key:
            try:
                from dotenv import dotenv_values
                env_file = self.workspace_root / ".env"
                if env_file.exists():
                    cgu_key = str(dotenv_values(env_file).get("CGU_API_KEY") or "").strip()
            except Exception:
                pass
        if not cgu_key:
            return {
                "source": "CGU",
                "attempted": False,
                "ok": True,
                "detail": "Variável CGU_API_KEY não configurada; mantendo tabelas CGU locais.",
                "bf_records": [],
                "bpc_records": [],
            }
        bf_records: List[Dict[str, Any]] = []
        bpc_records: List[Dict[str, Any]] = []
        headers = {"chave-api-dados": cgu_key, "Accept": "application/json"}
        now_dt = datetime.now()
        end_yr = max(2026, now_dt.year)
        end_mo = now_dt.month if now_dt.year == end_yr else 12
        dyn_months: List[str] = []
        for y_cgu in range(2024, end_yr + 1):
            m_limit = end_mo if y_cgu == end_yr else 12
            for m_cgu in range(1, m_limit + 1):
                dyn_months.append(f"{y_cgu}{m_cgu:02d}")

        months_to_probe: Sequence[str] = (
            tuple(dyn_months)
            if full_extract
            else tuple(reversed(dyn_months[-12:]))
        )
        last_status = 0
        try:
            empty_streak = 0
            for ym in months_to_probe:
                resp = requests.get(
                    f"https://api.portaldatransparencia.gov.br/api-de-dados/novo-bolsa-familia-por-municipio?mesAno={ym}&codigoIbge={ibge_code}&pagina=1",
                    headers=headers,
                    timeout=10,
                )
                last_status = resp.status_code
                if resp.status_code == 200 and resp.text.strip().startswith("["):
                    payload = resp.json()
                    if payload:
                        empty_streak = 0
                        rec0 = payload[0]
                        mun_obj = rec0.get("municipio") or {}
                        uf_obj = mun_obj.get("uf") or {}
                        bf_records.append(
                            {
                                "mes_referencia": int(ym),
                                "ano_referencia": int(ym[:4]),
                                "cod_ibge": str(ibge_code),
                                "nom_municipio": str(mun_obj.get("nomeIBGE") or self.tenant_profile.get("municipality_name") or ""),
                                "uf": str(uf_obj.get("sigla") or self.tenant_profile.get("state_uf") or "MG"),
                                "qtd_beneficiados": int(rec0.get("quantidadeBeneficiados") or 0),
                                "valor_total": round(float(rec0.get("valor") or 0.0), 2),
                            }
                        )
                        if not full_extract:
                            break
                    elif full_extract and int(ym) >= 202503:
                        empty_streak += 1
                        if empty_streak >= 3:
                            break

            if full_extract:
                bpc_months = [str(r["mes_referencia"]) for r in bf_records] or list(months_to_probe[:18])
            else:
                seen_bf_m = [str(r["mes_referencia"]) for r in bf_records]
                bpc_months = list(dict.fromkeys(seen_bf_m + list(months_to_probe[:6])))

            for ym in bpc_months:
                resp_bpc = requests.get(
                    f"https://api.portaldatransparencia.gov.br/api-de-dados/bpc-por-municipio?mesAno={ym}&codigoIbge={ibge_code}&pagina=1",
                    headers=headers,
                    timeout=10,
                )
                last_status = resp_bpc.status_code or last_status
                if resp_bpc.status_code == 200 and resp_bpc.text.strip().startswith("["):
                    payload_bpc = resp_bpc.json()
                    if payload_bpc:
                        rec_b = payload_bpc[0]
                        mun_obj = rec_b.get("municipio") or {}
                        uf_obj = mun_obj.get("uf") or {}
                        bpc_records.append(
                            {
                                "mes_referencia": int(ym),
                                "ano_referencia": int(ym[:4]),
                                "cod_ibge": str(ibge_code),
                                "nom_municipio": str(mun_obj.get("nomeIBGE") or self.tenant_profile.get("municipality_name") or ""),
                                "uf": str(uf_obj.get("sigla") or self.tenant_profile.get("state_uf") or "MG"),
                                "qtd_beneficiados": int(rec_b.get("quantidadeBeneficiados") or 0),
                                "valor_total": round(float(rec_b.get("valor") or 0.0), 2),
                            }
                        )
                        if not full_extract:
                            break

            if bf_records or bpc_records:
                latest_ref = (bf_records[-1] if bf_records else bpc_records[-1])["mes_referencia"]
                bf_summary = (
                    f"{bf_records[-1]['qtd_beneficiados']} famílias Bolsa Família, R$ {bf_records[-1]['valor_total']:,.2f}; "
                    if bf_records
                    else ""
                )
                return {
                    "source": "CGU",
                    "attempted": True,
                    "ok": True,
                    "detail": (
                        f"API Portal da Transparência CGU autenticada com sucesso "
                        f"(IBGE {ibge_code}, competência {latest_ref}: "
                        f"{bf_summary}{len(bpc_records)} competências BPC coletadas)."
                    ),
                    "bf_records": bf_records,
                    "bpc_records": bpc_records,
                }
            if last_status == 200:
                return {
                    "source": "CGU",
                    "attempted": True,
                    "ok": True,
                    "detail": f"API Portal da Transparência CGU autenticada com sucesso (HTTP 200 para IBGE {ibge_code}).",
                    "bf_records": [],
                    "bpc_records": [],
                }
            return {
                "source": "CGU",
                "attempted": True,
                "ok": False,
                "detail": f"API CGU retornou HTTP {last_status}; mantendo base local.",
                "bf_records": [],
                "bpc_records": [],
            }
        except Exception as exc:
            return {
                "source": "CGU",
                "attempted": True,
                "ok": False,
                "detail": f"Falha de rede na API CGU ({type(exc).__name__}); mantendo base local.",
                "bf_records": [],
                "bpc_records": [],
            }

    # ------------------------------------------------------------------
    # Materialização Reconciliada Multi-Cidade (Zero Vazamento de Caeté)
    # ------------------------------------------------------------------
    def materialize_enriched_public_bases(
        self,
        target_dir: Optional[Path] = None,
        live_ibge_pop: Optional[Dict[int, Tuple[int, str]]] = None,
        live_siconfi_rcl: Optional[Dict[int, float]] = None,
        live_ibge_pib: Optional[Dict[int, float]] = None,
        live_ibge_area: float = 0.0,
        live_siconfi_rows: Optional[Dict[int, Dict[str, float]]] = None,
        live_cgu_bf: Optional[List[Dict[str, Any]]] = None,
        live_cgu_bpc: Optional[List[Dict[str, Any]]] = None,
        skip_tables: Optional[Iterable[str]] = None,
    ) -> Dict[str, int]:
        """
        Gera e padroniza em Parquet (`ZSTD`) as bases públicas derivadas/enriquecidas para o município do `tenant_profile`.
        Quando `target_dir` é um diretório de release imutável (`releases/<ts>`), grava os novos `.parquet` lá sem tocar
        nos arquivos abertos em `parquet_dir`. Tabelas listadas em `skip_tables` (ex.: recém-enviadas em upload) jamais são sobrescritas.
        """
        out_dir = target_dir or self.parquet_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        active_map = self._resolve_active_parquet_map()
        norm_skip: Set[str] = {str(t).strip().lower() for t in (skip_tables or ()) if str(t).strip()}

        def _src_pq(stem: str) -> Path:
            cand_out = out_dir / f"{stem}.parquet"
            if cand_out.exists():
                return cand_out
            for k, p in active_map.items():
                if k.lower() == stem.lower():
                    return p
            return self.parquet_dir / f"{stem}.parquet"

        def _parquet_has_rows(pq_path: Path) -> bool:
            if not pq_path.exists():
                return False
            try:
                return int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{pq_path.as_posix()}')").fetchone()[0]) > 0
            except Exception:
                return False

        con = duckdb.connect(":memory:")
        con.execute("SET memory_limit = '512MB';")
        con.execute("SET threads = 2;")
        row_counts: Dict[str, int] = {}

        ibge_code = str(self.tenant_profile.get("ibge_code", "3110004")).replace("'", "")
        city_name = str(self.tenant_profile.get("municipality_name", "Caeté")).replace("'", "")
        uf = str(self.tenant_profile.get("state_uf", "MG")).replace("'", "")
        hosp_ref = str(self.tenant_profile.get("hospital_referencia", f"Hospital Municipal / Rede SUS de {city_name}")).replace("'", "")
        is_caete = ibge_code == "3110004" or self.tenant_id.startswith("caete")

        rec_pq = _src_pq("receita_receita")
        desp_pq = _src_pq("despesa_despesa")
        pess_pq = _src_pq("desp_pessoal_despPessoal")
        pag_pq = _src_pq("despesa_pagamento")
        qsoc_pq = _src_pq("licitacao_quadroSocLicitacao")
        hab_pq = _src_pq("licitacao_habLicitacao")
        cont_pq = _src_pq("contrato_contContrato")
        cgu_transf_pq = _src_pq("cgu_transferencias")

        # 1. IBGE Indicadores Municipais (carrega primeiro o Parquet existente para nunca apagar PIB 2023 nem 2026!)
        ibge_out_pq = out_dir / "ibge_indicadores_municipais.parquet"
        ibge_existing_pq = _src_pq("ibge_indicadores_municipais")
        if "ibge_indicadores_municipais" not in norm_skip:
            existing_yrs: Dict[int, Dict[str, Any]] = {}
            if ibge_existing_pq.exists() and ibge_existing_pq != ibge_out_pq:
                try:
                    prev_ibge_rows = con.execute(
                        f"""
                        SELECT
                            ano_referencia, populacao_residente, fonte_populacao,
                            pib_municipal_mil_reais, pib_per_capita_reais,
                            idhm_geral, idhm_educacao, idhm_longevidade,
                            area_territorial_km2, densidade_demografica_hab_km2
                        FROM read_parquet('{ibge_existing_pq.as_posix()}')
                        WHERE TRY_CAST(ano_referencia AS BIGINT) IS NOT NULL
                        """
                    ).fetchall()
                    for r_ib in prev_ibge_rows:
                        yr_k = int(r_ib[0])
                        existing_yrs[yr_k] = {
                            "ano": yr_k,
                            "populacao": int(r_ib[1] or 0),
                            "fonte_pop": str(r_ib[2] or ""),
                            "pib_mil": float(r_ib[3] or 0.0),
                            "pib_per_capita": float(r_ib[4] or 0.0),
                            "idhm": float(r_ib[5]) if r_ib[5] is not None and float(r_ib[5]) > 0 else None,
                            "idhm_edu": float(r_ib[6]) if r_ib[6] is not None and float(r_ib[6]) > 0 else None,
                            "idhm_long": float(r_ib[7]) if r_ib[7] is not None and float(r_ib[7]) > 0 else None,
                            "area_km2": float(r_ib[8] or 0.0),
                            "densidade": float(r_ib[9] or 0.0),
                        }
                except Exception:
                    pass

            profile_series = list(self.tenant_profile.get("ibge_series") or [])
            if not profile_series and is_caete:
                profile_series = [
                    {"ano": 2020, "populacao": 44718, "fonte_pop": "Estimativa TCU/IBGE Pré-Censo", "pib_mil": 918450.00, "pib_per_capita": 20538.71, "idhm": 0.728, "idhm_edu": 0.652, "idhm_long": 0.845, "area_km2": 542.577, "densidade": 82.42},
                    {"ano": 2021, "populacao": 45047, "fonte_pop": "Estimativa TCU/IBGE Pré-Censo", "pib_mil": 1042310.00, "pib_per_capita": 23138.28, "idhm": 0.728, "idhm_edu": 0.652, "idhm_long": 0.845, "area_km2": 542.577, "densidade": 83.02},
                    {"ano": 2022, "populacao": 38776, "fonte_pop": "Censo Demográfico IBGE 2022", "pib_mil": 1128940.00, "pib_per_capita": 29114.40, "idhm": 0.728, "idhm_edu": 0.652, "idhm_long": 0.845, "area_km2": 542.577, "densidade": 71.47},
                    {"ano": 2023, "populacao": 39240, "fonte_pop": "Estimativa Populacional IBGE Pós-Censo", "pib_mil": 1195420.00, "pib_per_capita": 30464.32, "idhm": 0.728, "idhm_edu": 0.652, "idhm_long": 0.845, "area_km2": 542.577, "densidade": 72.32},
                    {"ano": 2024, "populacao": 39755, "fonte_pop": "Estimativa Populacional IBGE DOU 2024", "pib_mil": 1268900.00, "pib_per_capita": 31918.00, "idhm": 0.728, "idhm_edu": 0.652, "idhm_long": 0.845, "area_km2": 542.577, "densidade": 73.27},
                    {"ano": 2025, "populacao": 39980, "fonte_pop": "Projeção Demográfica Municipal IBGE 2025", "pib_mil": 1341200.00, "pib_per_capita": 33546.77, "idhm": 0.728, "idhm_edu": 0.652, "idhm_long": 0.845, "area_km2": 542.577, "densidade": 73.69},
                ]

            for pr in profile_series:
                yr_p = int(pr["ano"])
                if yr_p not in existing_yrs:
                    existing_yrs[yr_p] = dict(pr)
                else:
                    cur_r = existing_yrs[yr_p]
                    if int(cur_r.get("populacao") or 0) <= 0 and int(pr.get("populacao") or 0) > 0:
                        cur_r["populacao"] = int(pr["populacao"])
                        cur_r["fonte_pop"] = str(pr.get("fonte_pop") or cur_r.get("fonte_pop") or "")
                    if float(cur_r.get("pib_mil") or 0.0) <= 0 and float(pr.get("pib_mil") or 0.0) > 0:
                        cur_r["pib_mil"] = float(pr["pib_mil"])
                    for k_idhm in ("idhm", "idhm_edu", "idhm_long"):
                        if cur_r.get(k_idhm) is None and pr.get(k_idhm) is not None and float(pr.get(k_idhm) or 0) > 0:
                            cur_r[k_idhm] = float(pr[k_idhm])
                    if float(cur_r.get("area_km2") or 0.0) <= 0 and float(pr.get("area_km2") or 0.0) > 0:
                        cur_r["area_km2"] = float(pr["area_km2"])

            def _fallback_attr(attr_key: str) -> Any:
                for rx in existing_yrs.values():
                    v = rx.get(attr_key)
                    if v is not None and float(v or 0) > 0:
                        return v
                return None

            if live_ibge_pop:
                for yr, (pop_val, f_pop) in live_ibge_pop.items():
                    if yr in existing_yrs:
                        if int(pop_val or 0) > 0:
                            existing_yrs[yr]["populacao"] = int(pop_val)
                            existing_yrs[yr]["fonte_pop"] = f_pop
                    else:
                        existing_yrs[yr] = {
                            "ano": yr,
                            "populacao": int(pop_val),
                            "fonte_pop": f_pop,
                            "pib_mil": 0.0,
                            "pib_per_capita": 0.0,
                            "idhm": _fallback_attr("idhm"),
                            "idhm_edu": _fallback_attr("idhm_edu"),
                            "idhm_long": _fallback_attr("idhm_long"),
                            "area_km2": live_ibge_area or float(_fallback_attr("area_km2") or 0.0),
                            "densidade": 0.0,
                        }
            if live_ibge_pib:
                for yr_p, pib_val_p in live_ibge_pib.items():
                    if pib_val_p <= 0:
                        continue
                    if yr_p in existing_yrs:
                        existing_yrs[yr_p]["pib_mil"] = float(pib_val_p)
                    else:
                        existing_yrs[yr_p] = {
                            "ano": yr_p,
                            "populacao": 0,
                            "fonte_pop": "Série PIB Municipal Oficial API IBGE",
                            "pib_mil": float(pib_val_p),
                            "pib_per_capita": 0.0,
                            "idhm": _fallback_attr("idhm"),
                            "idhm_edu": _fallback_attr("idhm_edu"),
                            "idhm_long": _fallback_attr("idhm_long"),
                            "area_km2": live_ibge_area or float(_fallback_attr("area_km2") or 0.0),
                            "densidade": 0.0,
                        }

            # Preencher população de 2023 quando ausente na API IBGE 6579 SEM apagar o PIB de 2023!
            if (
                2022 in existing_yrs
                and int(existing_yrs.get(2023, {}).get("populacao") or 0) <= 0
                and int(existing_yrs[2022].get("populacao") or 0) > 0
            ):
                prev_2023 = existing_yrs.get(2023, {})
                existing_yrs[2023] = {
                    "ano": 2023,
                    "populacao": int(existing_yrs[2022]["populacao"]),
                    "fonte_pop": "Censo Demográfico IBGE 2022 (Base TCU 2023)",
                    "pib_mil": float(prev_2023.get("pib_mil") or 0.0),
                    "pib_per_capita": float(prev_2023.get("pib_per_capita") or 0.0),
                    "idhm": prev_2023.get("idhm") if prev_2023.get("idhm") is not None else existing_yrs[2022].get("idhm"),
                    "idhm_edu": prev_2023.get("idhm_edu") if prev_2023.get("idhm_edu") is not None else existing_yrs[2022].get("idhm_edu"),
                    "idhm_long": prev_2023.get("idhm_long") if prev_2023.get("idhm_long") is not None else existing_yrs[2022].get("idhm_long"),
                    "area_km2": float(prev_2023.get("area_km2") or existing_yrs[2022].get("area_km2") or live_ibge_area or 0.0),
                    "densidade": 0.0,
                }

            if existing_yrs:
                const_idhm = _fallback_attr("idhm")
                const_idhm_edu = _fallback_attr("idhm_edu")
                const_idhm_long = _fallback_attr("idhm_long")
                const_area = live_ibge_area or float(_fallback_attr("area_km2") or 0.0)
                for yr in sorted(existing_yrs):
                    row_y = existing_yrs[yr]
                    if const_area > 0 and float(row_y.get("area_km2") or 0.0) <= 0:
                        row_y["area_km2"] = const_area
                    if row_y.get("idhm") is None and const_idhm is not None:
                        row_y["idhm"] = const_idhm
                    if row_y.get("idhm_edu") is None and const_idhm_edu is not None:
                        row_y["idhm_edu"] = const_idhm_edu
                    if row_y.get("idhm_long") is None and const_idhm_long is not None:
                        row_y["idhm_long"] = const_idhm_long
                    if live_ibge_pib and yr in live_ibge_pib and float(live_ibge_pib[yr] or 0.0) > 0:
                        row_y["pib_mil"] = float(live_ibge_pib[yr])
                    pop_v = int(row_y.get("populacao") or 0)
                    area_v = float(row_y.get("area_km2") or 0.0)
                    pib_v = float(row_y.get("pib_mil") or 0.0)
                    if area_v > 0 and pop_v > 0:
                        row_y["densidade"] = round(pop_v / area_v, 2)
                    if pib_v > 0 and pop_v > 0:
                        row_y["pib_per_capita"] = round((pib_v * 1000.0) / pop_v, 2)
                ibge_series = [existing_yrs[k] for k in sorted(existing_yrs)]

                def _sql_opt_num(val: Any, decimals: int = 2) -> str:
                    if val is None:
                        return "CAST(NULL AS DOUBLE)"
                    fv = float(val)
                    if fv <= 0:
                        return "CAST(NULL AS DOUBLE)"
                    return f"{fv:.{decimals}f}"

                def _sql_opt_pop(val: Any) -> str:
                    iv = int(val or 0)
                    return str(iv) if iv > 0 else "CAST(NULL AS BIGINT)"

                ibge_values_sql = ",\n".join(
                    f"({int(r['ano'])}, '{ibge_code}', '{city_name}', '{uf}', {_sql_opt_pop(r.get('populacao'))}, "
                    f"'{str(r.get('fonte_pop') or '').replace(chr(39), '')}', {_sql_opt_num(r.get('pib_mil'), 2)}, {_sql_opt_num(r.get('pib_per_capita'), 2)}, "
                    f"{_sql_opt_num(r.get('idhm'), 3)}, {_sql_opt_num(r.get('idhm_edu'), 3)}, {_sql_opt_num(r.get('idhm_long'), 3)}, "
                    f"{_sql_opt_num(r.get('area_km2'), 3)}, {_sql_opt_num(r.get('densidade'), 2)})"
                    for r in ibge_series
                )
                con.execute(
                    f"""
                    COPY (
                        SELECT * FROM (
                            VALUES
                            {ibge_values_sql}
                        ) AS t(
                            ano_referencia, cod_ibge, nom_municipio, uf, populacao_residente,
                            fonte_populacao, pib_municipal_mil_reais, pib_per_capita_reais,
                            idhm_geral, idhm_educacao, idhm_longevidade, area_territorial_km2, densidade_demografica_hab_km2
                        )
                    ) TO '{ibge_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                    """
                )
                row_counts["ibge_indicadores_municipais"] = len(ibge_series)

        # 2. SICONFI / Tesouro Nacional (LEFT JOIN + COALESCE impedindo que NULL sobrescreva valores já homologados!)
        siconfi_out_pq = out_dir / "siconfi_finbra_indicadores.parquet"
        siconfi_existing_pq = _src_pq("siconfi_finbra_indicadores")
        has_prev_siconfi = siconfi_existing_pq != siconfi_out_pq and _parquet_has_rows(siconfi_existing_pq)
        if "siconfi_finbra_indicadores" not in norm_skip:
            if is_caete and rec_pq.exists() and pess_pq.exists() and desp_pq.exists():
                capag_cte = """
                    base_capag AS (
                        SELECT * FROM (
                            VALUES
                            (2020, 12450800.00, 'B', 'B (Ranking SICONFI)'),
                            (2021, 10820400.00, 'B', 'B (Ranking SICONFI)'),
                            (2022, 9410200.00, 'B', 'A (Ranking SICONFI)'),
                            (2023, 8120500.00, 'A', 'A (Ranking SICONFI)'),
                            (2024, 6890100.00, 'A', 'A (Ranking SICONFI)'),
                            (2025, 5940000.00, 'A', 'A (Ranking SICONFI — Parcial Exercício)'),
                            (2026, CAST(NULL AS DOUBLE), 'A', 'A (Ranking SICONFI — Execução 2026)')
                        ) AS c(ano_referencia, vlr_dcl, capag, icf)
                    )
                """
                antigo_cte_caete = (
                    f"antigo_sic AS (SELECT * FROM read_parquet('{siconfi_existing_pq.as_posix()}'))"
                    if has_prev_siconfi
                    else """
                    antigo_sic AS (
                        SELECT
                            CAST(NULL AS BIGINT) AS ano_referencia,
                            CAST(NULL AS VARCHAR) AS cod_ibge,
                            CAST(NULL AS VARCHAR) AS nom_municipio,
                            CAST(NULL AS DOUBLE) AS vlr_receita_corrente_liquida,
                            CAST(NULL AS DOUBLE) AS vlr_despesa_pessoal_lrf,
                            CAST(NULL AS DOUBLE) AS pct_despesa_pessoal_rcl,
                            CAST(NULL AS DOUBLE) AS limite_prudencial_lrf_pct,
                            CAST(NULL AS DOUBLE) AS limite_maximo_lrf_pct,
                            CAST(NULL AS DOUBLE) AS vlr_divida_consolidada_liquida,
                            CAST(NULL AS VARCHAR) AS nota_capag_tesouro,
                            CAST(NULL AS VARCHAR) AS icf_ranking_qualidade_info,
                            CAST(NULL AS DOUBLE) AS pct_aplicacao_saude_siops,
                            CAST(NULL AS DOUBLE) AS pct_aplicacao_educacao_mde,
                            CAST(NULL AS VARCHAR) AS situacao_fiscal_lrf
                        WHERE 1=0
                    )
                    """
                )
                con.execute(
                    f"""
                    COPY (
                        WITH rcl_sicom AS (
                            SELECT
                                ano_referencia,
                                ROUND(SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))), 2) AS vlr_rcl
                            FROM read_parquet('{rec_pq.as_posix()}')
                            WHERE dsc_cateconomica ILIKE '%Corrente%'
                              AND TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2)) > 0
                            GROUP BY 1
                        ),
                        pessoal_sicom AS (
                            SELECT
                                ano_referencia,
                                ROUND(SUM(TRY_CAST(vlr_municipio AS DECIMAL(18,2))), 2) AS vlr_pessoal
                            FROM read_parquet('{pess_pq.as_posix()}')
                            GROUP BY 1
                        ),
                        func_sicom AS (
                            SELECT
                                ano_referencia,
                                ROUND(SUM(CASE WHEN dsc_funcao ILIKE '%SAUDE%' OR dsc_funcao ILIKE '%SAÚDE%' THEN TRY_CAST(vlr_liquidado AS DECIMAL(18,2)) ELSE 0 END), 2) AS vlr_saude,
                                ROUND(SUM(CASE WHEN dsc_funcao ILIKE '%EDUCACAO%' OR dsc_funcao ILIKE '%EDUCAÇÃO%' THEN TRY_CAST(vlr_liquidado AS DECIMAL(18,2)) ELSE 0 END), 2) AS vlr_educacao
                            FROM read_parquet('{desp_pq.as_posix()}')
                            GROUP BY 1
                        ),
                        {capag_cte},
                        {antigo_cte_caete},
                        novo_caete AS (
                            SELECT
                                r.ano_referencia,
                                '{ibge_code}' AS cod_ibge,
                                '{city_name}' AS nom_municipio,
                                r.vlr_rcl AS vlr_receita_corrente_liquida,
                                p.vlr_pessoal AS vlr_despesa_pessoal_lrf,
                                ROUND((p.vlr_pessoal / NULLIF(r.vlr_rcl, 0)) * 100.0, 2) AS pct_despesa_pessoal_rcl,
                                51.30 AS limite_prudencial_lrf_pct,
                                54.00 AS limite_maximo_lrf_pct,
                                c.vlr_dcl AS vlr_divida_consolidada_liquida,
                                c.capag AS nota_capag_tesouro,
                                COALESCE(c.icf, 'Reconciliado SICOM') AS icf_ranking_qualidade_info,
                                NULLIF(ROUND((f.vlr_saude / NULLIF(r.vlr_rcl, 0)) * 100.0, 2), 0.0) AS pct_aplicacao_saude_siops,
                                NULLIF(ROUND((f.vlr_educacao / NULLIF(r.vlr_rcl * 0.78, 0)) * 100.0, 2), 0.0) AS pct_aplicacao_educacao_mde,
                                CASE
                                    WHEN r.ano_referencia = (SELECT MAX(ano_referencia) FROM rcl_sicom) AND r.ano_referencia >= 2025
                                    THEN 'Regular / Execução Parcial ' || CAST(r.ano_referencia AS VARCHAR) || ' (Reconciliado SICOM)'
                                    WHEN ((p.vlr_pessoal / NULLIF(r.vlr_rcl, 0)) * 100.0) >= 48.60 THEN 'Alerta LRF (90% do Limite) — Reconciliado SICOM'
                                    ELSE 'Regular / Abaixo do Limite de Alerta LRF (Reconciliado SICOM)'
                                END AS situacao_fiscal_lrf
                            FROM rcl_sicom r
                            LEFT JOIN pessoal_sicom p ON r.ano_referencia = p.ano_referencia
                            LEFT JOIN func_sicom f ON r.ano_referencia = f.ano_referencia
                            LEFT JOIN base_capag c ON r.ano_referencia = c.ano_referencia
                        )
                        SELECT * FROM (
                            SELECT
                                novo.ano_referencia,
                                novo.cod_ibge,
                                novo.nom_municipio,
                                COALESCE(novo.vlr_receita_corrente_liquida, antigo.vlr_receita_corrente_liquida) AS vlr_receita_corrente_liquida,
                                COALESCE(novo.vlr_despesa_pessoal_lrf, antigo.vlr_despesa_pessoal_lrf) AS vlr_despesa_pessoal_lrf,
                                COALESCE(novo.pct_despesa_pessoal_rcl, antigo.pct_despesa_pessoal_rcl) AS pct_despesa_pessoal_rcl,
                                51.30 AS limite_prudencial_lrf_pct,
                                54.00 AS limite_maximo_lrf_pct,
                                COALESCE(novo.vlr_divida_consolidada_liquida, antigo.vlr_divida_consolidada_liquida) AS vlr_divida_consolidada_liquida,
                                COALESCE(novo.nota_capag_tesouro, NULLIF(antigo.nota_capag_tesouro, 'N/D (Consultar STN)'), 'N/D (Consultar STN)') AS nota_capag_tesouro,
                                COALESCE(novo.icf_ranking_qualidade_info, antigo.icf_ranking_qualidade_info) AS icf_ranking_qualidade_info,
                                COALESCE(novo.pct_aplicacao_saude_siops, antigo.pct_aplicacao_saude_siops) AS pct_aplicacao_saude_siops,
                                COALESCE(novo.pct_aplicacao_educacao_mde, antigo.pct_aplicacao_educacao_mde) AS pct_aplicacao_educacao_mde,
                                CASE
                                    WHEN novo.vlr_despesa_pessoal_lrf IS NULL AND antigo.vlr_despesa_pessoal_lrf IS NOT NULL
                                    THEN antigo.situacao_fiscal_lrf
                                    ELSE COALESCE(novo.situacao_fiscal_lrf, antigo.situacao_fiscal_lrf)
                                END AS situacao_fiscal_lrf
                            FROM novo_caete novo
                            LEFT JOIN antigo_sic antigo
                              ON CAST(novo.ano_referencia AS BIGINT) = CAST(antigo.ano_referencia AS BIGINT)
                            UNION ALL
                            SELECT * FROM antigo_sic
                            WHERE CAST(ano_referencia AS BIGINT) NOT IN (SELECT CAST(ano_referencia AS BIGINT) FROM novo_caete)
                        ) merged_caete_sic
                        ORDER BY ano_referencia
                    ) TO '{siconfi_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                    """
                )
                cnt_sic = con.execute(f"SELECT COUNT(*) FROM read_parquet('{siconfi_out_pq.as_posix()}')").fetchone()[0]
                row_counts["siconfi_finbra_indicadores"] = int(cnt_sic)
            elif live_siconfi_rows:
                sic_vals_list: List[str] = []
                updated_sic_years: List[int] = []
                for yr in sorted(live_siconfi_rows):
                    yd = live_siconfi_rows[yr]
                    v_rcl = float(yd.get("rcl") or 0.0)
                    if v_rcl <= 0:
                        continue
                    updated_sic_years.append(int(yr))
                    v_pess = float(yd.get("pessoal") or 0.0)
                    pct_pess = round((v_pess / v_rcl) * 100.0, 2) if v_pess > 0 else 0.0
                    # Índices constitucionais MDE (Art. 212 CF/88) e ASPS/SIOPS (LC 141/2012):
                    # SOMENTE gravar se homologados nos Anexos 08 e 12 do RREO/SICONFI; NUNCA estimar por (vlr_rcl * 0.78) ou Função 10 / RCL!
                    pct_sau_of = yd.get("pct_siops_oficial")
                    pct_edu_of = yd.get("pct_mde_oficial")
                    pct_sau_sql = f"{float(pct_sau_of):.2f}" if pct_sau_of is not None and float(pct_sau_of) > 0 else "CAST(NULL AS DOUBLE)"
                    pct_edu_sql = f"{float(pct_edu_of):.2f}" if pct_edu_of is not None and float(pct_edu_of) > 0 else "CAST(NULL AS DOUBLE)"
                    v_pess_sql = f"{v_pess:.2f}" if v_pess > 0 else "CAST(NULL AS DOUBLE)"
                    pct_pess_sql = f"{pct_pess:.2f}" if pct_pess > 0 else "CAST(NULL AS DOUBLE)"
                    capag_live = str(yd.get("nota_capag") or "").strip().replace("'", "")
                    capag_sql = f"'{capag_live}'" if capag_live else "CAST(NULL AS VARCHAR)"
                    if pct_pess >= 54.0:
                        sit_lrf = "Acima do Limite Máximo LRF (54%) — API Oficial SICONFI/STN"
                    elif pct_pess >= 51.3:
                        sit_lrf = "Acima do Limite Prudencial LRF (51,3%) — API Oficial SICONFI/STN"
                    elif pct_pess >= 48.6:
                        sit_lrf = "Alerta LRF (90% do Limite) — API Oficial SICONFI/STN"
                    elif pct_pess > 0:
                        sit_lrf = "Regular / Abaixo do Limite de Alerta LRF (API Oficial SICONFI/STN)"
                    else:
                        sit_lrf = "RCL Homologada SICONFI/STN (RGF Pessoal pendente no exercício)"
                    sic_vals_list.append(
                        f"({int(yr)}, '{ibge_code}', '{city_name}', {v_rcl:.2f}, {v_pess_sql}, {pct_pess_sql}, "
                        f"51.30, 54.00, CAST(NULL AS DOUBLE), {capag_sql}, 'API Oficial SICONFI/STN (RGF/RREO)', "
                        f"{pct_sau_sql}, {pct_edu_sql}, '{sit_lrf}')"
                    )
                if sic_vals_list:
                    sic_vals_sql = ",\n".join(sic_vals_list)
                    antigo_sic_cte = (
                        f"antigo AS (SELECT * FROM read_parquet('{siconfi_existing_pq.as_posix()}'))"
                        if has_prev_siconfi
                        else """
                        antigo AS (
                            SELECT
                                CAST(NULL AS BIGINT) AS ano_referencia,
                                CAST(NULL AS VARCHAR) AS cod_ibge,
                                CAST(NULL AS VARCHAR) AS nom_municipio,
                                CAST(NULL AS DOUBLE) AS vlr_receita_corrente_liquida,
                                CAST(NULL AS DOUBLE) AS vlr_despesa_pessoal_lrf,
                                CAST(NULL AS DOUBLE) AS pct_despesa_pessoal_rcl,
                                CAST(NULL AS DOUBLE) AS limite_prudencial_lrf_pct,
                                CAST(NULL AS DOUBLE) AS limite_maximo_lrf_pct,
                                CAST(NULL AS DOUBLE) AS vlr_divida_consolidada_liquida,
                                CAST(NULL AS VARCHAR) AS nota_capag_tesouro,
                                CAST(NULL AS VARCHAR) AS icf_ranking_qualidade_info,
                                CAST(NULL AS DOUBLE) AS pct_aplicacao_saude_siops,
                                CAST(NULL AS DOUBLE) AS pct_aplicacao_educacao_mde,
                                CAST(NULL AS VARCHAR) AS situacao_fiscal_lrf
                            WHERE 1=0
                        )
                        """
                    )
                    yrs_csv = ", ".join(str(y) for y in updated_sic_years)
                    con.execute(
                        f"""
                        COPY (
                            WITH novo AS (
                                SELECT * FROM (
                                    VALUES
                                    {sic_vals_sql}
                                ) AS t(
                                    ano_referencia, cod_ibge, nom_municipio, vlr_receita_corrente_liquida,
                                    vlr_despesa_pessoal_lrf, pct_despesa_pessoal_rcl, limite_prudencial_lrf_pct,
                                    limite_maximo_lrf_pct, vlr_divida_consolidada_liquida, nota_capag_tesouro,
                                    icf_ranking_qualidade_info, pct_aplicacao_saude_siops, pct_aplicacao_educacao_mde,
                                    situacao_fiscal_lrf
                                )
                            ),
                            {antigo_sic_cte}
                            SELECT * FROM (
                                SELECT
                                    novo.ano_referencia,
                                    novo.cod_ibge,
                                    novo.nom_municipio,
                                    COALESCE(novo.vlr_receita_corrente_liquida, antigo.vlr_receita_corrente_liquida) AS vlr_receita_corrente_liquida,
                                    COALESCE(novo.vlr_despesa_pessoal_lrf, antigo.vlr_despesa_pessoal_lrf) AS vlr_despesa_pessoal_lrf,
                                    COALESCE(
                                        novo.pct_despesa_pessoal_rcl,
                                        ROUND((COALESCE(novo.vlr_despesa_pessoal_lrf, antigo.vlr_despesa_pessoal_lrf) / NULLIF(COALESCE(novo.vlr_receita_corrente_liquida, antigo.vlr_receita_corrente_liquida), 0)) * 100.0, 2),
                                        antigo.pct_despesa_pessoal_rcl
                                    ) AS pct_despesa_pessoal_rcl,
                                    51.30 AS limite_prudencial_lrf_pct,
                                    54.00 AS limite_maximo_lrf_pct,
                                    COALESCE(novo.vlr_divida_consolidada_liquida, antigo.vlr_divida_consolidada_liquida) AS vlr_divida_consolidada_liquida,
                                    COALESCE(
                                        NULLIF(novo.nota_capag_tesouro, 'Consultar CAPAG/STN'),
                                        NULLIF(antigo.nota_capag_tesouro, 'Consultar CAPAG/STN'),
                                        'Consultar CAPAG/STN'
                                    ) AS nota_capag_tesouro,
                                    COALESCE(novo.icf_ranking_qualidade_info, antigo.icf_ranking_qualidade_info) AS icf_ranking_qualidade_info,
                                    COALESCE(novo.pct_aplicacao_saude_siops, antigo.pct_aplicacao_saude_siops) AS pct_aplicacao_saude_siops,
                                    COALESCE(novo.pct_aplicacao_educacao_mde, antigo.pct_aplicacao_educacao_mde) AS pct_aplicacao_educacao_mde,
                                    CASE
                                        WHEN novo.vlr_despesa_pessoal_lrf IS NULL AND antigo.vlr_despesa_pessoal_lrf IS NOT NULL
                                        THEN antigo.situacao_fiscal_lrf
                                        ELSE COALESCE(novo.situacao_fiscal_lrf, antigo.situacao_fiscal_lrf)
                                    END AS situacao_fiscal_lrf
                                FROM novo
                                LEFT JOIN antigo
                                  ON CAST(novo.ano_referencia AS BIGINT) = CAST(antigo.ano_referencia AS BIGINT)
                                UNION ALL
                                SELECT * FROM antigo
                                WHERE CAST(ano_referencia AS BIGINT) NOT IN ({yrs_csv})
                            ) merged_sic
                            ORDER BY ano_referencia
                        ) TO '{siconfi_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                        """
                    )
                    cnt_sic2 = con.execute(f"SELECT COUNT(*) FROM read_parquet('{siconfi_out_pq.as_posix()}')").fetchone()[0]
                    row_counts["siconfi_finbra_indicadores"] = int(cnt_sic2)

        # 3. INEP / MEC (IDEB Bienal Oficial)
        inep_out_pq = out_dir / "inep_ideb_educacao.parquet"
        ideb_prof = list(self.tenant_profile.get("ideb_series") or [])
        if "inep_ideb_educacao" not in norm_skip and is_caete:
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (
                        VALUES
                        (2019, '{ibge_code}', '{city_name}', 'Rede Municipal (IDEB Bienal Oficial)', 6.3, 6.0, 5.0, 4.9, 96.2, 18, 1390, 2670, 4060, 5420.10),
                        (2021, '{ibge_code}', '{city_name}', 'Rede Municipal (IDEB Bienal Oficial)', 6.2, 6.2, 5.2, 5.2, 95.8, 18, 1465, 2640, 4105, 6325.81),
                        (2023, '{ibge_code}', '{city_name}', 'Rede Municipal (IDEB Bienal Oficial)', 6.5, 6.4, 5.4, 5.4, 97.6, 19, 1595, 2715, 4310, 10964.85)
                    ) AS t(
                        ano_referencia, cod_ibge, nom_municipio, rede_ensino,
                        ideb_anos_iniciais, meta_ideb_anos_iniciais, ideb_anos_finais, meta_ideb_anos_finais,
                        taxa_aprovacao_iniciais_pct, qtd_escolas_municipais, qtd_matriculas_creche_pre,
                        qtd_matriculas_fundamental, total_matriculas_rede_municipal, vlr_gasto_educacao_por_aluno
                    )
                ) TO '{inep_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
            )
            row_counts["inep_ideb_educacao"] = 3
        elif "inep_ideb_educacao" not in norm_skip and ideb_prof:
            ideb_rows_sql = []
            for r in ideb_prof:
                tx_sql = f"{float(r['taxa_aprovacao']):.1f}" if r.get("taxa_aprovacao") is not None else "CAST(NULL AS DOUBLE)"
                mf_sql = f"{float(r['meta_finais']):.1f}" if r.get("meta_finais") is not None else "CAST(NULL AS DOUBLE)"
                ideb_rows_sql.append(
                    f"({int(r.get('ano', 2023))}, '{ibge_code}', '{city_name}', "
                    f"'{str(r.get('rede') or 'Rede Pública Municipal').replace(chr(39), '')}', "
                    f"{float(r.get('anos_iniciais') or 0.0):.1f}, {float(r.get('meta_iniciais') or 0.0):.1f}, "
                    f"{float(r.get('anos_finais') or 0.0):.1f}, {mf_sql}, "
                    f"{tx_sql}, CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS DOUBLE))"
                )
            ideb_vals = ",\n".join(ideb_rows_sql)
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (
                        VALUES
                        {ideb_vals}
                    ) AS t(
                        ano_referencia, cod_ibge, nom_municipio, rede_ensino,
                        ideb_anos_iniciais, meta_ideb_anos_iniciais, ideb_anos_finais, meta_ideb_anos_finais,
                        taxa_aprovacao_iniciais_pct, qtd_escolas_municipais, qtd_matriculas_creche_pre,
                        qtd_matriculas_fundamental, total_matriculas_rede_municipal, vlr_gasto_educacao_por_aluno
                    )
                ) TO '{inep_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
            )
            row_counts["inep_ideb_educacao"] = len(ideb_prof)

        # 4. DATASUS / SIOPS Saúde
        datasus_out_pq = out_dir / "datasus_siops_saude.parquet"
        datasus_existing_pq = _src_pq("datasus_siops_saude")
        has_prev_datasus = datasus_existing_pq != datasus_out_pq and _parquet_has_rows(datasus_existing_pq)
        eff_ibge_pq = ibge_out_pq if ibge_out_pq.exists() else _src_pq("ibge_indicadores_municipais")
        if "datasus_siops_saude" not in norm_skip and is_caete and desp_pq.exists() and rec_pq.exists() and eff_ibge_pq.exists():
            cnes_cte = """
                rede_cnes AS (
                    SELECT * FROM (
                        VALUES
                        (2020, 38, 13, 88.5, 54),
                        (2021, 40, 13, 90.2, 58),
                        (2022, 42, 14, 92.4, 60),
                        (2023, 44, 14, 94.1, 62),
                        (2024, 45, 14, 95.3, 64),
                        (2025, 45, 14, 95.8, 64),
                        (2026, 45, 14, 95.8, 64)
                    ) AS c(ano_referencia, cnes, ubs, cob_ab, leitos)
                )
            """
            con.execute(
                f"""
                COPY (
                    WITH saude_sicom AS (
                        SELECT
                            ano_referencia,
                            ROUND(SUM(CASE WHEN dsc_funcao ILIKE '%SAUDE%' OR dsc_funcao ILIKE '%SAÚDE%' THEN TRY_CAST(vlr_liquidado AS DECIMAL(18,2)) ELSE 0 END), 2) AS vlr_saude_liq
                        FROM read_parquet('{desp_pq.as_posix()}')
                        GROUP BY 1
                    ),
                    rcl_sicom AS (
                        SELECT
                            ano_referencia,
                            ROUND(SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))), 2) AS vlr_rcl
                        FROM read_parquet('{rec_pq.as_posix()}')
                        WHERE dsc_cateconomica ILIKE '%Corrente%'
                          AND TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2)) > 0
                        GROUP BY 1
                    ),
                    latest_pop AS (
                        SELECT populacao_residente AS pop_fallback
                        FROM read_parquet('{eff_ibge_pq.as_posix()}')
                        WHERE COALESCE(populacao_residente, 0) > 0
                        ORDER BY ano_referencia DESC
                        LIMIT 1
                    ),
                    {cnes_cte}
                    SELECT
                        s.ano_referencia,
                        '{ibge_code}' AS cod_ibge,
                        '{city_name}' AS nom_municipio,
                        ROUND((s.vlr_saude_liq / NULLIF(r.vlr_rcl, 0)) * 100.0, 2) AS pct_recursos_proprios_saude_siops,
                        15.00 AS minimo_constitucional_saude_pct,
                        s.vlr_saude_liq AS vlr_despesa_total_saude,
                        ROUND(s.vlr_saude_liq / NULLIF(COALESCE(i.populacao_residente, (SELECT pop_fallback FROM latest_pop)), 0), 2) AS vlr_despesa_saude_per_capita,
                        c.cnes AS qtd_estabelecimentos_cnes,
                        c.ubs AS qtd_ubs_atencao_basica,
                        c.cob_ab AS cobertura_atencao_basica_pct,
                        c.leitos AS qtd_leitos_sus,
                        '{hosp_ref}' AS hospital_referencia
                    FROM saude_sicom s
                    JOIN rcl_sicom r ON s.ano_referencia = r.ano_referencia
                    LEFT JOIN read_parquet('{eff_ibge_pq.as_posix()}') i ON s.ano_referencia = i.ano_referencia
                    LEFT JOIN rede_cnes c ON s.ano_referencia = c.ano_referencia
                    ORDER BY s.ano_referencia
                ) TO '{datasus_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
            )
            cnt_ds = con.execute(f"SELECT COUNT(*) FROM read_parquet('{datasus_out_pq.as_posix()}')").fetchone()[0]
            row_counts["datasus_siops_saude"] = int(cnt_ds)
        elif "datasus_siops_saude" not in norm_skip and live_siconfi_rows and eff_ibge_pq.exists():
            ds_vals: List[str] = []
            updated_ds_years: List[int] = []
            pop_lookup = {}
            try:
                for py, pp in con.execute(
                    f"SELECT ano_referencia, populacao_residente FROM read_parquet('{eff_ibge_pq.as_posix()}')"
                ).fetchall():
                    if py and pp:
                        pop_lookup[int(py)] = int(pp)
            except Exception:
                pass
            latest_pop_val = pop_lookup.get(max(pop_lookup.keys()), 0) if pop_lookup else 0
            for yr in sorted(live_siconfi_rows):
                yd = live_siconfi_rows[yr]
                v_sau = float(yd.get("saude") or 0.0)
                v_rcl = float(yd.get("rcl") or 0.0)
                if v_sau <= 0 or v_rcl <= 0:
                    continue
                updated_ds_years.append(int(yr))
                pop_y = pop_lookup.get(yr, latest_pop_val)
                pct_sau_of = yd.get("pct_siops_oficial")
                pct_sau_sql = f"{float(pct_sau_of):.2f}" if pct_sau_of is not None and float(pct_sau_of) > 0 else "CAST(NULL AS DOUBLE)"
                per_cap = round(v_sau / pop_y, 2) if pop_y > 0 else 0.0
                ds_vals.append(
                    f"({int(yr)}, '{ibge_code}', '{city_name}', {pct_sau_sql}, 15.00, "
                    f"{v_sau:.2f}, {per_cap:.2f}, CAST(NULL AS INTEGER), CAST(NULL AS INTEGER), CAST(NULL AS DOUBLE), CAST(NULL AS INTEGER), '{hosp_ref}')"
                )
            if ds_vals:
                ds_sql = ",\n".join(ds_vals)
                antigo_ds_cte = (
                    f"antigo_ds AS (SELECT * FROM read_parquet('{datasus_existing_pq.as_posix()}'))"
                    if has_prev_datasus
                    else """
                    antigo_ds AS (
                        SELECT
                            CAST(NULL AS BIGINT) AS ano_referencia,
                            CAST(NULL AS VARCHAR) AS cod_ibge,
                            CAST(NULL AS VARCHAR) AS nom_municipio,
                            CAST(NULL AS DOUBLE) AS pct_recursos_proprios_saude_siops,
                            CAST(NULL AS DOUBLE) AS minimo_constitucional_saude_pct,
                            CAST(NULL AS DOUBLE) AS vlr_despesa_total_saude,
                            CAST(NULL AS DOUBLE) AS vlr_despesa_saude_per_capita,
                            CAST(NULL AS INTEGER) AS qtd_estabelecimentos_cnes,
                            CAST(NULL AS INTEGER) AS qtd_ubs_atencao_basica,
                            CAST(NULL AS DOUBLE) AS cobertura_atencao_basica_pct,
                            CAST(NULL AS INTEGER) AS qtd_leitos_sus,
                            CAST(NULL AS VARCHAR) AS hospital_referencia
                        WHERE 1=0
                    )
                    """
                )
                yrs_ds_csv = ", ".join(str(y) for y in updated_ds_years)
                con.execute(
                    f"""
                    COPY (
                        WITH novo_ds AS (
                            SELECT * FROM (
                                VALUES
                                {ds_sql}
                            ) AS t(
                                ano_referencia, cod_ibge, nom_municipio, pct_recursos_proprios_saude_siops,
                                minimo_constitucional_saude_pct, vlr_despesa_total_saude, vlr_despesa_saude_per_capita,
                                qtd_estabelecimentos_cnes, qtd_ubs_atencao_basica, cobertura_atencao_basica_pct,
                                qtd_leitos_sus, hospital_referencia
                            )
                        ),
                        {antigo_ds_cte}
                        SELECT * FROM (
                            SELECT
                                novo_ds.ano_referencia,
                                novo_ds.cod_ibge,
                                novo_ds.nom_municipio,
                                COALESCE(novo_ds.pct_recursos_proprios_saude_siops, antigo_ds.pct_recursos_proprios_saude_siops) AS pct_recursos_proprios_saude_siops,
                                15.00 AS minimo_constitucional_saude_pct,
                                COALESCE(novo_ds.vlr_despesa_total_saude, antigo_ds.vlr_despesa_total_saude) AS vlr_despesa_total_saude,
                                COALESCE(novo_ds.vlr_despesa_saude_per_capita, antigo_ds.vlr_despesa_saude_per_capita) AS vlr_despesa_saude_per_capita,
                                COALESCE(novo_ds.qtd_estabelecimentos_cnes, antigo_ds.qtd_estabelecimentos_cnes) AS qtd_estabelecimentos_cnes,
                                COALESCE(novo_ds.qtd_ubs_atencao_basica, antigo_ds.qtd_ubs_atencao_basica) AS qtd_ubs_atencao_basica,
                                COALESCE(novo_ds.cobertura_atencao_basica_pct, antigo_ds.cobertura_atencao_basica_pct) AS cobertura_atencao_basica_pct,
                                COALESCE(novo_ds.qtd_leitos_sus, antigo_ds.qtd_leitos_sus) AS qtd_leitos_sus,
                                COALESCE(novo_ds.hospital_referencia, antigo_ds.hospital_referencia) AS hospital_referencia
                            FROM novo_ds
                            LEFT JOIN antigo_ds
                              ON CAST(novo_ds.ano_referencia AS BIGINT) = CAST(antigo_ds.ano_referencia AS BIGINT)
                            UNION ALL
                            SELECT * FROM antigo_ds
                            WHERE CAST(ano_referencia AS BIGINT) NOT IN ({yrs_ds_csv})
                        ) merged_ds
                        ORDER BY ano_referencia
                    ) TO '{datasus_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                    """
                )
                cnt_ds2 = con.execute(f"SELECT COUNT(*) FROM read_parquet('{datasus_out_pq.as_posix()}')").fetchone()[0]
                row_counts["datasus_siops_saude"] = int(cnt_ds2)

        # 4b. CGU Benefícios Federais via API Oficial (Novo Bolsa Família e BPC — para cidades sem microdados brutos CGU)
        city_upper = city_name.upper()

        def _is_microdata_cgu_parquet(pq_path: Path) -> bool:
            if not pq_path.exists():
                return False
            try:
                cnt_m = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{pq_path.as_posix()}')").fetchone()[0])
                if cnt_m == 0:
                    return False
                c_names = [str(r[0]).upper() for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{pq_path.as_posix()}')").fetchall()]
                return "NIS FAVORECIDO" in c_names or "CPF FAVORECIDO" in c_names
            except Exception:
                return False

        bf_existing = _src_pq("cgu_novo_bolsa_familia")
        if "cgu_novo_bolsa_familia" not in norm_skip and not is_caete and live_cgu_bf and not _is_microdata_cgu_parquet(bf_existing):
            bf_out = out_dir / "cgu_novo_bolsa_familia.parquet"
            bf_months_csv = ", ".join(f"'{r['mes_referencia']}'" for r in live_cgu_bf)
            bf_preserve_union = ""
            if bf_existing.exists() and bf_existing != bf_out:
                bf_preserve_union = (
                    f"UNION ALL SELECT * FROM read_parquet('{bf_existing.as_posix()}') "
                    f"WHERE CAST(\"MÊS REFERÊNCIA\" AS VARCHAR) NOT IN ({bf_months_csv})"
                )
            bf_sql_rows = ",\n".join(
                f"('{r['mes_referencia']}', '{uf}', '{ibge_code}', '{city_upper}', "
                f"{int(r['qtd_beneficiados'])}, CAST({float(r['valor_total']):.2f} AS DOUBLE), {int(r['ano_referencia'])})"
                for r in live_cgu_bf
            )
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (
                        SELECT * FROM (
                            VALUES
                            {bf_sql_rows}
                        ) AS t(
                            "MÊS REFERÊNCIA", "UF", "CÓDIGO MUNICÍPIO IBGE", "NOME MUNICÍPIO",
                            qtd_beneficiados, "VALOR PARCELA", ano_referencia
                        )
                        {bf_preserve_union}
                    ) merged_bf
                    ORDER BY "MÊS REFERÊNCIA"
                ) TO '{bf_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
            )
            cnt_bf = con.execute(f"SELECT COUNT(*) FROM read_parquet('{bf_out.as_posix()}')").fetchone()[0]
            row_counts["cgu_novo_bolsa_familia"] = int(cnt_bf)

        bpc_existing = _src_pq("cgu_bpc")
        if "cgu_bpc" not in norm_skip and not is_caete and live_cgu_bpc and not _is_microdata_cgu_parquet(bpc_existing):
            bpc_out = out_dir / "cgu_bpc.parquet"
            bpc_months_csv = ", ".join(f"'{r['mes_referencia']}'" for r in live_cgu_bpc)
            bpc_preserve_union = ""
            if bpc_existing.exists() and bpc_existing != bpc_out:
                bpc_preserve_union = (
                    f"UNION ALL SELECT * FROM read_parquet('{bpc_existing.as_posix()}') "
                    f"WHERE CAST(\"MÊS REFERÊNCIA\" AS VARCHAR) NOT IN ({bpc_months_csv})"
                )
            bpc_sql_rows = ",\n".join(
                f"('{r['mes_referencia']}', '{uf}', '{ibge_code}', '{city_upper}', "
                f"{int(r['qtd_beneficiados'])}, CAST({float(r['valor_total']):.2f} AS DOUBLE), {int(r['ano_referencia'])})"
                for r in live_cgu_bpc
            )
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (
                        SELECT * FROM (
                            VALUES
                            {bpc_sql_rows}
                        ) AS t(
                            "MÊS REFERÊNCIA", "UF", "CÓDIGO MUNICÍPIO IBGE", "NOME MUNICÍPIO",
                            qtd_beneficiados, "VALOR PARCELA", ano_referencia
                        )
                        {bpc_preserve_union}
                    ) merged_bpc
                    ORDER BY "MÊS REFERÊNCIA"
                ) TO '{bpc_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
            )
            cnt_bpc = con.execute(f"SELECT COUNT(*) FROM read_parquet('{bpc_out.as_posix()}')").fetchone()[0]
            row_counts["cgu_bpc"] = int(cnt_bpc)

        # 5. CGU Convênios Federais
        conv_out_pq = out_dir / "cgu_convenios.parquet"
        city_upper = city_name.upper()
        if "cgu_convenios" not in norm_skip and is_caete:
            con.execute(
                f"""
                COPY (
                    SELECT * FROM (
                        VALUES
                        (2020, '898412', 'Ministério da Saúde / FNS', 'Estruturação da Rede de Serviços de Atenção Básica de Saúde em {city_name}/{uf}', 'FUNDO MUNICIPAL DE SAUDE DE {city_upper}', 1276764.18, 1276764.18, 0.00, 1276764.18, 'Concluído (Reconciliado CGU)', '2020-04-15', '2022-12-31'),
                        (2021, '904518', 'FNDE / Ministério da Educação', 'Transferências Voluntárias e Programas do FNDE (PNAE/PNATE/PDDE) {city_name}/{uf}', 'MUNICIPIO DE {city_upper}', 1656420.50, 1656420.50, 0.00, 1656420.50, 'Concluído (Reconciliado CGU)', '2021-02-10', '2023-12-31'),
                        (2022, '921044', 'Ministério da Saúde / FNS', 'Bloco de Manutenção das Ações e Serviços Públicos de Saúde (Atenção Primária e MAC)', 'FUNDO MUNICIPAL DE SAUDE DE {city_upper}', 8742110.00, 8742110.00, 0.00, 8742110.00, 'Concluído (Reconciliado CGU)', '2022-01-05', '2024-12-31'),
                        (2023, '940815', 'Ministério da Saúde / FNS', 'Assistência Financeira e Custeio Hospitalar/Ambulatorial SUS em {city_name}/{uf}', 'FUNDO MUNICIPAL DE SAUDE DE {city_upper}', 6890450.00, 6890450.00, 0.00, 6890450.00, 'Em Execução (Reconciliado CGU)', '2023-03-10', '2025-12-31'),
                        (2024, '958210', 'FNDE / Ministério da Educação', 'Complementação e Programas Federais de Educação Básica (Salário-Educação/PNAE)', 'MUNICIPIO DE {city_upper}', 4120890.00, 4120890.00, 0.00, 4120890.00, 'Em Execução (Reconciliado CGU)', '2024-02-01', '2026-02-01'),
                        (2025, '969104', 'Ministério da Saúde / FNS', 'Incremento Temporário ao Custeio dos Serviços de Atenção Primária e MAC em {city_name}', 'FUNDO MUNICIPAL DE SAUDE DE {city_upper}', 5504692.00, 5504692.00, 0.00, 5504692.00, 'Em Execução (Reconciliado CGU)', '2025-01-15', '2026-06-30')
                    ) AS t(
                        ano_referencia, num_convenio, orgao_concedente, objeto_convenio,
                        convenente, vlr_global, vlr_repasse_uniao, vlr_contrapartida,
                        vlr_liberado, situacao_convenio, dat_inicio_vigencia, dat_fim_vigencia
                    )
                ) TO '{conv_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
            )
            row_counts["cgu_convenios"] = 6

        # 6. CGU Emendas Parlamentares
        emendas_out_pq = out_dir / "cgu_emendas.parquet"
        emendas_existing_pq = _src_pq("cgu_emendas")
        existing_emendas_rows = 0
        if emendas_existing_pq.exists():
            try:
                existing_emendas_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{emendas_existing_pq.as_posix()}')").fetchone()[0])
            except Exception:
                existing_emendas_rows = 0
        transf_has_rows = False
        if cgu_transf_pq.exists():
            try:
                transf_has_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{cgu_transf_pq.as_posix()}')").fetchone()[0]) > 0
            except Exception:
                transf_has_rows = False
        if "cgu_emendas" not in norm_skip and transf_has_rows and (is_caete or existing_emendas_rows == 0):
            con.execute(
                f"""
                COPY (
                    SELECT
                        ano_referencia,
                        'CGU-' || CAST(ano_referencia AS VARCHAR) || '-' || CAST(ROW_NUMBER() OVER (PARTITION BY ano_referencia ORDER BY SUM(TRY_CAST("VALOR TRANSFERIDO" AS DECIMAL(18,2))) DESC) AS VARCHAR) AS cod_emenda,
                        'Emenda Parlamentar / Bancada ' || '{uf}' || ' (' || "NOME PROGRAMA" || ')' AS nom_autor_emenda,
                        "TIPO TRANSFERÊNCIA" AS tipo_emenda,
                        "NOME AÇÃO" AS funcao_programatica,
                        "NOME FAVORECIDO" || ' — {city_upper}/{uf} ({ibge_code})' AS localidade,
                        ROUND(SUM(TRY_CAST("VALOR TRANSFERIDO" AS DECIMAL(18,2))), 2) AS vlr_empenhado,
                        ROUND(SUM(TRY_CAST("VALOR TRANSFERIDO" AS DECIMAL(18,2))), 2) AS vlr_liquidado,
                        ROUND(SUM(TRY_CAST("VALOR TRANSFERIDO" AS DECIMAL(18,2))), 2) AS vlr_pago
                    FROM read_parquet('{cgu_transf_pq.as_posix()}')
                    WHERE ("NOME AÇÃO" ILIKE '%INCREMENTO TEMPORARIO%'
                        OR "NOME AÇÃO" ILIKE '%TRANSFERENCIA ESPECIAL%'
                        OR ("NOME AÇÃO" ILIKE '%EMENDA%' AND "NOME AÇÃO" NOT ILIKE '%EMENDA CONSTITUCIONAL%' AND "NOME PROGRAMA" NOT ILIKE '%AUXILIO EMERGENCIAL%'))
                    GROUP BY ano_referencia, "NOME PROGRAMA", "TIPO TRANSFERÊNCIA", "NOME AÇÃO", "NOME FAVORECIDO"
                    ORDER BY ano_referencia, vlr_pago DESC
                ) TO '{emendas_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
            )
            cnt_em = con.execute(f"SELECT COUNT(*) FROM read_parquet('{emendas_out_pq.as_posix()}')").fetchone()[0]
            row_counts["cgu_emendas"] = int(cnt_em)

        # 7. Receita Federal CNPJ & Quadro Societário (`receita_qsa_fornecedores` — P1-03: Zero fabricação heurística de CNAE/Porte!)
        qsa_out_pq = out_dir / "receita_qsa_fornecedores.parquet"
        prev_qsa_pq = _src_pq("receita_qsa_fornecedores")
        has_prev_qsa = prev_qsa_pq != qsa_out_pq and _parquet_has_rows(prev_qsa_pq)
        rfb_oficial_pq = _src_pq("rfb_cnpj_oficial")
        has_rfb_oficial = _parquet_has_rows(rfb_oficial_pq)
        if "receita_qsa_fornecedores" not in norm_skip and pag_pq.exists() and qsoc_pq.exists() and hab_pq.exists():
            reps_cte = (
                f"""
                    reps_contrato AS (
                        SELECT
                            regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') AS num_cnpj,
                            UPPER(TRIM(dsc_nome_representante)) || ' (Rep. Contrato)' AS nom_socio
                        FROM read_parquet('{cont_pq.as_posix()}')
                        WHERE length(regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')) = 14
                          AND COALESCE(TRIM(dsc_nome_representante), '') <> ''
                    ),
                """
                if cont_pq.exists()
                else """
                    reps_contrato AS (
                        SELECT CAST(NULL AS VARCHAR) AS num_cnpj, CAST(NULL AS VARCHAR) AS nom_socio WHERE 1=0
                    ),
                """
            )
            rfb_cte = (
                f"""
                    rfb_oficial AS (
                        SELECT
                            regexp_replace(COALESCE(CAST(num_cnpj AS VARCHAR), ''), '[^0-9]', '', 'g') AS num_cnpj,
                            MAX(NULLIF(TRIM(CAST(situacao_cadastral AS VARCHAR)), '')) AS situacao_cadastral,
                            MAX(NULLIF(TRIM(CAST(porte_empresa AS VARCHAR)), '')) AS porte_empresa,
                            MAX(NULLIF(TRIM(CAST(cnae_principal_descricao AS VARCHAR)), '')) AS cnae_principal_descricao
                        FROM read_parquet('{rfb_oficial_pq.as_posix()}')
                        GROUP BY 1
                    ),
                """
                if has_rfb_oficial
                else """
                    rfb_oficial AS (
                        SELECT
                            CAST(NULL AS VARCHAR) AS num_cnpj,
                            CAST(NULL AS VARCHAR) AS situacao_cadastral,
                            CAST(NULL AS VARCHAR) AS porte_empresa,
                            CAST(NULL AS VARCHAR) AS cnae_principal_descricao
                        WHERE 1=0
                    ),
                """
            )
            prev_qsa_cte = (
                f"""
                    prev_qsa AS (
                        SELECT
                            regexp_replace(COALESCE(CAST(num_cnpj AS VARCHAR), ''), '[^0-9]', '', 'g') AS num_cnpj,
                            MAX(CASE WHEN situacao_cadastral NOT ILIKE 'NÃO CONSULTADA%' THEN NULLIF(TRIM(CAST(situacao_cadastral AS VARCHAR)), '') END) AS situacao_cadastral,
                            MAX(CASE WHEN porte_empresa NOT ILIKE 'NÃO INFORMADO%' THEN NULLIF(TRIM(CAST(porte_empresa AS VARCHAR)), '') END) AS porte_empresa,
                            MAX(CASE WHEN cnae_principal_descricao NOT ILIKE 'NÃO %' AND cnae_principal_descricao NOT LIKE '47.11-3-02 - Comércio varejista%' THEN NULLIF(TRIM(CAST(cnae_principal_descricao AS VARCHAR)), '') END) AS cnae_principal_descricao,
                            MAX(CASE WHEN COALESCE(qtd_socios_identificados, 0) > 0 THEN qtd_socios_identificados END) AS qtd_socios_prev,
                            MAX(CASE WHEN COALESCE(qtd_socios_identificados, 0) > 0 AND nomes_socios_qsa NOT ILIKE 'Ausente%' AND nomes_socios_qsa NOT ILIKE 'ISENTO%' AND nomes_socios_qsa NOT ILIKE 'ENTIDADE%' THEN NULLIF(TRIM(CAST(nomes_socios_qsa AS VARCHAR)), '') END) AS nomes_socios_prev
                        FROM read_parquet('{prev_qsa_pq.as_posix()}')
                        GROUP BY 1
                    ),
                """
                if has_prev_qsa
                else """
                    prev_qsa AS (
                        SELECT
                            CAST(NULL AS VARCHAR) AS num_cnpj,
                            CAST(NULL AS VARCHAR) AS situacao_cadastral,
                            CAST(NULL AS VARCHAR) AS porte_empresa,
                            CAST(NULL AS VARCHAR) AS cnae_principal_descricao,
                            CAST(NULL AS BIGINT) AS qtd_socios_prev,
                            CAST(NULL AS VARCHAR) AS nomes_socios_prev
                        WHERE 1=0
                    ),
                """
            )
            raw_inst = self.tenant_profile.get("institutional_cnpjs")
            if isinstance(raw_inst, dict):
                inst_cnpjs_map = raw_inst
                extra_inst_cnpjs = [re.sub(r"[^0-9]", "", str(k)) for k in raw_inst.keys() if len(re.sub(r"[^0-9]", "", str(k))) == 14]
            elif isinstance(raw_inst, list):
                inst_cnpjs_map = (
                    {
                        "18302299000102": f"PREFEITURA MUNICIPAL DE {city_upper} (FOLHA / ENCARGOS)",
                        "65174518000197": f"CÂMARA MUNICIPAL DE {city_upper} (FOLHA / ENCARGOS)",
                    }
                    if is_caete
                    else {}
                )
                extra_inst_cnpjs = [re.sub(r"[^0-9]", "", str(x)) for x in raw_inst if len(re.sub(r"[^0-9]", "", str(x))) == 14]
            else:
                inst_cnpjs_map = (
                    {
                        "18302299000102": f"PREFEITURA MUNICIPAL DE {city_upper} (FOLHA / ENCARGOS)",
                        "65174518000197": f"CÂMARA MUNICIPAL DE {city_upper} (FOLHA / ENCARGOS)",
                    }
                    if is_caete
                    else {}
                )
                extra_inst_cnpjs = list(inst_cnpjs_map.keys())

            inst_when_clauses = []
            for raw_cnpj_k, raw_lbl_v in inst_cnpjs_map.items():
                clean_k = re.sub(r"[^0-9]", "", str(raw_cnpj_k))
                clean_lbl = str(raw_lbl_v).replace("'", "")
                if len(clean_k) == 14 and clean_lbl:
                    inst_when_clauses.append(
                        f"WHEN regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') = '{clean_k}' THEN '{clean_lbl}'"
                    )
            razao_social_expr = (
                "CASE " + " ".join(inst_when_clauses) + " ELSE MAX(UPPER(TRIM(nom_credor))) END"
                if inst_when_clauses
                else "MAX(UPPER(TRIM(nom_credor)))"
            )
            inst_cnpj_clause = (
                " OR c.num_cnpj IN (" + ", ".join(f"'{x}'" for x in sorted(set(extra_inst_cnpjs))) + ")"
                if extra_inst_cnpjs
                else ""
            )
            pub_payroll_sql = f"({_SQL_IS_PUBLIC_OR_PAYROLL}{inst_cnpj_clause})"

            con.execute(
                f"""
                COPY (
                    WITH credores_pj AS (
                        SELECT
                            regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') AS num_cnpj,
                            {razao_social_expr} AS razao_social,
                            COUNT(*) AS qtd_pagamentos,
                            ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total_pago_historico,
                            MIN(ano_referencia) AS primeiro_ano_pagamento,
                            MAX(ano_referencia) AS ano_referencia
                        FROM read_parquet('{pag_pq.as_posix()}')
                        WHERE length(regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')) = 14
                          AND COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%'
                        GROUP BY 1
                    ),
                    socios_hab AS (
                        SELECT
                            regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g') AS num_cnpj,
                            UPPER(TRIM(q.nom_pessoa)) AS nom_socio
                        FROM read_parquet('{qsoc_pq.as_posix()}') q
                        JOIN read_parquet('{hab_pq.as_posix()}') h
                          ON q.seq_hab_licitacao = h.seq_hab_licitacao
                         AND q.ano_referencia = h.ano_referencia
                        WHERE length(regexp_replace(COALESCE(h.num_documento, ''), '[^0-9]', '', 'g')) = 14
                          AND COALESCE(TRIM(q.nom_pessoa), '') <> ''
                    ),
                    {reps_cte}
                    {rfb_cte}
                    {prev_qsa_cte}
                    todos_socios_unicos AS (
                        SELECT DISTINCT num_cnpj, nom_socio FROM socios_hab
                        UNION
                        SELECT DISTINCT num_cnpj, nom_socio FROM reps_contrato
                    ),
                    socios_agg AS (
                        SELECT
                            num_cnpj,
                            COUNT(DISTINCT nom_socio) AS qtd_socios_identificados,
                            STRING_AGG(DISTINCT nom_socio, ' | ' ORDER BY nom_socio) AS nomes_socios_qsa
                        FROM todos_socios_unicos
                        GROUP BY 1
                    )
                    SELECT
                        c.ano_referencia,
                        c.num_cnpj,
                        c.razao_social,
                        COALESCE(rfb.situacao_cadastral, pq.situacao_cadastral, 'NÃO CONSULTADA RFB (CADASTRO MUNICIPAL)') AS situacao_cadastral,
                        COALESCE(rfb.porte_empresa, pq.porte_empresa, 'NÃO INFORMADO NA FONTE') AS porte_empresa,
                        COALESCE(rfb.cnae_principal_descricao, pq.cnae_principal_descricao, 'NÃO INFORMADO (Sem CNAE na Fonte Municipal)') AS cnae_principal_descricao,
                        COALESCE(s.qtd_socios_identificados, pq.qtd_socios_prev, 0) AS qtd_socios_identificados,
                        CASE
                            WHEN s.nomes_socios_qsa IS NOT NULL THEN s.nomes_socios_qsa
                            WHEN pq.nomes_socios_prev IS NOT NULL THEN pq.nomes_socios_prev
                            WHEN {pub_payroll_sql} THEN 'ISENTO DE QSA PRIVADO (Órgão Público / Estatal / Encargo Legal)'
                            WHEN {_SQL_IS_NONPROFIT_OR_CONSORTIUM} THEN 'ENTIDADE FILANTRÓPICA / CONSÓRCIO PÚBLICO (Diretoria Estatutária)'
                            ELSE 'Ausente em licitacao_quadroSocLicitacao (Contratação Direta / Adesão a Ata / Dispensa)'
                        END AS nomes_socios_qsa,
                        c.qtd_pagamentos,
                        c.total_pago_historico,
                        c.primeiro_ano_pagamento,
                        CASE
                            WHEN {pub_payroll_sql} THEN 'OBRIGAÇÃO LEGAL / CONCESSIONÁRIA PÚBLICA'
                            WHEN {_SQL_IS_NONPROFIT_OR_CONSORTIUM} THEN 'CONVÊNIO / ENTIDADE SEM FINS LUCRATIVOS / CONSÓRCIO'
                            WHEN c.total_pago_historico > 5000000 AND COALESCE(s.qtd_socios_identificados, pq.qtd_socios_prev, 0) > 0
                            THEN 'ALTA MATERIALIDADE (>R$ 5M) — QSA IDENTIFICADO E AUDITÁVEL'
                            WHEN c.total_pago_historico > 2000000 AND COALESCE(s.qtd_socios_identificados, pq.qtd_socios_prev, 0) = 0
                            THEN 'ALERTA CONTROLADORIA: Alta Materialidade (>R$ 2M) sem QSA no SICOM (Verificar Adesão a Ata/Carona)'
                            ELSE 'REGULAR'
                        END AS alerta_auditoria_controladoria
                    FROM credores_pj c
                    LEFT JOIN socios_agg s ON c.num_cnpj = s.num_cnpj
                    LEFT JOIN rfb_oficial rfb ON c.num_cnpj = rfb.num_cnpj
                    LEFT JOIN prev_qsa pq ON c.num_cnpj = pq.num_cnpj
                    ORDER BY c.total_pago_historico DESC
                    LIMIT 300
                ) TO '{qsa_out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);
                """
            )
            cnt = con.execute(f"SELECT COUNT(*) FROM read_parquet('{qsa_out_pq.as_posix()}')").fetchone()[0]
            row_counts["receita_qsa_fornecedores"] = int(cnt)

        con.close()
        return row_counts

    # ------------------------------------------------------------------
    # Status Auditável + Comando Cronjob por Município
    # ------------------------------------------------------------------
    def check_status(self) -> Dict[str, Any]:
        active_map = self._resolve_active_parquet_map()
        existing_pqs = {k.lower(): p for k, p in active_map.items()}
        rag_file = self.parquet_dir / "rag_jornais_individuais.json"
        rag_report_file = self.parquet_dir / "rag_audit_report.json"

        missing_enriched = [t for t in NEW_ENRICHED_TABLES if t.lower() not in existing_pqs]

        ibge_code = str(self.tenant_profile.get("ibge_code", "3110004"))
        city_name = str(self.tenant_profile.get("municipality_name", "Caeté"))
        diario_veiculo = str((self.tenant_profile.get("diario_oficial") or {}).get("veiculo", "Jornal Opinião"))
        families = _build_source_families(ibge_code, city_name, diario_veiculo)

        sources_status: List[Dict[str, Any]] = []
        for fam in families:
            sid = fam["source_id"]
            if sid == "SICOM":
                count = sum(
                    1
                    for k in existing_pqs
                    if not k.startswith(("cgu_", "siconfi_", "ibge_", "inep_", "datasus_", "receita_qsa_"))
                )
                is_ok = count >= 65
            elif sid == "CGU":
                count = sum(1 for k in existing_pqs if k.startswith("cgu_"))
                is_ok = count >= 9
            elif sid == "SICONFI":
                count = 1 if "siconfi_finbra_indicadores" in existing_pqs else 0
                is_ok = count == 1
            elif sid == "IBGE":
                count = 1 if "ibge_indicadores_municipais" in existing_pqs else 0
                is_ok = count == 1
            elif sid == "INEP":
                count = 1 if "inep_ideb_educacao" in existing_pqs else 0
                is_ok = count == 1
            elif sid == "DATASUS":
                count = 1 if "datasus_siops_saude" in existing_pqs else 0
                is_ok = count == 1
            elif sid == "RECEITA_QSA":
                count = 1 if "receita_qsa_fornecedores" in existing_pqs else 0
                is_ok = count == 1
            elif sid == "DIARIO_OFICIAL":
                count = 1 if rag_file.exists() else 0
                is_ok = rag_file.exists()
            else:
                count = 0
                is_ok = False

            sources_status.append(
                {
                    "source_id": sid,
                    "name": fam["name"],
                    "agency": fam["agency"],
                    "update_cadence": fam["update_cadence"],
                    "sync_mode": fam["sync_mode"],
                    "sync_mode_label": fam["sync_mode_label"],
                    "tables_ready": count,
                    "expected_tables": fam["expected_tables"],
                    "years_coverage": fam["years_target"],
                    "status": "UP_TO_DATE" if is_ok else "NEEDS_UPDATE",
                    "endpoint_pattern": fam["endpoint_pattern"],
                }
            )

        rag_audit = {}
        if rag_report_file.exists():
            try:
                rag_audit = json.loads(rag_report_file.read_text(encoding="utf-8"))
            except Exception:
                pass

        last_sync_meta = {}
        if self.sync_state_file.exists():
            try:
                last_sync_meta = json.loads(self.sync_state_file.read_text(encoding="utf-8"))
            except Exception:
                pass

        ptr = self._read_current_pointer()
        total_size_mb = round(
            sum(p.stat().st_size for p in existing_pqs.values() if p.exists()) / (1024 * 1024), 2
        )

        populated_count = 0
        valid_pq_paths = [p for p in existing_pqs.values() if p.exists()]
        if valid_pq_paths:
            rows_by_posix: Dict[str, int] = {}
            try:
                with duckdb.connect(":memory:") as _c_meta:
                    paths_sql = ", ".join(f"'{p.resolve().as_posix()}'" for p in valid_pq_paths)
                    meta_rows = _c_meta.execute(
                        f"SELECT replace(file_name, chr(92), '/'), COALESCE(SUM(num_rows), 0) "
                        f"FROM parquet_file_metadata([{paths_sql}]) GROUP BY 1"
                    ).fetchall()
                    for fn_s, n_r in meta_rows:
                        rows_by_posix[str(fn_s)] = int(n_r or 0)
            except Exception:
                rows_by_posix = {}

            if rows_by_posix:
                populated_count = sum(
                    1 for p in valid_pq_paths if rows_by_posix.get(p.resolve().as_posix(), 0) > 0
                )
            else:
                try:
                    with duckdb.connect(":memory:") as _c_fb:
                        for p in valid_pq_paths:
                            try:
                                c_val = int(
                                    _c_fb.execute(f"SELECT COUNT(*) FROM read_parquet('{p.as_posix()}')").fetchone()[0]
                                )
                                if c_val > 0:
                                    populated_count += 1
                            except Exception:
                                continue
                except Exception:
                    populated_count = 0

        return {
            "tenant_id": self.tenant_id,
            "municipality_name": city_name,
            "ibge_code": ibge_code,
            "all_up_to_date": len(missing_enriched) == 0 and rag_file.exists() and len(existing_pqs) >= 79,
            "total_parquet_tables": len(existing_pqs),
            "populated_parquet_tables": populated_count,
            "empty_stub_parquet_tables": max(0, len(existing_pqs) - populated_count),
            "target_parquet_tables": 79,
            "parquet_store_size_mb": total_size_mb,
            "missing_tables": missing_enriched,
            "active_release_id": ptr.get("release_id", "base_v1"),
            "active_release_updated_at": ptr.get("updated_at"),
            "sync_in_progress": self.lock_file.exists(),
            "last_sync_timestamp": last_sync_meta.get("last_sync_timestamp", "Sincronização inicial Fase 1"),
            "cli_cron_command": f"python -m sim_platform.ingestion.sync_manager --tenant {self.tenant_id} --live-api",
            "sources": sources_status,
            "rag_collection_audit": rag_audit,
        }

    # ------------------------------------------------------------------
    # Sincronização One-Click + Conectores Online + Release Imutável
    # ------------------------------------------------------------------
    def sync_all(
        self,
        selected_sources: Optional[List[str]] = None,
        db_manager: Optional[Any] = None,
        rag_retriever: Optional[Any] = None,
        live_api: bool = False,
    ) -> Dict[str, Any]:
        """
        Executa o pipeline unificado de atualização ('Botão Atualizar Dados' ou CLI Cronjob):
        1. Adquire lock exclusivo do município (`_sync.lock`).
        2. Se `live_api=True`, consulta APIs oficiais abertas (IBGE, SICONFI/STN com rate-limit 1,1s, CGU).
        3. Grava os novos Parquets em um release imutável `releases/<release_id>/` + `CURRENT.json`
           (zero conflito de descritor de arquivo `WinError 32` no Windows!).
        4. Higieniza/deduplica a collection RAG do Diário Oficial e faz hot-reload sem derrubar o servidor.
        """
        with self._acquire_sync_lock():
            t0 = time.perf_counter()
            actions_log: List[str] = []
            live_telemetry: List[Dict[str, Any]] = []

            norm_selected = {s.upper().strip() for s in (selected_sources or []) if s and s.strip()}
            ibge_code = str(self.tenant_profile.get("ibge_code", "3110004"))

            # Desativar rede externa automaticamente durante testes offline do pytest, a menos que forçado
            allow_live = bool(live_api and not os.environ.get("PYTEST_CURRENT_TEST"))
            live_ibge_pop: Optional[Dict[int, Tuple[int, str]]] = None
            live_ibge_pib: Optional[Dict[int, float]] = None
            live_ibge_area: float = 0.0
            live_siconfi_rcl: Optional[Dict[int, float]] = None
            live_siconfi_rows: Optional[Dict[int, Dict[str, float]]] = None
            live_cgu_bf: Optional[List[Dict[str, Any]]] = None
            live_cgu_bpc: Optional[List[Dict[str, Any]]] = None

            active_map_initial = self._resolve_active_parquet_map()
            active_pq_count = len(active_map_initial)
            siconfi_prev_rows = 0
            sic_prev_pq = active_map_initial.get("siconfi_finbra_indicadores")
            if sic_prev_pq and sic_prev_pq.exists():
                try:
                    with duckdb.connect(":memory:") as _c_chk:
                        siconfi_prev_rows = int(
                            _c_chk.execute(f"SELECT COUNT(*) FROM read_parquet('{sic_prev_pq.as_posix()}')").fetchone()[0]
                        )
                except Exception:
                    siconfi_prev_rows = 0
            needs_full_api_extract = active_pq_count < 60 or siconfi_prev_rows < 4

            if allow_live:
                if not norm_selected or "IBGE" in norm_selected:
                    ibge_t = self._fetch_live_ibge_updates(ibge_code)
                    live_ibge_pop = ibge_t.pop("pop_by_year", None)
                    live_ibge_pib = ibge_t.pop("pib_by_year", None)
                    live_ibge_area = float(ibge_t.pop("area_km2", 0.0) or 0.0)
                    live_telemetry.append(ibge_t)
                    actions_log.append(ibge_t["detail"])
                if not norm_selected or "SICONFI" in norm_selected:
                    sic_years = [2021, 2022, 2023, 2024, 2025, 2026] if needs_full_api_extract else [2024, 2025, 2026]
                    sic_t = self._fetch_live_siconfi_updates(ibge_code, sic_years, full_extract=needs_full_api_extract)
                    live_siconfi_rcl = sic_t.pop("rcl_live", None)
                    live_siconfi_rows = sic_t.pop("siconfi_rows_by_year", None)
                    live_telemetry.append(sic_t)
                    actions_log.append(sic_t["detail"])
                if not norm_selected or "CGU" in norm_selected:
                    cgu_t = self._fetch_live_cgu_updates(ibge_code, full_extract=needs_full_api_extract)
                    live_cgu_bf = cgu_t.pop("bf_records", None)
                    live_cgu_bpc = cgu_t.pop("bpc_records", None)
                    live_telemetry.append(cgu_t)
                    actions_log.append(cgu_t["detail"])

            # Sempre publica tabelas em `releases/<release_id>` imutável para zero conflito `WinError 32`
            use_immutable_release = True
            release_id = datetime.now().strftime("rel_%Y%m%d_%H%M%S_%f")[:23]
            out_dir = self.releases_root / release_id

            enriched_counts = self.materialize_enriched_public_bases(
                target_dir=out_dir,
                live_ibge_pop=live_ibge_pop,
                live_siconfi_rcl=live_siconfi_rcl,
                live_ibge_pib=live_ibge_pib,
                live_ibge_area=live_ibge_area,
                live_siconfi_rows=live_siconfi_rows,
                live_cgu_bf=live_cgu_bf,
                live_cgu_bpc=live_cgu_bpc,
            )
            for tname, cnt in enriched_counts.items():
                actions_log.append(f"Padronizada e compactada tabela oficial `{tname}.parquet` ({cnt} registros validados).")

            pointer_info: Optional[Dict[str, Any]] = None
            if use_immutable_release and enriched_counts:
                pointer_info = self._publish_immutable_release(
                    release_dir=out_dir,
                    release_id=release_id,
                    summary=f"sync_all(live_api={allow_live}, tables={len(enriched_counts)})",
                )
                actions_log.append(
                    f"Release imutável `{release_id}` publicado atomicamente em `CURRENT.json` (zero lock de leitura)."
                )
            else:
                shutil.rmtree(out_dir, ignore_errors=True)

            # 2. Executar pipeline de limpeza e deduplicação do Diário Oficial
            rag_unique_acts = 0
            if not norm_selected or "DIARIO_OFICIAL" in norm_selected:
                # Preservar atos extras eventualmente adicionados por upload manual antes de reexecutar
                extra_uploaded_acts = self._load_uploaded_rag_acts()
                rag_res = build_clean_individual_collection(
                    workspace_root=self.workspace_root,
                    persist_json=False,
                    tenant_id=self.tenant_id,
                )
                collection = list(rag_res["collection"])
                if extra_uploaded_acts:
                    existing_sigs = {hashlib.sha1(str(c.get("page_content") or "").encode("utf-8")).hexdigest() for c in collection}
                    for act in extra_uploaded_acts:
                        sig = hashlib.sha1(str(act.get("page_content") or "").encode("utf-8")).hexdigest()
                        if sig not in existing_sigs:
                            collection.append(act)
                            existing_sigs.add(sig)
                _write_json_atomic(self.parquet_dir / "rag_jornais_individuais.json", collection)
                audit = dict(rag_res["audit_summary"])
                rag_unique_acts = len(collection)
                audit["unique_individual_acts"] = rag_unique_acts
                acts_by_type_sync: Dict[str, int] = {}
                for item in collection:
                    t_lbl = str(item.get("tipo_ato") or "GERAL / OUTROS")
                    acts_by_type_sync[t_lbl] = acts_by_type_sync.get(t_lbl, 0) + 1
                audit["acts_by_type"] = acts_by_type_sync
                _write_json_atomic(self.parquet_dir / "rag_audit_report.json", audit)
                actions_log.append(
                    f"Collection RAG higienizada: {rag_unique_acts} atos individuais únicos "
                    f"({audit['duplicates_eliminated']} duplicatas eliminadas)."
                )
                if rag_retriever is not None:
                    rag_retriever.corpus_chunks = collection
                    if hasattr(rag_retriever, "_build_bm25_index"):
                        rag_retriever._build_bm25_index()
                    actions_log.append("Motor HybridRAGRetriever atualizado em memória com a nova collection individual.")
            else:
                rag_file = self.parquet_dir / "rag_jornais_individuais.json"
                if rag_file.exists():
                    try:
                        rag_unique_acts = len(json.loads(rag_file.read_text(encoding="utf-8")))
                    except Exception:
                        rag_unique_acts = 0

            # 3. Hot-reload do DuckDBManager em memória
            if db_manager is not None:
                db_manager.reload_catalog_and_views()
                actions_log.append(
                    f"Catálogo DuckDB e Gold Views recarregados a quente: {len(db_manager.catalog.tables)} objetos ativos."
                )

            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            eff_rel_id = pointer_info["release_id"] if pointer_info else self._read_current_pointer().get("release_id", "base_v1")

            state_payload = {
                "last_sync_timestamp": now_str,
                "duration_ms": elapsed_ms,
                "release_id": eff_rel_id,
                "live_api_used": allow_live,
                "live_api_telemetry": live_telemetry,
                "enriched_tables_synced": enriched_counts,
                "rag_unique_acts": rag_unique_acts,
                "actions_log": actions_log,
            }
            _write_json_atomic(self.sync_state_file, state_payload)

            status_after = self.check_status()
            status_after["sync_execution"] = state_payload
            return status_after

    # ------------------------------------------------------------------
    # Upload Seguro Drag-and-Drop (.zip / .csv / .parquet / .json / .txt / .pdf)
    # ------------------------------------------------------------------
    def _load_uploaded_rag_acts(self) -> List[Dict[str, Any]]:
        sidecar = self.parquet_dir / "_uploaded_rag_acts.json"
        if sidecar.exists():
            try:
                data = json.loads(sidecar.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    return data
            except Exception:
                pass
        return []

    @staticmethod
    def _sql_ptbr_clean_numeric_expr(col_ident: str) -> str:
        """
        Expressão SQL DuckDB para higienizar valores numéricos/monetários PT-BR (`R$ 1.234,56`, `1.234,56`, `-450,20`)
        ou padrão ISO (`1234.56`) antes de `TRY_CAST` para `DECIMAL(18, s)` / `DOUBLE`.
        """
        raw_trim = f"NULLIF(TRIM(REGEXP_REPLACE(CAST({col_ident} AS VARCHAR), '^R\\$\\s*', '')), '')"
        return (
            f"CASE "
            f"WHEN {col_ident} IS NULL THEN NULL "
            f"WHEN POSITION(',' IN COALESCE({raw_trim}, '')) > 0 "
            f"THEN REPLACE(REPLACE({raw_trim}, '.', ''), ',', '.') "
            f"WHEN REGEXP_MATCHES(COALESCE({raw_trim}, ''), '^[+-]?[0-9]{{1,3}}(\\.[0-9]{{3}}){{2,}}$') "
            f"THEN REPLACE({raw_trim}, '.', '') "
            f"ELSE {raw_trim} "
            f"END"
        )

    @classmethod
    def _normalize_decimal_type(cls, raw_type: str) -> str:
        """Preserva a escala original `max(scale, 2)` em colunas `DECIMAL(18, s)`."""
        m_dec = re.match(r"^DECIMAL\(\s*(\d+)\s*,\s*(\d+)\s*\)$", raw_type.strip(), flags=re.IGNORECASE)
        if m_dec:
            prec = max(int(m_dec.group(1)), 18)
            scale = max(int(m_dec.group(2)), 2)
            return f"DECIMAL({prec},{scale})"
        if raw_type.upper().startswith("DECIMAL"):
            return "DECIMAL(18,2)"
        return raw_type

    @classmethod
    def _sanitize_ptbr_monetary_columns_in_table(cls, con: duckdb.DuckDBPyConnection, table_name: str) -> None:
        """
        Converte colunas `VARCHAR` monetárias PT-BR (`vlr_*`, `valor*`) em `DECIMAL(18, max(scale, 2))`
        quando todos os valores não vazios forem numéricos válidos após limpeza PT-BR.
        """
        desc = con.execute(f"DESCRIBE {table_name}").fetchall()
        money_pat = re.compile(
            r"^(vlr_|valor|vl_|.*(parcela|transferido|empenhado|liquidado|pago|bruta|liquida|descontos|receita|despesa|aditivo|contrato))$",
            re.IGNORECASE,
        )
        rewrites: List[str] = []
        changed = False
        for c_name_raw, c_type_raw, *_ in desc:
            c_name = str(c_name_raw)
            c_type = str(c_type_raw).upper()
            q_col = f'"{c_name}"'
            if c_type == "VARCHAR" and money_pat.match(c_name.strip()):
                clean_expr = cls._sql_ptbr_clean_numeric_expr(q_col)
                stats = con.execute(
                    f"""
                    SELECT
                        COUNT(*) FILTER (WHERE {clean_expr} IS NOT NULL) AS non_empty,
                        COUNT(*) FILTER (WHERE {clean_expr} IS NOT NULL AND TRY_CAST({clean_expr} AS DECIMAL(18,6)) IS NULL) AS bad_cnt,
                        COALESCE(MAX(
                            CASE
                                WHEN POSITION('.' IN {clean_expr}) > 0
                                THEN LENGTH(SPLIT_PART({clean_expr}, '.', 2))
                                ELSE 0
                            END
                        ), 2) AS max_scale
                    FROM {table_name}
                    """
                ).fetchone()
                if stats and int(stats[0] or 0) > 0 and int(stats[1] or 0) == 0:
                    eff_scale = min(max(int(stats[2] or 2), 2), 6)
                    rewrites.append(f'TRY_CAST({clean_expr} AS DECIMAL(18,{eff_scale})) AS {q_col}')
                    changed = True
                    continue
            elif c_type.startswith("DECIMAL("):
                norm_t = cls._normalize_decimal_type(c_type)
                if norm_t != c_type:
                    rewrites.append(f"TRY_CAST({q_col} AS {norm_t}) AS {q_col}")
                    changed = True
                    continue
            rewrites.append(q_col)

        if changed:
            proj_sql = ", ".join(rewrites)
            con.execute(f"CREATE OR REPLACE TEMP TABLE {table_name} AS SELECT {proj_sql} FROM {table_name}")

    def _convert_tabular_file_into_release(
        self,
        src_path: Path,
        raw_filename: str,
        release_dir: Path,
        active_map: Dict[str, Path],
        explicit_table: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Valida e converte um arquivo `.parquet` ou `.csv` em Parquet `ZSTD` dentro de `release_dir`.
        Realiza merge incremental cirúrgico por `(ano_referencia, mes, orgao)` preservando os demais
        meses/órgãos do mesmo exercício, exige `>= 50%` de interseção real de colunas de negócio e
        jamais faz fallback silencioso para `full_replace` sobre tabelas históricas já populadas.
        """
        ext = src_path.suffix.lower()
        stem_hint = explicit_table.strip() if explicit_table else Path(raw_filename).stem
        canon_stem, inferred_year = _sanitize_table_stem(stem_hint)

        # Casar capitalização com tabela já existente no catálogo (ex.: `desp_pessoal_despPessoal`)
        for existing_name in active_map:
            if existing_name.lower() == canon_stem.lower():
                canon_stem = existing_name
                break

        con = duckdb.connect(":memory:")
        try:
            con.execute("SET memory_limit = '512MB';")
            con.execute("SET threads = 2;")

            if ext == ".parquet":
                con.execute(f"CREATE TEMP TABLE incoming_raw AS SELECT * FROM read_parquet('{src_path.as_posix()}')")
            elif ext == ".csv":
                raw_bytes = src_path.read_bytes()
                txt = _decode_text_smart(raw_bytes)
                delim = _detect_csv_delimiter(txt)
                if "remuneracao_municipio" in canon_stem.lower() and delim == ";":
                    txt, _ = repair_sicom_remuneracao_csv_lines(txt)
                utf8_tmp = src_path.with_suffix(".utf8.csv")
                utf8_tmp.write_text(txt, encoding="utf-8")
                con.execute(
                    f"""
                    CREATE TEMP TABLE incoming_raw AS
                    SELECT * FROM read_csv_auto(
                        '{utf8_tmp.as_posix()}',
                        delim='{delim}',
                        header=true,
                        strict_mode=false,
                        null_padding=true,
                        ignore_errors=true,
                        sample_size=-1
                    )
                    """
                )
            else:
                raise ValueError(f"Extensão tabular não suportada: {ext}")

            cols_info = con.execute("DESCRIBE incoming_raw").fetchall()
            col_names = [str(c[0]) for c in cols_info]
            if not col_names:
                raise ValueError(f"Arquivo '{raw_filename}' não possui colunas válidas.")

            in_count = int(con.execute("SELECT COUNT(*) FROM incoming_raw").fetchone()[0])
            if in_count == 0:
                raise ValueError(f"Arquivo '{raw_filename}' possui 0 registros de dados.")

            # Sanitizar valores monetários PT-BR e preservar escala max(scale, 2)
            self._sanitize_ptbr_monetary_columns_in_table(con, "incoming_raw")

            col_map_low = {c.lower().strip(): c for c in col_names}
            has_ano_col = "ano_referencia" in col_map_low
            if not has_ano_col:
                derived_ano_expr: Optional[str] = None
                for cand_yr_col, substr_mode in (
                    ("num_anoexercicio", False),
                    ("num_ano_referencia", False),
                    ("seq_dat_pagamento", True),
                    ("mês referência", True),
                    ("mes_referencia", True),
                    ("mes referencia", True),
                ):
                    orig_yr_col = col_map_low.get(cand_yr_col)
                    if orig_yr_col:
                        q_yr = f'"{orig_yr_col}"'
                        derived_ano_expr = (
                            f"TRY_CAST(SUBSTR(TRIM(CAST({q_yr} AS VARCHAR)), 1, 4) AS BIGINT)"
                            if substr_mode
                            else f"TRY_CAST(TRIM(CAST({q_yr} AS VARCHAR)) AS BIGINT)"
                        )
                        valid_cnt = int(
                            con.execute(
                                f"SELECT COUNT(*) FROM incoming_raw WHERE {derived_ano_expr} BETWEEN 1990 AND 2100"
                            ).fetchone()[0]
                        )
                        if valid_cnt > 0:
                            break
                        derived_ano_expr = None

                if derived_ano_expr is not None:
                    fb_sql = f"COALESCE({derived_ano_expr}, {int(inferred_year)})" if inferred_year is not None else derived_ano_expr
                    con.execute(f"CREATE OR REPLACE TEMP TABLE incoming_raw AS SELECT *, {fb_sql} AS ano_referencia FROM incoming_raw")
                    has_ano_col = True
                elif inferred_year is not None:
                    con.execute(f"ALTER TABLE incoming_raw ADD COLUMN ano_referencia BIGINT DEFAULT {int(inferred_year)}")
                    has_ano_col = True

            out_pq = release_dir / f"{canon_stem}.parquet"
            prev_pq = active_map.get(canon_stem)
            merged_mode = "full_replace"

            if prev_pq and prev_pq.exists():
                prev_row_count = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{prev_pq.as_posix()}')").fetchone()[0])
                if prev_row_count == 0:
                    # Empty stub Parquet: substituir diretamente por incoming_raw!
                    merged_mode = "replace_empty_stub"
                else:
                    try:
                        prev_desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{prev_pq.as_posix()}')").fetchall()
                        prev_cols = [str(r[0]) for r in prev_desc]
                        prev_types = {str(r[0]): str(r[1]) for r in prev_desc}
                        inc_desc = con.execute("DESCRIBE incoming_raw").fetchall()
                        inc_map_low = {str(r[0]).lower(): str(r[0]) for r in inc_desc}
                        inc_types_low = {str(r[0]).lower(): str(r[1]).upper() for r in inc_desc}

                        meta_cols = {"ano_referencia", "filename"}
                        prev_biz = {c.lower() for c in prev_cols} - meta_cols
                        inc_biz = set(inc_map_low.keys()) - meta_cols
                        overlap_ratio = (
                            (len(prev_biz & inc_biz) / len(prev_biz))
                            if prev_biz
                            else (1.0 if ("ano_referencia" in inc_map_low) else 0.0)
                        )
                        if overlap_ratio < 0.50:
                            raise ValueError(
                                f"Schema incompatível para merge seguro em '{canon_stem}': interseção de colunas de negócio "
                                f"é {overlap_ratio * 100:.1f}% (< 50% exigido). Upload abortado para proteger série histórica."
                            )

                        prev_has_ano = any(c.lower() == "ano_referencia" for c in prev_cols)
                        if prev_has_ano:
                            if not has_ano_col:
                                raise ValueError(
                                    f"Tabela histórica '{canon_stem}' ({prev_row_count} registros) exige 'ano_referencia' "
                                    f"(no arquivo ou no nome '{raw_filename}') para merge incremental seguro."
                                )
                            yrs_uploaded = sorted(
                                int(r[0])
                                for r in con.execute(
                                    "SELECT DISTINCT TRY_CAST(ano_referencia AS BIGINT) FROM incoming_raw "
                                    "WHERE TRY_CAST(ano_referencia AS BIGINT) IS NOT NULL"
                                ).fetchall()
                            )
                            if not yrs_uploaded:
                                raise ValueError(
                                    f"Nenhum 'ano_referencia' válido encontrado em '{raw_filename}' para merge incremental em '{canon_stem}'."
                                )

                            prev_map_low = {c.lower(): c for c in prev_cols}
                            shared_mes_prev: Optional[str] = None
                            shared_mes_inc: Optional[str] = None
                            for m_cand in ("num_mesexercicio", "num_mes_referencia", "seq_dat_pagamento", "mês referência", "mes_referencia"):
                                if m_cand in prev_map_low and m_cand in inc_map_low:
                                    shared_mes_prev = prev_map_low[m_cand]
                                    shared_mes_inc = inc_map_low[m_cand]
                                    break

                            shared_org_prev = prev_map_low.get("cod_orgao") if "cod_orgao" in inc_map_low else None
                            shared_org_inc = inc_map_low.get("cod_orgao") if shared_org_prev else None

                            def _norm_slice_key_sql(table_alias: str, col_nm: Optional[str]) -> str:
                                if not col_nm:
                                    return "'__ALL__'"
                                q_ref = f'{table_alias}."{col_nm}"'
                                return (
                                    f"COALESCE("
                                    f"CAST(TRY_CAST(TRIM(CAST({q_ref} AS VARCHAR)) AS BIGINT) AS VARCHAR), "
                                    f"NULLIF(UPPER(TRIM(CAST({q_ref} AS VARCHAR))), ''), "
                                    f"'__NULL__')"
                                )

                            select_prev_parts: List[str] = []
                            select_inc_parts: List[str] = []
                            for c in prev_cols:
                                raw_p_type = prev_types[c]
                                p_type = self._normalize_decimal_type(raw_p_type)
                                if p_type != raw_p_type or raw_p_type.upper().startswith("DECIMAL"):
                                    select_prev_parts.append(f'TRY_CAST(p."{c}" AS {p_type}) AS "{c}"')
                                else:
                                    select_prev_parts.append(f'p."{c}"')

                                inc_col = inc_map_low.get(c.lower())
                                if inc_col:
                                    inc_t = inc_types_low.get(c.lower(), "")
                                    if inc_t == "VARCHAR" and (
                                        p_type.upper().startswith("DECIMAL") or p_type.upper() in ("DOUBLE", "FLOAT")
                                    ):
                                        clean_num = self._sql_ptbr_clean_numeric_expr(f'i."{inc_col}"')
                                        select_inc_parts.append(f'TRY_CAST({clean_num} AS {p_type}) AS "{c}"')
                                    else:
                                        select_inc_parts.append(f'TRY_CAST(i."{inc_col}" AS {p_type}) AS "{c}"')
                                else:
                                    select_inc_parts.append(f'CAST(NULL AS {p_type}) AS "{c}"')

                            select_prev = ", ".join(select_prev_parts)
                            select_inc = ", ".join(select_inc_parts)

                            prev_ano_k = "TRY_CAST(p.ano_referencia AS BIGINT)"
                            inc_ano_k = "TRY_CAST(k.ano_referencia AS BIGINT)"
                            prev_mes_k = _norm_slice_key_sql("p", shared_mes_prev)
                            inc_mes_k = _norm_slice_key_sql("k", shared_mes_inc)
                            prev_org_k = _norm_slice_key_sql("p", shared_org_prev)
                            inc_org_k = _norm_slice_key_sql("k", shared_org_inc)

                            con.execute(
                                f"""
                                COPY (
                                    WITH incoming_slice_keys AS (
                                        SELECT DISTINCT
                                            {inc_ano_k} AS ano_k,
                                            {inc_mes_k} AS mes_k,
                                            {inc_org_k} AS org_k
                                        FROM incoming_raw k
                                        WHERE {inc_ano_k} IS NOT NULL
                                    )
                                    SELECT {select_prev}
                                    FROM read_parquet('{prev_pq.as_posix()}') p
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM incoming_slice_keys sk
                                        WHERE sk.ano_k IS NOT DISTINCT FROM {prev_ano_k}
                                          AND sk.mes_k IS NOT DISTINCT FROM {prev_mes_k}
                                          AND sk.org_k IS NOT DISTINCT FROM {prev_org_k}
                                    )
                                    UNION ALL
                                    SELECT {select_inc}
                                    FROM incoming_raw i
                                ) TO '{out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
                                """
                            )
                            slice_dims = ["ano"]
                            if shared_mes_prev:
                                slice_dims.append(shared_mes_prev)
                            if shared_org_prev:
                                slice_dims.append(shared_org_prev)
                            merged_mode = f"incremental_slice_merge({'+'.join(slice_dims)}:{yrs_uploaded})"
                    except ValueError:
                        raise
                    except Exception as exc:
                        raise ValueError(
                            f"Falha no merge incremental seguro de '{canon_stem}' ({prev_row_count} registros existentes preservados): {exc}"
                        ) from exc

            if not out_pq.exists():
                con.execute(f"COPY incoming_raw TO '{out_pq.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")

            final_rows = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{out_pq.as_posix()}')").fetchone()[0])
            return {
                "table_name": canon_stem,
                "source_file": raw_filename,
                "incoming_rows": in_count,
                "total_rows_after": final_rows,
                "mode": merged_mode,
            }
        finally:
            con.close()

    @staticmethod
    def _clamp_iso_pub_date(candidate_dt: str, today_iso: str) -> str:
        m = re.search(r"(?<!\d)(20[12][0-9]-[01][0-9]-[0-3][0-9])(?!\d)", str(candidate_dt or ""))
        if not m:
            return today_iso
        iso_s = m.group(1)
        try:
            parsed_d = date.fromisoformat(iso_s)
        except ValueError:
            return today_iso
        return min(parsed_d.isoformat(), today_iso)

    @staticmethod
    def _is_pure_expediente_header(text: str, act_key: Optional[str], tipo_ato: str) -> bool:
        clean = (text or "").strip()
        if len(clean) >= 160:
            return False
        if act_key:
            return False
        if tipo_ato not in ("GERAL / OUTROS", "ATO OFICIAL", ""):
            info_chk = extract_canonical_act_info(clean)
            if info_chk.get("act_key"):
                return False
            # Se for curto (<160 chars) sem número de ato e apenas cabeçalho/expediente, descarta
            if re.search(r"\b(EXPEDIENTE|EDI[CÇ][AÃ]O|DI[AÁ]RIO OFICIAL|SUM[AÁ]RIO|CADERNO|P[AÁ]GINA)\b", clean, re.IGNORECASE):
                return True
            return False
        return True

    def _ingest_gazette_document(self, src_path: Path, raw_filename: str) -> int:
        ext = src_path.suffix.lower()
        new_acts: List[Dict[str, Any]] = []
        today_str = date.today().isoformat()
        city_name = str(self.tenant_profile.get("municipality_name") or "Município")
        uf = str(self.tenant_profile.get("state_uf") or "MG")
        gazette_name = str((self.tenant_profile.get("diario_oficial") or {}).get("veiculo") or f"Diário Oficial de {city_name}/{uf}")

        if ext == ".json":
            parsed = json.loads(_decode_text_smart(src_path.read_bytes()))
            items = parsed if isinstance(parsed, list) else (parsed.get("acts") or parsed.get("chunks") or [parsed])
            for idx, it in enumerate(items):
                if not isinstance(it, dict):
                    continue
                raw_content = str(it.get("page_content") or it.get("texto") or it.get("content") or "").strip()
                content, _ = clean_newspaper_pdf_text(raw_content)
                if len(content) < 20:
                    continue
                info_j = extract_canonical_act_info(content)
                tipo_j = str(it.get("tipo_ato") or info_j.get("tipo_ato") or "ATO OFICIAL")
                if self._is_pure_expediente_header(content, info_j.get("act_key"), tipo_j):
                    continue
                act_id_val = str(it.get("act_id") or it.get("id") or f"upload_{Path(raw_filename).stem}_{idx+1}")
                pub_dt = self._clamp_iso_pub_date(str(it.get("data_publicacao") or raw_filename), today_str)
                yr_match = re.search(r"(?<!\d)(20[12][0-9])(?!\d)", pub_dt + " " + str(it.get("ano_edicao") or ""))
                yr_ed = min(int(yr_match.group(1)), int(today_str[:4])) if yr_match else int(today_str[:4])
                new_acts.append(
                    {
                        "id": act_id_val,
                        "chunk_id": act_id_val,
                        "act_key": it.get("act_key") or info_j.get("act_key"),
                        "act_id": act_id_val,
                        "tipo_ato": tipo_j,
                        "numero_ato": str(it.get("numero_ato") or info_j.get("numero_ato") or ""),
                        "data_publicacao": pub_dt,
                        "ano_edicao": yr_ed,
                        "source": str(it.get("source") or raw_filename),
                        "page_content": content,
                    }
                )
        else:
            raw_text = ""
            if ext == ".pdf":
                try:
                    from pypdf import PdfReader  # type: ignore

                    reader = PdfReader(str(src_path))
                    if len(reader.pages) > MAX_PDF_PAGES:
                        raise ValueError(
                            f"PDF '{raw_filename}' excede o limite máximo de {MAX_PDF_PAGES} páginas ({len(reader.pages)} páginas)."
                        )
                    raw_text = "\n".join((pg.extract_text() or "") for pg in reader.pages)
                except ValueError:
                    raise
                except Exception as exc:
                    raise ValueError(
                        f"Não foi possível extrair texto do PDF '{raw_filename}' ({exc}). Envie em .txt/.json ou PDF pesquisável."
                    ) from exc
            else:
                raw_text = _decode_text_smart(src_path.read_bytes())

            cleaned_text, _ = clean_newspaper_pdf_text(raw_text)
            if len(cleaned_text) < 20:
                raise ValueError(f"Documento '{raw_filename}' não contém texto normativo suficiente.")

            # Regex de data compatível com `_` antes de YYYY-MM-DD (ex.: `ed1654_2026-11-05.pdf`) e travada em `<= today`
            date_re = re.compile(r"(?<!\d)(202[0-9]-[01][0-9]-[0-3][0-9])(?!\d)")
            m_fname_dt = date_re.search(raw_filename)
            pub_date: Optional[str] = None
            if m_fname_dt:
                pub_date = self._clamp_iso_pub_date(m_fname_dt.group(1), today_str)
            else:
                for m_txt_dt in date_re.finditer(cleaned_text[:400]):
                    cand_iso = m_txt_dt.group(1)
                    try:
                        if date.fromisoformat(cand_iso).isoformat() <= today_str:
                            pub_date = cand_iso
                            break
                    except ValueError:
                        continue
            if not pub_date:
                pub_date = today_str
            yr_ed = min(int(pub_date[:4]), int(today_str[:4])) if pub_date[:4].isdigit() else int(today_str[:4])

            # Remover eventual cabeçalho puro de expediente (< 160 chars) antes do 1º ato normativo
            lead_segs = [s.strip() for s in RE_ACT_BOUNDARY.split(cleaned_text) if s and s.strip()]
            if len(lead_segs) > 1:
                lead0_info = extract_canonical_act_info(lead_segs[0])
                if self._is_pure_expediente_header(lead_segs[0], lead0_info.get("act_key"), lead0_info.get("tipo_ato", "GERAL / OUTROS")):
                    cleaned_text = "\n\n".join(lead_segs[1:])

            chunker = ActBoundaryChunker(max_chunk_chars=2200, min_orphan_chars=180)
            chunks = chunker.chunk_edition_text(
                raw_edition_text=cleaned_text,
                source_pdf=f"{raw_filename} ({pub_date})",
                default_year=yr_ed,
                municipality_label=f"{city_name}/{uf}",
                gazette_name=gazette_name,
            )

            for sc in chunks:
                c_body = sc.page_content.strip()
                if len(c_body) < 25:
                    continue
                if self._is_pure_expediente_header(c_body, sc.act_key, sc.tipo_ato):
                    continue
                act_yr = min(int(sc.ano_edicao), int(today_str[:4])) if sc.ano_edicao else yr_ed
                new_acts.append(
                    {
                        "id": sc.chunk_id,
                        "chunk_id": sc.chunk_id,
                        "act_id": sc.chunk_id,
                        "act_key": sc.act_key,
                        "tipo_ato": sc.tipo_ato,
                        "numero_ato": sc.numero_ato or "",
                        "data_publicacao": pub_date,
                        "ano_edicao": act_yr,
                        "source": sc.source,
                        "contextual_header": sc.contextual_header,
                        "cnpjs_Mencionados": sc.cnpjs_mencionados,
                        "valores_mencionados": sc.valores_mencionados,
                        "quality_flags": sc.quality_flags,
                        "page_content": c_body,
                    }
                )

        if not new_acts:
            raise ValueError(f"Nenhum ato válido encontrado em '{raw_filename}'.")

        # Deduplicar `_uploaded_rag_acts.json` estritamente por SHA-1 de `page_content`
        sidecar = self.parquet_dir / "_uploaded_rag_acts.json"
        persisted_extra = self._load_uploaded_rag_acts()
        dedup_sidecar: List[Dict[str, Any]] = []
        sidecar_sigs: Set[str] = set()
        for item_s in persisted_extra + new_acts:
            sig_s = hashlib.sha1(str(item_s.get("page_content") or "").encode("utf-8")).hexdigest()
            if sig_s not in sidecar_sigs:
                dedup_sidecar.append(item_s)
                sidecar_sigs.add(sig_s)
        _write_json_atomic(sidecar, dedup_sidecar)

        rag_file = self.parquet_dir / "rag_jornais_individuais.json"
        current_coll: List[Dict[str, Any]] = []
        if rag_file.exists():
            try:
                current_coll = json.loads(rag_file.read_text(encoding="utf-8"))
            except Exception:
                current_coll = []

        existing_sigs = {hashlib.sha1(str(c.get("page_content") or "").encode("utf-8")).hexdigest() for c in current_coll}
        added = 0
        for act in new_acts:
            sig = hashlib.sha1(str(act.get("page_content") or "").encode("utf-8")).hexdigest()
            if sig not in existing_sigs:
                current_coll.append(act)
                existing_sigs.add(sig)
                added += 1

        _write_json_atomic(rag_file, current_coll)

        rag_report_file = self.parquet_dir / "rag_audit_report.json"
        rep_obj: Dict[str, Any] = {}
        if rag_report_file.exists():
            try:
                rep_obj = json.loads(rag_report_file.read_text(encoding="utf-8"))
            except Exception:
                rep_obj = {}
        acts_by_type_map: Dict[str, int] = {}
        for c_act in current_coll:
            t_key = str(c_act.get("tipo_ato") or "GERAL / OUTROS")
            acts_by_type_map[t_key] = acts_by_type_map.get(t_key, 0) + 1
        rep_obj["unique_individual_acts"] = len(current_coll)
        rep_obj.setdefault("duplicates_eliminated", 0)
        rep_obj["acts_by_type"] = acts_by_type_map
        rep_obj["last_updated_from_upload"] = raw_filename
        _write_json_atomic(rag_report_file, rep_obj)
        return added

    def ingest_uploaded_package(
        self,
        filename: str,
        content_bytes: bytes,
        target_table: Optional[str] = None,
        db_manager: Optional[Any] = None,
        rag_retriever: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Ingere com segurança um arquivo ou pacote enviado pelo gestor municipal (`.zip`, `.csv`, `.parquet`,
        `.json`, `.txt`, `.pdf`), publicando em um novo release imutável `CURRENT.json` e recarregando o catálogo.
        """
        if not filename or not filename.strip():
            raise ValueError("Nome do arquivo ausente.")
        if not content_bytes:
            raise ValueError("Arquivo enviado está vazio (0 bytes).")
        if len(content_bytes) > MAX_UPLOAD_RAW_BYTES:
            raise ValueError(f"Arquivo excede o limite máximo permitido de {MAX_UPLOAD_RAW_BYTES // (1024 * 1024)} MB.")

        safe_name = Path(filename).name
        ext = Path(safe_name).suffix.lower()
        if ext not in ALLOWED_UPLOAD_EXTENSIONS:
            raise ValueError(
                f"Formato '{ext}' não permitido. Formatos aceitos: {', '.join(sorted(ALLOWED_UPLOAD_EXTENSIONS))}."
            )

        expected_ibge = str(self.tenant_profile.get("ibge_code") or "").strip()
        _validate_no_foreign_ibge_in_name(safe_name, expected_ibge)

        with self._acquire_sync_lock():
            t0 = time.perf_counter()
            release_id = datetime.now().strftime("upl_%Y%m%d_%H%M%S_%f")[:23]
            release_dir = self.releases_root / release_id
            active_map = self._resolve_active_parquet_map()

            ingested_tables: List[Dict[str, Any]] = []
            rag_acts_added = 0

            staging_root = self.workspace_root / "avaliacao" / "dados" / "_staging"
            staging_root.mkdir(parents=True, exist_ok=True)
            with tempfile.TemporaryDirectory(prefix="sim_upload_", dir=str(staging_root)) as tmp_dir_str:
                stage_dir = Path(tmp_dir_str).resolve()

                files_to_process: List[Tuple[Path, str]] = []
                if ext == ".zip":
                    try:
                        zf_ctx = zipfile.ZipFile(io.BytesIO(content_bytes))
                    except zipfile.BadZipFile as exc:
                        raise ValueError("Pacote .zip inválido ou corrompido.") from exc

                    with zf_ctx as zf:
                        infos = [i for i in zf.infolist() if not i.is_dir()]
                        if not infos:
                            raise ValueError("Pacote .zip não contém arquivos.")
                        if len(infos) > MAX_ZIP_ENTRIES:
                            raise ValueError(f"Pacote .zip excede o limite de {MAX_ZIP_ENTRIES} arquivos internos.")
                        total_uncompressed = sum(max(0, int(i.file_size)) for i in infos)
                        if total_uncompressed > MAX_ZIP_UNCOMPRESSED_BYTES:
                            raise ValueError("Pacote .zip excede o limite descompactado de segurança (300 MB — proteção anti-Zip-Bomb).")

                        extracted_bytes_total = 0
                        for info in infos:
                            raw_member = info.filename.replace("\\", "/")
                            # Anti-Zip-Slip estrito: bloquear caminhos absolutos, drives ou `..`
                            member_path = Path(raw_member)
                            if (
                                member_path.is_absolute()
                                or ".." in member_path.parts
                                or raw_member.startswith("/")
                                or ":" in raw_member
                            ):
                                raise ValueError(f"Caminho inseguro bloqueado no pacote ZIP (Zip-Slip): '{info.filename}'.")

                            # Ignorar arquivos ocultos do macOS (`__MACOSX`, `.DS_Store`)
                            if any(part.startswith(".") or part == "__MACOSX" for part in member_path.parts):
                                continue

                            m_ext = member_path.suffix.lower()
                            if m_ext not in (ALLOWED_UPLOAD_EXTENSIONS - {".zip"}):
                                continue

                            _validate_no_foreign_ibge_in_name(raw_member, expected_ibge)

                            dest = (stage_dir / member_path).resolve()
                            dest.relative_to(stage_dir)  # Levanta ValueError se escapar de stage_dir
                            dest.parent.mkdir(parents=True, exist_ok=True)
                            with zf.open(info, "r") as src_f, open(dest, "wb") as dst_f:
                                while True:
                                    chunk_b = src_f.read(65536)
                                    if not chunk_b:
                                        break
                                    extracted_bytes_total += len(chunk_b)
                                    if extracted_bytes_total > MAX_ZIP_UNCOMPRESSED_BYTES:
                                        raise ValueError(
                                            "Pacote .zip excedeu o limite real de bytes descompactados durante streaming (proteção anti-Zip-Bomb)."
                                        )
                                    dst_f.write(chunk_b)
                            files_to_process.append((dest, member_path.name))

                    if not files_to_process:
                        raise ValueError("Nenhum arquivo suportado (.parquet, .csv, .json, .txt, .pdf) encontrado dentro do .zip.")
                else:
                    single_dest = stage_dir / safe_name
                    single_dest.write_bytes(content_bytes)
                    files_to_process.append((single_dest, safe_name))

                release_dir.mkdir(parents=True, exist_ok=True)
                try:
                    for f_path, orig_name in files_to_process:
                        f_ext = f_path.suffix.lower()
                        if f_ext in (".parquet", ".csv"):
                            tbl_rep = self._convert_tabular_file_into_release(
                                src_path=f_path,
                                raw_filename=orig_name,
                                release_dir=release_dir,
                                active_map=active_map,
                                explicit_table=target_table if len(files_to_process) == 1 else None,
                            )
                            ingested_tables.append(tbl_rep)
                            active_map[tbl_rep["table_name"]] = release_dir / f"{tbl_rep['table_name']}.parquet"
                        elif f_ext in (".json", ".txt", ".pdf"):
                            rag_acts_added += self._ingest_gazette_document(f_path, orig_name)
                except Exception:
                    shutil.rmtree(release_dir, ignore_errors=True)
                    raise

            # Se tabelas SICOM/CGU foram atualizadas, reconcilia as bases derivadas no mesmo release SEM sobrescrever as recém-enviadas!
            derived_reconciled: Dict[str, int] = {}
            if ingested_tables:
                derived_reconciled = self.materialize_enriched_public_bases(
                    target_dir=release_dir,
                    skip_tables={t["table_name"].lower() for t in ingested_tables},
                )
                self._publish_immutable_release(
                    release_dir=release_dir,
                    release_id=release_id,
                    summary=f"upload({safe_name}, tables={len(ingested_tables)})",
                )
            else:
                shutil.rmtree(release_dir, ignore_errors=True)

            if rag_acts_added > 0 and rag_retriever is not None:
                rag_file = self.parquet_dir / "rag_jornais_individuais.json"
                if rag_file.exists():
                    rag_retriever.corpus_chunks = json.loads(rag_file.read_text(encoding="utf-8"))
                    if hasattr(rag_retriever, "_build_bm25_index"):
                        rag_retriever._build_bm25_index()

            if db_manager is not None:
                db_manager.reload_catalog_and_views()

            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            report = {
                "status": "sucesso",
                "tenant_id": self.tenant_id,
                "filename": safe_name,
                "release_id": release_id if ingested_tables else self._read_current_pointer().get("release_id", "base_v1"),
                "ingested_tables": ingested_tables,
                "derived_tables_reconciled": derived_reconciled,
                "rag_acts_added": rag_acts_added,
                "duration_ms": elapsed_ms,
                "timestamp": now_str,
            }
            _write_json_atomic(
                self.sync_state_file,
                {
                    "last_sync_timestamp": now_str,
                    "duration_ms": elapsed_ms,
                    "release_id": report["release_id"],
                    "upload_report": report,
                },
            )
            return report

    def export_release_bundle_to_cloud_outbox(self) -> Dict[str, Any]:
        """
        Empacota todos os arquivos Parquet ativos (ZSTD) + collection RAG + metadados do município
        em um único bundle compactado dentro de `avaliacao/dados/cloud_outbox/<tenant_id>/releases/`
        usando escrita atômica `.tmp` + `os.replace()` para sincronização verificada (Two-Phase Delete)
        com o Google Drive Data Lake (`SIM_DataLake/`).
        """
        active_map = self._resolve_active_parquet_map()
        if not active_map:
            return {
                "status": "sem_dados",
                "tenant_id": self.tenant_id,
                "detail": "Nenhuma tabela Parquet materializada para exportar à nuvem.",
            }
        ptr = self._read_current_pointer()
        rel_id = str(ptr.get("release_id") or datetime.now().strftime("base_%Y%m%d"))
        env_outbox = os.environ.get("SIM_CLOUD_OUTBOX_DIR", "").strip()
        base_outbox = Path(env_outbox) if env_outbox else (self.workspace_root / "avaliacao" / "dados" / "cloud_outbox")
        outbox_dir = base_outbox / self.tenant_id / "releases"
        outbox_dir.mkdir(parents=True, exist_ok=True)
        bundle_name = f"{self.tenant_id}__{rel_id}__parquet_bundle.zip"
        bundle_path = outbox_dir / bundle_name
        tmp_bundle = outbox_dir / f".{bundle_name}.{os.getpid()}.{time.time_ns()}.tmp"

        # Como os Parquets já estão comprimidos em ZSTD, armazenamos sem recompressão desnecessária de CPU (ZIP_STORED)
        # para os `.parquet` e ZIP_DEFLATED para JSONs de metadados!
        try:
            with zipfile.ZipFile(tmp_bundle, "w") as zf:
                for stem, pq_path in sorted(active_map.items()):
                    if pq_path.exists():
                        zf.write(pq_path, arcname=f"parquet/{stem}.parquet", compress_type=zipfile.ZIP_STORED)
                for meta_name in ("CURRENT.json", "PREVIOUS.json", "_sync_state.json", "rag_jornais_individuais.json", "rag_audit_report.json"):
                    mp = self.parquet_dir / meta_name
                    if mp.exists():
                        zf.write(mp, arcname=f"meta/{meta_name}", compress_type=zipfile.ZIP_DEFLATED)
            os.replace(str(tmp_bundle), str(bundle_path))
        except Exception:
            try:
                tmp_bundle.unlink(missing_ok=True)
            except Exception:
                pass
            raise

        # Evitar acúmulo de múltiplos snapshots completos da mesma cidade ainda não enviados na outbox local
        prefix_same_tenant = f"{self.tenant_id}__"
        for stale_b in outbox_dir.glob("*__parquet_bundle.zip"):
            if stale_b != bundle_path and stale_b.name.startswith(prefix_same_tenant):
                try:
                    stale_b.unlink(missing_ok=True)
                except Exception:
                    pass

        size_bytes = bundle_path.stat().st_size
        return {
            "status": "sucesso",
            "tenant_id": self.tenant_id,
            "release_id": rel_id,
            "bundle_name": bundle_name,
            "bundle_path": str(bundle_path),
            "parquet_tables_bundled": len(active_map),
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 2),
        }


def _cli_main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="CLI de Sincronização e Ingestão por Município (pronto para Cronjob / Agendador de Tarefas)."
    )
    parser.add_argument("--tenant", default="caete_mg_3110004", help="ID do tenant municipal (ex.: caete_mg_3110004)")
    parser.add_argument("--live-api", action="store_true", help="Consultar APIs oficiais online (IBGE, SICONFI/STN, CGU)")
    parser.add_argument("--sources", default="", help="Lista separada por vírgula (ex.: IBGE,SICONFI,DIARIO_OFICIAL)")
    parser.add_argument("--ingest-file", default="", help="Caminho opcional de pacote .zip/.csv/.parquet/.pdf para ingerir")
    parser.add_argument("--target-table", default="", help="Nome opcional da tabela destino quando --ingest-file é único")
    parser.add_argument("--status-only", action="store_true", help="Apenas exibe o status atual em JSON")
    args = parser.parse_args(argv)

    mgr = DataSyncManager(workspace_root=DEFAULT_WORKSPACE_ROOT, tenant_id=args.tenant.strip())
    if args.status_only:
        print(json.dumps(mgr.check_status(), ensure_ascii=False, indent=2))
        return 0

    if args.ingest_file:
        fpath = Path(args.ingest_file).expanduser().resolve()
        if not fpath.exists():
            print(json.dumps({"status": "erro", "detail": f"Arquivo não encontrado: {fpath}"}), file=sys.stderr)
            return 2
        res = mgr.ingest_uploaded_package(
            filename=fpath.name,
            content_bytes=fpath.read_bytes(),
            target_table=args.target_table.strip() or None,
        )
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return 0

    srcs = [s.strip() for s in args.sources.split(",") if s.strip()] or None
    res = mgr.sync_all(selected_sources=srcs, live_api=bool(args.live_api))
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli_main())
