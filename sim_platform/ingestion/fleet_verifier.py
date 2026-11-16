"""
Verificador Multi-Cidades (Cronjob), Roteador Automático de `inbox/` pós-CAPTCHA e Sincronizador
com Google Drive Cloud Data Lake (`SIM_DataLake/`) usando protocolo estrito Two-Phase Delete.

Arquitetura Híbrida (Opção A — SIM v4.5):
1. Lock global exclusivo (`_fleet.lock` com `pid` + `lock_token`) impedindo corridas entre cronjobs.
2. Fontes Automáticas (`IBGE`, `SICONFI/STN`, `CGU`): sincronizadas sequencialmente cidade a cidade
   respeitando rate-limits oficiais (`1,1s` STN) sem intervenção humana.
3. Fontes com Portal / CAPTCHA (`SICOM/TCE-MG` e `Diário Oficial`): o verificador JAMAIS tenta burlar
   ou resolver CAPTCHAs; em vez disso, monta a Fila Assistida (`captcha_assisted_queue`) com links
   diretos aos portais oficiais. O operador baixa o `.zip`/`.pdf`/`.csv` para `avaliacao/dados/inbox/`.
4. Roteador Determinístico Blindado (`process_inbox`):
   - Rejeita symlinks e valida `size <= MAX_UPLOAD_RAW_BYTES` antes de `read_bytes()`;
   - Coleta TODOS os códigos IBGE válidos de 7 dígitos (bloqueando CSVs estaduais com múltiplos municípios);
   - Se o tenant estiver ocupado (`SyncAlreadyRunningError`), preserva o arquivo em `inbox/` (`skipped_busy_tenant`),
     JAMAIS movendo para `_quarentena_revisao/`;
   - Evita gerar 2 bundles seguidos na mesma rodada (`process_inbox` + `sync_all`).
5. Ciclo de Vida Sem Lotar Disco Local (Two-Phase Delete + Auto-Provisioning Drive):
   - Valida `^[A-Za-z0-9_-]{10,128}$` em todos os IDs do Google Drive;
   - Auto-provisiona `<UF>/<ibge>_<slug>/{raw_packages,releases}` para novas cidades;
   - Evita re-upload duplicado verificando `(filename, local_size)` na pasta remota / manifesto;
   - Valida `size == local_size`, `not trashed` e `md5Checksum` (quando retornado), e salva
     `_cloud_datalake_manifest.json` **antes** de `local_file.unlink()`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
import unicodedata
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from sim_platform.ingestion.city_onboarding import (
    CityOnboardingManager,
    is_valid_ibge7,
    load_tenant_profile,
)
from sim_platform.ingestion.sync_manager import (
    ALLOWED_UPLOAD_EXTENSIONS,
    DEFAULT_WORKSPACE_ROOT,
    MAX_UPLOAD_RAW_BYTES,
    DataSyncManager,
    SyncAlreadyRunningError,
    _decode_text_smart,
    _detect_csv_delimiter,
    _is_pid_alive,
    _write_json_atomic,
)


TRANSIENT_DOWNLOAD_SUFFIXES = {".crdownload", ".part", ".tmp", ".partial", ".download"}
QUARANTINE_SUBDIR = "_quarentena_revisao"
RE_DRIVE_ID = re.compile(r"^[A-Za-z0-9_-]{10,128}$")


class FleetAlreadyRunningError(RuntimeError):
    """Disparado quando já existe uma execução ativa do `FleetVerifier` segurando `_fleet.lock`."""


def _norm_ascii(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    clean = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", clean).strip("_")


def _compute_file_md5(fpath: Path) -> str:
    h = hashlib.md5()
    with open(fpath, "rb") as fh:
        while True:
            chunk = fh.read(65536)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest().lower()


def _is_valid_drive_id(candidate: str) -> bool:
    return bool(RE_DRIVE_ID.match(str(candidate or "").strip()))


class FleetVerifier:
    """
    Coordenador sequencial de atualização multi-município, fila assistida pós-CAPTCHA,
    ingestão automática de `inbox/` e sincronização Two-Phase Delete com Google Drive.
    """

    def __init__(self, workspace_root: Path = DEFAULT_WORKSPACE_ROOT) -> None:
        self.workspace_root = workspace_root
        self.onboarding = CityOnboardingManager(workspace_root=workspace_root)
        env_inbox = os.environ.get("SIM_INBOX_DIR", "").strip()
        env_outbox = os.environ.get("SIM_CLOUD_OUTBOX_DIR", "").strip()
        self.inbox_dir = Path(env_inbox) if env_inbox else (workspace_root / "avaliacao" / "dados" / "inbox")
        self.quarantine_dir = self.inbox_dir / QUARANTINE_SUBDIR
        self.cloud_outbox_dir = Path(env_outbox) if env_outbox else (workspace_root / "avaliacao" / "dados" / "cloud_outbox")
        self.cloud_manifest_path = workspace_root / "tenants" / "_cloud_datalake_manifest.json"
        self.fleet_state_path = workspace_root / "avaliacao" / "dados" / "_fleet_verifier_state.json"
        self.fleet_lock_file = workspace_root / "avaliacao" / "dados" / "_fleet.lock"
        self._fleet_lock_held: bool = False

    @contextmanager
    def _acquire_fleet_lock(self, stale_after_sec: int = 1800) -> Iterator[None]:
        """Lock global exclusivo reentrante (`_fleet.lock`) com validação de `pid` ativo e `lock_token`."""
        if self._fleet_lock_held:
            yield
            return

        self.fleet_lock_file.parent.mkdir(parents=True, exist_ok=True)
        if self.fleet_lock_file.exists():
            try:
                age = time.time() - self.fleet_lock_file.stat().st_mtime
                old_pid = 0
                try:
                    meta = json.loads(self.fleet_lock_file.read_text(encoding="utf-8"))
                    if isinstance(meta, dict):
                        old_pid = int(meta.get("pid") or 0)
                except Exception:
                    old_pid = 0
                if old_pid > 0:
                    if not _is_pid_alive(old_pid):
                        self.fleet_lock_file.unlink(missing_ok=True)
                elif age > stale_after_sec:
                    self.fleet_lock_file.unlink(missing_ok=True)
            except Exception:
                pass

        lock_token = secrets.token_hex(16)
        try:
            fd = os.open(str(self.fleet_lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "pid": os.getpid(),
                        "lock_token": lock_token,
                        "started_at": datetime.now().isoformat(),
                    },
                    fh,
                )
        except FileExistsError as exc:
            raise FleetAlreadyRunningError(
                "Já existe uma execução do FleetVerifier em andamento (`_fleet.lock` ativo)."
            ) from exc

        self._fleet_lock_held = True
        try:
            yield
        finally:
            self._fleet_lock_held = False
            try:
                if self.fleet_lock_file.exists():
                    cur_meta = json.loads(self.fleet_lock_file.read_text(encoding="utf-8"))
                    if isinstance(cur_meta, dict) and cur_meta.get("lock_token") == lock_token:
                        self.fleet_lock_file.unlink(missing_ok=True)
            except Exception:
                pass

    def _load_cloud_manifest(self) -> Dict[str, Any]:
        if self.cloud_manifest_path.exists():
            try:
                data = json.loads(self.cloud_manifest_path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {"provider": "google_drive", "tenants": {}}

    def _save_cloud_manifest(self, manifest: Dict[str, Any]) -> None:
        manifest["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _write_json_atomic(self.cloud_manifest_path, manifest)

    def _list_inbox_candidates(self) -> List[Path]:
        if not self.inbox_dir.exists():
            return []
        candidates: List[Path] = []
        for p in sorted(self.inbox_dir.iterdir()):
            # Se for diretório real (não symlink), ignora; symlinks entram na lista para rejeição explícita em process_inbox
            if not p.is_symlink() and p.is_dir():
                continue
            name = p.name
            if name.startswith((".", "~$")) or name.endswith(".reason.json"):
                continue
            low_name = name.lower()
            if any(low_name.endswith(suf) for suf in TRANSIENT_DOWNLOAD_SUFFIXES):
                continue
            if p.suffix.lower() not in ALLOWED_UPLOAD_EXTENSIONS:
                continue
            candidates.append(p)
        return candidates

    @staticmethod
    def _is_file_stable_and_ready(fpath: Path, wait_sec: Optional[float] = None) -> bool:
        if fpath.is_symlink() or not fpath.exists() or not fpath.is_file():
            return False
        eff_wait = (
            wait_sec
            if wait_sec is not None
            else (0.05 if os.environ.get("PYTEST_CURRENT_TEST") else 1.0)
        )
        try:
            s1 = fpath.stat(follow_symlinks=False).st_size
            if s1 <= 0:
                return False
            if eff_wait > 0:
                time.sleep(eff_wait)
            s2 = fpath.stat(follow_symlinks=False).st_size
            if s1 != s2:
                return False
            with open(fpath, "rb") as fh:
                fh.read(16)
            return True
        except Exception:
            return False

    @staticmethod
    def _scan_csv_ibge_codes(fpath: Path, ext: str) -> Set[str]:
        """
        Inspeciona amostras CSV (avulsas ou dentro de `.zip`) coletando TODOS os códigos IBGE válidos
        de 7 dígitos, inclusive municípios ainda não cadastrados, para barrar CSVs estaduais/nacionais.
        """
        csv_samples: List[bytes] = []
        try:
            if ext == ".csv":
                with open(fpath, "rb") as fh:
                    csv_samples.append(fh.read(2 * 1024 * 1024))
            elif ext == ".zip":
                with zipfile.ZipFile(fpath, "r") as zf:
                    for info in zf.infolist()[:30]:
                        if not info.is_dir() and info.filename.lower().endswith(".csv"):
                            with zf.open(info, "r") as zf_csv:
                                csv_samples.append(zf_csv.read(1024 * 1024))
                            if len(csv_samples) >= 5:
                                break
        except Exception:
            return set()

        all_ibges: Set[str] = set()
        target_ibge_cols = {
            "cod_ibge",
            "codigo_ibge",
            "id_ente",
            "cod_municipio_ibge",
            "codigo_municipio_ibge",
            "co_municipio_ibge",
            "ibge",
        }
        for sample_bytes in csv_samples:
            txt = _decode_text_smart(sample_bytes)
            delim = _detect_csv_delimiter(txt)
            reader = csv.DictReader(io.StringIO(txt), delimiter=delim)
            if not reader.fieldnames:
                continue
            col_map = {_norm_ascii(c): c for c in reader.fieldnames if c}
            matched_cols = [
                orig_c
                for norm_c, orig_c in col_map.items()
                if norm_c in target_ibge_cols or "ibge" in norm_c
            ]
            if not matched_cols:
                continue
            for idx, row in enumerate(reader):
                if idx >= 10000:
                    break
                for mc in matched_cols:
                    val = re.sub(r"[^0-9]", "", str(row.get(mc) or ""))
                    if is_valid_ibge7(val):
                        all_ibges.add(val)
        return all_ibges

    def detect_tenant_for_file(self, fpath: Path, tenants: List[Dict[str, Any]]) -> Tuple[Optional[str], str]:
        """
        Resolução determinística em 3 níveis (Zero Chute + Bloqueio de CSV Estadual):
        - Coleta TODOS os códigos IBGE válidos de 7 dígitos (não apenas os presentes em `by_ibge`).
        - Se houver múltiplos códigos IBGE (ex.: CSV estadual de MG com 853 municípios), quarentena imediatamente!
        - Nível 1: `tenant_id` explícito ou código IBGE de 7 dígitos no nome do arquivo ou nos membros internos do `.zip`.
        - Nível 2: Slug inequívoco do nome do município no nome do arquivo (validado contra conteúdo CSV).
        - Nível 3: Colunas IBGE no conteúdo CSV.
        """
        by_tid = {str(t["tenant_id"]).lower(): str(t["tenant_id"]) for t in tenants if t.get("tenant_id")}
        by_ibge = {str(t["ibge_code"]).strip(): str(t["tenant_id"]) for t in tenants if t.get("ibge_code")}

        fname = fpath.name
        fname_low = fname.lower()
        fname_norm = _norm_ascii(fpath.stem)
        ext = fpath.suffix.lower()

        # Inspecionar códigos IBGE internos em CSV/ZIP antes de aceitar roteamento para bloquear arquivos estaduais
        content_all_ibges = self._scan_csv_ibge_codes(fpath, ext) if ext in (".csv", ".zip") else set()
        if len(content_all_ibges) > 1:
            sample_codes = sorted(content_all_ibges)[:6]
            return (
                None,
                f"quarentena_multiplos_municipios_no_conteudo({len(content_all_ibges)}_codigos_ibge_ex_{sample_codes})",
            )

        # Nível 1A: prefixo explícito `<tenant_id>__` ou `<tenant_id>` contido no nome
        matched_tids = [orig_tid for low_tid, orig_tid in by_tid.items() if low_tid in fname_low]
        if len(matched_tids) > 1:
            return None, f"ambiguo_multiplos_tenant_ids_no_nome({matched_tids})"
        if len(matched_tids) == 1:
            cand_tid = matched_tids[0]
            if len(content_all_ibges) == 1:
                only_ibge = next(iter(content_all_ibges))
                if by_ibge.get(only_ibge) != cand_tid:
                    return None, f"conflito_ibge_conteudo_vs_nome({only_ibge}_vs_{cand_tid})"
            return cand_tid, f"nivel_1_tenant_id_nome({cand_tid})"

        # Nível 1B: TODOS os códigos IBGE válidos de 7 dígitos no nome do arquivo
        fname_all_ibges = {
            m for m in re.findall(r"(?<!\d)(\d{7})(?!\d)", fname) if is_valid_ibge7(m)
        }
        if len(fname_all_ibges) > 1:
            return None, f"ambiguo_multiplos_ibge_no_nome({sorted(fname_all_ibges)})"
        if len(fname_all_ibges) == 1:
            code7 = next(iter(fname_all_ibges))
            if code7 not in by_ibge:
                return None, f"municipio_ibge_nao_cadastrado_no_nome({code7})"
            if len(content_all_ibges) == 1 and next(iter(content_all_ibges)) != code7:
                return None, f"conflito_ibge_nome_vs_conteudo({code7}_vs_{next(iter(content_all_ibges))})"
            tid = by_ibge[code7]
            return tid, f"nivel_1_ibge_no_nome({tid})"

        # Nível 1C: dentro de pacote `.zip` (ex.: `2024.3137205.despesa.pagamento.csv` do TCE-MG)
        if ext == ".zip":
            try:
                with zipfile.ZipFile(fpath, "r") as zf:
                    zip_all_ibges: Set[str] = set()
                    for member in zf.namelist()[:200]:
                        for code7 in re.findall(r"(?<!\d)(\d{7})(?!\d)", member):
                            if is_valid_ibge7(code7):
                                zip_all_ibges.add(code7)
                    if len(zip_all_ibges) > 1:
                        return None, f"ambiguo_multiplos_ibge_no_zip({sorted(zip_all_ibges)[:10]})"
                    if len(zip_all_ibges) == 1:
                        z_code = next(iter(zip_all_ibges))
                        if z_code not in by_ibge:
                            return None, f"municipio_ibge_nao_cadastrado_no_zip({z_code})"
                        tid = by_ibge[z_code]
                        return tid, f"nivel_1_ibge_membro_zip({tid})"
            except Exception as exc:
                return None, f"zip_invalido_ou_corrompido({type(exc).__name__})"

        # Nível 2: slug normalizado inequívoco do nome do município no nome do arquivo
        slug_hits: List[str] = []
        padded_fname = f"_{fname_norm}_"
        for t in tenants:
            m_name = str(t.get("municipality_name") or "").strip()
            slug = _norm_ascii(m_name)
            if slug and len(slug) >= 4 and f"_{slug}_" in padded_fname:
                slug_hits.append(str(t["tenant_id"]))
        if len(slug_hits) > 1:
            return None, f"ambiguo_multiplos_municipios_no_nome({slug_hits})"
        if len(slug_hits) == 1:
            cand_tid = slug_hits[0]
            if len(content_all_ibges) == 1:
                only_ibge = next(iter(content_all_ibges))
                if by_ibge.get(only_ibge) != cand_tid:
                    return None, f"conflito_slug_vs_ibge_conteudo({cand_tid}_vs_{only_ibge})"
            return cand_tid, f"nivel_2_slug_municipio({cand_tid})"

        # Nível 3: código IBGE único no conteúdo CSV
        if len(content_all_ibges) == 1:
            only_ibge = next(iter(content_all_ibges))
            if only_ibge in by_ibge:
                tid = by_ibge[only_ibge]
                return tid, f"nivel_3_coluna_ibge_conteudo({tid})"
            return None, f"municipio_ibge_nao_cadastrado_no_conteudo({only_ibge})"

        return None, "municipio_nao_identificado_renomeie_com_codigo_ibge_7_digitos"

    def _quarantine_file(self, fpath: Path, reason: str, detail: Optional[str] = None) -> Dict[str, Any]:
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest_name = fpath.name if not (self.quarantine_dir / fpath.name).exists() else f"{ts}__{fpath.name}"
        dest_path = self.quarantine_dir / dest_name
        if fpath.is_symlink():
            # Jamais seguir symlink: remover apenas o ponteiro simbólico da inbox
            fpath.unlink(missing_ok=True)
        else:
            shutil.move(str(fpath), str(dest_path))
        reason_payload = {
            "quarantined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "original_filename": fpath.name,
            "quarantine_path": str(dest_path) if not dest_path.is_symlink() and dest_path.exists() else None,
            "reason_code": reason,
            "detail": detail or (
                "Não foi possível vincular este arquivo a um único município cadastrado com 100% de certeza. "
                "Renomeie prefixando o código IBGE (ex.: '3137205__' + nome do arquivo) e mova de volta para inbox/."
            ),
        }
        reason_file = self.quarantine_dir / f"{dest_name}.reason.json"
        _write_json_atomic(reason_file, reason_payload)
        return reason_payload

    def process_inbox(
        self,
        runtime_pool: Optional[Any] = None,
        defer_release_bundles_for: Optional[Set[str]] = None,
    ) -> Dict[str, Any]:
        """
        Varre `avaliacao/dados/inbox/`, rejeita symlinks e arquivos gigantes antes de `read_bytes()`,
        roteia cada pacote ao município exato, preserva arquivos em `skipped_busy_tenant` caso o tenant
        esteja sincronizando (`SyncAlreadyRunningError`), converte em Parquet ZSTD e prepara `cloud_outbox/`.
        """
        with self._acquire_fleet_lock():
            self.inbox_dir.mkdir(parents=True, exist_ok=True)
            tenants = self.onboarding.list_tenants()
            candidates = self._list_inbox_candidates()
            deferred_tids = {str(x).lower() for x in (defer_release_bundles_for or set())}

            ingested: List[Dict[str, Any]] = []
            quarantined: List[Dict[str, Any]] = []
            skipped_downloading: List[str] = []
            skipped_busy_tenant: List[str] = []

            for fpath in candidates:
                # 1. Bloquear symlinks estritamente ANTES de qualquer leitura ou stat com follow
                if fpath.is_symlink():
                    q_sym = self._quarantine_file(
                        fpath,
                        reason="symlink_bloqueado_seguranca",
                        detail="Links simbólicos em `inbox/` são estritamente proibidos por segurança.",
                    )
                    quarantined.append(q_sym)
                    continue

                # 2. Validar tamanho bruto ANTES de _is_file_stable_and_ready / read_bytes()
                try:
                    raw_stat = fpath.stat(follow_symlinks=False)
                except Exception:
                    skipped_downloading.append(fpath.name)
                    continue

                if raw_stat.st_size > MAX_UPLOAD_RAW_BYTES:
                    q_big = self._quarantine_file(
                        fpath,
                        reason="arquivo_excede_limite_maximo_bytes",
                        detail=(
                            f"Arquivo '{fpath.name}' ({raw_stat.st_size} bytes) excede o limite máximo de "
                            f"{MAX_UPLOAD_RAW_BYTES // (1024 * 1024)} MB."
                        ),
                    )
                    quarantined.append(q_big)
                    continue

                if not self._is_file_stable_and_ready(fpath):
                    skipped_downloading.append(fpath.name)
                    continue

                # Revalidar tamanho após espera de estabilidade
                final_size = fpath.stat(follow_symlinks=False).st_size
                if final_size > MAX_UPLOAD_RAW_BYTES:
                    q_big2 = self._quarantine_file(
                        fpath,
                        reason="arquivo_excede_limite_maximo_bytes",
                        detail=(
                            f"Arquivo '{fpath.name}' ({final_size} bytes) excede o limite máximo de "
                            f"{MAX_UPLOAD_RAW_BYTES // (1024 * 1024)} MB."
                        ),
                    )
                    quarantined.append(q_big2)
                    continue

                matched_tid, match_reason = self.detect_tenant_for_file(fpath, tenants)
                if not matched_tid:
                    detail_msg = None
                    if "multiplos_municipios" in match_reason or "multiplos_ibge" in match_reason:
                        detail_msg = (
                            f"Pacote estadual/multi-municipal detectado ({match_reason}). "
                            "Filtre apenas o código IBGE do município alvo antes de depositar em `inbox/`."
                        )
                    q_info = self._quarantine_file(fpath, reason=match_reason, detail=detail_msg)
                    quarantined.append(q_info)
                    continue

                try:
                    content_bytes = fpath.read_bytes()
                    db_mgr = None
                    rag_ret = None
                    rt = None
                    if runtime_pool is not None:
                        getter = getattr(runtime_pool, "get_if_loaded", None) or getattr(runtime_pool, "peek_loaded", None)
                        if callable(getter):
                            rt = getter(matched_tid)
                            if rt is not None:
                                db_mgr = rt.db_manager
                                rag_ret = rt.rag_retriever

                    sync_mgr = DataSyncManager(workspace_root=self.workspace_root, tenant_id=matched_tid)
                    rep = sync_mgr.ingest_uploaded_package(
                        filename=fpath.name,
                        content_bytes=content_bytes,
                        db_manager=db_mgr,
                        rag_retriever=rag_ret,
                    )
                    if rt is not None and hasattr(rt, "orchestrator") and hasattr(rt.orchestrator, "refresh_after_data_update"):
                        rt.orchestrator.refresh_after_data_update()

                    # Enfileirar pacote bruto em `cloud_outbox/<tid>/raw_packages/` para arquivamento no Google Drive
                    raw_outbox = self.cloud_outbox_dir / matched_tid / "raw_packages"
                    raw_outbox.mkdir(parents=True, exist_ok=True)
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    archived_raw_path = raw_outbox / f"{matched_tid}__{ts}__{fpath.name}"
                    shutil.copy2(str(fpath), str(archived_raw_path))

                    # Evitar gerar 2 bundles seguidos na mesma rodada quando `verify_and_sync_fleet` vai exportar logo após `sync_all`
                    bundle_name_staged: Optional[str] = None
                    if matched_tid.lower() not in deferred_tids:
                        bundle_info = sync_mgr.export_release_bundle_to_cloud_outbox()
                        bundle_name_staged = bundle_info.get("bundle_name")

                    # Somente APÓS Parquet materializado + outbox garantido, remover o arquivo bruto de `inbox/`
                    fpath.unlink(missing_ok=True)

                    ingested.append(
                        {
                            "filename": fpath.name,
                            "tenant_id": matched_tid,
                            "detection_rule": match_reason,
                            "release_id": rep.get("release_id"),
                            "ingested_tables_count": len(rep.get("ingested_tables") or []),
                            "rag_acts_added": rep.get("rag_acts_added", 0),
                            "staged_for_cloud_raw": archived_raw_path.name,
                            "staged_for_cloud_release": bundle_name_staged,
                        }
                    )
                except SyncAlreadyRunningError:
                    # JAMAIS mover para `_quarentena_revisao/` quando o tenant apenas está com sync em andamento!
                    skipped_busy_tenant.append(fpath.name)
                except Exception as exc:
                    q_info = self._quarantine_file(
                        fpath,
                        reason="falha_validacao_ingestao",
                        detail=f"Município detectado ({matched_tid}), porém ocorreu erro ao converter pacote: {exc}",
                    )
                    quarantined.append(q_info)

            return {
                "inbox_path": str(self.inbox_dir),
                "scanned_files": len(candidates),
                "ingested_count": len(ingested),
                "quarantined_count": len(quarantined),
                "skipped_still_downloading": skipped_downloading,
                "skipped_busy_tenant": skipped_busy_tenant,
                "ingested": ingested,
                "quarantined": quarantined,
            }

    def build_captcha_assisted_queue(self, tenants: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Gera a Fila Assistida Humana apenas para portais com CAPTCHA ou download manual obrigatório
        (TCE-MG SICOM e Diário Oficial), sem jamais tentar contornar ou automatizar CAPTCHAs.
        """
        t_list = tenants if tenants is not None else self.onboarding.list_tenants()
        queue: List[Dict[str, Any]] = []
        for t in t_list:
            tid = str(t["tenant_id"])
            city = str(t.get("municipality_name") or tid)
            uf = str(t.get("state_uf") or "MG")
            ibge = str(t.get("ibge_code") or "")
            prof = load_tenant_profile(tid, workspace_root=self.workspace_root)
            sync_mgr = DataSyncManager(workspace_root=self.workspace_root, tenant_id=tid)
            st = sync_mgr.check_status()
            sources_by_id = {s["source_id"]: s for s in st.get("sources", [])}

            sicom_st = sources_by_id.get("SICOM") or {}
            sicom_ready = int(sicom_st.get("tables_ready") or 0)
            if sicom_ready < 60:
                queue.append(
                    {
                        "tenant_id": tid,
                        "municipality": f"{city}/{uf}",
                        "ibge_code": ibge,
                        "source_id": "SICOM",
                        "priority": "ALTA",
                        "reason": (
                            f"Catálogo SICOM/TCE-MG pendente ({sicom_ready}/65 tabelas). "
                            "As APIs automáticas federais (IBGE, SICONFI/STN, CGU) já operam sem CAPTCHA."
                        ),
                        "portal_url": "https://sicomconsulta.tce.mg.gov.br/",
                        "human_step": (
                            f"1) Abra o Portal SICOM/TCE-MG, selecione '{city}' (IBGE {ibge}) e resolva o CAPTCHA manualmente; "
                            f"2) Salve o arquivo `.zip` em `avaliacao/dados/inbox/` (ex.: `{ibge}_sicom_2025.zip`). "
                            "O verificador converte para Parquet ZSTD, envia cópia ao Google Drive (5 TB) e apaga o bruto local."
                        ),
                    }
                )

            do_meta = prof.get("diario_oficial") or {}
            rag_st = sources_by_id.get("DIARIO_OFICIAL") or {}
            rag_acts = int((st.get("rag_collection_audit") or {}).get("unique_individual_acts") or 0)
            if rag_st.get("status") != "UP_TO_DATE" or (not tid.startswith("caete") and rag_acts == 0):
                do_url = str(do_meta.get("url_consulta") or "https://www.diariomunicipal.com.br/amm-mg/")
                queue.append(
                    {
                        "tenant_id": tid,
                        "municipality": f"{city}/{uf}",
                        "ibge_code": ibge,
                        "source_id": "DIARIO_OFICIAL",
                        "priority": "MEDIA",
                        "reason": f"Collection RAG do Diário Oficial ({do_meta.get('veiculo', 'Diário Oficial')}) com 0 atos indexados.",
                        "portal_url": do_url,
                        "human_step": (
                            f"Baixe edições pesquisáveis (`.pdf`, `.txt` ou `.json`) do Diário Oficial de {city} "
                            f"e solte em `avaliacao/dados/inbox/{ibge}__diario_oficial.pdf`."
                        ),
                    }
                )
        return queue

    def _count_pending_outbox_files(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        if not self.cloud_outbox_dir.exists():
            return counts
        for t_dir in sorted(self.cloud_outbox_dir.iterdir()):
            if not t_dir.is_dir():
                continue
            c = sum(1 for p in t_dir.rglob("*") if p.is_file() and not p.name.startswith("."))
            if c > 0:
                counts[t_dir.name] = c
        return counts

    @staticmethod
    def _parse_gdrive_json_items(stdout_txt: str) -> List[Dict[str, Any]]:
        txt = (stdout_txt or "").strip()
        if not txt:
            return []
        try:
            obj = json.loads(txt)
            if isinstance(obj, list):
                return [x for x in obj if isinstance(x, dict)]
            if isinstance(obj, dict):
                for k in ("files", "items", "entries", "results"):
                    val = obj.get(k)
                    if isinstance(val, list):
                        return [x for x in val if isinstance(x, dict)]
        except Exception:
            pass
        return []

    @classmethod
    def _ensure_drive_subfolder(cls, gdrive_bin: str, parent_id: str, folder_name: str) -> Optional[str]:
        if not _is_valid_drive_id(parent_id):
            return None
        try:
            proc_ls = subprocess.run(
                [gdrive_bin, "readonly", "ls", parent_id, "--json"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=60,
            )
            if proc_ls.returncode == 0:
                for it in cls._parse_gdrive_json_items(proc_ls.stdout):
                    if bool(it.get("trashed") or it.get("explicitlyTrashed")):
                        continue
                    mime = str(it.get("mimeType") or it.get("type") or "").lower()
                    is_folder = ("folder" in mime) or bool(it.get("isFolder"))
                    cand_id = str(it.get("id") or it.get("fileId") or "").strip()
                    if is_folder and str(it.get("name") or "").strip() == folder_name and _is_valid_drive_id(cand_id):
                        return cand_id
        except Exception:
            pass

        try:
            proc_mk = subprocess.run(
                [gdrive_bin, "mutate", "mkdir", folder_name, "--parent", parent_id, "--json"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=60,
            )
            if proc_mk.returncode == 0:
                mk_txt = (proc_mk.stdout or "").strip()
                cand_id = ""
                if mk_txt.startswith("{"):
                    try:
                        mk_obj = json.loads(mk_txt)
                        cand_id = str(mk_obj.get("id") or mk_obj.get("fileId") or "").strip()
                    except Exception:
                        cand_id = ""
                if not cand_id:
                    m_id = re.search(r"\bID:\s*([A-Za-z0-9_-]{10,128})", mk_txt)
                    if m_id:
                        cand_id = m_id.group(1)
                if _is_valid_drive_id(cand_id):
                    return cand_id
        except Exception:
            pass
        return None

    def _auto_provision_tenant_drive_folders(
        self,
        gdrive_bin: str,
        tid: str,
        manifest: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Auto-provisiona `<UF>/<ibge>_<slug>/{raw_packages,releases}` no Google Drive para novas cidades
        quando `states[<UF>].folder_id` ou `root_folder_id` está configurado no manifesto.
        """
        prof = load_tenant_profile(tid, workspace_root=self.workspace_root)
        uf = str(prof.get("state_uf") or "MG").upper().strip()
        ibge = str(prof.get("ibge_code") or "").strip()
        city_name = str(prof.get("municipality_name") or tid).strip()
        slug = _norm_ascii(city_name)
        city_folder_name = f"{ibge}_{slug}" if is_valid_ibge7(ibge) and slug else tid

        states_map = manifest.setdefault("states", {})
        uf_entry = states_map.get(uf) if isinstance(states_map.get(uf), dict) else {}
        state_folder_id = str(uf_entry.get("folder_id") or "").strip()
        if not _is_valid_drive_id(state_folder_id):
            root_id = str(manifest.get("root_folder_id") or "").strip()
            if not _is_valid_drive_id(root_id):
                return None
            created_uf_id = self._ensure_drive_subfolder(gdrive_bin, root_id, uf)
            if not created_uf_id:
                return None
            state_folder_id = created_uf_id
            states_map[uf] = {
                "folder_id": state_folder_id,
                "folder_url": f"https://drive.google.com/drive/folders/{state_folder_id}",
            }

        tenants_cloud = manifest.setdefault("tenants", {})
        t_cloud = dict(tenants_cloud.get(tid) or {})
        city_folder_id = str(t_cloud.get("folder_id") or "").strip()
        if not _is_valid_drive_id(city_folder_id):
            created_city_id = self._ensure_drive_subfolder(gdrive_bin, state_folder_id, city_folder_name)
            if not created_city_id:
                return None
            city_folder_id = created_city_id

        raw_id = str(t_cloud.get("raw_packages_folder_id") or "").strip()
        if not _is_valid_drive_id(raw_id):
            created_raw = self._ensure_drive_subfolder(gdrive_bin, city_folder_id, "raw_packages")
            if not created_raw:
                return None
            raw_id = created_raw

        rel_id = str(t_cloud.get("releases_folder_id") or "").strip()
        if not _is_valid_drive_id(rel_id):
            created_rel = self._ensure_drive_subfolder(gdrive_bin, city_folder_id, "releases")
            if not created_rel:
                return None
            rel_id = created_rel

        city_url = f"https://drive.google.com/drive/folders/{city_folder_id}"
        t_cloud.update(
            {
                "municipality_name": city_name,
                "state_uf": uf,
                "ibge_code": ibge,
                "drive_folder_name": city_folder_name,
                "city_folder_name": city_folder_name,
                "folder_id": city_folder_id,
                "folder_url": city_url,
                "city_folder_url": city_url,
                "raw_packages_folder_id": raw_id,
                "releases_folder_id": rel_id,
            }
        )
        t_cloud.setdefault("synced_files", [])
        tenants_cloud[tid] = t_cloud
        self._save_cloud_manifest(manifest)
        return t_cloud

    @classmethod
    def _verify_remote_drive_file(
        cls,
        gdrive_bin: str,
        file_id: str,
        expected_size: int,
        expected_md5: str,
    ) -> Tuple[bool, str, str]:
        """
        Verifica via `gdrive readonly info <file_id> --json` que:
        - `file_id` obedece `^[A-Za-z0-9_-]{10,128}$`
        - `id == file_id`, `size == expected_size > 0`
        - `not trashed`
        - `md5Checksum == expected_md5` (sempre que `md5Checksum` vier preenchido no JSON).
        Retorna `(ok, web_url, error_detail)`.
        """
        if not _is_valid_drive_id(file_id):
            return False, "", f"Drive ID inválido: '{file_id}'"
        proc_ver = subprocess.run(
            [gdrive_bin, "readonly", "info", file_id, "--json"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=60,
        )
        remote_web_url = f"https://drive.google.com/file/d/{file_id}/view"
        if proc_ver.returncode != 0:
            return False, remote_web_url, (proc_ver.stderr or proc_ver.stdout or "Verificação falhou")[:240]

        ver_txt = (proc_ver.stdout or "").strip()
        if ver_txt.startswith("{"):
            try:
                ver_obj = json.loads(ver_txt)
            except Exception:
                ver_obj = {}
            rem_id = str(ver_obj.get("id") or "")
            is_trashed = bool(ver_obj.get("trashed") or ver_obj.get("explicitlyTrashed"))
            if is_trashed:
                return False, remote_web_url, "Arquivo remoto encontra-se na lixeira (`trashed=true`)."
            raw_rem_size = ver_obj.get("size")
            rem_size = int(raw_rem_size) if raw_rem_size is not None and str(raw_rem_size).isdigit() else -1
            rem_md5 = str(ver_obj.get("md5Checksum") or "").strip().lower()
            if rem_md5 and rem_md5 != expected_md5.lower():
                return False, remote_web_url, f"Divergência de MD5 remoto ({rem_md5} != {expected_md5})."
            if rem_id == file_id and rem_size == expected_size and rem_size > 0:
                remote_web_url = str(ver_obj.get("webViewLink") or remote_web_url)
                return True, remote_web_url, ""
            return False, remote_web_url, f"Metadados remotos divergentes (id={rem_id}, size={rem_size}, esperado={expected_size})."

        if re.search(r"\bTrashed:\s*(true|yes)\b", ver_txt, flags=re.IGNORECASE):
            return False, remote_web_url, "Arquivo remoto marcado como Trashed."
        m_md5 = re.search(r"\bMD5:\s*([0-9a-fA-F]{32})\b", ver_txt)
        if m_md5 and m_md5.group(1).lower() != expected_md5.lower():
            return False, remote_web_url, "Divergência de MD5 remoto."
        m_sz = re.search(r"\bSize:\s*(\d+)\s*B\b", ver_txt, flags=re.IGNORECASE)
        if file_id in ver_txt and m_sz and int(m_sz.group(1)) == expected_size and expected_size > 0:
            return True, remote_web_url, ""
        return False, remote_web_url, (ver_txt or "Verificação textual falhou")[:240]

    def push_outbox_to_gdrive(self) -> Dict[str, Any]:
        """
        Sincroniza `avaliacao/dados/cloud_outbox/` com `SIM_DataLake/` no Google Drive (5 TB) via `gdrive` CLI.
        Protocolo Two-Phase Delete Blindado:
        1. Valida `^[A-Za-z0-9_-]{10,128}$` em todos os IDs e auto-provisiona pastas de novas cidades;
        2. Evita re-upload duplicado checando `(filename, local_size)` na pasta remota e no manifesto;
        3. Confirma `size == local_size > 0`, `not trashed` e `md5Checksum`;
        4. Salva `_cloud_datalake_manifest.json` **ANTES** de `local_file.unlink()`.
        """
        with self._acquire_fleet_lock():
            gdrive_bin = shutil.which("gdrive")
            pending_before = self._count_pending_outbox_files()
            if not gdrive_bin:
                return {
                    "status": "skipped_no_gdrive_cli",
                    "detail": (
                        "CLI `gdrive` não encontrada neste ambiente (ex.: interior do container Docker). "
                        "Os pacotes permanecem seguros em `avaliacao/dados/cloud_outbox/` até a execução no host Windows."
                    ),
                    "pending_outbox_by_tenant": pending_before,
                    "uploaded": [],
                }

            manifest = self._load_cloud_manifest()
            tenants_cloud = manifest.setdefault("tenants", {})
            uploaded_files: List[Dict[str, Any]] = []
            skipped_duplicates: List[Dict[str, Any]] = []
            errors: List[Dict[str, Any]] = []

            if not self.cloud_outbox_dir.exists():
                return {
                    "status": "ok",
                    "detail": "Nenhum arquivo pendente em `cloud_outbox/`.",
                    "pending_outbox_by_tenant": {},
                    "uploaded": [],
                }

            for t_dir in sorted(self.cloud_outbox_dir.iterdir()):
                if not t_dir.is_dir():
                    continue
                tid = t_dir.name
                t_cloud = tenants_cloud.get(tid) or {}
                has_valid_folders = (
                    isinstance(t_cloud, dict)
                    and _is_valid_drive_id(str(t_cloud.get("raw_packages_folder_id") or ""))
                    and _is_valid_drive_id(str(t_cloud.get("releases_folder_id") or ""))
                )
                if not has_valid_folders:
                    provisioned = self._auto_provision_tenant_drive_folders(gdrive_bin, tid, manifest)
                    if provisioned is not None:
                        t_cloud = provisioned
                    elif not t_cloud:
                        errors.append(
                            {
                                "tenant_id": tid,
                                "error": "Tenant ausente em `_cloud_datalake_manifest.json` e sem `root_folder_id` válido para auto-provisionamento.",
                            }
                        )
                        continue

                for sub_cat, folder_key in (("raw_packages", "raw_packages_folder_id"), ("releases", "releases_folder_id")):
                    sub_dir = t_dir / sub_cat
                    parent_id = str(t_cloud.get(folder_key) or "").strip()
                    if not sub_dir.exists() or not parent_id:
                        continue
                    if not _is_valid_drive_id(parent_id):
                        errors.append(
                            {
                                "tenant_id": tid,
                                "stage": "validate_folder_id",
                                "error": f"ID de pasta do Google Drive inválido para '{folder_key}': '{parent_id}'.",
                            }
                        )
                        continue

                    local_candidates = [
                        p for p in sorted(sub_dir.iterdir())
                        if not p.is_symlink() and p.is_file() and not p.name.startswith(".") and p.stat().st_size > 0
                    ]
                    if not local_candidates:
                        continue

                    # Mapear arquivos já existentes na pasta remota (evita re-upload duplicado!)
                    remote_by_name_size: Dict[Tuple[str, int], Dict[str, Any]] = {}
                    try:
                        proc_ls = subprocess.run(
                            [gdrive_bin, "readonly", "ls", parent_id, "--json"],
                            capture_output=True,
                            text=True,
                            encoding="utf-8",
                            errors="replace",
                            timeout=60,
                        )
                        if proc_ls.returncode == 0:
                            for rem_it in self._parse_gdrive_json_items(proc_ls.stdout):
                                if bool(rem_it.get("trashed") or rem_it.get("explicitlyTrashed")):
                                    continue
                                r_name = str(rem_it.get("name") or "").strip()
                                r_sz_raw = rem_it.get("size")
                                r_id = str(rem_it.get("id") or rem_it.get("fileId") or "").strip()
                                if r_name and r_sz_raw is not None and str(r_sz_raw).isdigit() and _is_valid_drive_id(r_id):
                                    remote_by_name_size[(r_name, int(r_sz_raw))] = rem_it
                    except Exception:
                        remote_by_name_size = {}

                    synced_list: List[Dict[str, Any]] = t_cloud.setdefault("synced_files", [])
                    manifest_by_name_size: Dict[Tuple[str, int], str] = {}
                    for sf in synced_list:
                        if not isinstance(sf, dict):
                            continue
                        sf_id = str(sf.get("drive_file_id") or "").strip()
                        sf_nm = str(sf.get("filename") or "").strip()
                        sf_sz = int(sf.get("size_bytes") or 0)
                        if sf_nm and sf_sz > 0 and _is_valid_drive_id(sf_id):
                            manifest_by_name_size[(sf_nm, sf_sz)] = sf_id

                    for local_file in local_candidates:
                        local_size = local_file.stat().st_size
                        local_md5 = _compute_file_md5(local_file)

                        try:
                            # Checar duplicata já sincronizada (na pasta remota ou no manifesto)
                            dup_id = ""
                            rem_hit = remote_by_name_size.get((local_file.name, local_size))
                            if rem_hit:
                                rem_hit_md5 = str(rem_hit.get("md5Checksum") or "").strip().lower()
                                if not rem_hit_md5 or rem_hit_md5 == local_md5:
                                    dup_id = str(rem_hit.get("id") or rem_hit.get("fileId") or "").strip()
                            if not dup_id:
                                dup_id = manifest_by_name_size.get((local_file.name, local_size), "")

                            if dup_id and _is_valid_drive_id(dup_id):
                                ok_dup, dup_url, _ = self._verify_remote_drive_file(
                                    gdrive_bin, dup_id, local_size, local_md5
                                )
                                if ok_dup:
                                    if (local_file.name, local_size) not in manifest_by_name_size:
                                        dup_rec = {
                                            "filename": local_file.name,
                                            "category": sub_cat,
                                            "drive_file_id": dup_id,
                                            "parent_folder_id": parent_id,
                                            "size_bytes": local_size,
                                            "md5_checksum": local_md5,
                                            "two_phase_verified": True,
                                            "uploaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                            "web_url": dup_url,
                                        }
                                        synced_list.append(dup_rec)
                                        if len(synced_list) > 50:
                                            t_cloud["synced_files"] = synced_list[-50:]
                                    self._save_cloud_manifest(manifest)
                                    local_file.unlink(missing_ok=True)
                                    skipped_duplicates.append(
                                        {"tenant_id": tid, "filename": local_file.name, "drive_file_id": dup_id}
                                    )
                                    continue

                            # Fase 1: Upload para a pasta exata do município no Google Drive
                            proc_up = subprocess.run(
                                [gdrive_bin, "mutate", "upload", str(local_file), "--parent", parent_id, "--json"],
                                capture_output=True,
                                text=True,
                                encoding="utf-8",
                                errors="replace",
                                timeout=300,
                            )
                            if proc_up.returncode != 0:
                                errors.append(
                                    {
                                        "tenant_id": tid,
                                        "file": local_file.name,
                                        "stage": "upload",
                                        "error": (proc_up.stderr or proc_up.stdout or "")[:240],
                                    }
                                )
                                continue

                            out_txt = (proc_up.stdout or "").strip()
                            file_id = ""
                            if out_txt.startswith("{"):
                                try:
                                    up_json = json.loads(out_txt)
                                    file_id = str(up_json.get("id") or up_json.get("fileId") or "").strip()
                                except Exception:
                                    pass
                            if not file_id:
                                m_id = re.search(r"\bID:\s*([A-Za-z0-9_-]{10,128})", out_txt)
                                if m_id:
                                    file_id = m_id.group(1)
                            if not _is_valid_drive_id(file_id):
                                errors.append(
                                    {
                                        "tenant_id": tid,
                                        "file": local_file.name,
                                        "stage": "parse_file_id",
                                        "error": f"Upload retornou 0 mas sem file_id válido: {out_txt[:180]}",
                                    }
                                )
                                continue

                            # Fase 2 (Two-Phase Delete): Confirmar metadados, MD5 e not trashed antes de apagar local!
                            verified_ok, remote_web_url, ver_err = self._verify_remote_drive_file(
                                gdrive_bin, file_id, local_size, local_md5
                            )
                            if not verified_ok:
                                errors.append(
                                    {
                                        "tenant_id": tid,
                                        "file": local_file.name,
                                        "stage": "verify_remote_metadata",
                                        "error": ver_err,
                                    }
                                )
                                continue

                            # Fase 2 confirmada! Persistir manifesto ANTES de remover o arquivo local!
                            rec_entry = {
                                "filename": local_file.name,
                                "category": sub_cat,
                                "drive_file_id": file_id,
                                "parent_folder_id": parent_id,
                                "size_bytes": local_size,
                                "md5_checksum": local_md5,
                                "two_phase_verified": True,
                                "uploaded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "web_url": remote_web_url,
                            }
                            synced_list.append(rec_entry)
                            if len(synced_list) > 50:
                                t_cloud["synced_files"] = synced_list[-50:]
                                synced_list = t_cloud["synced_files"]

                            self._save_cloud_manifest(manifest)
                            local_file.unlink(missing_ok=True)
                            uploaded_files.append({"tenant_id": tid, **rec_entry})
                        except Exception as exc:
                            errors.append(
                                {
                                    "tenant_id": tid,
                                    "file": local_file.name,
                                    "stage": "exception",
                                    "error": str(exc)[:240],
                                }
                            )

            return {
                "status": "ok" if not errors else ("parcial" if (uploaded_files or skipped_duplicates) else "erro"),
                "uploaded_count": len(uploaded_files),
                "uploaded": uploaded_files,
                "skipped_duplicates_count": len(skipped_duplicates),
                "skipped_duplicates": skipped_duplicates,
                "errors": errors,
                "pending_outbox_by_tenant": self._count_pending_outbox_files(),
                "root_folder_url": manifest.get("root_folder_url"),
            }

    def get_fleet_status(self) -> Dict[str, Any]:
        tenants = self.onboarding.list_tenants()
        manifest = self._load_cloud_manifest()
        inbox_candidates = [p.name for p in self._list_inbox_candidates()]
        quarantined_files = (
            sorted(p.name for p in self.quarantine_dir.iterdir() if p.is_file() and not p.name.endswith(".reason.json"))
            if self.quarantine_dir.exists()
            else []
        )
        captcha_queue = self.build_captcha_assisted_queue(tenants=tenants)
        last_run = {}
        if self.fleet_state_path.exists():
            try:
                last_run = json.loads(self.fleet_state_path.read_text(encoding="utf-8"))
            except Exception:
                last_run = {}

        schedule_plan = None
        try:
            from sim_platform.ingestion.connectors.calendar_scheduler import MGCalendarScheduler

            schedule_plan = MGCalendarScheduler().get_schedule_plan(tenants)
        except Exception:
            schedule_plan = None

        return {
            "status": "ok",
            "total_cities": len(tenants),
            "cities": tenants,
            "fleet_lock_active": self.fleet_lock_file.exists(),
            "inbox_dir": str(self.inbox_dir),
            "inbox_pending_files": inbox_candidates,
            "quarantined_files": quarantined_files,
            "captcha_assisted_queue": captcha_queue,
            "mg_monthly_schedule_plan": schedule_plan,
            "cloud_datalake": {
                "provider": manifest.get("provider", "google_drive"),
                "account_hint": manifest.get("account_hint"),
                "root_folder_name": manifest.get("root_folder_name", "SIM_DataLake"),
                "root_folder_url": manifest.get("root_folder_url"),
                "gdrive_cli_available_here": bool(shutil.which("gdrive")),
                "pending_outbox_by_tenant": self._count_pending_outbox_files(),
            },
            "cli_commands": {
                "cron_full_host": "python -m sim_platform.ingestion.fleet_verifier --live-api --process-inbox --push-cloud",
                "inbox_only": "python -m sim_platform.ingestion.fleet_verifier --process-inbox --push-cloud",
                "status_only": "python -m sim_platform.ingestion.fleet_verifier --status-only",
            },
            "last_fleet_run": last_run,
        }

    def verify_and_sync_fleet(
        self,
        live_api: bool = True,
        process_inbox_flag: bool = True,
        export_cloud_bundles: bool = True,
        push_cloud: bool = False,
        target_tenants: Optional[List[str]] = None,
        runtime_pool: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Executa sequencialmente cidade a cidade sob lock exclusivo `_fleet.lock`:
        1. Processa pacotes depositados em `avaliacao/dados/inbox/` (adiando geração de bundle duplicado);
        2. Atualiza fontes automáticas via API (`IBGE`, `SICONFI/STN`, `CGU`) sem CAPTCHA;
        3. Exporta 1 único bundle consolidado por cidade para `cloud_outbox/` e, se `push_cloud=True`, envia ao Google Drive.
        """
        with self._acquire_fleet_lock():
            t0 = time.perf_counter()
            tenants = self.onboarding.list_tenants()
            filter_set = {x.strip().lower() for x in (target_tenants or []) if x.strip()}
            scheduled_tids = {
                str(t["tenant_id"]).lower()
                for t in tenants
                if not filter_set or str(t["tenant_id"]).lower() in filter_set
            }

            inbox_report = (
                self.process_inbox(
                    runtime_pool=runtime_pool,
                    defer_release_bundles_for=scheduled_tids if export_cloud_bundles else None,
                )
                if process_inbox_flag
                else {
                    "ingested_count": 0,
                    "quarantined_count": 0,
                    "skipped_busy_tenant": [],
                    "ingested": [],
                    "quarantined": [],
                }
            )

            city_runs: List[Dict[str, Any]] = []
            for t in tenants:
                tid = str(t["tenant_id"])
                if filter_set and tid.lower() not in filter_set:
                    continue

                db_mgr = None
                rag_ret = None
                rt = None
                if runtime_pool is not None:
                    getter = getattr(runtime_pool, "get_if_loaded", None) or getattr(runtime_pool, "peek_loaded", None)
                    if callable(getter):
                        rt = getter(tid)
                        if rt is not None:
                            db_mgr = rt.db_manager
                            rag_ret = rt.rag_retriever

                mgr = DataSyncManager(workspace_root=self.workspace_root, tenant_id=tid)
                try:
                    sync_res = mgr.sync_all(
                        db_manager=db_mgr,
                        rag_retriever=rag_ret,
                        live_api=live_api,
                    )
                except SyncAlreadyRunningError as busy_exc:
                    city_runs.append(
                        {
                            "tenant_id": tid,
                            "municipality_name": t.get("municipality_name"),
                            "ibge_code": t.get("ibge_code"),
                            "status": "skipped_busy_tenant",
                            "detail": str(busy_exc),
                        }
                    )
                    continue

                if rt is not None and hasattr(rt, "orchestrator") and hasattr(rt.orchestrator, "refresh_after_data_update"):
                    rt.orchestrator.refresh_after_data_update()
                bundle_res = mgr.export_release_bundle_to_cloud_outbox() if export_cloud_bundles else None
                city_runs.append(
                    {
                        "tenant_id": tid,
                        "municipality_name": t.get("municipality_name"),
                        "ibge_code": t.get("ibge_code"),
                        "total_parquet_tables": sync_res.get("total_parquet_tables"),
                        "populated_parquet_tables": sync_res.get("populated_parquet_tables"),
                        "active_release_id": sync_res.get("active_release_id"),
                        "cloud_bundle": bundle_res.get("bundle_name") if isinstance(bundle_res, dict) else None,
                        "duration_ms": (sync_res.get("sync_execution") or {}).get("duration_ms"),
                    }
                )

            cloud_push_report = self.push_outbox_to_gdrive() if push_cloud else None
            elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            summary = {
                "status": "sucesso",
                "executed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_ms": elapsed_ms,
                "live_api": live_api,
                "cities_synced": city_runs,
                "inbox_report": inbox_report,
                "cloud_push_report": cloud_push_report,
                "captcha_assisted_queue": self.build_captcha_assisted_queue(tenants=tenants),
            }
            try:
                _write_json_atomic(self.fleet_state_path, summary)
            except Exception:
                pass
            return summary

    def check_fleet_readiness(self, target_tenants: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Gate determinístico de homologação B2G (12 invariantes baseados em `sim_platform/ATTENTION_POINTS.md`)
        para validar qualquer cidade ativa ou recém-onboardada em segundos, sem depender de reviews manuais.
        """
        from datetime import date
        from sim_platform.engine.duckdb_manager import DuckDBManager, TenantContext
        from sim_platform.rag.hybrid_retriever import HybridRAGRetriever

        tenants = self.onboarding.list_tenants()
        allow_filter = {t.strip().lower() for t in (target_tenants or []) if t.strip()}
        today_iso = date.today().isoformat()
        reports: List[Dict[str, Any]] = []
        all_passed = True

        for t in tenants:
            tid = str(t["tenant_id"])
            if allow_filter and tid.lower() not in allow_filter:
                continue

            prof = load_tenant_profile(tenant_id=tid, workspace_root=self.workspace_root)
            pq_dir = self.workspace_root / "avaliacao" / "dados" / "parquet" / tid
            db_dir = self.workspace_root / "avaliacao" / "dados" / "database"
            tctx = TenantContext(
                tenant_id=tid,
                municipality_name=str(prof.get("municipality_name") or tid),
                state_uf=str(prof.get("state_uf") or "MG"),
                ibge_code=str(prof.get("ibge_code") or ""),
                data_dir=db_dir,
                parquet_dir=pq_dir,
            )
            db = DuckDBManager(tenant=tctx)
            checks: List[Dict[str, Any]] = []

            def _add_chk(code: str, ok: bool, detail: Any) -> None:
                checks.append({"check": code, "passed": bool(ok), "detail": detail})

            try:
                con = db.conn
                cat = db.catalog
                pop_cnt = sum(1 for m in cat.tables.values() if getattr(m, "is_populated", True))
                _add_chk("01_populated_tables_min_15", pop_cnt >= 15, {"populated_tables": pop_cnt})

                sel_res = cat.select_tables_for_query("Quais os maiores contratos e licitações em 2026?")
                empty_leaked = [tb for tb in sel_res.selected_tables if not getattr(cat.tables.get(tb.lower()), "is_populated", True)]
                _add_chk("02_zero_empty_stubs_in_ddl", len(empty_leaked) == 0, {"empty_leaked": empty_leaked})

                sau_row = con.execute(
                    "SELECT COALESCE(SUM(TRY_CAST(vlr_liquidado AS DOUBLE)), 0) FROM despesa_despesa "
                    "WHERE strip_accents(UPPER(COALESCE(dsc_funcao, ''))) ILIKE '%SAUDE%'"
                ).fetchone()
                sau_val = round(float(sau_row[0] or 0.0), 2)
                _add_chk("03_saude_despesa_gt_zero", sau_val > 0, {"saude_liquidado_total": sau_val})

                edu_row = con.execute(
                    "SELECT COALESCE(SUM(TRY_CAST(vlr_liquidado AS DOUBLE)), 0) FROM despesa_despesa "
                    "WHERE strip_accents(UPPER(COALESCE(dsc_funcao, ''))) ILIKE '%EDUCA%'"
                ).fetchone()
                edu_val = round(float(edu_row[0] or 0.0), 2)
                _add_chk("04_educacao_despesa_gt_zero", edu_val > 0, {"educacao_liquidado_total": edu_val})

                lrf_row = con.execute(
                    "SELECT COUNT(*) FROM siconfi_finbra_indicadores WHERE pct_despesa_pessoal_rcl IS NOT NULL AND pct_despesa_pessoal_rcl > 0"
                ).fetchone()
                lrf_cnt = int(lrf_row[0] or 0)
                _add_chk("05_siconfi_lrf_pessoal_present", lrf_cnt >= 1, {"years_with_lrf_pct": lrf_cnt})

                ibge_nulls = con.execute(
                    "SELECT COUNT(*) FROM ibge_indicadores_municipais WHERE ano_referencia >= 2022 AND (populacao_residente IS NULL OR populacao_residente <= 0)"
                ).fetchone()
                null_pop = int(ibge_nulls[0] or 0)
                _add_chk("06_ibge_population_no_gaps_2022_plus", null_pop == 0, {"null_pop_years": null_pop})

                pol_row = con.execute(
                    "SELECT MAX(ano_referencia), COUNT(DISTINCT CASE WHEN dsc_situacao ILIKE '%Agente Pol%' THEN seq_dim_pessoa END) "
                    "FROM vw_gold_folha_servidores WHERE ano_referencia = (SELECT MAX(ano_referencia) FROM vw_gold_folha_servidores)"
                ).fetchone()
                pol_cnt = int(pol_row[1] or 0) if pol_row else 0
                _add_chk("07_folha_political_agents_sane_bounds", 1 <= pol_cnt <= 80, {"max_year": pol_row[0] if pol_row else None, "political_agents": pol_cnt})

                ct_row = con.execute(
                    "SELECT COUNT(*), SUM(CASE WHEN COALESCE(nom_contratado, '') = '' OR nom_contratado ILIKE '%NÃO INFORMADO%' OR nom_contratado ILIKE '%NAO INFORMADO%' THEN 1 ELSE 0 END) "
                    "FROM vw_gold_contratos_vigencia_aditivos WHERE ano_referencia = (SELECT MAX(ano_referencia) FROM vw_gold_contratos_vigencia_aditivos)"
                ).fetchone()
                ct_tot = int(ct_row[0] or 0) if ct_row else 0
                ct_miss = int(ct_row[1] or 0) if ct_row else 0
                _add_chk("08_contracts_latest_year_supplier_identified", ct_tot > 0 and ct_miss == 0, {"total": ct_tot, "missing_supplier": ct_miss})

                gold_names = [
                    "vw_gold_folha_servidores",
                    "vw_gold_execucao_despesa_credor",
                    "vw_gold_contratos_vigencia_aditivos",
                    "vw_gold_frota_consolidada",
                    "vw_gold_fornecedores_qsa_360",
                    "vw_gold_conflito_interesses_licitacao",
                    "vw_gold_orcamento_previsto_vs_realizado",
                    "vw_gold_orcamento_por_orgao",
                ]
                gold_ok = True
                for gv in gold_names:
                    con.execute(f"SELECT 1 FROM {gv} LIMIT 1").fetchall()
                _add_chk("09_all_8_gold_views_operational", gold_ok, {"gold_views_checked": len(gold_names)})

                ret = HybridRAGRetriever(tenant_id=tid)
                chunks = ret.corpus_chunks or []
                fut_dates = []
                dup_headers = 0
                for ch in chunks:
                    dt_s = str(ch.get("data_publicacao") if isinstance(ch, dict) else getattr(ch, "data_publicacao", "") or "")
                    pc_s = str(ch.get("page_content") if isinstance(ch, dict) else getattr(ch, "page_content", "") or "")
                    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", dt_s) and dt_s > today_iso:
                        fut_dates.append(dt_s)
                    if pc_s.lstrip().startswith("[Município:"):
                        dup_headers += 1
                _add_chk("10_rag_populated_and_no_future_dates", len(chunks) >= 10 and len(fut_dates) == 0, {"rag_acts": len(chunks), "future_dates": len(fut_dates)})
                _add_chk("11_rag_no_duplicated_bracket_header", dup_headers == 0, {"duplicated_headers": dup_headers})

                citizen_q = db.execute_safe_query("SELECT nom_servidor, num_cpf FROM vw_gold_folha_servidores LIMIT 10", role="citizen")
                leaked_cpfs = [
                    r.get("num_cpf")
                    for r in (citizen_q.get("dados") or [])
                    if len(re.sub(r"[^0-9]", "", str(r.get("num_cpf") or ""))) >= 9
                ]
                _add_chk("12_lgpd_citizen_cpf_strictly_masked", citizen_q.get("status") == "sucesso" and len(leaked_cpfs) == 0, {"leaked_cpfs": len(leaked_cpfs)})
            finally:
                db.close()

            city_ok = all(c["passed"] for c in checks)
            if not city_ok:
                all_passed = False
            reports.append(
                {
                    "tenant_id": tid,
                    "municipality_name": t.get("municipality_name"),
                    "ibge_code": t.get("ibge_code"),
                    "ready": city_ok,
                    "passed_count": sum(1 for c in checks if c["passed"]),
                    "total_checks": len(checks),
                    "checks": checks,
                }
            )

        return {
            "status": "homologado" if all_passed else "atencao_pendencias",
            "all_cities_ready": all_passed,
            "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cities": reports,
        }


def _cli_main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verificador Sequencial Multi-Cidades (Cronjob), Auto-Router de inbox/ e Upload Google Drive (Two-Phase Delete)."
    )
    parser.add_argument("--status-only", action="store_true", help="Exibe apenas o diagnóstico completo da frota e da fila CAPTCHA.")
    parser.add_argument("--check-readiness", action="store_true", help="Executa os 12 checks determinísticos de homologação (ATTENTION_POINTS.md) por cidade.")
    parser.add_argument("--live-api", action="store_true", help="Sincroniza APIs oficiais abertas (IBGE, SICONFI/STN, CGU) cidade a cidade.")
    parser.add_argument("--process-inbox", action="store_true", help="Processa pacotes depositados em avaliacao/dados/inbox/.")
    parser.add_argument("--push-cloud", action="store_true", help="Envia cloud_outbox/ para o Google Drive (SIM_DataLake) com Two-Phase Delete.")
    parser.add_argument("--tenants", default="", help="Lista opcional de tenant_ids separados por vírgula.")
    args = parser.parse_args(argv)

    verifier = FleetVerifier(workspace_root=DEFAULT_WORKSPACE_ROOT)
    if args.status_only:
        print(json.dumps(verifier.get_fleet_status(), ensure_ascii=False, indent=2))
        return 0
    if args.check_readiness:
        t_list = [x.strip() for x in args.tenants.split(",") if x.strip()] or None
        rep = verifier.check_fleet_readiness(target_tenants=t_list)
        print(json.dumps(rep, ensure_ascii=False, indent=2))
        return 0 if rep["all_cities_ready"] else 1

    if args.push_cloud and not args.live_api and not args.process_inbox:
        with verifier._acquire_fleet_lock():
            t_filter = {x.strip().lower() for x in args.tenants.split(",") if x.strip()}
            for t in verifier.onboarding.list_tenants():
                tid = str(t["tenant_id"])
                if t_filter and tid.lower() not in t_filter:
                    continue
                DataSyncManager(workspace_root=DEFAULT_WORKSPACE_ROOT, tenant_id=tid).export_release_bundle_to_cloud_outbox()
            print(json.dumps(verifier.push_outbox_to_gdrive(), ensure_ascii=False, indent=2))
        return 0

    target_list = [x.strip() for x in args.tenants.split(",") if x.strip()] or None
    res = verifier.verify_and_sync_fleet(
        live_api=bool(args.live_api),
        process_inbox_flag=bool(args.process_inbox or not args.live_api),
        export_cloud_bundles=True,
        push_cloud=bool(args.push_cloud),
        target_tenants=target_list,
    )
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli_main())
