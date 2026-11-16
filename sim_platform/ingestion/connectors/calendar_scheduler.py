"""
Régua Mensal Governada de Atualização da Frota MG (`MGCalendarScheduler`).

Define formalmente as **4 Janelas Mensais de Minas Gerais** alinhadas aos prazos legais da LRF,
Instruções Normativas do TCE-MG, Portal da Transparência Federal (CGU) e STN/SICONFI:

1. **Dia 03 (Mensal)** — `portais_abertos_pncp_diarios`:
   Coleta incremental de Portais de Transparência Abertos (`BethaCloudConnector`, `Cidade360Connector`),
   Contratos PNCP (`Lei 14.133/2021`) e Diários Oficiais (`AmmMgGazetteConnector` / Diário Próprio).
2. **Dia 10 (Mensal)** — `lote_assistido_sicom_tcemg`:
   Fechamento da remessa mensal obrigatória do SICOM/TCE-MG (até 40 dias após o encerramento do mês):
   aciona a Fila Assistida pós-CAPTCHA e processa pacotes `.zip` em `avaliacao/dados/inbox/` (`SicomTceMgConnector`).
3. **Dia 18 (Mensal)** — `lote_federal_cgu`:
   Consolidação mensal das folhas de benefícios federais e transferências da União via API CGU
   (`Novo Bolsa Família`, `BPC`, `Transferências` e `Emendas`).
4. **Dia 02 dos Meses Pares (Fev, Abr, Jun, Ago, Out, Dez)** — `lote_fiscal_siconfi_siops`:
   Pós-prazo legal de 30 dias de publicação bimestral do `RREO` (Anexos 02, 03, 08 MDE, 12 SIOPS)
   e quadrimestral/semestral do `RGF` (Anexo 01 Pessoal LRF) no SICONFI/STN + atualização `IBGE` e `DATASUS/SIOPS`.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class MonthlyWindowSpec:
    """Especificação imutável de uma janela da Régua Mensal de Minas Gerais."""

    window_id: str
    day_of_month: int
    even_months_only: bool
    title: str
    legal_basis: str
    connectors: Tuple[str, ...]
    sync_manager_sources: Tuple[str, ...]
    requires_assisted_inbox: bool

    def is_due_on(self, target_date: date) -> bool:
        if target_date.day != self.day_of_month:
            return False
        if self.even_months_only and (target_date.month % 2 != 0):
            return False
        return True

    def next_occurrence(self, from_date: date) -> date:
        """Retorna a próxima data `>= from_date` em que esta janela deve disparar."""
        yr, mo = from_date.year, from_date.month
        for _ in range(24):
            max_day = calendar.monthrange(yr, mo)[1]
            eff_day = min(self.day_of_month, max_day)
            cand = date(yr, mo, eff_day)
            if cand >= from_date and (not self.even_months_only or (mo % 2 == 0)):
                return cand
            mo += 1
            if mo > 12:
                mo = 1
                yr += 1
        return date(from_date.year, from_date.month, self.day_of_month)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "window_id": self.window_id,
            "day_of_month": self.day_of_month,
            "cadence": "Bimestral (Meses Pares: 02, 04, 06, 08, 10, 12)" if self.even_months_only else "Mensal (Todos os Meses)",
            "even_months_only": self.even_months_only,
            "title": self.title,
            "legal_basis": self.legal_basis,
            "connectors": list(self.connectors),
            "sync_manager_sources": list(self.sync_manager_sources),
            "requires_assisted_inbox": self.requires_assisted_inbox,
        }


MG_MONTHLY_WINDOWS: Tuple[MonthlyWindowSpec, ...] = (
    MonthlyWindowSpec(
        window_id="lote_fiscal_siconfi_siops",
        day_of_month=2,
        even_months_only=True,
        title="Dia 02 (Meses Pares) — Lote Fiscal LRF, SICONFI/STN, SIOPS & IBGE",
        legal_basis="Art. 52 e 55 da LRF (LC 101/2000) — RREO bimestral (até 30 dias após o bimestre) e RGF Pessoal",
        connectors=("siconfi_stn", "ibge_sidra", "siops_datasus"),
        sync_manager_sources=("SICONFI", "IBGE", "DATASUS"),
        requires_assisted_inbox=False,
    ),
    MonthlyWindowSpec(
        window_id="portais_abertos_pncp_diarios",
        day_of_month=3,
        even_months_only=False,
        title="Dia 03 (Mensal) — Portais Abertos (Betha/Cidade360), PNCP Contratos & Diários Oficiais",
        legal_basis="LC 131/2009 (Tempo Real), Lei 14.133/2021 (PNCP) e Publicidade de Atos Oficiais (AMM-MG)",
        connectors=("betha_cloud", "cidade360", "amm_mg_gazette"),
        sync_manager_sources=("DIARIO_OFICIAL", "RECEITA_QSA"),
        requires_assisted_inbox=False,
    ),
    MonthlyWindowSpec(
        window_id="lote_assistido_sicom_tcemg",
        day_of_month=10,
        even_months_only=False,
        title="Dia 10 (Mensal) — Lote Assistido SICOM / TCE-MG (65 Tabelas Padronizadas)",
        legal_basis="IN TCE-MG — Remessa mensal obrigatória do módulo Acompanhamento Mensal (AM) do SICOM",
        connectors=("sicom_tcemg",),
        sync_manager_sources=("SICOM", "RECEITA_QSA", "DATASUS"),
        requires_assisted_inbox=True,
    ),
    MonthlyWindowSpec(
        window_id="lote_federal_cgu",
        day_of_month=18,
        even_months_only=False,
        title="Dia 18 (Mensal) — Lote Federal Portal da Transparência / CGU (Bolsa Família, BPC, Emendas)",
        legal_basis="Calendário Oficial MDS / Caixa / Portal da Transparência da Controladoria-Geral da União",
        connectors=("cgu_portal_transparencia",),
        sync_manager_sources=("CGU",),
        requires_assisted_inbox=False,
    ),
)


class MGCalendarScheduler:
    """
    Orquestrador determinístico da Régua Mensal de Minas Gerais.
    Calcula janelas ativas em qualquer data e monta o plano operacional completo por município.
    """

    WINDOWS: Tuple[MonthlyWindowSpec, ...] = MG_MONTHLY_WINDOWS

    def __init__(self, windows: Sequence[MonthlyWindowSpec] = MG_MONTHLY_WINDOWS) -> None:
        self.windows: Tuple[MonthlyWindowSpec, ...] = tuple(windows)
        self._by_id: Dict[str, MonthlyWindowSpec] = {w.window_id: w for w in self.windows}

    @staticmethod
    def infer_tenant_connectors(tenant: Dict[str, Any]) -> List[str]:
        """
        Determina os conectores aplicáveis a um município com base no seu perfil declarativo
        (`portal_vendor` / `connector_id` / `diario_oficial.veiculo`).
        """
        explicit = str(tenant.get("portal_connector") or tenant.get("connector_id") or "").strip().lower()
        gazette = str(
            tenant.get("official_gazette_name")
            or (tenant.get("diario_oficial") or {}).get("veiculo")
            or ""
        ).upper()
        tid = str(tenant.get("tenant_id") or "").lower()

        assigned: List[str] = []
        if explicit in ("betha_cloud", "betha"):
            assigned.append("betha_cloud")
        elif explicit in ("cidade360", "cidade_360"):
            assigned.append("cidade360")
        elif "lagoa_da_prata" in tid:
            assigned.append("betha_cloud")
        elif "sabara" in tid:
            assigned.append("cidade360")

        # Todas as prefeituras de MG têm cobertura universal garantida pelo pacote SICOM/TCE-MG
        assigned.append("sicom_tcemg")

        if "AMM" in gazette or explicit == "amm_mg_gazette":
            assigned.append("amm_mg_gazette")

        return list(dict.fromkeys(assigned))

    def get_due_windows(self, target_date: Optional[date] = None) -> List[MonthlyWindowSpec]:
        """Retorna todas as janelas da régua mensal que devem disparar exatamente em `target_date`."""
        ref = target_date or date.today()
        return [w for w in self.windows if w.is_due_on(ref)]

    def get_jobs_for_date(
        self,
        target_date: date,
        tenants: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Retorna quais janelas, conectores, fontes do `DataSyncManager` e municípios devem rodar
        na data informada `target_date`.
        """
        due_windows = self.get_due_windows(target_date)
        if not due_windows:
            next_candidates = sorted((w.next_occurrence(target_date), w) for w in self.windows)
            nxt_dt, nxt_win = next_candidates[0]
            return {
                "date": target_date.isoformat(),
                "has_due_jobs": False,
                "due_window_ids": [],
                "due_windows": [],
                "city_executions": [],
                "next_scheduled_date": nxt_dt.isoformat(),
                "next_scheduled_window": nxt_win.window_id,
            }

        due_ids = [w.window_id for w in due_windows]
        combined_sources: List[str] = []
        due_connectors_set: set[str] = set()
        needs_inbox = False

        for w in due_windows:
            for s in w.sync_manager_sources:
                if s not in combined_sources:
                    combined_sources.append(s)
            due_connectors_set.update(w.connectors)
            if w.requires_assisted_inbox:
                needs_inbox = True

        city_executions: List[Dict[str, Any]] = []
        for t in tenants:
            tid = str(t.get("tenant_id") or "").strip()
            if not tid:
                continue
            t_conns = self.infer_tenant_connectors(t)
            active_portal_conns = [c for c in t_conns if c in due_connectors_set]
            # Lotes federais/fiscais (SICONFI, IBGE, CGU) e SICOM aplicam-se a todos os municípios ativos
            has_universal_federal = any(
                w.window_id in ("lote_fiscal_siconfi_siops", "lote_federal_cgu", "lote_assistido_sicom_tcemg")
                for w in due_windows
            )
            if active_portal_conns or has_universal_federal or "DIARIO_OFICIAL" in combined_sources:
                city_executions.append(
                    {
                        "tenant_id": tid,
                        "municipality_name": t.get("municipality_name") or tid,
                        "ibge_code": t.get("ibge_code"),
                        "windows": due_ids,
                        "connectors_to_run": active_portal_conns,
                        "sync_manager_sources": list(combined_sources),
                        "process_inbox": needs_inbox,
                        "cli_command": (
                            f"python -m sim_platform.ingestion.sync_manager --tenant {tid} "
                            f"--live-api --sources {','.join(combined_sources)}"
                        ),
                    }
                )

        return {
            "date": target_date.isoformat(),
            "has_due_jobs": len(city_executions) > 0,
            "due_window_ids": due_ids,
            "due_windows": [w.to_dict() for w in due_windows],
            "requires_assisted_inbox": needs_inbox,
            "city_executions": city_executions,
        }

    def get_schedule_plan(
        self,
        tenants: Sequence[Dict[str, Any]],
        reference_date: Optional[date] = None,
    ) -> Dict[str, Any]:
        """
        Gera o Plano Governado completo da Régua Mensal de MG para toda a frota cadastrada (`tenants`),
        incluindo as próximas datas de disparo a partir de `reference_date` e o mapeamento de conectores por cidade.
        """
        ref = reference_date or date.today()
        windows_plan: List[Dict[str, Any]] = []
        for w in self.windows:
            nxt = w.next_occurrence(ref)
            w_dict = w.to_dict()
            w_dict["next_run_date"] = nxt.isoformat()
            w_dict["days_until_next_run"] = (nxt - ref).days
            windows_plan.append(w_dict)

        windows_plan.sort(key=lambda x: (x["next_run_date"], x["day_of_month"]))

        tenant_matrix: List[Dict[str, Any]] = []
        for t in tenants:
            tid = str(t.get("tenant_id") or "").strip()
            if not tid:
                continue
            tenant_matrix.append(
                {
                    "tenant_id": tid,
                    "municipality_name": t.get("municipality_name") or tid,
                    "state_uf": t.get("state_uf", "MG"),
                    "ibge_code": t.get("ibge_code"),
                    "assigned_connectors": self.infer_tenant_connectors(t),
                    "windows_subscribed": [w.window_id for w in self.windows],
                }
            )

        return {
            "reference_date": ref.isoformat(),
            "total_windows": len(self.windows),
            "total_tenants": len(tenant_matrix),
            "windows": windows_plan,
            "due_today": self.get_jobs_for_date(ref, tenants),
            "tenant_connector_matrix": tenant_matrix,
        }
