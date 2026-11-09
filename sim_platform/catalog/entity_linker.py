"""
Vinculador de Valores do Banco (Entity Value Linker) e Selecionador Dinâmico de Few-Shots (`EntityLinker`).

Resolve o problema clássico de Text-to-SQL em bases governamentais:
- O usuário pergunta por `"Santa Casa"`, `"Unicapa"`, `"Câmara"`, `"Saúde"` ou `"Professor"`,
  mas no banco SICOM/TCE-MG os valores exatos são `'SOC CIVIL DE BENEFICENCIA CAETEENSE'`,
  `'UNICAPA - UNIAO DOS CAMINHAOZEIROS...'`, `'10 - SAÚDE'`, `'PROFESSOR DE EDUCACAO BASICA - PEB'`.
- Este módulo indexa em memória os valores reais das principais dimensões do banco DuckDB
  e injeta os *Value Bindings* exatos + 2 *Few-Shots Dinâmicos* semanticamente similares no prompt.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb


def _norm(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    clean = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", clean).strip()


def _tokens(text: str) -> Set[str]:
    stop = {
        "que", "para", "com", "por", "uma", "dos", "das", "nos", "nas", "qual", "quais",
        "quanto", "quantos", "foi", "foram", "ano", "anos", "entre", "municipio", "caete",
        "prefeitura", "municipal", "total", "valor", "gasto", "pago", "todos", "todas",
        "ltda", "eireli", "servicos", "comercio", "empresa",
    }
    return {t for t in re.findall(r"[a-z0-9]{3,}", _norm(text)) if t not in stop}


class EntityLinker:
    """
    Indexador leve em memória de entidades reais do município (Credores, Órgãos, Funções, Cargos)
    e recuperador de exemplos Few-Shot Dinâmicos do Golden Dataset (somente leitura).
    """

    def __init__(
        self,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        workspace_root: Optional[Path] = None,
    ) -> None:
        self.workspace_root = workspace_root or Path(__file__).resolve().parents[2]
        self.creditors: List[Tuple[str, str, Set[str]]] = []  # (nom_credor, num_doc_credor, tokens)
        self.orgaos: List[Tuple[str, str, Set[str]]] = []  # (nom_orgao, cod_orgao, tokens)
        self.funcoes: List[Tuple[str, Set[str]]] = []  # (dsc_funcao, tokens)
        self.cargos: List[Tuple[str, Set[str]]] = []  # (dsc_cargo, tokens)
        self.few_shots: List[Dict[str, Any]] = []

        if conn is not None:
            self._index_db_entities(conn)
        self._load_golden_few_shots()

    def _index_db_entities(self, conn: duckdb.DuckDBPyConnection) -> None:
        cur = conn.cursor()
        # 1. Top 250 Credores de despesa_pagamento
        try:
            rows = cur.execute(
                """
                SELECT MAX(nom_credor) AS nom, MAX(num_doc_credor) AS doc,
                       SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))) AS tot
                FROM despesa_pagamento
                WHERE COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%'
                GROUP BY UPPER(TRIM(nom_credor))
                ORDER BY tot DESC NULLS LAST
                LIMIT 250
                """
            ).fetchall()
            for nom, doc, _ in rows:
                if nom:
                    toks = _tokens(str(nom))
                    if toks:
                        self.creditors.append((str(nom).strip(), str(doc or "").strip(), toks))
        except Exception:
            pass

        # 2. Órgãos de orgao_orgao
        try:
            rows = cur.execute(
                "SELECT DISTINCT nom_orgao, CAST(cod_orgao AS VARCHAR) FROM orgao_orgao WHERE nom_orgao IS NOT NULL"
            ).fetchall()
            for nom, cod in rows:
                toks = _tokens(str(nom))
                if toks:
                    self.orgaos.append((str(nom).strip(), str(cod or "").strip(), toks))
        except Exception:
            pass

        # 3. Funções Orçamentárias de despesa_despesa
        try:
            rows = cur.execute(
                "SELECT DISTINCT dsc_funcao FROM despesa_despesa WHERE dsc_funcao IS NOT NULL LIMIT 50"
            ).fetchall()
            for (fn,) in rows:
                toks = _tokens(str(fn))
                if toks:
                    self.funcoes.append((str(fn).strip(), toks))
        except Exception:
            pass

        # 4. Principais Cargos de remuneracao_municipio
        try:
            rows = cur.execute(
                """
                SELECT dsc_cargo, COUNT(*) AS qtd
                FROM remuneracao_municipio
                WHERE dsc_cargo IS NOT NULL
                GROUP BY 1 ORDER BY 2 DESC LIMIT 80
                """
            ).fetchall()
            for cargo, _ in rows:
                toks = _tokens(str(cargo))
                if toks:
                    self.cargos.append((str(cargo).strip(), toks))
        except Exception:
            pass

    def _load_golden_few_shots(self) -> None:
        golden_csv = self.workspace_root / "avaliacao" / "dados" / "golden" / "golden_dataset_v1.0.csv"
        if not golden_csv.exists():
            return
        try:
            with open(golden_csv, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f, delimiter=";"):
                    q = (row.get("input_usuario") or "").strip()
                    sql = (row.get("query_referencia") or "").strip()
                    tbls = (row.get("tabelas_esperadas") or "").strip()
                    if q and sql:
                        self.few_shots.append(
                            {
                                "question": q,
                                "sql": sql,
                                "tables": tbls,
                                "tokens": _tokens(q),
                            }
                        )
        except Exception:
            pass

    def extract_value_bindings(self, question: str, max_bindings: int = 6) -> List[str]:
        """
        Retorna dicas determinísticas de filtro (`WHERE`) para valores reais do banco
        mencionados na pergunta do usuário.
        """
        q_norm = _norm(question)
        q_toks = _tokens(question)
        if not q_toks:
            return []

        bindings: List[str] = []

        # Atalhos semânticos conhecidos de Caeté (ex: "Santa Casa" -> SOC CIVIL DE BENEFICENCIA CAETEENSE / IRMANDADE)
        if "santa casa" in q_norm or "hospital" in q_norm:
            bindings.append(
                "Entidade 'Santa Casa / Hospital': em `despesa_pagamento` use ` (nom_credor ILIKE '%SANTA CASA%' OR nom_credor ILIKE '%BENEFICENCIA%') `"
            )
        if "camara" in q_norm or "vereador" in q_norm or "legislativo" in q_norm:
            bindings.append(
                "Órgão 'Câmara Municipal': filtre `CAST(cod_orgao AS INTEGER) = 1` ou `nom_orgao ILIKE '%CAMARA%'`"
            )

        # Match em Credores
        scored_creds: List[Tuple[float, str, str]] = []
        for nom, doc, toks in self.creditors:
            inter = q_toks.intersection(toks)
            if inter:
                score = len(inter) / max(1, min(len(q_toks), len(toks)))
                if len(inter) >= 1 and any(len(t) >= 5 for t in inter):
                    scored_creds.append((score, nom, doc))
        scored_creds.sort(key=lambda x: x[0], reverse=True)
        for _, nom, doc in scored_creds[:2]:
            bindings.append(f"Credor identificado no banco: `nom_credor = '{nom}'` (`num_doc_credor = '{doc}'`)")

        # Match em Funções Orçamentárias
        for fn, toks in self.funcoes:
            if q_toks.intersection(toks):
                bindings.append(f"Função orçamentária exata em `despesa_despesa`: `dsc_funcao = '{fn}'`")
                if len(bindings) >= max_bindings:
                    break

        # Match em Cargos da Folha
        for cargo, toks in self.cargos:
            inter = q_toks.intersection(toks)
            if inter and any(len(t) >= 5 for t in inter):
                bindings.append(f"Cargo na folha (`remuneracao_municipio`): `dsc_cargo ILIKE '%{cargo[:25]}%'`")
                break

        return bindings[:max_bindings]

    def select_dynamic_few_shots(self, question: str, top_k: int = 2) -> List[Dict[str, str]]:
        """
        Recupera até `top_k` exemplos SQL de alta precisão semanticamente próximos à pergunta.
        """
        q_toks = _tokens(question)
        if not q_toks or not self.few_shots:
            return []

        scored: List[Tuple[float, Dict[str, Any]]] = []
        for item in self.few_shots:
            it_toks = item["tokens"]
            if not it_toks:
                continue
            inter = len(q_toks.intersection(it_toks))
            if inter > 0:
                jaccard = inter / len(q_toks.union(it_toks))
                scored.append((jaccard, item))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [
            {"question": s[1]["question"], "sql": s[1]["sql"], "tables": s[1]["tables"]}
            for s in scored[:top_k]
            if s[0] >= 0.12
        ]

    def link_entities(self, question: str, top_k: int = 6) -> List[str]:
        return self.extract_value_bindings(question=question, max_bindings=top_k)

    def select_few_shots(self, question: str, top_k: int = 3) -> List[Dict[str, str]]:
        return self.select_dynamic_few_shots(question=question, top_k=top_k)

