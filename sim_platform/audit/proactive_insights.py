"""
Motor Proativo de Insights e Auditoria Contínua (`ProactiveAuditEngine`).

Princípio de Engenharia:
- ZERO valores hardcoded arbitrários ou alertas falsos que nunca disparam.
- Cada insight é calculado dinamicamente via SQL sobre as tabelas Parquet reais do município,
  expondo de forma transparente:
  1. A **Base Legal ou Critério Estatístico** (ex: Art. 65 §1º Lei 8.666 / Art. 125 Lei 14.133 — teto de 25% em aditivos;
     Art. 75 §1º Lei 14.133 — recorrência de dispensas no mesmo exercício; Variação Anual YoY > 100% em contratos de alta materialidade).
  2. A **Query SQL Exata** que originou o achado (para que o controlador possa auditar ou ajustar o parâmetro).
  3. A **Nota Técnica de Interpretação** (explicando quando o achado é legítimo, como prorrogação plurianual de serviços contínuos
     ou compra emergencial de medicamentos por ordem judicial, versus quando exige diligência).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import duckdb


DEFAULT_WORKSPACE_ROOT = Path(__file__).resolve().parents[2]


class ProactiveAuditEngine:
    """
    Executa verificações dinâmicas de controladoria sobre o banco Parquet do município.
    """

    def __init__(self, workspace_root: Path = DEFAULT_WORKSPACE_ROOT, tenant_id: str = "caete_mg_3110004") -> None:
        self.workspace_root = workspace_root
        self.tenant_id = tenant_id
        self.parquet_dir = workspace_root / "avaliacao" / "dados" / "parquet" / tenant_id

    def run_all_checks(
        self,
        aditivo_pct_threshold: float = 25.0,
        min_dispensas_recorrentes: int = 5,
        yoy_growth_pct_threshold: float = 100.0,
    ) -> Dict[str, Any]:
        con = duckdb.connect(":memory:")
        p = self.parquet_dir.as_posix()

        def _query(sql: str) -> List[Dict[str, Any]]:
            rel = con.execute(sql)
            cols = [d[0] for d in rel.description]
            out = []
            for r in rel.fetchall():
                row_dict = {}
                for i, c in enumerate(cols):
                    v = r[i]
                    if hasattr(v, "__float__") and not isinstance(v, (int, bool)):
                        v = round(float(v), 2)
                    row_dict[c] = v
                out.append(row_dict)
            return out

        # 1. Contratos com Termos Aditivos > 25% do Valor Original
        sql_aditivos = f"""
            WITH adit AS (
                SELECT seq_contrato, COUNT(*) AS qtd_aditivos,
                       ROUND(SUM(TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2))), 2) AS total_aditivado
                FROM read_parquet('{p}/contrato_termoContrato.parquet')
                WHERE TRY_CAST(vlr_termo_aditivo AS DECIMAL(18,2)) > 0
                GROUP BY 1
            )
            SELECT c.ano_referencia, c.num_contrato,
                   SUBSTR(TRIM(c.dsc_objetocontrato), 1, 85) AS objeto_contrato,
                   ROUND(TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)), 2) AS vlr_original,
                   a.qtd_aditivos, a.total_aditivado,
                   ROUND((a.total_aditivado / NULLIF(TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)), 0)) * 100, 1) AS pct_acrescimo
            FROM read_parquet('{p}/contrato_contratos.parquet') c
            JOIN adit a ON c.seq_contrato = a.seq_contrato
            WHERE TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)) > 10000
              AND ((a.total_aditivado / NULLIF(TRY_CAST(c.vlr_contrato AS DECIMAL(18,2)), 0)) * 100) > {aditivo_pct_threshold}
            ORDER BY a.total_aditivado DESC
            LIMIT 8
        """
        rows_aditivos = _query(sql_aditivos)

        # 2. Fornecedores com Múltiplas Dispensas de Licitação no Mesmo Exercício
        sql_dispensas = f"""
            SELECT f.ano_referencia,
                   UPPER(TRIM(f.dsc_nom_fornecedor)) AS fornecedor,
                   f.num_doc_fornecedor AS documento,
                   COUNT(DISTINCT f.seq_dispensa) AS qtd_dispensas_no_ano,
                   COUNT(*) AS qtd_itens,
                   ROUND(SUM(TRY_CAST(f.vlr_item AS DECIMAL(18,2)) * COALESCE(TRY_CAST(f.num_quant_item AS DECIMAL(18,2)), 1)), 2) AS valor_total_itens
            FROM read_parquet('{p}/licitacao_fornDispensa.parquet') f
            WHERE COALESCE(f.dsc_nom_fornecedor, '') <> ''
            GROUP BY 1, 2, 3
            HAVING COUNT(DISTINCT f.seq_dispensa) >= {min_dispensas_recorrentes}
            ORDER BY qtd_dispensas_no_ano DESC, valor_total_itens DESC
            LIMIT 8
        """
        rows_dispensas = _query(sql_dispensas)

        # 3. Salto Anual Expressivo de Pagamentos a Fornecedores PJ (> 100% YoY e > R$ 1M)
        sql_yoy = f"""
            WITH anual AS (
                SELECT ano_referencia,
                       MAX(UPPER(TRIM(nom_credor))) AS credor,
                       regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g') AS cnpj,
                       ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total_ano
                FROM read_parquet('{p}/despesa_pagamento.parquet')
                WHERE length(regexp_replace(COALESCE(num_doc_credor, ''), '[^0-9]', '', 'g')) = 14
                  AND COALESCE(nom_credor, '') NOT ILIKE '%NAO INFORMADO%'
                GROUP BY 1, 3
            )
            SELECT a2.ano_referencia AS exercicio,
                   a2.credor,
                   a2.cnpj,
                   a1.total_ano AS pago_ano_anterior,
                   a2.total_ano AS pago_ano_atual,
                   ROUND(((a2.total_ano - a1.total_ano) / NULLIF(a1.total_ano, 0)) * 100, 1) AS crescimento_anual_pct
            FROM anual a1
            JOIN anual a2 ON a1.cnpj = a2.cnpj AND a2.ano_referencia = a1.ano_referencia + 1
            WHERE a1.total_ano > 200000 AND a2.total_ano > 1000000
              AND (((a2.total_ano - a1.total_ano) / NULLIF(a1.total_ano, 0)) * 100) > {yoy_growth_pct_threshold}
            ORDER BY (a2.total_ano - a1.total_ano) DESC
            LIMIT 8
        """
        rows_yoy = _query(sql_yoy)

        con.close()

        insights = [
            {
                "id": "audit_aditivos_25pct",
                "title": f"Termos Aditivos Acima de {aditivo_pct_threshold:.0f}% do Contrato Original",
                "legal_basis": "Art. 125 da Lei 14.133/2021 (e Art. 65, §1º da Lei 8.666/93)",
                "parameter_explained": f"Soma dos termos aditivos (`contrato_termoContrato`) > {aditivo_pct_threshold:.0f}% de `vlr_contrato` (`contrato_contratos`).",
                "auditor_guidance": (
                    "Nota de Controladoria: Acréscimos quantitativos de objeto têm teto legal de 25% (50% p/ reforma). "
                    "Valores superiores a 100% são regulares apenas quando decorrem de **prorrogação de vigência plurianual** "
                    "de serviços contínuos (locação de veículos, software, limpeza). Recomenda-se verificar se o termo aditivo é de prazo ou de escopo."
                ),
                "count": len(rows_aditivos),
                "sql_executed": sql_aditivos.strip(),
                "rows": rows_aditivos,
            },
            {
                "id": "audit_dispensas_recorrentes",
                "title": f"Fornecedores com ≥ {min_dispensas_recorrentes} Dispensas de Licitação no Mesmo Ano",
                "legal_basis": "Art. 75, §1º da Lei 14.133/2021 (Prevenção ao Fracionamento de Despesa)",
                "parameter_explained": f"`COUNT(DISTINCT seq_dispensa) >= {min_dispensas_recorrentes}` para o mesmo CNPJ no mesmo exercício em `licitacao_fornDispensa`.",
                "auditor_guidance": (
                    "Nota de Controladoria: Em Caeté, a maior recorrência ocorre em drogarias e distribuidoras de medicamentos "
                    "(`DROGARIA HUMANAS`, `LIDER SBX`, `MSR EXPRESS`). Quando motivadas por **cumprimento de liminares judiciais de saúde** "
                    "(Art. 75, VIII), as dispensas individuais são lícitas, mas a recorrência sinaliza oportunidade de abrir Ata de Registro de Preços."
                ),
                "count": len(rows_dispensas),
                "sql_executed": sql_dispensas.strip(),
                "rows": rows_dispensas,
            },
            {
                "id": "audit_salto_anual_fornecedor",
                "title": f"Expansão Anual de Pagamentos > {yoy_growth_pct_threshold:.0f}% (Alta Materialidade > R$ 1M)",
                "legal_basis": "Trilha de Auditoria Baseada em Risco e Materialidade (TCE-MG / NBC TA 315)",
                "parameter_explained": f"Crescimento anual `((pago_atual - pago_anterior) / pago_anterior) > {yoy_growth_pct_threshold:.0f}%` em credores PJ acima de R$ 1 milhão.",
                "auditor_guidance": (
                    "Nota de Controladoria: Identifica automaticamente os contratos que mais pressionaram o desembolso de caixa "
                    "de um exercício para o outro (ex: obras de pavimentação `UNICAPA` e engenharia `QUANTUM` em 2024, e transporte `TRANSCOL` em 2023)."
                ),
                "count": len(rows_yoy),
                "sql_executed": sql_yoy.strip(),
                "rows": rows_yoy,
            },
        ]

        return {
            "tenant_id": self.tenant_id,
            "total_insights_categories": len(insights),
            "insights": insights,
        }
