"""
Executa a bateria de Experimentos Comparativos (E-A a E-E) para sustentar empiricamente
o relatório `AUDIT/EXPERIMENTS.md` e a modernização da plataforma SIM.
"""

from __future__ import annotations

import csv
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Set

import sqlglot
from sqlglot import exp

from sim_platform.engine.duckdb_manager import DuckDBManager, TenantContext
from sim_platform.prompts.builder import ModularPromptBuilder


WORKSPACE = Path(__file__).resolve().parents[1]
DATA_DIR = WORKSPACE / "avaliacao" / "dados" / "database"
FIX2023_DIR = WORKSPACE / "avaliacao" / "dados" / "database_fix2023"
GOLDEN_V1 = WORKSPACE / "avaliacao" / "dados" / "golden" / "golden_dataset_v1.0.csv"
GOLDEN_V2 = WORKSPACE / "avaliacao" / "dados" / "golden" / "v2.0" / "golden_dataset_v2.0.csv"
PROMPT_V1_PATH = WORKSPACE / "avaliacao" / "eval" / "prompts" / "prompt_sql_compacto.txt"
PROMPT_V2_PATH = WORKSPACE / "avaliacao" / "eval" / "prompts" / "prompt_sql_v2_insights.txt"
RAG_EXP_DIR = WORKSPACE / "avaliacao" / "experimentos"


def run_all() -> Dict[str, Any]:
    results: Dict[str, Any] = {}

    # ==========================================================================
    # E-A: Impacto Financeiro e Fiscal da Corrupção de 2023 (Bruto vs Reparado)
    # ==========================================================================
    mgr_raw = DuckDBManager(
        TenantContext(
            tenant_id="caete_raw",
            municipality_name="Caeté",
            state_uf="MG",
            ibge_code="3110004",
            data_dir=DATA_DIR,
            repaired_2023=False,
        )
    )
    mgr_fix = DuckDBManager(
        TenantContext(
            tenant_id="caete_fix",
            municipality_name="Caeté",
            state_uf="MG",
            ibge_code="3110004",
            data_dir=DATA_DIR,
            fix2023_dir=FIX2023_DIR,
            repaired_2023=True,
        )
    )

    ea_tables = [
        ("despesa_pagamento", "vlr_pag_fonte"),
        ("despesa_despesa", "vlr_liquidado"),
        ("despesa_liquidacaoFonte", "vlr_liq_fonte"),
        ("empenho_empenhoFonte", "vlr_empenho"),
        ("receita_receita", "vlr_realizadoateperiodo"),
        ("contrato_creditoContrato", "vlr_recurso"),
        ("licitacao_recLicitacao", "vlr_recurso"),
        ("empenho_movFonteRsp", "vlr_mov_fonte"),
    ]
    ea_summary: List[Dict[str, Any]] = []
    for tname, vcol in ea_tables:
        raw_bad, raw_sum = mgr_raw.conn.execute(
            f'SELECT COUNT(*) FILTER (WHERE "{vcol}" IS NOT NULL AND TRY_CAST("{vcol}" AS DECIMAL(18,3)) IS NULL), '
            f'COALESCE(SUM(TRY_CAST("{vcol}" AS DECIMAL(18,2))), 0) '
            f'FROM "{tname}" WHERE ano_referencia = 2023'
        ).fetchone()
        fix_bad, fix_sum = mgr_fix.conn.execute(
            f'SELECT COUNT(*) FILTER (WHERE "{vcol}" IS NOT NULL AND TRY_CAST("{vcol}" AS DECIMAL(18,3)) IS NULL), '
            f'COALESCE(SUM(TRY_CAST("{vcol}" AS DECIMAL(18,2))), 0) '
            f'FROM "{tname}" WHERE ano_referencia = 2023'
        ).fetchone()
        diff_brl = float(fix_sum) - float(raw_sum)
        ea_summary.append(
            {
                "table": tname,
                "value_column": vcol,
                "corrupted_rows_raw": int(raw_bad),
                "corrupted_rows_repaired": int(fix_bad),
                "sum_raw_brl": round(float(raw_sum), 2),
                "sum_repaired_brl": round(float(fix_sum), 2),
                "recovered_brl": round(diff_brl, 2),
            }
        )

    # Indicadores Fiscais 2023 (ISF-M, IES-C, IEQ-C) antes e depois do reparo
    isf_sql = (
        "WITH rc AS (SELECT SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))) AS r FROM receita_receita WHERE ano_referencia = 2023 AND dsc_cateconomica ILIKE '%Corrente%'), "
        "dc AS (SELECT SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) AS d FROM despesa_despesa WHERE ano_referencia = 2023 AND dsc_naturezadespesa LIKE '3%') "
        "SELECT ROUND(rc.r / NULLIF(dc.d, 0), 4) FROM rc, dc"
    )
    isf_raw = float(mgr_raw.conn.execute(isf_sql).fetchone()[0])
    isf_fix = float(mgr_fix.conn.execute(isf_sql).fetchone()[0])

    results["E_A_2023_repair"] = {
        "tables": ea_summary,
        "isf_m_2023_raw": isf_raw,
        "isf_m_2023_repaired": isf_fix,
    }

    # ==========================================================================
    # E-B: Schema Estático (72 tabelas) vs. Catálogo Dinâmico + Prompt Modular
    # ==========================================================================
    p1_bytes = len(PROMPT_V1_PATH.read_text(encoding="utf-8"))
    p2_bytes = len(PROMPT_V2_PATH.read_text(encoding="utf-8"))
    p1_tokens = p1_bytes // 4
    p2_tokens = p2_bytes // 4

    builder = ModularPromptBuilder()
    eb_datasets: Dict[str, Any] = {}
    for label, gpath in [("golden_v1_n80", GOLDEN_V1), ("golden_v2_n14", GOLDEN_V2)]:
        with open(gpath, encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f, delimiter=";"))
        total_q = 0
        tsa_hits = 0
        sel_counts: List[int] = []
        red_pcts: List[float] = []
        mod_tokens: List[int] = []

        for r in rows:
            sql = (r.get("query_referencia") or "").strip()
            if not sql:
                continue
            total_q += 1
            q_text = r.get("input_usuario") or ""
            ref_tables: Set[str] = set()
            for st in sqlglot.parse(sql, read="duckdb"):
                if not st:
                    continue
                ctes = {c.alias_or_name.lower() for c in st.find_all(exp.CTE)}
                for tb in st.find_all(exp.Table):
                    if tb.name and tb.name.lower() not in ctes:
                        ref_tables.add(tb.name.lower())

            sel = mgr_fix.catalog.select_tables_for_query(q_text)
            sel_low = {t.lower() for t in sel.selected_tables}
            if ref_tables.issubset(sel_low):
                tsa_hits += 1
            sel_counts.append(sel.selected_count)
            red_pcts.append(sel.reduction_pct)
            pkg = builder.assemble(user_question=q_text, selection=sel)
            mod_tokens.append(pkg.estimated_tokens)

        avg_tok = round(sum(mod_tokens) / len(mod_tokens), 1)
        eb_datasets[label] = {
            "queries_evaluated": total_q,
            "tsa_hits": tsa_hits,
            "tsa_pct": round(100.0 * tsa_hits / total_q, 2),
            "avg_tables_selected": round(sum(sel_counts) / len(sel_counts), 2),
            "min_tables_selected": min(sel_counts),
            "max_tables_selected": max(sel_counts),
            "avg_schema_reduction_pct": round(sum(red_pcts) / len(red_pcts), 2),
            "avg_modular_prompt_tokens": avg_tok,
            "token_reduction_vs_v1_pct": round(100.0 * (1.0 - avg_tok / p1_tokens), 2),
            "token_reduction_vs_v2_pct": round(100.0 * (1.0 - avg_tok / p2_tokens), 2),
        }

    results["E_B_dynamic_catalog"] = {
        "static_v1_prompt_bytes": p1_bytes,
        "static_v1_prompt_tokens_est": p1_tokens,
        "static_v2_prompt_bytes": p2_bytes,
        "static_v2_prompt_tokens_est": p2_tokens,
        "benchmarks": eb_datasets,
    }

    # ==========================================================================
    # E-C: Linhagem de Dados por Regex Ingênuo (Legado) vs. AST (SQLGuard)
    # ==========================================================================
    with open(GOLDEN_V1, encoding="utf-8-sig") as f:
        v1_rows = list(csv.DictReader(f, delimiter=";"))

    all_view_names = [t.lower() for t in mgr_fix.catalog.tables.keys()]
    regex_between_failures = 0
    regex_substring_false_positives = 0
    ast_exact_matches = 0

    for r in v1_rows:
        sql = (r.get("query_referencia") or "").strip()
        q_low = sql.lower()
        # Simular lógica exata do scripts_python/main.py:
        # 1) Match de tabela por substring: `if view.lower() in query_lower`
        legacy_tables = [v for v in all_view_names if v in q_low]
        # 2) Match de anos por regex: `re.findall(r'\b(202[0-6])\b', sql)`
        legacy_years = sorted({int(y) for y in re.findall(r"\b(202[0-6])\b", sql)})

        val = mgr_fix.sql_guard.validate_and_rewrite(sql, enforce_limit=False)
        if val.is_valid:
            ast_exact_matches += 1
        if set(legacy_tables) != set(val.referenced_tables):
            regex_substring_false_positives += 1
        if set(legacy_years) != set(val.referenced_years):
            regex_between_failures += 1

    results["E_C_lineage_accuracy"] = {
        "total_golden_v1_queries": len(v1_rows),
        "ast_valid_and_exact": ast_exact_matches,
        "legacy_substring_table_errors": regex_substring_false_positives,
        "legacy_year_range_errors": regex_between_failures,
    }

    # ==========================================================================
    # E-D: Latência de Execução: VIEW sobre CSV vs. Tabela Materializada
    # ==========================================================================
    benchmark_queries = [
        "SELECT ano_referencia, SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))) FROM despesa_pagamento GROUP BY ano_referencia",
        "SELECT ano_referencia, SUM(TRY_CAST(vlr_realizadoateperiodo AS DECIMAL(18,2))) FROM receita_receita GROUP BY ano_referencia",
        "SELECT ano_referencia, SUM(TRY_CAST(vlr_liquidado AS DECIMAL(18,2))) FROM despesa_despesa GROUP BY ano_referencia",
        "SELECT COUNT(DISTINCT seq_dim_pessoa), SUM(TRY_CAST(vlr_rmn_liquida AS DECIMAL(18,2))) FROM remuneracao_municipio WHERE ano_referencia = 2025",
    ]
    view_times: List[float] = []
    for q in benchmark_queries:
        t0 = time.perf_counter()
        mgr_fix.conn.execute(q).fetchall()
        view_times.append((time.perf_counter() - t0) * 1000.0)

    # Materializar essas 4 tabelas em memória e medir novamente
    mat_con = mgr_fix.conn.cursor()
    for t in ["despesa_pagamento", "receita_receita", "despesa_despesa", "remuneracao_municipio"]:
        mat_con.execute(f"CREATE TEMP TABLE mat_{t} AS SELECT * FROM {t}")

    mat_times: List[float] = []
    for q in benchmark_queries:
        q_mat = q
        for t in ["despesa_pagamento", "receita_receita", "despesa_despesa", "remuneracao_municipio"]:
            q_mat = q_mat.replace(f"FROM {t}", f"FROM mat_{t}")
        t0 = time.perf_counter()
        mat_con.execute(q_mat).fetchall()
        mat_times.append((time.perf_counter() - t0) * 1000.0)

    avg_view_ms = round(sum(view_times) / len(view_times), 2)
    avg_mat_ms = round(sum(mat_times) / len(mat_times), 2)
    results["E_D_materialization_speedup"] = {
        "avg_view_csv_scan_ms": avg_view_ms,
        "avg_materialized_table_ms": avg_mat_ms,
        "speedup_factor": round(avg_view_ms / max(0.01, avg_mat_ms), 2),
    }

    # ==========================================================================
    # E-E: Comparativo RAG (Denso vs Dedup vs Híbrido vs Híbrido+Dedup+Filtro)
    # ==========================================================================
    rag_summaries: Dict[str, Any] = {}
    for folder in ["rag_golden_v13_denso", "rag_golden_v13_dedup", "rag_golden_v13_hibrido", "rag_golden_v13_filtro_dedup"]:
        sp = RAG_EXP_DIR / folder / "summary.json"
        if sp.exists():
            rag_summaries[folder] = json.loads(sp.read_text(encoding="utf-8"))
    results["E_E_rag_ablation"] = rag_summaries

    out_path = WORKSPACE / "AUDIT" / "comparative_experiments_results.json"
    out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return results


if __name__ == "__main__":
    run_all()
