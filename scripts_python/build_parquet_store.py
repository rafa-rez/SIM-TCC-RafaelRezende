"""
Fase 1 de Modernização do Banco Analítico:
Converte todas as 72 views (65 SICOM com reparo 2023 + 7 CGU) de CSV bruto para Apache Parquet (ZSTD)
e mede o ganho real de espaço em disco, tempo de boot do DuckDB e latência de consulta.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from sim_platform.engine.duckdb_manager import DuckDBManager, TenantContext


WORKSPACE = Path("/home/antigravity/sim-workspace")
PARQUET_DIR = WORKSPACE / "avaliacao" / "dados" / "parquet" / "caete_mg_3110004"
BENCHMARK_JSON = WORKSPACE / "AUDIT" / "PARQUET_PHASE1_BENCHMARK.json"


def build_parquet_store() -> None:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Medir boot em CSV bruto/reparado
    t0_csv_boot = time.perf_counter()
    csv_tenant = TenantContext(
        tenant_id="caete-csv",
        municipality_name="Caeté",
        state_uf="MG",
        ibge_code="3110004",
        data_dir=WORKSPACE / "avaliacao" / "dados" / "database",
        cgu_dir=WORKSPACE / "avaliacao" / "dados" / "staging_cgu",
        fix2023_dir=WORKSPACE / "avaliacao" / "dados" / "database_fix2023",
        parquet_dir=None,
        repaired_2023=True,
    )
    csv_mgr = DuckDBManager(tenant=csv_tenant)
    csv_boot_ms = round((time.perf_counter() - t0_csv_boot) * 1000.0, 2)

    # Medir query pesada em CSV
    test_sql = (
        "SELECT ano_referencia, COUNT(*) AS qtd, ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18,2))), 2) AS total "
        "FROM despesa_pagamento GROUP BY ano_referencia ORDER BY ano_referencia"
    )
    t0_q_csv = time.perf_counter()
    csv_mgr.execute_safe_query(test_sql, role="auditor")
    csv_query_ms = round((time.perf_counter() - t0_q_csv) * 1000.0, 2)

    # 2. Exportar todas as 72 tabelas para Parquet com compressão ZSTD
    tables = sorted(csv_mgr.catalog.tables.keys())
    exported = 0
    for t_low in tables:
        real_name = csv_mgr.catalog.tables[t_low].table_name
        out_file = PARQUET_DIR / f"{real_name}.parquet"
        csv_mgr.conn.execute(
            f"COPY (SELECT * FROM {real_name}) TO '{out_file.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD);"
        )
        exported += 1

    # 3. Calcular tamanho em disco (CSVs vs Parquet)
    csv_bytes = sum(f.stat().st_size for f in (WORKSPACE / "avaliacao" / "dados" / "database").glob("*.csv"))
    cgu_bytes = sum(f.stat().st_size for f in (WORKSPACE / "avaliacao" / "dados" / "staging_cgu").glob("*.csv"))
    total_csv_mb = round((csv_bytes + cgu_bytes) / (1024 * 1024), 2)
    total_parquet_mb = round(sum(f.stat().st_size for f in PARQUET_DIR.glob("*.parquet")) / (1024 * 1024), 2)
    storage_reduction_pct = round((1.0 - (total_parquet_mb / max(0.01, total_csv_mb))) * 100.0, 1)

    # 4. Medir boot e query sobre Parquet
    t0_pq_boot = time.perf_counter()
    pq_tenant = TenantContext(
        tenant_id="caete-parquet",
        municipality_name="Caeté",
        state_uf="MG",
        ibge_code="3110004",
        data_dir=WORKSPACE / "avaliacao" / "dados" / "database",
        cgu_dir=WORKSPACE / "avaliacao" / "dados" / "staging_cgu",
        fix2023_dir=WORKSPACE / "avaliacao" / "dados" / "database_fix2023",
        parquet_dir=PARQUET_DIR,
        repaired_2023=True,
    )
    pq_mgr = DuckDBManager(tenant=pq_tenant)
    pq_boot_ms = round((time.perf_counter() - t0_pq_boot) * 1000.0, 2)

    t0_q_pq = time.perf_counter()
    pq_res = pq_mgr.execute_safe_query(test_sql, role="auditor")
    pq_query_ms = round((time.perf_counter() - t0_q_pq) * 1000.0, 2)

    report = {
        "tables_exported": exported,
        "parquet_dir": str(PARQUET_DIR),
        "csv_size_mb": total_csv_mb,
        "parquet_size_mb": total_parquet_mb,
        "storage_reduction_pct": storage_reduction_pct,
        "csv_boot_ms": csv_boot_ms,
        "parquet_boot_ms": pq_boot_ms,
        "boot_speedup_x": round(csv_boot_ms / max(1.0, pq_boot_ms), 1),
        "csv_query_ms": csv_query_ms,
        "parquet_query_ms": pq_query_ms,
        "query_speedup_x": round(csv_query_ms / max(0.1, pq_query_ms), 1),
        "sample_rows_verified": len(pq_res.get("dados", [])),
    }
    BENCHMARK_JSON.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    build_parquet_store()
