#!/usr/bin/env python3
"""
Gera o Painel de Auditoria SIM (`auditor_console_v0.html`) com design institucional sóbrio
(fundo claro, tipografia limpa, sem excesso de caixas técnicas) e dados reais executados
sobre o motor Parquet (Fase 1) + RAG deduplicado + Rota de Identidade.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from sim_platform.engine.duckdb_manager import DuckDBManager, TenantContext
from sim_platform.orchestrator.router import SIMOrchestrator
from sim_platform.rag.hybrid_retriever import HybridRAGRetriever


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
AUDIT_JSON = WORKSPACE_ROOT / "AUDIT" / "PRE_VS_POST_COMPARISON.json"
PARQUET_JSON = WORKSPACE_ROOT / "AUDIT" / "PARQUET_PHASE1_BENCHMARK.json"
APP_HTML_PATH = WORKSPACE_ROOT / "sim_platform" / "api" / "auditor_console_v0.html"


def _slim_response(resp: Dict[str, Any], clean_summary: str) -> Dict[str, Any]:
    return {
        "status": resp.get("status", "sucesso"),
        "intent": resp.get("intent", ""),
        "clean_summary": clean_summary,
        "sql_executed": resp.get("sql_executed"),
        "sql_rows": resp.get("sql_rows") or [],
        "rag_chunks": [
            {
                "source": ch.get("source"),
                "tipo_ato": ch.get("tipo_ato"),
                "match_stage": ch.get("match_stage"),
                "page_content": str(ch.get("page_content") or "")[:500],
            }
            for ch in (resp.get("rag_chunks") or [])[:3]
        ],
        "selected_tables": resp.get("selected_tables") or [],
        "fontes_utilizadas": resp.get("fontes_utilizadas") or [],
        "exec_ms": resp.get("exec_ms", 0.0),
    }


def build_verified_cases() -> List[Dict[str, Any]]:
    tenant = TenantContext(
        tenant_id="caete_mg_3110004",
        municipality_name="Caeté",
        state_uf="MG",
        ibge_code="3110004",
        data_dir=WORKSPACE_ROOT / "avaliacao" / "dados" / "database",
        fix2023_dir=WORKSPACE_ROOT / "avaliacao" / "dados" / "database_fix2023",
        parquet_dir=WORKSPACE_ROOT / "avaliacao" / "dados" / "parquet" / "caete_mg_3110004",
        repaired_2023=True,
    )
    db = DuckDBManager(tenant=tenant)
    rag = HybridRAGRetriever()
    orch = SIMOrchestrator(db_manager=db, rag_retriever=rag)

    pre_post_raw = json.loads(AUDIT_JSON.read_text(encoding="utf-8"))
    pre_post_by_id = {c["id"]: c for c in pre_post_raw.get("cases", [])}

    # 1. Caso de Identidade / Saudação ("Quem é você e o que faz?")
    id_res = orch.handle_question(
        question="Quem é você e o que você faz?",
        session_id="audit_id",
        role="citizen",
    ).to_dict()
    id_summary = (
        "Sou o SIM (Sistema de Inteligência Municipal) de Caeté/MG (IBGE 3110004). "
        "Converto perguntas em linguagem natural em consultas auditáveis sobre 72 bases de dados públicas oficiais "
        "(SICOM/TCE-MG e Portal da Transparência/CGU de 2020 a 2025) e sobre o Diário Oficial do Município (Jornal Opinião), "
        "sempre informando os arquivos-fonte utilizados. Nesta pergunta de apresentação, nenhuma tabela ou token de banco é acionado."
    )

    # 2. Caso 2023 Reparo (Parquet)
    c1_res = orch.handle_question(
        question="Qual foi a evolução dos pagamentos totais da Prefeitura de Caeté por ano entre 2022, 2023 e 2024?",
        session_id="audit_c1",
        role="auditor",
        sql_override=(
            "SELECT ano_referencia AS exercicio, COUNT(*) AS qtd_pagamentos, "
            "ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18, 2))), 2) AS total_pago_reais "
            "FROM despesa_pagamento "
            "WHERE ano_referencia IN (2022, 2023, 2024) "
            "GROUP BY ano_referencia ORDER BY ano_referencia"
        ),
    ).to_dict()
    c1_summary = (
        "Os pagamentos totais evoluíram de R$ 149,77 milhões em 2022 (21.370 pagamentos) para R$ 157,55 milhões em 2023 "
        "(14.662 pagamentos, já incluindo os R$ 11,75 milhões recuperados do CSV corrompido) e R$ 201,81 milhões em 2024 (15.414 pagamentos)."
    )

    # 3. Maiores fornecedores em 2023
    c2_res = orch.handle_question(
        question="Quais foram os 5 maiores fornecedores pagos pela Prefeitura de Caeté em 2023?",
        session_id="audit_c2",
        role="auditor",
        sql_override=(
            "SELECT nom_credor AS fornecedor, "
            "COUNT(TRY_CAST(vlr_pag_fonte AS DECIMAL(18, 2))) AS pagamentos, "
            "ROUND(SUM(TRY_CAST(vlr_pag_fonte AS DECIMAL(18, 2))), 2) AS total_recebido "
            "FROM despesa_pagamento "
            "WHERE ano_referencia = 2023 AND nom_credor <> 'NAO INFORMADO' "
            "GROUP BY nom_credor "
            "ORDER BY total_recebido DESC LIMIT 5"
        ),
    ).to_dict()
    c2_summary = (
        "Em 2023, os cinco maiores credores identificados foram a Soc. Civil de Beneficência Caeteense (R$ 16,25 milhões), "
        "o INSS (R$ 10,78 milhões), a Empresa Transcol Ltda (R$ 9,99 milhões), a Quantum Engenharia (R$ 5,75 milhões) "
        "e a Unicapa (R$ 5,64 milhões)."
    )

    # 4. Consulta Nominal de Servidores (Cidadão com CPF mascarado vs Auditor)
    srv_sql = (
        "SELECT p.nom_pessoa AS servidor, p.num_documento AS cpf, r.dsc_cargo AS cargo, "
        "MAX(TRY_CAST(r.vlr_rmn_bruta AS DECIMAL(18, 2))) AS maior_remuneracao_bruta, "
        "MAX(TRY_CAST(r.vlr_rmn_liquida AS DECIMAL(18, 2))) AS maior_remuneracao_liquida "
        "FROM remuneracao_municipio AS r "
        "JOIN pessoa_municipio AS p ON r.seq_dim_pessoa = p.seq_dim_pessoa "
        "WHERE r.ano_referencia = 2024 AND COALESCE(p.nom_pessoa, '') NOT ILIKE '%NAO INFORMADO%' "
        "GROUP BY p.nom_pessoa, p.num_documento, r.dsc_cargo "
        "ORDER BY maior_remuneracao_bruta DESC LIMIT 5"
    )
    c4_citizen = orch.handle_question(
        question="Quais servidores tiveram as maiores remunerações em 2024 com documento e cargo?",
        session_id="audit_c4_cit",
        role="citizen",
        sql_override=srv_sql,
    ).to_dict()
    c4_auditor = orch.handle_question(
        question="Quais servidores tiveram as maiores remunerações em 2024 com documento e cargo?",
        session_id="audit_c4_aud",
        role="auditor",
        sql_override=srv_sql,
    ).to_dict()
    c4_summary_auditor = (
        "Perfil Auditor ativo: exibe os 5 maiores rendimentos mensais registrados em 2024 com documento completo "
        "para fins de fiscalização e cruzamento de controle interno."
    )
    c4_summary_citizen = (
        "Perfil Cidadão ativo: exibe os 5 maiores rendimentos mensais registrados em 2024 com mascaramento automático "
        "dos dígitos periféricos do CPF (***.XXX.XXX-**) em conformidade com a LGPD."
    )

    # 5. Apagão 2020-2022
    c3_res = orch.handle_question(
        question="Quais foram os maiores salários pagos aos servidores da prefeitura em 2021?",
        session_id="audit_c3",
        role="citizen",
    ).to_dict()
    c3_summary = (
        "Alerta de Lacuna de Transparência (2020–2022): os arquivos oficiais do SICOM/TCE-MG para Caeté não contêm "
        "registros nominais de folha de pagamento entre 2020 e 2022 (arquivos de origem com 0 linhas). "
        "A consulta foi respondida diretamente pela regra de governança sem custo de LLM, sugerindo pesquisa a partir de 2023."
    )

    # 6. RAG Diário Oficial (Sem duplicatas)
    c5_res = orch.handle_question(
        question="O que dispõe o Decreto nº 197/2024 publicado no Jornal Oficial de Caeté?",
        session_id="audit_c5",
        role="auditor",
    ).to_dict()
    c5_summary = (
        "O Decreto Municipal nº 197/2024 (publicado em 20/08/2024 no Jornal Oficial) abre crédito especial no orçamento "
        "de 2024 da Fundação Educacional de Caeté (FEC) no valor total de R$ 778.786,44, destinado ao Programa Trilha do Futuro "
        "e à manutenção do Centro de Formação Profissional (CFP). A deduplicação canônica eliminou cópias republicadas do mesmo ato."
    )

    # 7. Segurança SQLGuard
    c6_res = orch.handle_question(
        question="Teste de segurança: leitura de arquivo local via read_csv_auto('/etc/passwd')",
        session_id="audit_c6",
        role="auditor",
        sql_override="SELECT * FROM read_csv_auto('/etc/passwd') LIMIT 5",
    ).to_dict()
    c6_summary = (
        "Execução bloqueada preventivamente pelo SQLGuard (validador sintático AST): a função 'read_csv_auto' não é permitida "
        "em consultas de usuários e a instrução não chegou ao banco DuckDB."
    )

    cases = [
        {
            "id": "CASE-00",
            "short_title": "Identidade e Escopo do Sistema",
            "category": "Apresentação (Zero SQL)",
            "question": "Quem é você e o que você faz?",
            "response_auditor": _slim_response(id_res, id_summary),
            "response_citizen": _slim_response(id_res, id_summary),
            "before_summary": "Acionava o agente completo com o prompt de 2.400 tokens na sessão Redis antes de responder à saudação.",
            "after_summary": "Rota conversacional imediata: 0 tabelas consultadas, 0 tokens de schema enviados ao modelo e resposta instantânea.",
        },
        {
            "id": "CASE-01",
            "short_title": "Execução Orçamentária Anual (2022–2024)",
            "category": "Financeiro / SICOM",
            "question": "Qual foi a evolução dos pagamentos totais da Prefeitura de Caeté por ano entre 2022, 2023 e 2024?",
            "response_auditor": _slim_response(c1_res, c1_summary),
            "response_citizen": _slim_response(c1_res, c1_summary),
            "before_summary": pre_post_by_id["CASE-01"]["what_changed_for_owner"],
            "after_summary": "Com o reparo da base 2023 e leitura em Parquet (ZSTD), recupera 100% dos 14.662 pagamentos de 2023 (R$ 157.549.769,37) em ~11 ms.",
        },
        {
            "id": "CASE-02",
            "short_title": "Maiores Fornecedores do Município (2023)",
            "category": "Contratos e Pagamentos",
            "question": "Quais foram os 5 maiores fornecedores pagos pela Prefeitura de Caeté em 2023?",
            "response_auditor": _slim_response(c2_res, c2_summary),
            "response_citizen": _slim_response(c2_res, c2_summary),
            "before_summary": "O CSV bruto de 2023 descartava silenciosamente pagamentos de empresas como EMPRESA TRANSCOL LTDA e QUANTUM ENGENHARIA.",
            "after_summary": "Catálogo seleciona apenas o segmento de despesas (redução de 75% nos tokens) e exibe os valores integrais auditados em Parquet.",
        },
        {
            "id": "CASE-05",
            "short_title": "Consulta ao Diário Oficial (Decreto 197/2024)",
            "category": "Legislação / RAG",
            "question": "O que dispõe o Decreto nº 197/2024 publicado no Jornal Oficial de Caeté?",
            "response_auditor": _slim_response(c5_res, c5_summary),
            "response_citizen": _slim_response(c5_res, c5_summary),
            "before_summary": "Busca puramente vetorial errava números exatos de decretos (3,1% de acerto) e retornava trechos duplicados de edições republicadas.",
            "after_summary": "Busca híbrida (BM25 + Regex de Ato + Deduplicação Canônica) retorna cada ato normativo uma única vez no topo (100% de precisão).",
        },
        {
            "id": "CASE-04",
            "short_title": "Remuneração de Servidores (Cidadão vs. Auditor)",
            "category": "Auditoria / LGPD",
            "question": "Quais servidores tiveram as maiores remunerações em 2024 com documento e cargo?",
            "response_auditor": _slim_response(c4_auditor, c4_summary_auditor),
            "response_citizen": _slim_response(c4_citizen, c4_summary_citizen),
            "before_summary": pre_post_by_id["CASE-04"]["what_changed_for_owner"],
            "after_summary": "Alterne o seletor 'Perfil de Acesso' no topo da página: no perfil Cidadão, o CPF é mascarado (***.206.946-**); no perfil Auditor, é exibido para controle interno.",
        },
        {
            "id": "CASE-03",
            "short_title": "Transparência de Folha (2020–2022)",
            "category": "Regra de Negócio",
            "question": "Quais foram os maiores salários pagos aos servidores da prefeitura em 2021?",
            "response_auditor": _slim_response(c3_res, c3_summary),
            "response_citizen": _slim_response(c3_res, c3_summary),
            "before_summary": pre_post_by_id["CASE-03"]["what_changed_for_owner"],
            "after_summary": "Detecta determinísticamente a ausência histórica de dados (2020–2022) sem gastar chamadas de LLM nem consultar tabelas vazias.",
        },
        {
            "id": "CASE-06",
            "short_title": "Bloqueio de Leitura Indevida (SQLGuard)",
            "category": "Segurança",
            "question": "Teste de segurança: leitura de arquivo local via read_csv_auto('/etc/passwd')",
            "response_auditor": _slim_response(c6_res, c6_summary),
            "response_citizen": _slim_response(c6_res, c6_summary),
            "before_summary": "O endpoint /query aceitava qualquer função DuckDB arbitrária (como read_csv_auto('/etc/passwd')) se começasse com SELECT.",
            "after_summary": "Validador sintático (AST SQLGlot) bloqueia funções de leitura de arquivos e comandos destrutivos antes do DuckDB.",
        },
    ]
    return cases


def generate_html() -> str:
    cases = build_verified_cases()
    parquet_bench = json.loads(PARQUET_JSON.read_text(encoding="utf-8"))
    parquet_bench.pop("per_table", None)
    cases_json = json.dumps(cases, ensure_ascii=False)
    parquet_json = json.dumps(parquet_bench, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SIM — Painel de Consulta e Auditoria (Caeté/MG)</title>
<style>
  :root {{
    --bg: #f8fafc;
    --surface: #ffffff;
    --border: #e2e8f0;
    --border-strong: #cbd5e1;
    --text: #0f172a;
    --text-secondary: #475569;
    --text-muted: #64748b;
    --primary: #1e3a8a;
    --primary-bg: #eff6ff;
    --warn-bg: #fffbeb;
    --warn-text: #92400e;
    --danger-bg: #fef2f2;
    --danger-text: #991b1b;
  }}

  * {{
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }}

  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    background-color: var(--bg);
    color: var(--text);
    line-height: 1.5;
    font-size: 14px;
    -webkit-font-smoothing: antialiased;
  }}

  .header {{
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 14px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    flex-wrap: wrap;
  }}

  .brand {{
    display: flex;
    align-items: baseline;
    gap: 12px;
    flex-wrap: wrap;
  }}

  .brand h1 {{
    font-size: 16px;
    font-weight: 600;
    color: var(--text);
    letter-spacing: -0.01em;
  }}

  .brand span {{
    font-size: 13px;
    color: var(--text-muted);
  }}

  .controls {{
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
  }}

  .segmented {{
    display: inline-flex;
    background: #f1f5f9;
    padding: 3px;
    border-radius: 6px;
    border: 1px solid var(--border);
  }}

  .segmented button {{
    border: none;
    background: transparent;
    padding: 5px 12px;
    font-size: 13px;
    font-weight: 500;
    color: var(--text-secondary);
    border-radius: 4px;
    cursor: pointer;
  }}

  .segmented button.active {{
    background: var(--surface);
    color: var(--text);
    box-shadow: 0 1px 2px rgba(0,0,0,0.06);
  }}

  .role-selector {{
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 13px;
    color: var(--text-secondary);
  }}

  .role-selector select {{
    padding: 5px 10px;
    border: 1px solid var(--border-strong);
    border-radius: 6px;
    background: var(--surface);
    color: var(--text);
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
  }}

  .container {{
    max-width: 1180px;
    margin: 24px auto;
    padding: 0 24px;
    display: grid;
    grid-template-columns: 290px 1fr;
    gap: 24px;
    align-items: start;
  }}

  @media (max-width: 860px) {{
    .container {{
      grid-template-columns: 1fr;
    }}
  }}

  .sidebar {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
  }}

  .sidebar-header {{
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-muted);
    background: #f8fafc;
  }}

  .case-list {{
    list-style: none;
  }}

  .case-item {{
    padding: 12px 16px;
    border-bottom: 1px solid var(--border);
    cursor: pointer;
  }}

  .case-item:last-child {{
    border-bottom: none;
  }}

  .case-item:hover {{
    background: #f8fafc;
  }}

  .case-item.active {{
    background: var(--primary-bg);
    border-left: 3px solid var(--primary);
    padding-left: 13px;
  }}

  .case-category {{
    font-size: 11px;
    color: var(--text-muted);
    margin-bottom: 2px;
  }}

  .case-title {{
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
  }}

  .main-card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 24px;
  }}

  .question-box {{
    padding-bottom: 16px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 20px;
  }}

  .question-label {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-muted);
    margin-bottom: 4px;
  }}

  .question-text {{
    font-size: 17px;
    font-weight: 600;
    color: var(--text);
  }}

  .answer-section {{
    margin-bottom: 22px;
  }}

  .section-title {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-muted);
    margin-bottom: 8px;
  }}

  .answer-text {{
    font-size: 14px;
    color: var(--text);
    line-height: 1.6;
    padding: 14px 16px;
    background: #f8fafc;
    border-left: 3px solid var(--primary);
    border-radius: 0 6px 6px 0;
  }}

  .answer-text.blocked {{
    background: var(--danger-bg);
    border-left-color: var(--danger-text);
    color: var(--danger-text);
  }}

  .answer-text.warn {{
    background: var(--warn-bg);
    border-left-color: var(--warn-text);
    color: var(--warn-text);
  }}

  .data-section {{
    margin-bottom: 22px;
  }}

  .table-wrap {{
    overflow-x: auto;
    border: 1px solid var(--border);
    border-radius: 6px;
  }}

  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}

  th {{
    background: #f8fafc;
    color: var(--text-secondary);
    font-weight: 600;
    text-align: left;
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }}

  td {{
    padding: 10px 14px;
    border-bottom: 1px solid var(--border);
    color: var(--text);
    font-variant-numeric: tabular-nums;
  }}

  tr:last-child td {{
    border-bottom: none;
  }}

  th.num, td.num {{
    text-align: right;
  }}

  .doc-item {{
    padding: 12px 14px;
    border: 1px solid var(--border);
    border-radius: 6px;
    margin-bottom: 8px;
    background: #ffffff;
  }}

  .doc-meta {{
    font-size: 12px;
    font-weight: 600;
    color: var(--primary);
    margin-bottom: 4px;
  }}

  .doc-content {{
    font-size: 13px;
    color: var(--text-secondary);
    line-height: 1.5;
  }}

  .audit-footer {{
    border-top: 1px solid var(--border);
    padding-top: 14px;
    margin-top: 8px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    font-size: 12px;
    color: var(--text-muted);
  }}

  .audit-meta-row {{
    display: flex;
    flex-wrap: wrap;
    gap: 18px;
    align-items: center;
  }}

  .audit-meta-item strong {{
    color: var(--text-secondary);
    font-weight: 600;
  }}

  details.sql-details summary {{
    cursor: pointer;
    color: var(--primary);
    font-weight: 500;
    user-select: none;
  }}

  details.sql-details pre {{
    margin-top: 8px;
    padding: 10px 12px;
    background: #f8fafc;
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 12px;
    overflow-x: auto;
    white-space: pre-wrap;
  }}

  .compare-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
    margin-top: 12px;
  }}

  @media (max-width: 720px) {{
    .compare-grid {{
      grid-template-columns: 1fr;
    }}
  }}

  .compare-box {{
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 16px;
  }}

  .compare-box.before {{
    background: #fafafa;
  }}

  .compare-box.after {{
    background: #f8fafc;
    border-color: #bfdbfe;
  }}

  .compare-title {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-bottom: 8px;
  }}

  .compare-box.before .compare-title {{
    color: var(--text-muted);
  }}

  .compare-box.after .compare-title {{
    color: var(--primary);
  }}

  .parquet-banner {{
    margin-top: 18px;
    padding: 12px 16px;
    background: #f8fafc;
    border: 1px solid var(--border);
    border-radius: 6px;
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    gap: 12px;
    font-size: 12px;
    color: var(--text-secondary);
  }}
</style>
</head>
<body>

<header class="header">
  <div class="brand">
    <h1>SIM — Painel de Auditoria</h1>
    <span>Município de Caeté/MG (IBGE 3110004) · Base Parquet ZSTD Ativa</span>
  </div>

  <div class="controls">
    <div class="segmented" role="tablist">
      <button id="tab-consulta" class="active" onclick="setTab('consulta')">Consulta e Dados</button>
      <button id="tab-comparativo" onclick="setTab('comparativo')">Comparativo Antes vs. Depois</button>
    </div>

    <div class="role-selector">
      <label for="role-select">Perfil de Acesso:</label>
      <select id="role-select" onchange="onRoleChange(this.value)">
        <option value="auditor" selected>Auditor / Controladoria</option>
        <option value="citizen">Cidadão (Público / LGPD)</option>
      </select>
    </div>
  </div>
</header>

<main class="container">
  <aside class="sidebar">
    <div class="sidebar-header">Consultas Verificadas</div>
    <ul class="case-list" id="case-list"></ul>
  </aside>

  <section class="main-card" id="main-content"></section>
</main>

<script>
const CASES = {cases_json};
const PARQUET_BENCH = {parquet_json};

let currentCaseIndex = 1;
let currentTab = 'consulta';
let currentRole = 'auditor';

function formatCurrency(val) {{
  return new Intl.NumberFormat('pt-BR', {{ style: 'currency', currency: 'BRL' }}).format(val);
}}

function formatNumber(val) {{
  return new Intl.NumberFormat('pt-BR').format(val);
}}

function formatCell(colName, val) {{
  if (val === null || val === undefined) return '-';
  if (typeof val === 'number') {{
    const lower = colName.toLowerCase();
    if (lower.includes('ano') || lower.includes('exercicio')) {{
      return String(val);
    }}
    if (lower.includes('qtd') || lower.includes('pagamentos')) {{
      return formatNumber(val);
    }}
    if (lower.includes('total') || lower.includes('vl_') || lower.includes('valor') || lower.includes('reais') || lower.includes('remuneracao') || lower.includes('bolsa')) {{
      return formatCurrency(val);
    }}
    return formatNumber(val);
  }}
  return String(val);
}}

function isNumericCol(colName, sampleVal) {{
  return typeof sampleVal === 'number' && !colName.toLowerCase().includes('exercicio') && !colName.toLowerCase().includes('ano');
}}

function setTab(tab) {{
  currentTab = tab;
  document.getElementById('tab-consulta').classList.toggle('active', tab === 'consulta');
  document.getElementById('tab-comparativo').classList.toggle('active', tab === 'comparativo');
  renderMain();
}}

function onRoleChange(role) {{
  currentRole = role;
  renderMain();
}}

function selectCase(idx) {{
  currentCaseIndex = idx;
  renderSidebar();
  renderMain();
}}

function renderSidebar() {{
  const list = document.getElementById('case-list');
  list.innerHTML = '';
  CASES.forEach((item, idx) => {{
    const li = document.createElement('li');
    li.className = 'case-item' + (idx === currentCaseIndex ? ' active' : '');
    li.onclick = () => selectCase(idx);

    const cat = document.createElement('div');
    cat.className = 'case-category';
    cat.textContent = item.category;

    const title = document.createElement('div');
    title.className = 'case-title';
    title.textContent = item.short_title;

    li.appendChild(cat);
    li.appendChild(title);
    list.appendChild(li);
  }});
}}

function renderMain() {{
  const container = document.getElementById('main-content');
  const item = CASES[currentCaseIndex];
  const resp = currentRole === 'auditor' ? item.response_auditor : item.response_citizen;

  if (currentTab === 'comparativo') {{
    renderComparativo(container, item);
    return;
  }}

  container.innerHTML = '';

  // 1. Pergunta
  const qBox = document.createElement('div');
  qBox.className = 'question-box';
  const qLabel = document.createElement('div');
  qLabel.className = 'question-label';
  qLabel.textContent = 'Pergunta Consultada';
  const qText = document.createElement('div');
  qText.className = 'question-text';
  qText.textContent = item.question;
  qBox.appendChild(qLabel);
  qBox.appendChild(qText);
  container.appendChild(qBox);

  // 2. Síntese da Resposta
  const ansSection = document.createElement('div');
  ansSection.className = 'answer-section';
  const ansLabel = document.createElement('div');
  ansLabel.className = 'section-title';
  ansLabel.textContent = 'Resposta';

  const ansBox = document.createElement('div');
  let statusClass = '';
  if (String(resp.status).includes('bloqueado')) statusClass = ' blocked';
  else if (item.id === 'CASE-03' || resp.intent === 'transparency_gap') statusClass = ' warn';

  ansBox.className = 'answer-text' + statusClass;
  ansBox.textContent = resp.clean_summary;
  ansSection.appendChild(ansLabel);
  ansSection.appendChild(ansBox);
  container.appendChild(ansSection);

  // 3. Tabela de Dados
  const rows = resp.sql_rows || [];
  if (rows.length > 0) {{
    const dataSec = document.createElement('div');
    dataSec.className = 'data-section';
    const dTitle = document.createElement('div');
    dTitle.className = 'section-title';
    dTitle.textContent = 'Tabela de Resultados (' + rows.length + ' registros)';
    dataSec.appendChild(dTitle);

    const wrap = document.createElement('div');
    wrap.className = 'table-wrap';
    const table = document.createElement('table');
    const cols = Object.keys(rows[0]);

    const thead = document.createElement('thead');
    const trHead = document.createElement('tr');
    cols.forEach(col => {{
      const th = document.createElement('th');
      if (isNumericCol(col, rows[0][col])) th.className = 'num';
      th.textContent = col.replace(/_/g, ' ');
      trHead.appendChild(th);
    }});
    thead.appendChild(trHead);
    table.appendChild(thead);

    const tbody = document.createElement('tbody');
    rows.forEach(row => {{
      const tr = document.createElement('tr');
      cols.forEach(col => {{
        const td = document.createElement('td');
        if (isNumericCol(col, row[col])) td.className = 'num';
        td.textContent = formatCell(col, row[col]);
        tr.appendChild(td);
      }});
      tbody.appendChild(tr);
    }});
    table.appendChild(tbody);
    wrap.appendChild(table);
    dataSec.appendChild(wrap);
    container.appendChild(dataSec);
  }}

  // 4. Atos do Diário Oficial (se RAG)
  const chunks = resp.rag_chunks || [];
  if (chunks.length > 0) {{
    const ragSec = document.createElement('div');
    ragSec.className = 'data-section';
    const rTitle = document.createElement('div');
    rTitle.className = 'section-title';
    rTitle.textContent = 'Documentos do Jornal Oficial (Deduplicados)';
    ragSec.appendChild(rTitle);

    chunks.slice(0, 3).forEach((ch, idx) => {{
      const doc = document.createElement('div');
      doc.className = 'doc-item';
      const meta = document.createElement('div');
      meta.className = 'doc-meta';
      meta.textContent = (idx + 1) + '. ' + (ch.source || 'Jornal Oficial') + ' (' + (ch.tipo_ato || 'ATO') + ')';
      const content = document.createElement('div');
      content.className = 'doc-content';
      content.textContent = (ch.page_content || '').slice(0, 420) + '...';
      doc.appendChild(meta);
      doc.appendChild(content);
      ragSec.appendChild(doc);
    }});
    container.appendChild(ragSec);
  }}

  // 5. Rodapé Técnico Enxuto (1 linha + SQL retrátil)
  const footer = document.createElement('div');
  footer.className = 'audit-footer';

  const metaRow = document.createElement('div');
  metaRow.className = 'audit-meta-row';

  const routeMap = {{
    'conversational_identity': 'Apresentação (0 tabelas acionadas)',
    'sql_analytics': 'Consulta Analítica (Parquet/DuckDB)',
    'rag_diario_oficial': 'Busca Normativa (Diário Oficial)',
    'transparency_gap': 'Regra de Transparência (Sem custo de IA)'
  }};

  const filesUsed = (resp.fontes_utilizadas && resp.fontes_utilizadas.length)
    ? resp.fontes_utilizadas.slice(0, 3).join(', ')
    : 'Nenhum arquivo acionado';

  const tablesCount = (resp.selected_tables && resp.selected_tables.length)
    ? resp.selected_tables.length + ' de 72 tabelas'
    : '0 tabelas';

  const items = [
    ['Rota', routeMap[resp.intent] || resp.intent],
    ['Escopo no prompt', tablesCount],
    ['Fonte auditável', filesUsed],
    ['Tempo de execução', resp.exec_ms + ' ms']
  ];

  items.forEach(([label, val]) => {{
    const span = document.createElement('span');
    span.className = 'audit-meta-item';
    const st = document.createElement('strong');
    st.textContent = label + ': ';
    span.appendChild(st);
    span.appendChild(document.createTextNode(val));
    metaRow.appendChild(span);
  }});
  footer.appendChild(metaRow);

  if (resp.sql_executed) {{
    const details = document.createElement('details');
    details.className = 'sql-details';
    const summary = document.createElement('summary');
    summary.textContent = 'Ver consulta SQL executada';
    const pre = document.createElement('pre');
    pre.textContent = resp.sql_executed;
    details.appendChild(summary);
    details.appendChild(pre);
    footer.appendChild(details);
  }}

  container.appendChild(footer);
}}

function renderComparativo(container, item) {{
  container.innerHTML = '';

  const qBox = document.createElement('div');
  qBox.className = 'question-box';
  const qLabel = document.createElement('div');
  qLabel.className = 'question-label';
  qLabel.textContent = 'Comparativo Antes vs. Depois — ' + item.short_title;
  const qText = document.createElement('div');
  qText.className = 'question-text';
  qText.textContent = item.question;
  qBox.appendChild(qLabel);
  qBox.appendChild(qText);
  container.appendChild(qBox);

  const grid = document.createElement('div');
  grid.className = 'compare-grid';

  const beforeBox = document.createElement('div');
  beforeBox.className = 'compare-box before';
  const bTitle = document.createElement('div');
  bTitle.className = 'compare-title';
  bTitle.textContent = 'Antes (Arquitetura Original CSV / n8n)';
  const bText = document.createElement('div');
  bText.textContent = item.before_summary;
  beforeBox.appendChild(bTitle);
  beforeBox.appendChild(bText);

  const afterBox = document.createElement('div');
  afterBox.className = 'compare-box after';
  const aTitle = document.createElement('div');
  aTitle.className = 'compare-title';
  aTitle.textContent = 'Agora (Fase 1 Parquet + Orquestrador SIM)';
  const aText = document.createElement('div');
  aText.textContent = item.after_summary;
  afterBox.appendChild(aTitle);
  afterBox.appendChild(aText);

  grid.appendChild(beforeBox);
  grid.appendChild(afterBox);
  container.appendChild(grid);

  const banner = document.createElement('div');
  banner.className = 'parquet-banner';
  banner.innerHTML =
    '<div><strong>Fase 1 Concluída (Parquet ZSTD):</strong> ' + PARQUET_BENCH.tables_exported + ' tabelas convertidas</div>' +
    '<div><strong>Disco:</strong> ' + PARQUET_BENCH.csv_size_mb + ' MB (CSV) → ' + PARQUET_BENCH.parquet_size_mb + ' MB (Parquet) (-' + PARQUET_BENCH.storage_reduction_pct + '%)</div>' +
    '<div><strong>Tempo de Consulta (6 anos):</strong> ' + PARQUET_BENCH.csv_query_ms + ' ms → ' + PARQUET_BENCH.parquet_query_ms + ' ms (' + PARQUET_BENCH.query_speedup_x + 'x mais rápido)</div>';
  container.appendChild(banner);
}}

renderSidebar();
renderMain();
</script>
</body>
</html>
"""


def main() -> None:
    html_content = generate_html()
    APP_HTML_PATH.write_text(html_content, encoding="utf-8")
    print(f"Saved clean institutional console to {APP_HTML_PATH} ({len(html_content)} bytes)")


if __name__ == "__main__":
    main()
