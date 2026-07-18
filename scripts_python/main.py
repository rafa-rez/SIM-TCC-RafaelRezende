from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import duckdb
import os
import re
import pandas as pd
from pathlib import Path
import json
import time

app = FastAPI(title="DuckDB SICOM API", description="API para consultas SQL no database SICOM usando DuckDB.")

# ==============================================================================
# 0. Sistema de Logs (Terminal Real-time)
# ==============================================================================
system_logs = []
MAX_SYS_LOGS = 200

def add_log(level: str, message: str):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] [{level}] {message}"
    system_logs.append(log_line)
    if len(system_logs) > MAX_SYS_LOGS:
        system_logs.pop(0)
    print(log_line)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    
    if request.url.path != "/api/logs":
        add_log("INFO", f"{request.method} {request.url.path} - HTTP {response.status_code} - {process_time:.1f}ms")
    return response

@app.get("/api/logs")
def get_logs():
    return {"logs": system_logs}

# ==============================================================================
# 1. Configuração da Conexão Global
# ==============================================================================
con = duckdb.connect(database=':memory:')

PASTA_DATABASE = Path('/gov/dados/database')
PASTA_CGU = Path('/gov/dados/staging_cgu')

# ==============================================================================
# 2.A Inicialização e Mapeamento Dinâmico de Views (SICOM)
# ==============================================================================
add_log("INFO", f"Iniciando mapeamento de tabelas no DuckDB a partir de: {PASTA_DATABASE}...")

if PASTA_DATABASE.exists():
    sufixos_tabelas = set()
    for arquivo in PASTA_DATABASE.glob('*.csv'):
        partes_nome = arquivo.stem.split('.')
        # Lógica igual ao ETL: ignora o código da cidade se ele existir após o ano
        if len(partes_nome) > 1 and partes_nome[1].isdigit():
            sufixo = f"{partes_nome[-2]}.{partes_nome[-1]}"
            nome_base = "_".join(partes_nome[2:])
        else:
            sufixo = f"{partes_nome[-2]}.{partes_nome[-1]}"
            nome_base = "_".join(partes_nome[1:])
            
        nome_tabela = f"sicom_{nome_base.replace('-', '_')}" if nome_base[0].isdigit() else nome_base.replace('.', '_').replace('-', '_')
        
        padrao_busca = f"{PASTA_DATABASE.as_posix()}/*{sufixo}.csv"
        
        query_view = f"""
            CREATE OR REPLACE VIEW {nome_tabela} AS 
            SELECT 
                *, 
                CAST(string_split(string_split(filename, '/')[-1], '.')[1] AS INTEGER) AS ano_referencia
            FROM read_csv_auto('{padrao_busca}', delim='|', header=True, filename=true, union_by_name=true, ignore_errors=true);
        """
        try:
            con.execute(query_view)
            # Como a criação deu certo, não precisamos logar tabela por tabela para não floodar, 
            # mas mantive seu try/except para segurança.
        except Exception as e:
            add_log("ERROR", f"Erro ao mapear a tabela SICOM {nome_tabela}: {e}")
            
    add_log("SUCCESS", f"Mapeamento SICOM concluído.")

# ==============================================================================
# 2.B Inicialização e Mapeamento da View Federal (CGU)
# ==============================================================================
add_log("INFO", f"Verificando base de repasses federais em: {PASTA_CGU}...")

if PASTA_CGU.exists():
    categorias_cgu = set()
    for arquivo in PASTA_CGU.glob('*.csv'):
        partes = arquivo.stem.split('_')
        # Padrão: 2024_05_categoria_cgu
        if len(partes) >= 3:
            categorias_cgu.add(partes[2])
            
    for categoria in categorias_cgu:
        nome_view = f"cgu_{categoria.replace('-', '_')}"
        padrao_busca = f"{PASTA_CGU.as_posix()}/*_{categoria}_cgu.csv"
        
        # Extrai o ano da primeira parte do nome do arquivo usando string_split
        query_cgu = f"""
            CREATE OR REPLACE VIEW {nome_view} AS 
            SELECT 
                *, 
                CAST(string_split(string_split(filename, '/')[-1], '_')[1] AS INTEGER) AS ano_referencia
            FROM read_csv_auto('{padrao_busca}', delim='|', header=True, filename=true, union_by_name=true, ignore_errors=true);
        """
        try:
            con.execute(query_cgu)
            add_log("SUCCESS", f"Tabela Federal '{nome_view}' unificada com sucesso.")
        except Exception as e:
            add_log("ERROR", f"Erro ao mapear a tabela CGU {nome_view}: {e}")
else:
    add_log("WORK", "Pasta da CGU não encontrada. Apenas dados municipais disponíveis.")

# ==============================================================================
# 2.C Materialização (tabelas em memória) e trava de segurança
# ==============================================================================
# Views sobre read_csv_auto relêem os CSVs a CADA query (~1,5s por COUNT).
# Materializar uma vez no boot deixa as consultas em milissegundos e permite
# desabilitar acesso a arquivos (bloqueia COPY TO etc. em SQL arbitrário).
MATERIALIZAR_TABELAS = os.environ.get("DUCKDB_MATERIALIZE", "1") != "0"

if MATERIALIZAR_TABELAS:
    add_log("INFO", "Materializando views em tabelas em memoria...")
    _t0 = time.time()
    _views = con.execute(
        "SELECT table_name FROM information_schema.views WHERE table_schema = 'main' "
        "AND table_name NOT LIKE 'duckdb_%' AND table_name NOT LIKE 'sqlite_%' "
        "AND table_name NOT LIKE 'pragma_%'"
    ).df()["table_name"].tolist()
    _ok = 0
    for _v in _views:
        try:
            con.execute(f'CREATE TABLE "__tbl_{_v}" AS SELECT * FROM "{_v}"')
            con.execute(f'DROP VIEW "{_v}"')
            con.execute(f'ALTER TABLE "__tbl_{_v}" RENAME TO "{_v}"')
            _ok += 1
        except Exception as e:
            add_log("ERROR", f"Falha ao materializar {_v}: {e}")
    add_log("SUCCESS", f"{_ok}/{len(_views)} tabelas materializadas em {time.time() - _t0:.1f}s.")
    try:
        con.execute("SET enable_external_access = false")
        add_log("INFO", "Acesso a arquivos desabilitado no DuckDB (seguranca).")
    except Exception as e:
        add_log("ERROR", f"Nao foi possivel desabilitar acesso externo: {e}")

# ==============================================================================
# 3. Modelos e Memória
# ==============================================================================
class QueryRequest(BaseModel):
    query: str
    max_rows: int | None = None   # opcional: teto de linhas na resposta
    format: str = "records"       # opcional: records (padrão) | split | csv

MAX_AUDIT_LOGS = 50
audit_log = [] 

# ==============================================================================
# 4. Rotas da API (/query e /ping)
# ==============================================================================
@app.post("/query")
async def execute_query(request: QueryRequest):
    global audit_log
    add_log("DEBUG", "Iniciando parse da Query SQL recebida...")
    
    current_call = {
        "id": int(time.time() * 1000),
        "time": time.strftime("%d/%m/%Y %H:%M:%S"),
        "query_sql": request.query,
        "views_used": [],
        "status": "N/A",
        "output": ""
    }
    
    try:
        todas_views = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'").df()['table_name'].tolist()
        views_identificadas = [v for v in todas_views if v.lower() in request.query.lower()]
        current_call["views_used"] = views_identificadas
        if views_identificadas:
            add_log("DEBUG", f"Views ativadas: {', '.join(views_identificadas)}")
    except:
        current_call["views_used"] = ["Erro ao inferir views"]
        views_identificadas = []

    # Bloco de Rastreabilidade de Arquivos
    arquivos_origem = []
    try:
        if views_identificadas:
            anos_mencionados = list(set(re.findall(r'\b(202[0-6])\b', request.query)))
            for view in views_identificadas:
                if anos_mencionados:
                    anos_in = ', '.join(anos_mencionados)
                    q_files = f"SELECT DISTINCT string_split(filename, '/')[-1] as file FROM {view} WHERE ano_referencia IN ({anos_in})"
                else:
                    q_files = f"SELECT DISTINCT string_split(filename, '/')[-1] as file FROM {view}"
                
                df_files = con.execute(q_files).df()
                if not df_files.empty:
                    arquivos_origem.extend(df_files['file'].tolist())
    except Exception as e:
        add_log("ERROR", f"Falha ao rastrear arquivos de origem: {e}")

    try:
        add_log("WORK", "Executando carga em memoria no DuckDB...")
        result_df = con.execute(request.query).df()
        
        arquivos_unicos = list(set(arquivos_origem))
        
        truncado = False
        if request.max_rows and len(result_df) > request.max_rows:
            result_df = result_df.head(request.max_rows)
            truncado = True

        if result_df.empty:
            resposta = {"status": "success", "data": [], "fontes_csv": arquivos_unicos}
            add_log("SUCCESS", "Query executada. (0 linhas retornadas)")
        else:
            if request.format == "split":
                split = json.loads(result_df.to_json(orient="split", date_format="iso"))
                result_json = {"columns": split["columns"], "rows": split["data"]}
            elif request.format == "csv":
                result_json = result_df.to_csv(index=False, sep=";")
            else:
                result_json = json.loads(result_df.to_json(orient="records", date_format="iso"))
            resposta = {"status": "success", "data": result_json, "fontes_csv": arquivos_unicos}
            if truncado:
                resposta["truncated"] = True
            add_log("SUCCESS", f"Query executada. ({len(result_df)} linhas mapeadas)")
        
        out_str = json.dumps(resposta, indent=2)
        current_call["output"] = out_str if len(out_str) < 3000 else out_str[:3000] + "\n\n... [TRUNCADO PARA EXIBICAO NO DASHBOARD] ..."
        current_call["status"] = 200
        
        audit_log.insert(0, current_call)
        audit_log[:] = audit_log[:MAX_AUDIT_LOGS]
        return resposta
    
    except Exception as e:
        erro_bruto = str(e)
        add_log("ERROR", f"DuckDB rejeitou a query: {erro_bruto.split(chr(10))[0]}")
        
        resposta_erro = {
            "status": "error",
            "mensagem_llm": "O DuckDB rejeitou a execucao devido a um erro de sintaxe SQL ou tipagem incompativel.",
            "erro_tecnico": erro_bruto,
            "dicas_correcao": [
                "1. Se o erro for 'Catalog Error', voce tentou acessar uma tabela/coluna inexistente.",
                "2. Se o erro for 'Binder Error' alertando sobre VARCHAR vs DECIMAL, aplique CAST(coluna AS DECIMAL).",
                "3. Use aspas duplas nas colunas com espaco."
            ]
        }
        
        current_call["output"] = json.dumps(resposta_erro, indent=2)
        current_call["status"] = 400
        audit_log.insert(0, current_call)
        audit_log[:] = audit_log[:MAX_AUDIT_LOGS]
        return resposta_erro

@app.get("/health")
def health():
    """Healthcheck real: 503 se o catálogo estiver vazio (mount quebrado etc.)."""
    try:
        n = int(
            con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchone()[0]
        )
    except Exception:
        n = 0
    return JSONResponse({"ok": n > 0, "tabelas": n}, status_code=200 if n > 0 else 503)


@app.get("/ping", response_class=HTMLResponse)
async def health_check():
    try:
        views_info = con.execute("SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = 'main'").df().to_dict(orient="records")
    except:
        views_info = []

    qtd_views = len(views_info)
    last_call = audit_log[0] if audit_log else {"time": "-", "status": "N/A", "query_sql": "Nenhuma consulta.", "views_used": [], "output": "Sem dados."}
    status_code = last_call.get('status', 'N/A')
    
    status_color = "bg-green-500/20 text-green-400 border-green-500/30" if status_code == 200 else ("bg-gray-500/20 text-gray-400 border-gray-500/30" if status_code == "N/A" else "bg-red-500/20 text-red-400 border-red-500/30")
    status_icon = "fa-check-circle" if status_code == 200 else ("fa-minus-circle" if status_code == "N/A" else "fa-exclamation-circle")

    try:
        cols_df = con.execute("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'main'").df()
        schema_info, col_to_tables = {}, {}
        colunas_ignoradas = ['filename', 'ano_referencia', 'id', 'valor', 'data', 'tipo']
        for _, row in cols_df.iterrows():
            t, c, dt = row['table_name'], row['column_name'], row['data_type']
            if t not in schema_info: schema_info[t] = []
            schema_info[t].append({'name': c, 'type': dt})
            if c.lower() not in colunas_ignoradas:
                if c not in col_to_tables: col_to_tables[c] = []
                col_to_tables[c].append(t)
        relationships = {t: {} for t in schema_info.keys()}
        for c, tables in col_to_tables.items():
            if len(tables) > 1:
                for t1 in tables:
                    for t2 in tables:
                        if t1 != t2:
                            if t2 not in relationships[t1]: relationships[t1][t2] = []
                            if c not in relationships[t1][t2]: relationships[t1][t2].append(c)
    except:
        schema_info, relationships = {}, {}

    schema_json, relations_json, audit_log_json = json.dumps(schema_info), json.dumps(relationships), json.dumps(audit_log)

    badges_views = "" if not last_call['views_used'] else "".join([f'<span class="bg-indigo-500/20 text-indigo-300 border border-indigo-500/30 text-[10px] px-2 py-1 rounded font-mono mr-2">{view}</span>' for view in last_call['views_used']])

    html_content = f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>DuckDB SICOM Monitor</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
            body {{ font-family: 'Inter', sans-serif; }}
            code, pre, .terminal-text {{ font-family: 'JetBrains Mono', monospace; }}
            ::-webkit-scrollbar {{ width: 8px; height: 8px; }}
            ::-webkit-scrollbar-track {{ background: #0f172a; }}
            ::-webkit-scrollbar-thumb {{ background: #334155; border-radius: 4px; }}
            ::-webkit-scrollbar-thumb:hover {{ background: #475569; }}
            .tab-active {{ border-bottom: 2px solid #3b82f6; color: #60a5fa; }}
            .tab-inactive {{ border-bottom: 2px solid transparent; color: #94a3b8; }}
        </style>
    </head>
    <body class="bg-slate-950 text-slate-200 min-h-screen p-4 md:p-8">
        <header class="mb-6 flex flex-col md:flex-row md:items-center justify-between border-b border-slate-800 pb-6 gap-4">
            <div class="flex items-center gap-4">
                <div class="bg-blue-600/20 p-3 rounded-2xl border border-blue-500/30"><i class="fa-solid fa-database text-2xl text-blue-400"></i></div>
                <div><h1 class="text-2xl font-bold text-white tracking-tight">DuckDB SICOM API</h1><p class="text-sm text-slate-400">Monitoramento e Gateway de Dados</p></div>
            </div>
            <div class="flex gap-6 border-b border-slate-800 w-full md:w-auto mt-4 md:mt-0 px-2 overflow-x-auto">
                <button onclick="switchTab('dashboard')" id="tab-dashboard" class="tab-active pb-3 text-sm font-semibold transition-colors flex items-center gap-2 whitespace-nowrap"><i class="fa-solid fa-terminal"></i> Última Execução</button>
                <button onclick="switchTab('history')" id="tab-history" class="tab-inactive pb-3 text-sm font-semibold transition-colors flex items-center gap-2 whitespace-nowrap"><i class="fa-solid fa-clock-rotate-left"></i> Memória</button>
                <button onclick="switchTab('dictionary')" id="tab-dictionary" class="tab-inactive pb-3 text-sm font-semibold transition-colors flex items-center gap-2 whitespace-nowrap"><i class="fa-solid fa-project-diagram"></i> Dicionário</button>
                <button onclick="switchTab('terminal')" id="tab-terminal" class="tab-inactive pb-3 text-sm font-semibold transition-colors flex items-center gap-2 whitespace-nowrap text-emerald-400 hover:text-emerald-300"><i class="fa-solid fa-code"></i> Logs Live</button>
            </div>
        </header>

        <div id="content-dashboard" class="block">
            <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
                <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-lg flex items-center gap-5"><div class="bg-indigo-500/20 w-12 h-12 rounded-full flex items-center justify-center border border-indigo-500/30"><i class="fa-solid fa-layer-group text-indigo-400 text-lg"></i></div><div><p class="text-sm text-slate-400 font-medium">Views Mapeadas</p><p class="text-2xl font-bold text-white">{qtd_views}</p></div></div>
                <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-lg flex items-center gap-5"><div class="bg-amber-500/20 w-12 h-12 rounded-full flex items-center justify-center border border-amber-500/30"><i class="fa-solid fa-clock-rotate-left text-amber-400 text-lg"></i></div><div><p class="text-sm text-slate-400 font-medium">Última Consulta</p><p class="text-lg font-bold text-white">{last_call['time']}</p></div></div>
                <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-lg flex items-center gap-5"><div class="{status_color} w-12 h-12 rounded-full flex items-center justify-center border"><i class="fa-solid {status_icon} text-lg"></i></div><div><p class="text-sm text-slate-400 font-medium">Status HTTP (Última)</p><p class="text-2xl font-bold text-white">{status_code}</p></div></div>
            </div>
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div class="flex flex-col gap-6">
                    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-lg flex flex-col h-[300px]"><h2 class="text-lg font-semibold text-white mb-4 flex items-center gap-2"><i class="fa-solid fa-code text-blue-400"></i> SQL Executado (Input)</h2><div class="flex-1 bg-slate-950 rounded-xl border border-slate-800 p-4 overflow-y-auto"><pre class="text-xs text-blue-300"><code>{last_call['query_sql']}</code></pre></div></div>
                    <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-lg flex flex-col h-[200px]"><h2 class="text-lg font-semibold text-white mb-2 flex items-center gap-2"><i class="fa-solid fa-magnifying-glass-chart text-purple-400"></i> Tabelas Consultadas</h2><div class="flex flex-wrap gap-2">{badges_views}</div></div>
                </div>
                <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-lg flex flex-col h-[524px]"><div class="flex items-center justify-between mb-4"><h2 class="text-lg font-semibold text-white flex items-center gap-2"><i class="fa-solid fa-server text-emerald-400"></i> Resposta da API</h2></div><div class="flex-1 bg-slate-950 rounded-xl border border-slate-800 p-4 overflow-y-auto"><pre class="text-xs text-emerald-300"><code>{last_call['output']}</code></pre></div></div>
            </div>
        </div>

        <div id="content-history" class="hidden">
            <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 h-[700px]">
                <div class="lg:col-span-1 bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-col shadow-lg overflow-hidden"><div class="flex items-center justify-between mb-4 border-b border-slate-800 pb-2"><h2 class="text-md font-semibold text-white flex items-center gap-2"><i class="fa-solid fa-list text-blue-400"></i> Memória ({len(audit_log)})</h2></div><div class="overflow-y-auto flex-1 pr-2 space-y-3" id="auditList"></div></div>
                <div class="lg:col-span-2 bg-slate-900 border border-slate-800 rounded-2xl p-6 flex flex-col shadow-lg overflow-hidden"><div id="historyPlaceholder" class="flex-1 flex flex-col items-center justify-center text-slate-500"><i class="fa-solid fa-hand-pointer text-4xl mb-4"></i><p>Clique numa requisição para ver detalhes.</p></div><div id="historyDetails" class="hidden flex-col h-full"><div class="flex justify-between items-start mb-4"><div><h2 class="text-xl font-bold text-white mb-1" id="histTime"></h2><div class="flex items-center gap-2 mt-2" id="histViews"></div></div><span id="histStatus"></span></div><div class="grid grid-rows-2 gap-4 flex-1 min-h-0"><div class="flex flex-col min-h-0"><span class="text-xs uppercase text-slate-400 mb-2 font-semibold">Query Executada</span><div class="flex-1 bg-slate-950 border border-slate-800 rounded-lg p-3 overflow-y-auto"><pre class="text-xs text-blue-300 whitespace-pre-wrap"><code id="histSql"></code></pre></div></div><div class="flex flex-col min-h-0"><span class="text-xs uppercase text-slate-400 mb-2 font-semibold">Resposta (Output)</span><div class="flex-1 bg-slate-950 border border-slate-800 rounded-lg p-3 overflow-y-auto"><pre class="text-xs text-emerald-300 whitespace-pre-wrap"><code id="histOutput"></code></pre></div></div></div></div></div>
            </div>
        </div>

        <div id="content-dictionary" class="hidden">
             <div class="grid grid-cols-1 lg:grid-cols-4 gap-6 h-[700px]">
                <div class="lg:col-span-1 bg-slate-900 border border-slate-800 rounded-2xl p-4 flex flex-col shadow-lg overflow-hidden"><h2 class="text-md font-semibold text-white mb-4 border-b border-slate-800 pb-2">Selecione a Tabela</h2><div class="overflow-y-auto flex-1 pr-2 space-y-2" id="tableSelectorList"></div></div>
                <div class="lg:col-span-3 bg-slate-900 border border-slate-800 rounded-2xl p-6 flex flex-col shadow-lg overflow-hidden"><div id="tableDetailsPlaceholder" class="flex-1 flex flex-col items-center justify-center text-slate-500"><i class="fa-solid fa-hand-pointer text-4xl mb-4"></i><p>Clique numa tabela ao lado.</p></div><div id="tableDetailsContent" class="hidden flex-col h-full"><h2 id="detailTableName" class="text-2xl font-bold text-blue-400 font-mono mb-6"></h2><div class="grid grid-cols-1 md:grid-cols-2 gap-8 flex-1 overflow-y-auto pr-4"><div><h3 class="text-sm uppercase tracking-wider text-slate-400 font-semibold mb-3 border-b border-slate-800 pb-2 flex items-center gap-2"><i class="fa-solid fa-columns"></i> Colunas</h3><div class="bg-slate-950 rounded-lg border border-slate-800 p-2" id="columnsList"></div></div><div><h3 class="text-sm uppercase tracking-wider text-slate-400 font-semibold mb-3 border-b border-slate-800 pb-2 flex items-center gap-2"><i class="fa-solid fa-link"></i> Joins Possíveis</h3><div class="space-y-4" id="relationsList"></div></div></div></div></div>
            </div>
        </div>

        <div id="content-terminal" class="hidden h-[700px] flex-col shadow-2xl">
            <div class="bg-slate-800 flex items-center justify-between p-3 rounded-t-xl border border-slate-700 border-b-0">
                <div class="flex gap-2 px-2"><div class="w-3.5 h-3.5 rounded-full bg-red-500 border border-red-600"></div><div class="w-3.5 h-3.5 rounded-full bg-yellow-500 border border-yellow-600"></div><div class="w-3.5 h-3.5 rounded-full bg-green-500 border border-green-600"></div></div>
                <span class="text-xs text-slate-400 font-mono flex items-center gap-2"><i class="fa-solid fa-server"></i> root@duckdb-api:~</span>
                <div class="px-4 flex items-center gap-2"><span class="text-[10px] text-green-400 font-mono uppercase tracking-wider">Live Polling</span><span class="relative flex h-2.5 w-2.5"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span><span class="relative inline-flex rounded-full h-2.5 w-2.5 bg-green-500"></span></span></div>
            </div>
            <div id="terminal-screen" class="flex-1 bg-[#0d1117] p-4 border border-slate-700 rounded-b-xl overflow-y-auto shadow-inner"></div>
        </div>

        <script>
            const schemaData = {schema_json};
            const relationsData = {relations_json};
            const auditData = {audit_log_json};

            function switchTab(tabName) {{
                ['dashboard', 'history', 'dictionary', 'terminal'].forEach(t => {{
                    document.getElementById('content-' + t).classList.add('hidden');
                    document.getElementById('content-' + t).classList.remove('flex');
                    let classStr = 'tab-inactive pb-3 text-sm font-semibold transition-colors flex items-center gap-2 whitespace-nowrap';
                    if (t === 'terminal') classStr += ' text-emerald-500/50 hover:text-emerald-400';
                    document.getElementById('tab-' + t).className = classStr;
                }});
                
                const activeTab = document.getElementById('content-' + tabName);
                activeTab.classList.remove('hidden');
                if(tabName === 'terminal') activeTab.classList.add('flex');
                
                let activeClassStr = 'tab-active pb-3 text-sm font-semibold transition-colors flex items-center gap-2 whitespace-nowrap';
                if (tabName === 'terminal') activeClassStr = 'pb-3 text-sm font-semibold transition-colors flex items-center gap-2 whitespace-nowrap border-b-2 border-emerald-500 text-emerald-400';
                document.getElementById('tab-' + tabName).className = activeClassStr;
                
                if(tabName === 'terminal') scrollTerminalToBottom();
            }}

            let isTerminalScrolledUp = false;
            const termScreen = document.getElementById('terminal-screen');
            
            termScreen.addEventListener('scroll', () => {{
                const isAtBottom = termScreen.scrollHeight - termScreen.scrollTop <= termScreen.clientHeight + 15;
                isTerminalScrolledUp = !isAtBottom;
            }});

            function scrollTerminalToBottom() {{
                termScreen.scrollTop = termScreen.scrollHeight;
            }}

            async function fetchTerminalLogs() {{
                if(document.getElementById('content-terminal').classList.contains('hidden')) return;
                try {{
                    const res = await fetch('/api/logs');
                    const data = await res.json();
                    let html = '';
                    data.logs.forEach(line => {{
                        let colorClass = 'text-slate-300';
                        if(line.includes('[ERROR]')) colorClass = 'text-red-400 font-bold';
                        else if(line.includes('[SUCCESS]')) colorClass = 'text-emerald-400';
                        else if(line.includes('[DEBUG]')) colorClass = 'text-purple-400';
                        else if(line.includes('[WORK]')) colorClass = 'text-amber-300';
                        
                        let formattedLine = line
                            .replace('[INFO]', '<span class="text-blue-400">INFO</span> ')
                            .replace('[ERROR]', '<span class="bg-red-500 text-white px-1 rounded text-[10px]">ERROR</span>')
                            .replace('[SUCCESS]', '<span class="text-emerald-500">SUCC</span> ')
                            .replace('[DEBUG]', '<span class="text-purple-400">DBUG</span> ')
                            .replace('[WORK]', '<span class="text-amber-400">EXEC</span> ');

                        html += `<div class="${{colorClass}} text-[13px] terminal-text leading-relaxed hover:bg-slate-800/30 px-2 py-0.5 rounded">${{formattedLine}}</div>`;
                    }});
                    termScreen.innerHTML = html;
                    if(!isTerminalScrolledUp) scrollTerminalToBottom();
                }} catch(e) {{
                    console.error("Erro ao puxar logs:", e);
                }}
            }}
            setInterval(fetchTerminalLogs, 1500);

            const auditList = document.getElementById('auditList');
            if (auditData.length === 0) {{
                auditList.innerHTML = '<p class="text-sm text-slate-500 italic text-center mt-10">Nenhuma query executada ainda.</p>';
            }} else {{
                auditData.forEach(call => {{
                    const isSuccess = call.status === 200;
                    const statusDot = isSuccess ? 'bg-green-500' : 'bg-red-500';
                    const sqlPreview = call.query_sql.length > 50 ? call.query_sql.substring(0, 50) + '...' : call.query_sql;
                    
                    const card = document.createElement('div');
                    card.className = "p-3 rounded-lg border border-slate-800 bg-slate-950 hover:border-blue-500 cursor-pointer transition-colors";
                    card.onclick = () => showHistoryDetails(call.id);
                    
                    card.innerHTML = `
                        <div class="flex justify-between items-center mb-1"><span class="text-xs text-slate-400"><i class="fa-regular fa-clock"></i> ${{call.time.split(' ')[1]}}</span><div class="w-2 h-2 rounded-full ${{statusDot}}"></div></div>
                        <div class="text-xs text-blue-300 font-mono truncate">${{sqlPreview}}</div>
                        <div class="mt-2 flex flex-wrap gap-1">
                            ${{call.views_used.slice(0, 2).map(v => `<span class="text-[9px] bg-slate-800 text-slate-300 px-1 rounded">${{v}}</span>`).join('')}}
                            ${{call.views_used.length > 2 ? `<span class="text-[9px] text-slate-500">...</span>` : ''}}
                        </div>
                    `;
                    auditList.appendChild(card);
                }});
            }}

            function showHistoryDetails(id) {{
                const call = auditData.find(c => c.id === id);
                if(!call) return;
                document.getElementById('historyPlaceholder').classList.add('hidden');
                document.getElementById('historyDetails').classList.remove('hidden');
                document.getElementById('historyDetails').classList.add('flex');
                document.getElementById('histTime').innerText = call.time;
                document.getElementById('histSql').innerText = call.query_sql;
                document.getElementById('histOutput').innerText = call.output;
                
                const stBadge = document.getElementById('histStatus');
                stBadge.innerText = 'HTTP ' + call.status;
                stBadge.className = call.status === 200 ? 'px-3 py-1 rounded font-bold text-sm bg-green-500/20 text-green-400 border border-green-500/30' : 'px-3 py-1 rounded font-bold text-sm bg-red-500/20 text-red-400 border border-red-500/30';
                
                const viewsContainer = document.getElementById('histViews');
                viewsContainer.innerHTML = call.views_used.length > 0 ? 
                    call.views_used.map(v => `<span class="bg-indigo-500/20 text-indigo-300 border border-indigo-500/30 text-[10px] px-2 py-0.5 rounded font-mono">${{v}}</span>`).join('') : 
                    '<span class="text-xs text-slate-500 italic">Nenhuma view identificada</span>';
            }}

            const tableSelectorList = document.getElementById('tableSelectorList');
            Object.keys(schemaData).sort().forEach(tableName => {{
                const btn = document.createElement('button');
                btn.className = "w-full text-left p-3 rounded-lg text-sm font-mono text-slate-300 hover:bg-slate-800 hover:text-blue-400 border border-transparent hover:border-slate-700 transition-all truncate";
                btn.innerHTML = `<i class="fa-solid fa-table text-slate-500 mr-2"></i> ${{tableName}}`;
                btn.onclick = () => showTableDetails(tableName);
                tableSelectorList.appendChild(btn);
            }});

            function showTableDetails(tableName) {{
                document.getElementById('tableDetailsPlaceholder').classList.add('hidden');
                const content = document.getElementById('tableDetailsContent');
                content.classList.remove('hidden');
                content.classList.add('flex');
                document.getElementById('detailTableName').innerText = tableName;

                const colsList = document.getElementById('columnsList');
                colsList.innerHTML = '';
                schemaData[tableName].forEach(col => {{
                    colsList.innerHTML += `<div class="flex justify-between items-center p-2 border-b border-slate-800/50 hover:bg-slate-900/50"><span class="text-sm text-emerald-300 font-mono">${{col.name}}</span><span class="text-[10px] bg-slate-800 text-slate-400 px-2 py-1 rounded uppercase">${{col.type}}</span></div>`;
                }});

                const relsList = document.getElementById('relationsList');
                relsList.innerHTML = '';
                const tableRels = relationsData[tableName];
                
                if (!tableRels || Object.keys(tableRels).length === 0) {{
                    relsList.innerHTML = `<p class="text-sm text-slate-500 italic p-4 bg-slate-950 rounded-lg border border-slate-800">Nenhum relacionamento encontrado.</p>`;
                    return;
                }}

                Object.keys(tableRels).sort().forEach(relatedTable => {{
                    const keysHtml = tableRels[relatedTable].map(k => `<span class="bg-indigo-500/20 text-indigo-300 border border-indigo-500/30 text-xs px-2 py-0.5 rounded font-mono mr-1">${{k}}</span>`).join('');
                    relsList.innerHTML += `<div class="bg-slate-950 p-3 rounded-lg border border-slate-800"><div class="text-sm font-semibold text-slate-200 mb-2 font-mono flex items-center gap-2"><i class="fa-solid fa-arrow-right-arrow-left text-slate-500"></i> ${{relatedTable}}</div><div class="flex flex-wrap gap-1">${{keysHtml}}</div></div>`;
                }});
            }}
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)