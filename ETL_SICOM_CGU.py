import os
import io
import csv
import time
import random
import zipfile
import requests
import urllib3
import pandas as pd
from pathlib import Path

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# CONFIGURAÇÕES GERAIS E CAMINHOS
# ==========================================
DIRETORIO_RAIZ = Path(r'D:\tiokk-n8n')
PASTA_CGU = DIRETORIO_RAIZ / r'dados\staging_cgu'
PASTA_DATABASE = DIRETORIO_RAIZ / r'dados\database'
BASE_ORIGEM_SICOM = DIRETORIO_RAIZ / r'dados\SICOM'
ARQUIVO_DDL = DIRETORIO_RAIZ / 'novo_dicionario_sicom_cgu_n8n.txt'

PASTA_CGU.mkdir(parents=True, exist_ok=True)
PASTA_DATABASE.mkdir(parents=True, exist_ok=True)


# ==========================================
# FASE 1: EXTRAÇÃO CGU (SNIPER)
# ==========================================
def fase_extracao_cgu():
    print("\n" + "="*50)
    print("🚀 FASE 1: INICIANDO EXTRAÇÃO CGU (PORTAL DA TRANSPARÊNCIA)")
    print("="*50)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive'
    }

    anos = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
    meses = [str(m).zfill(2) for m in range(1, 13)]

    bases_alvo = {
        "transferencias": {"col_municipio": "NOME MUNICÍPIO", "col_uf": "UF", "col_valor": "VALOR TRANSFERIDO", "manter": ["ANO / MÊS", "TIPO TRANSFERÊNCIA", "NOME PROGRAMA", "NOME AÇÃO", "CÓDIGO FAVORECIDO", "NOME FAVORECIDO", "VALOR TRANSFERIDO"]},
        "auxilio-emergencial": {"col_municipio": "NOME MUNICÍPIO", "col_uf": "UF", "col_valor": "VALOR BENEFÍCIO", "manter": ["MÊS DISPONIBILIZAÇÃO", "NIS BENEFICIÁRIO", "CPF BENEFICIÁRIO", "NOME BENEFICIÁRIO", "ENQUADRAMENTO", "VALOR BENEFÍCIO"]},
        "bolsa-familia-pagamentos": {"col_municipio": "NOME MUNICÍPIO", "col_uf": "UF", "col_valor": "VALOR PARCELA", "manter": ["MÊS REFERÊNCIA", "NIS FAVORECIDO", "CPF FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"]},
        "novo-bolsa-familia": {"col_municipio": "NOME MUNICÍPIO", "col_uf": "UF", "col_valor": "VALOR PARCELA", "manter": ["MÊS REFERÊNCIA", "NIS FAVORECIDO", "CPF FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"]},
        "auxilio-brasil": {"col_municipio": "NOME MUNICÍPIO", "col_uf": "UF", "col_valor": "VALOR PARCELA", "manter": ["MÊS REFERÊNCIA", "NIS FAVORECIDO", "CPF FAVORECIDO", "NOME FAVORECIDO", "VALOR PARCELA"]},
        "bpc": {"col_municipio": "NOME MUNICÍPIO", "col_uf": "UF", "col_valor": "VALOR PARCELA", "manter": ["MÊS REFERÊNCIA", "NIS BENEFICIÁRIO", "CPF BENEFICIÁRIO", "NOME BENEFICIÁRIO", "VALOR PARCELA"]},
        "pe-de-meia": {"col_municipio": "NOME MUNICÍPIO", "col_uf": "UF", "col_valor": "VALOR PARCELA", "manter": ["MÊS REFERÊNCIA", "NIS BENEFICIÁRIO", "CPF BENEFICIÁRIO", "NOME BENEFICIÁRIO", "ETAPA ENSINO", "VALOR PARCELA"]},
        "despesas-favorecidos": {"col_municipio": "Nome Município", "col_uf": "Sigla UF", "col_valor": "Valor Recebido", "manter": ["Ano e mês do lançament", "Código Favorecido", "Nome Favorecido", "Nome Órgão", "Valor Recebido"]}
    }

    session = requests.Session()
    session.headers.update(headers)
    total_requisicoes = len(bases_alvo) * len(anos) * len(meses)
    feitas = 0

    for categoria, config in bases_alvo.items():
        for ano in anos:
            for mes in meses:
                feitas += 1
                caminho_salvar = PASTA_CGU / f"{ano}_{mes}_{categoria}_cgu.csv"
                
                if caminho_salvar.exists():
                    print(f"  ⏭️ {categoria} ({mes}/{ano}): Já existe (Pulando).", end="\r")
                    continue
                
                url_zip = f"https://portaldatransparencia.gov.br/download-de-dados/{categoria}/{ano}{mes}"
                time.sleep(random.uniform(2.0, 5.0))
                
                try:
                    response = session.get(url_zip, timeout=60, verify=False)
                    if response.status_code == 200:
                        try:
                            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                                csvs = [f for f in z.namelist() if f.endswith('.csv')]
                                if not csvs: continue
                                with z.open(csvs[0]) as f:
                                    chunks = pd.read_csv(f, sep=';', encoding='latin-1', dtype=str, chunksize=100000)
                                    df_caete = pd.DataFrame()
                                    for chunk in chunks:
                                        col_mun = config["col_municipio"] if config["col_municipio"] in chunk.columns else [c for c in chunk.columns if "munic" in c.lower()][0]
                                        col_uf = config["col_uf"] if config["col_uf"] in chunk.columns else [c for c in chunk.columns if "uf" in c.lower()][0]
                                        mun_norm = chunk[col_mun].astype(str).str.strip().str.upper().str.replace('É', 'E')
                                        uf_norm = chunk[col_uf].astype(str).str.strip().str.upper()
                                        filtrado = chunk[(mun_norm == 'CAETE') & (uf_norm == 'MG')].copy()
                                        
                                        if not filtrado.empty:
                                            filtrado = filtrado[[c for c in config["manter"] if c in filtrado.columns]]
                                            if config["col_valor"] in filtrado.columns:
                                                filtrado[config["col_valor"]] = filtrado[config["col_valor"]].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                                            df_caete = pd.concat([df_caete, filtrado], ignore_index=True)
                                            
                                    if not df_caete.empty:
                                        df_caete.to_csv(caminho_salvar, sep='|', index=False, encoding='utf-8-sig')
                                        print(f"\n  ✅ {categoria} ({mes}/{ano}): Salvo!")
                        except zipfile.BadZipFile:
                            pass
                    elif response.status_code == 403:
                        time.sleep(10)
                except Exception:
                    pass
    print("\n✅ FASE 1 CONCLUÍDA: CGU Extraída.")


# ==========================================
# FASE 2: LIMPEZA SICOM (VACINA)
# ==========================================
def limpar_numero_sicom(valor):
    if not valor: return ""
    v = str(valor).strip().replace('R$', '').replace('"', '').replace("'", "")
    if '.' in v and ',' in v: v = v.replace('.', '').replace(',', '.')
    elif ',' in v: v = v.replace(',', '.')
    try: return float(v)
    except ValueError: return valor 

def fase_limpeza_sicom():
    print("\n" + "="*50)
    print("🏥 FASE 2: INICIANDO HOSPITAL DE DADOS SICOM")
    print("="*50)
    
    if not BASE_ORIGEM_SICOM.exists(): 
        return print(f"❌ Pasta origem não encontrada: {BASE_ORIGEM_SICOM}")
    
    arquivos_limpos = {f.name for f in PASTA_DATABASE.glob('*.csv')}
    fila = [f for f in BASE_ORIGEM_SICOM.rglob('*.csv') if f.name not in arquivos_limpos]
    
    print(f"📦 Arquivos para limpar: {len(fila)}")

    for arquivo in fila:
        caminho_destino = PASTA_DATABASE / arquivo.name
        
        # O BLOCO TRY...EXCEPT RESTAURADO AQUI PARA BLINDAR O LOOP!
        try:
            enc = 'utf-8'
            try:
                with open(arquivo, 'r', encoding='utf-8') as f: f.read(1024)
            except UnicodeDecodeError:
                enc = 'latin1'

            with open(arquivo, 'r', encoding=enc) as f:
                linhas_raw = f.readlines()
                
            if not linhas_raw: continue
                
            leitor_cabecalho = csv.reader([linhas_raw[0]], delimiter=';', quotechar='"')
            cabecalho = [str(col).strip().lower() for col in next(leitor_cabecalho)]
            
            if 'movimentacaoRsp' in arquivo.name:
                cabecalho = ["seq_mov_rsp", "seq_rsp", "seq_orgao", "cod_orgao", "num_ano_referencia", "num_mes_referencia", "cod_unidade", "cod_subunidade", "num_empenho", "dat_empenho", "num_ano_empenho", "dsc_dotacao", "dsc_tipo_rsp", "dsc_tipo_movimentacao", "dat_movimentacao", "vlr_movimentacao", "num_documento", "dat_documento", "dsc_historico"]
            elif 'remessaMunicipio' in arquivo.name:
                cabecalho.append("status_remessa")

            qtd_oficial = len(cabecalho)
            cols_numericas = [i for i, c in enumerate(cabecalho) if c.startswith(('vlr_', 'num_quant_', 'perc_'))]
            dados_limpos = []

            for texto_linha in linhas_raw[1:]:
                valores = next(csv.reader([texto_linha.strip('\n').strip('\r')], delimiter=';', quotechar='"'))
                qtd_atual = len(valores)
                
                if qtd_atual > qtd_oficial: valores = valores[:qtd_oficial]
                if qtd_atual < qtd_oficial: valores.extend([''] * (qtd_oficial - qtd_atual))

                linha_final = []
                for i, val in enumerate(valores):
                    v_str = str(val).replace('\n', ' ').replace('\r', ' ').strip()
                    if i in cols_numericas and v_str:
                        v_str = limpar_numero_sicom(v_str)
                    linha_final.append(v_str)
                dados_limpos.append(linha_final)

            df = pd.DataFrame(dados_limpos, columns=cabecalho)
            df.to_csv(caminho_destino, sep='|', index=False, encoding='utf-8')
            print(f"  ⚙️ Sanitizado: {arquivo.name}")
            
        except Exception as e:
            print(f"  ❌ ERRO Crítico no arquivo {arquivo.name}: {e}")

    print("✅ FASE 2 CONCLUÍDA: SICOM Vacinado.")


# ==========================================
# FASE 3: GERAÇÃO DO DDL (MAPA DE DADOS)
# ==========================================
def mapear_tabelas(pasta, prefixo_tipo='sicom'):
    tabelas = {}
    if not pasta.exists(): return tabelas

    for arquivo in pasta.glob('*.csv'):
        if os.path.getsize(arquivo) < 50: continue
            
        if prefixo_tipo == 'sicom':
            partes = arquivo.name.replace('.csv', '').split('.')
            ano = int(partes[0])
            nome_base = "_".join(partes[2:]) if (len(partes) > 1 and partes[1].isdigit()) else "_".join(partes[1:])
            nome_tabela = f"sicom_{nome_base.replace('-', '_')}" if nome_base[0].isdigit() else nome_base.replace('.', '_').replace('-', '_')
        else:
            partes = arquivo.name.split('_')
            if len(partes) < 3: continue
            ano = int(partes[0])
            nome_tabela = f"cgu_{partes[2].replace('-', '_')}"

        with open(arquivo, 'r', encoding='utf-8') as f:
            qtd_cols = len(f.readline().split('|'))

        if nome_tabela not in tabelas:
            tabelas[nome_tabela] = {'ano': ano, 'caminho': arquivo, 'cols': qtd_cols}
        else:
            atual = tabelas[nome_tabela]
            if qtd_cols > atual['cols'] or (qtd_cols == atual['cols'] and ano > atual['ano']):
                tabelas[nome_tabela] = {'ano': ano, 'caminho': arquivo, 'cols': qtd_cols}
    return tabelas

def fase_gerar_ddl():
    print("\n" + "="*50)
    print("🧠 FASE 3: GERANDO DDL (TEXT-TO-SQL)")
    print("="*50)
    
    tabelas_mapeadas = {}
    tabelas_mapeadas.update(mapear_tabelas(PASTA_DATABASE, 'sicom'))
    tabelas_mapeadas.update(mapear_tabelas(PASTA_CGU, 'cgu'))

    linhas_ddl = [
        "-- ESQUEMA DE DADOS SICOM E CGU (DUCKDB) PARA GERAÇÃO DE TEXT-TO-SQL",
        "-- ATENÇÃO: Todas as tabelas possuem a coluna virtual 'ano_referencia' gerada pelo banco.\n"
    ]

    for nome_tabela, info in sorted(tabelas_mapeadas.items()):
        try: df = pd.read_csv(info['caminho'], sep='|', nrows=2000, dtype=str)
        except Exception: continue
            
        linhas_ddl.append(f"CREATE TABLE {nome_tabela} (")
        linhas_ddl.append("    ano_referencia INTEGER, -- [CHAVE TEMPORAL] Filtre por esta coluna para buscar anos específicos.")
        
        for col in df.columns:
            col_limpa = col.strip()
            if col_limpa.lower() in ['cod_municipio', 'nom_municipio']: continue
            col_upper = col_limpa.upper()
            col_sql = f'"{col_limpa}"' if ' ' in col_limpa else col_limpa
            
            if any(x in col_upper for x in ['FAVORECIDO', 'BENEFICIÁRIO', 'DOC', 'CNPJ', 'CPF', 'NIS', 'NOME', 'NOM_']):
                tipo = "VARCHAR"
            elif col_limpa.startswith(('vlr_', 'perc_')) or 'VALOR' in col_upper:
                tipo = "DECIMAL"
            elif col_limpa.startswith('dat_'):
                tipo = "DATE"
            elif col_limpa.startswith(('seq_', 'cod_', 'num_')) or 'CÓDIGO' in col_upper:
                tipo = "INTEGER"
            else:
                tipo = "VARCHAR"
                
            amostras = [str(x).strip() for x in df[col].dropna().unique() if str(x).strip() != '']
            comentario = f" -- Ex: '{', '.join(amostras[:5])[:77]}...'" if amostras else ""
            linhas_ddl.append(f"    {col_sql} {tipo},{comentario}")
            
        linhas_ddl.append(");\n")

    with open(ARQUIVO_DDL, 'w', encoding='utf-8') as out:
        out.write("\n".join(linhas_ddl))
        
    print(f"✅ FASE 3 CONCLUÍDA: DDL de ouro salvo em {ARQUIVO_DDL.name}")

# ==========================================
# EXECUÇÃO DO PIPELINE ORQUESTRADO
# ==========================================
if __name__ == "__main__":
    start_time = time.time()
    
    # fase_extracao_cgu()
    # fase_limpeza_sicom()
    fase_gerar_ddl()
    
    end_time = time.time()
    print("\n" + "="*50)
    print(f"🎉 PIPELINE ETL COMPLETO EXECUTADO COM SUCESSO!")
    print(f"⏱️ Tempo total: {(end_time - start_time) / 60:.2f} minutos.")
    print("="*50)