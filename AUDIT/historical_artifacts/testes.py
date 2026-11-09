import duckdb
from pathlib import Path

def scan_raw_csvs(output_file="relatorio_auditoria_bruto.txt"):
    print(f"🚀 INICIANDO O SCANNER BRUTO... O resultado será salvo em '{output_file}'\n")
    
    # Cria o banco em memória
    con = duckdb.connect(':memory:')
    
    # Exatamente os caminhos do seu ambiente
    pastas = [
        Path('dados\database'),
        Path('dados\staging_cgu')
    ]
    
    # Abre o arquivo de texto para escrita
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# RELATÓRIO DE PERFILAMENTO DE DADOS (CSVs BRUTOS)\n\n")

        tabelas_criadas = []

        # 1. CARREGAR CADA CSV INDIVIDUALMENTE COM NOME SEGURO
        for pasta in pastas:
            if not pasta.exists():
                print(f"⚠️ Pasta não encontrada: {pasta}")
                f.write(f"⚠️ Pasta não encontrada: {pasta}\n")
                continue
                
            print(f"📂 Lendo arquivos na pasta: {pasta}")
            
            # Pega todos os arquivos CSV
            for arquivo in pasta.glob('*.csv'):
                nome_original = arquivo.stem
                
                # O SEGREDO AQUI: Colocamos 'csv_' na frente para nunca começar com número (ex: 2025_12)
                # E tiramos traços e espaços para o DuckDB não chorar.
                nome_seguro = "csv_" + nome_original.replace('.', '_').replace('-', '_').replace(' ', '_').lower()
                
                try:
                    # Carrega o CSV forçando o delimitador | que a sua API usa
                    con.execute(f"CREATE VIEW {nome_seguro} AS SELECT * FROM read_csv_auto('{arquivo.as_posix()}', delim='|', header=True, ignore_errors=true)")
                    tabelas_criadas.append(nome_seguro)
                except Exception as e:
                    print(f"  ❌ Erro ao ler {arquivo.name}: {e}")

        if not tabelas_criadas:
            print("❌ Nenhum CSV foi carregado com sucesso.")
            return

        print(f"⏳ Analisando {len(tabelas_criadas)} arquivos CSV. Aguarde, isso leva alguns segundos...")

        # 2. O PENTE FINO DA AUDITORIA
        for tabela in tabelas_criadas:
            # Tira o 'csv_' só pra ficar bonito no relatório
            f.write(f"## ARQUIVO: {tabela.replace('csv_', '')}.csv\n")
            
            try:
                total_linhas = con.execute(f"SELECT COUNT(*) FROM {tabela}").fetchone()[0]
                f.write(f"- Total de Registros: {total_linhas}\n")
                
                if total_linhas == 0:
                    f.write("- STATUS: Arquivo Vazio\n\n")
                    continue

                colunas_info = con.execute(f"DESCRIBE {tabela}").fetchall()
                
                datas_suspeitas = []
                status_suspeitos = []
                lixos_encontrados = []

                for col in colunas_info:
                    nome_col = col[0]
                    tipo_col = col[1]

                    # 3.1 Datas disfarçadas (Caça aos BIGINTs que deveriam ser DATE)
                    if any(termo in nome_col.lower() for termo in ['dat', 'mes', 'ano', 'referencia']) and tipo_col not in ['DATE', 'TIMESTAMP']:
                        try:
                            amostra = con.execute(f"SELECT {nome_col} FROM {tabela} WHERE {nome_col} IS NOT NULL LIMIT 3").fetchall()
                            amostra_vals = [str(a[0]) for a in amostra] if amostra else []
                            if amostra_vals:
                                datas_suspeitas.append(f"Coluna '{nome_col}' (Lida como {tipo_col}). Ex: {amostra_vals}")
                        except:
                            pass

                    # 3.2 Dicionário de Status (Caça aos ENUMs)
                    if any(termo in nome_col.lower() for termo in ['situacao', 'status', 'tipo', 'dsc_sit', 'modalidade', 'enquadramento']):
                        try:
                            status_freq = con.execute(f"""
                                SELECT {nome_col}, COUNT(*) as qtd 
                                FROM {tabela} 
                                GROUP BY {nome_col} 
                                ORDER BY qtd DESC LIMIT 10
                            """).fetchall()
                            status_suspeitos.append((nome_col, status_freq))
                        except:
                            pass

                    # 3.3 Lixo de Cadastro (Caça ao 'NÃO INFORMADO')
                    if tipo_col == 'VARCHAR':
                        try:
                            query_lixo = f"""
                                SELECT COUNT(*) FROM {tabela} 
                                WHERE {nome_col} ILIKE '%NAO INFORMADO%' 
                                   OR {nome_col} ILIKE '%DIVERSOS%' 
                                   OR {nome_col} = '-1'
                            """
                            qtd_lixo = con.execute(query_lixo).fetchone()[0]
                            if qtd_lixo > 0:
                                perc = (qtd_lixo / total_linhas) * 100
                                lixos_encontrados.append(f"Coluna '{nome_col}': {qtd_lixo} registros sujos ({perc:.2f}%)")
                        except:
                            pass

                # Escreve os resultados no TXT
                if datas_suspeitas:
                    f.write("### Alerta de Datas (Necessita CAST no SQL):\n")
                    for d in datas_suspeitas:
                        f.write(f"- {d}\n")

                if status_suspeitos:
                    f.write("### Dicionário de Status (Valores Exatos):\n")
                    for status in status_suspeitos:
                        col_name, freq = status
                        f.write(f"- Coluna '{col_name}':\n")
                        for f_val in freq:
                            f.write(f"  * `{f_val[0]}` (Qtd: {f_val[1]})\n")

                if lixos_encontrados:
                    f.write("### Sujeira de Cadastro (Exige filtro WHERE):\n")
                    for lixo in lixos_encontrados:
                        f.write(f"- {lixo}\n")

            except Exception as e:
                f.write(f"- ERRO AO ANALISAR ARQUIVO: {e}\n")
            
            f.write("\n---\n\n")

    print(f"✅ CONCLUÍDO! Abra o arquivo '{output_file}' para ver o resultado.")

if __name__ == "__main__":
    scan_raw_csvs()