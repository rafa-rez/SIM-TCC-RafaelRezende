import requests
import json
import time
from datetime import datetime

API_URL = "http://localhost:8000/query"

# ==============================================================================
# BATERIA DE TESTES V3.0 (STRESS TEST: QUERIES REAIS GERADAS PELO LLM)
# ==============================================================================
TESTES = [
    {
        "nome": "1. Calculo Fiscal (ISF-M com BETWEEN)",
        "objetivo": "Verificar se o DuckDB aceita a divisao de CTEs de 2020 a 2025.",
        "query": """WITH receita_corrente AS (SELECT ano_referencia, SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS total_receita_corrente FROM receita_receita WHERE dsc_cateconomica ILIKE '%Corrente%' GROUP BY ano_referencia), despesa_corrente AS (SELECT ano_referencia, SUM(CAST(vlr_liquidado AS DECIMAL)) AS total_despesa_corrente FROM despesa_despesa WHERE dsc_naturezadespesa LIKE '3%' GROUP BY ano_referencia) SELECT r.ano_referencia, r.total_receita_corrente / NULLIF(d.total_despesa_corrente, 0) AS isf_m FROM receita_corrente r JOIN despesa_corrente d ON r.ano_referencia = d.ano_referencia WHERE r.ano_referencia BETWEEN 2020 AND 2025;"""
    },
    {
        "nome": "2. Cruzamento Investigativo (Servidores Ativos x Bolsa Familia)",
        "objetivo": "Verificar se o TRIM(UPPER()) cruza as pessoas sem dar erro de memoria ou vazio.",
        "query": """WITH servidores AS (SELECT TRIM(UPPER(p.nom_pessoa)) AS nome_servidor, MAX(CAST(r.vlr_rmn_liquida AS DECIMAL)) AS salario_maximo FROM remuneracao_municipio r JOIN pessoa_municipio p ON r.seq_dim_pessoa = p.seq_dim_pessoa WHERE r.ano_referencia = 2024 AND r.dsc_sit_serv_pens ILIKE '%Ativo%' GROUP BY nome_servidor), beneficios AS (SELECT TRIM(UPPER(cgu."NOME FAVORECIDO")) AS nome_beneficiario, SUM(CAST(cgu."VALOR PARCELA" AS DECIMAL)) AS total_beneficio FROM cgu_novo_bolsa_familia cgu WHERE cgu.ano_referencia = 2024 GROUP BY nome_beneficiario) SELECT s.nome_servidor, s.salario_maximo, b.total_beneficio FROM servidores s JOIN beneficios b ON s.nome_servidor = b.nome_beneficiario;"""
    },
    {
        "nome": "3. Cruzamento Investigativo (Top 5 Empresas SICOM x CGU)",
        "objetivo": "Verificar se o JOIN pelo CNPJ em VARCHAR funciona entre pagamentos e transferencias.",
        "query": """WITH cte_pagamentos AS (SELECT dp.num_doc_credor, SUM(CAST(dp.vlr_pag_fonte AS DECIMAL)) AS total_pagamentos FROM despesa_pagamento dp WHERE dp.ano_referencia = 2025 AND COALESCE(dp.nom_credor, '') NOT ILIKE '%NAO INFORMADO%' AND dp.num_doc_credor NOT IN ('-1', '0') GROUP BY dp.num_doc_credor), cte_transferencias AS (SELECT ct."CÓDIGO FAVORECIDO" AS num_doc_credor, SUM(CAST(ct."VALOR TRANSFERIDO" AS DECIMAL)) AS total_transferencias FROM cgu_transferencias ct WHERE ct.ano_referencia = 2025 GROUP BY ct."CÓDIGO FAVORECIDO") SELECT p.num_doc_credor, p.total_pagamentos, t.total_transferencias FROM cte_pagamentos p JOIN cte_transferencias t ON CAST(p.num_doc_credor AS VARCHAR) = CAST(t.num_doc_credor AS VARCHAR) ORDER BY (p.total_pagamentos + t.total_transferencias) DESC LIMIT 5;"""
    },
    {
        "nome": "4. Regra de Cadastros (Frota sem regra de higiene)",
        "objetivo": "A query falha da IA: ver se ela agrupa marcas sujas do SICOM como 'DIVERSOS'.",
        "query": """SELECT f.dsc_marca, SUM(CAST(g.vlr_gasto AS DECIMAL)) AS total_gasto FROM frota_gastoFrota g JOIN frota_frota f ON g.seq_veiculo = f.seq_veiculo WHERE g.ano_referencia = 2025 AND (g.dsc_tipo_gasto ILIKE '%GASOLINA%' OR g.dsc_tipo_gasto ILIKE '%DIESEL%') GROUP BY f.dsc_marca;"""
    },
    {
        "nome": "5. Calculo Fiscal (IEQ-C e validacao IF/CASE)",
        "objetivo": "Verificar se a query com CROSS JOIN oculto (d, r) sem ON funciona e calcula a %. ",
        "query": """WITH despesa_mde AS (SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS total_despesa FROM educacao_educacao WHERE ano_referencia = 2023), receita_impostos AS (SELECT SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS total_receita FROM receita_receita WHERE ano_referencia = 2023 AND dsc_especie ILIKE '%Imposto%') SELECT (d.total_despesa / NULLIF(r.total_receita, 0)) * 100 AS indice_educacao, CASE WHEN (d.total_despesa / NULLIF(r.total_receita, 0)) * 100 >= 25 THEN 'Sim' ELSE 'Não' END AS atingiu_minimo_constitucional FROM despesa_mde d, receita_impostos r;"""
    },
    {
        "nome": "6. Regra de Orgaos (Restos a Pagar Prefeitura)",
        "objetivo": "Verificar se o filtro IN (2,4,5) extrai dados reais de cancelamento.",
        "query": """SELECT SUM(CAST(vlr_movimentacao AS DECIMAL)) AS total_cancelado FROM empenho_movimentacaoRsp WHERE dsc_tipo_movimentacao ILIKE '%CANCELAMENTO%' AND CAST(cod_orgao AS INTEGER) IN (2, 4, 5) AND ano_referencia = 2024;"""
    },
    {
        "nome": "7. Regra Anti-Duplicata (Licitações)",
        "objetivo": "Verificar se o GROUP BY em duas colunas resolve a duplicata de itens.",
        "query": """WITH licitacoes_2025 AS (SELECT h.seq_licitacao, h.nom_vencedor, SUM(CAST(h.vlr_global AS DECIMAL)) AS total_valor FROM licitacao_homologLicitacao h JOIN licitacao_licitacao l ON h.seq_licitacao = l.seq_licitacao AND h.ano_referencia = l.ano_referencia WHERE h.ano_referencia = 2025 AND h.vlr_global > 0 GROUP BY h.seq_licitacao, h.nom_vencedor) SELECT nom_vencedor, total_valor FROM licitacoes_2025 ORDER BY total_valor DESC LIMIT 10;"""
    },
    {
        "nome": "8. Regra de Cadastros (Contratos x Aditivos Anos Diferentes)",
        "objetivo": "Verificar se os LEFT JOINs múltiplos com anos separados não quebram o DuckDB.",
        "query": """SELECT c.num_contrato, cc.dsc_nome_credor, t.vlr_termo_aditivo FROM contrato_contratos c LEFT JOIN contrato_contContrato cc ON c.seq_contrato = cc.seq_contrato AND c.ano_referencia = cc.ano_referencia LEFT JOIN contrato_termoContrato t ON c.seq_contrato = t.seq_contrato AND t.ano_referencia = 2024 WHERE c.num_ano_contrato = 2023 AND t.dsc_tipo_alteracao ILIKE '%ACRÉSCIMO%';"""
    },
    {
        "nome": "9. Calculo Fiscal (Gastos da Câmara IEV-C)",
        "objetivo": "Garantir que isolar cod_orgao = 1 funciona para a despesa_despesa.",
        "query": """WITH receita_corrente AS (SELECT SUM(CAST(vlr_realizadoateperiodo AS DECIMAL)) AS total_receita_corrente FROM receita_receita WHERE dsc_cateconomica ILIKE '%Corrente%' AND ano_referencia = 2025), despesas_camara AS (SELECT SUM(CAST(vlr_liquidado AS DECIMAL)) AS total_despesas_camara FROM despesa_despesa WHERE cod_orgao = 1 AND ano_referencia = 2025) SELECT (despesas_camara.total_despesas_camara / NULLIF(receita_corrente.total_receita_corrente, 0)) * 100 AS porcentagem_gasto_camara FROM receita_corrente, despesas_camara;"""
    },
    {
        "nome": "10. Manipulacao de Datas (Mês do RH)",
        "objetivo": "Verificar se o casting bruto de string e substr(5,2) extrai corretamente os meses 1 a 12.",
        "query": """SELECT CAST(SUBSTR(CAST(seq_dat_pagamento AS VARCHAR), 5, 2) AS INTEGER) AS mes, SUM(CAST(vlr_rmn_liquida AS DECIMAL)) AS total_pago FROM remuneracao_municipio WHERE ano_referencia = 2024 GROUP BY mes ORDER BY mes;"""
    }
]

# ==============================================================================
# MOTOR DE EXECUÇÃO
# ==============================================================================
def executar_bateria():
    print(f"Iniciando RAIO-X do SICOM e CGU em {API_URL}...")
    print("=" * 70)
    
    relatorio = {
        "data_execucao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "estatisticas": {"total": len(TESTES), "sucessos": 0, "falhas": 0, "vazias": 0},
        "resultados": []
    }

    for i, teste in enumerate(TESTES, 1):
        print(f"\n[{i}/{len(TESTES)}] {teste['nome']}")
        
        payload = {"query": teste['query'].strip().replace("\n", " ")}
        inicio = time.time()
        
        try:
            resposta = requests.post(API_URL, json=payload, timeout=30)
            tempo_exec = (time.time() - inicio) * 1000
            dados_resposta = resposta.json()
            status_http = resposta.status_code
            
            resultado_teste = {
                "nome_teste": teste["nome"],
                "objetivo": teste["objetivo"],
                "status_http": status_http,
                "tempo_ms": round(tempo_exec, 2),
                "query": payload["query"],
                "dados_retornados": None,
                "erro": None
            }
            
            if status_http == 200:
                linhas = dados_resposta.get('data', [])
                if len(linhas) > 0:
                    print(f"  [+] SUCESSO ({tempo_exec:.0f}ms) | {len(linhas)} linhas retornadas.")
                    relatorio["estatisticas"]["sucessos"] += 1
                    resultado_teste["dados_retornados"] = linhas
                else:
                    print(f"  [!] VAZIO ({tempo_exec:.0f}ms) | Query rodou sem erro, mas retornou 0 linhas.")
                    relatorio["estatisticas"]["vazias"] += 1
                    resultado_teste["dados_retornados"] = []
            else:
                erro_msg = dados_resposta.get('erro_tecnico', 'Erro desconhecido')
                print(f"  [-] FALHA SQL {status_http} ({tempo_exec:.0f}ms) | {erro_msg.split(chr(10))[0]}")
                relatorio["estatisticas"]["falhas"] += 1
                resultado_teste["erro"] = dados_resposta
                
            relatorio["resultados"].append(resultado_teste)
            
        except Exception as e:
            print(f"  [X] ERRO DE CONEXÃO DA API: {e}")
            relatorio["estatisticas"]["falhas"] += 1
            relatorio["resultados"].append({
                "nome_teste": teste["nome"],
                "status_http": 500,
                "erro": str(e)
            })

    arquivo_saida = f"relatorio_raiox_sicom_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(arquivo_saida, 'w', encoding='utf-8') as f:
        json.dump(relatorio, f, indent=4, ensure_ascii=False)
        
    print("\n" + "=" * 70)
    print(f"BATERIA FINALIZADA!")
    print(f"✅ Sucessos com dados: {relatorio['estatisticas']['sucessos']}")
    print(f"⚠️ Sucessos (mas vazias): {relatorio['estatisticas']['vazias']}")
    print(f"❌ Falhas SQL/API: {relatorio['estatisticas']['falhas']}")
    print(f"Arquivo gerado: {arquivo_saida}")

if __name__ == "__main__":
    executar_bateria()