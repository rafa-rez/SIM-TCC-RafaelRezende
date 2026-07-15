# SIM — Sistema de Informações Municipais

Repositório de artefatos, dados e scripts de **avaliação Text-to-SQL** do **SIM** (*Sistema de Informações Municipais*), com **instância de referência em Caeté (MG)**.

**Autor:** Rafael Alves Silva Rezende (UFLA)  
**Orientador:** Prof. Dr. Denilson

## O que é o SIM

O SIM é um assistente conversacional para consulta a dados públicos municipais (finanças, contratos, licitações, folha, etc.). Cada município pode ter sua própria instância; neste trabalho, a validação usa os dados abertos de **Caeté** (código IBGE 3110004), período 2020–2025.

Visão geral: [`docs/SIM.md`](docs/SIM.md).

## Estrutura do repositório

| Pasta | Descrição |
|-------|-----------|
| [`avaliacao/`](avaliacao/) | Dados, experimentos, pipeline de avaliação e scripts |
| [`avaliacao/dados/`](avaliacao/dados/) | Bases SICOM/CGU e conjunto golden |
| [`avaliacao/experimentos/`](avaliacao/experimentos/) | Quatro experimentos (E1–E4) com checkpoints e métricas v3 |
| [`avaliacao/eval/`](avaliacao/eval/) | Pipeline Text-to-SQL e biblioteca de comparação |
| [`avaliacao/scripts/`](avaliacao/scripts/) | Recálculo offline, exportação e tabelas comparativas |
| [`docs/`](docs/) | Definições formais, métricas e guia de replicação |

## Resultados principais (métricas v3, E1)

| Métrica | Valor |
|---------|------:|
| **EX resposta** (principal) | **62,5%** |
| EX proj | 60,0% |
| EX proj + τ (0,5%) | 61,25% |
| EX colmap | 57,5% |
| EX estrita (literatura) | 10,0% |
| VSR | 97,5% |

Detalhes: [`docs/RELATORIO_METRICAS_V3.md`](docs/RELATORIO_METRICAS_V3.md) e [`avaliacao/experimentos/comparison_table.md`](avaliacao/experimentos/comparison_table.md).

## Replicação rápida

```powershell
cd avaliacao
pip install -r requirements.txt
python scripts/enrich_golden_colunas.py
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
python scripts/compare_all_experiments.py
```

Ou, em um comando: `avaliacao/run_replicacao.ps1`.

Requisitos: Python 3.11+. O recálculo usa DuckDB em memória sobre `avaliacao/dados/database/` (não exige Docker).

Guia completo: [`docs/REPLICACAO.md`](docs/REPLICACAO.md).

## Citação

```
Rezende, R. A. S. SIM — Sistema de Informações Municipais: validação Text-to-SQL
(instância Caeté/MG). Repositório: https://github.com/rafa-rez/tcc-sistema-de-informa-es-municipais
```

## Licença

Dados públicos municipais e federais (SICOM/CGU). Código e documentação: consultar [`LICENSE`](LICENSE).
