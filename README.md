# Sistema de Informações Municipais — validação Text-to-SQL (TCC)

Repositório de artefatos, dados e scripts de avaliação do agente **CIC** (Centro de Informações Caetense) para consulta conversacional a dados públicos de Caeté (MG).

**Autor:** Rafael Alves Silva Rezende (UFLA)  
**Orientador:** Prof. Dr. Denilson

## Conteúdo

| Pasta | Descrição |
|-------|-----------|
| `dados/` | Bases SICOM/CGU, Diário Oficial e conjunto golden |
| `dados/golden/` | 80 consultas de referência + resultados tabulares exportados |
| `experimentos/` | Quatro experimentos (E1–E4) com checkpoints e métricas v2 |
| `metricas/` | Documentação por métrica (EX, VSR, NEA, TSA, CHS, JAR) |
| `eval/` | Pipeline de avaliação e biblioteca de comparação |
| `scripts/` | Recálculo offline, exportação e tabelas comparativas |
| `docs/` | Definições formais e guia de replicação |

## Resultados principais (métricas v3, E1)

| Métrica | Valor |
|---------|------:|
| **EX resposta** (principal) | **62,5%** |
| EX proj | 60,0% |
| EX proj + τ (0,5%) | 61,25% |
| EX colmap | 57,5% |
| EX estrita (literatura) | 10,0% |
| VSR | 97,5% |

Ver `docs/RELATORIO_METRICAS_V3.md` e `experimentos/comparison_table_v3.md`.

## Replicação rápida

```bash
pip install -r eval/requirements.txt
python scripts/enrich_golden_colunas.py
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
python scripts/compare_all_experiments.py
```

Requisitos: Python 3.11+, dependências em `eval/requirements.txt`. O recálculo usa DuckDB em memória sobre `dados/database/` (não exige Docker).

Guia completo: `docs/REPLICACAO.md`.

## Citação

```
Rezende, R. A. S. Sistema de Informações Municipais — validação Text-to-SQL.
Repositório: https://github.com/rafa-rez/tcc-sistema-de-informa-es-municipais
```

## Licença

Dados públicos municipais e federais (SICOM/CGU). Código e documentação: consultar `LICENSE`.
