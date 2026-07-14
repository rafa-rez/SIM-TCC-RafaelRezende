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

## Resultados principais (métricas v2, E1)

| Métrica | Valor |
|---------|------:|
| EX estrita | 10,0% |
| EX colmap (aliases tolerados) | 58,75% |
| VSR | 97,5% |
| Casos com falha estrita apenas por alias | 48,75% |

Ver tabela completa em `experimentos/comparison_table_v2.md` e definições em `docs/METRICAS.md`.

## Replicação rápida

```bash
pip install -r eval/requirements.txt
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
