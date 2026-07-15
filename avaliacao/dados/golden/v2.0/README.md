# Golden dataset v2.0 — SIM (instância Caeté)

Conjunto curado a partir de **perguntas reais de testers**, com SQL assistida (prompt de produção) e **aprovação humana** antes de congelar.

## Fluxo

```
1. Perguntas em golden_dataset_v2.0.csv (status=rascunho)
2. python scripts/golden_assist.py --id 001        # propõe SQL + prévia DuckDB
3. Editar SQL / colunas_resposta no CSV ou draft/
4. python scripts/golden_review_export.py          # pacote para o revisor
5. Marcar autor_ok / revisor_ok → status=congelado
6. python scripts/export_result_sets.py --dataset dados/golden/v2.0/golden_dataset_v2.0.csv ...
7. Re-rodar experimentos E1–E4 com --dataset v2.0
```

## Estrutura

| Caminho | Descrição |
|---------|-----------|
| `golden_dataset_v2.0.csv` | Fonte canônica v2 |
| `draft/` | JSON com SQL gerada, prévia tabular e metadados do assist |
| `reference_results/` | Resultados executados da SQL aprovada |
| `revisao/` | Pacotes exportados para checklist do avaliador |

## Status de cada item

| Status | Significado |
|--------|-------------|
| `rascunho` | Só a pergunta; SQL pendente ou em edição |
| `sql_valida` | SQL executa no DuckDB |
| `autor_ok` | Autor aprova pergunta + SQL + colunas |
| `revisor_ok` | Avaliador concorda |
| `congelado` | Entra no benchmark oficial v2 |

## Origem desta versão

As primeiras entradas (`origem=tester`) vêm de feedback de usuários iniciais do SIM em Caeté (jul/2026).

O golden v1.0 permanece em `../golden_dataset_v1.0.csv` para histórico dos experimentos E1–E4.
