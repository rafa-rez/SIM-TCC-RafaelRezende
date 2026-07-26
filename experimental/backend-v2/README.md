# Backend v2 — experimental (não é produção)

Melhorias de infraestrutura desenvolvidas durante o TCC, **isoladas da stack oficial** do repositório.

A API em `scripts_python/main.py` e o serviço `duckdb_api` no `docker-compose.yml` da raiz permanecem na **versão de produção documentada no TCC**.

## Conteúdo desta pasta

| Arquivo | Descrição |
|---------|-----------|
| `main.py` | API DuckDB com materialização no boot, `/health`, execução async em thread, modo `DUCKDB_TYPED` opt-in |
| `Dockerfile` | Imagem com dependências pinadas |
| `requirements.txt` | Versões fixas das bibliotecas |
| `scripts/validate_database.py` | Relatório de linhas corrompidas no export 2023 |
| `scripts/repair_export_2023.py` | Reparo lateral do export 2023 (cópia em `database_fix2023/`) |

## Como testar localmente (opcional)

Substitua temporariamente `scripts_python/main.py` pela cópia desta pasta **ou** monte o Dockerfile:

```powershell
# Exemplo: copiar API experimental para teste local
Copy-Item experimental\backend-v2\main.py scripts_python\main.py -Force
# Ajustar docker-compose duckdb_api para build: ./experimental/backend-v2
```

Não use em produção sem revisão e validação do golden v1.

## Relação com o TCC

As métricas reportadas na monografia e no artigo foram obtidas com o **protocolo de avaliação** (`avaliacao/eval/`), independente desta API experimental. Detalhes em `docs/experimentos-v2/LOG.md`.
