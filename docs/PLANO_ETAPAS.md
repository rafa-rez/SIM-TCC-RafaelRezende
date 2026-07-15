# Plano de etapas — TCC, métricas, repositório e artigo SBC

> **Atualização (jul/2026):** o repositório foi reorganizado em `avaliacao/` + `docs/`. O sistema chama-se **SIM** (*Sistema de Informações Municipais*), com instância de referência em Caeté. Caminhos antigos (`eval/`, `dados/`, `scripts/` na raiz) foram movidos para `avaliacao/`. Ver [`SIM.md`](SIM.md) e [`REPLICACAO.md`](REPLICACAO.md).

Documento de planejamento interno. Linguagem objetiva para orientação do trabalho e do repositório público de validação.

Repositório alvo: https://github.com/rafa-rez/tcc-sistema-de-informa-es-municipais

---

## Diagnóstico atual (ponto de partida)

| Item | Situação |
|------|----------|
| EX estrita (literatura) | 11–19% nos quatro experimentos |
| VSR | 90–97% |
| JAR (amostra n=30) | 100% com EX de 20% na mesma amostra |
| Golden dataset | 80 consultas em `eval/datasets/golden_dataset_v1.0.csv` |
| Resultados SQL | `checkpoint.jsonl` dos 4 experimentos (SQL gerada + métricas, sem JSON completo das linhas) |
| Documento | Monografia UFLA (`template-ufla/`) |
| Código de EX | `eval/lib/compare.py` — multiset de assinaturas de linha **com nomes de coluna** |

**Por que a EX estrita é baixa (além da abstração das perguntas):**

A implementação atual **já tolera** ordem diferente de linhas e de colunas. O que mais derruba a EX são:

1. **Aliases de coluna** (`total` versus `total_pago`) com valores idênticos.
2. **Colunas extras ou faltantes** na SQL gerada (mesmo número de linhas correto).
3. **SQL semanticamente distinta** da referência manual (casos legítimos de discordância).

Isso deve ser explicado ao orientador com evidência (amostra de casos EX=0, VSR=1 no `checkpoint.jsonl`).

---

## Etapa 0 — Alinhamento com o orientador (1 reunião)

**Objetivo:** fechar o que entra no TCC e no repositório antes de codar.

**Pauta sugerida:**

1. Qual variante de EX será a **métrica principal** no texto (ver Etapa 1).
2. Confirmar formato final: artigo SBC (evento ou periódico) em substituição à monografia, ou artigo + anexos.
3. O que a banca precisa reproduzir: só métricas offline, ou também reexecução com API?
4. Dados brutos SICOM/CGU: **não** subir no GitHub (volume e licença); subir apenas esquema, golden e resultados derivados.

**Entregável:** ata curta (bullet points) com decisões registradas neste arquivo.

---

## Etapa 1 — Redefinição formal das métricas

**Objetivo:** métricas com definição, fórmula e procedimento reprodutível, adequadas à crítica sobre EX.

### 1.1 Família EX (proposta para discussão)

Manter a métrica da literatura, mas **não** usá-la como única evidência.

| Símbolo | Nome | Definição resumida |
|---------|------|-------------------|
| EX | EX estrita | Multiset de assinaturas `(coluna, valor)` por linha, com normalização numérica e textual. Igual à implementação atual. |
| EX-L | EX por cardinalidade | Para a consulta *i*: `\|R_i\| = \|G_i\|` (mesmo número de linhas). Métrica diagnóstica, não suficiente sozinha. |
| EX-C | EX por conteúdo | Mesmo `\|R_i\|`, mesmo número de colunas, multiset de tuplas de **valores** por linha (nomes de coluna ignorados). Endereça aliases e ordem de colunas. |
| EX-S | EX estrutural completa | EX estrita **ou** (EX-L ∧ EX-C ∧ multiset de valores igual). Variante intermediária a calibrar com o orientador. |

**Fórmula agregada (qualquer variante):**

```
EX* = (1/N) * sum_{i=1..N} 1[consulta i satisfaz critério *]
```

onde *N* = 80 e a consulta só entra se ambas as SQL (referência e gerada) executarem sem erro.

**Normalização de célula** (documentar no artigo): nulos unificados; números com 4 casas decimais; strings em maiúsculas sem espaços laterais.

### 1.2 Demais métricas (formalizar no mesmo capítulo)

| Métrica | Fórmula / regra |
|---------|-----------------|
| VSR | `(1/N) sum 1[SQL gerada executa sem erro]` |
| NEA | `(1/N) sum 1[se espera_dados=sim então \|G_i\|>0, senão verdadeiro]` |
| TSA | `(1/N) sum 1[tabelas_esperadas ⊆ tabelas(FROM/JOIN da SQL gerada)]` |
| CHS | `(1/N) sum (hits(condicao_esperada) / \|condicoes\|)` |
| JAR | `(1/M) sum 1[veredito ∈ {correto, parcialmente_correto}]` na amostra *M* |

### 1.3 Implementação

1. Estender `eval/lib/compare.py` com `execution_match_strict`, `execution_match_content`, `row_count_match`, etc.
2. Script `eval/recompute_metrics.py` que lê `checkpoint.jsonl` **sem chamar API** (reexecuta SQL no DuckDB local).
3. Gerar `metrics_summary_v2.json` com todas as métricas agregadas e por dificuldade.
4. Tabela de transição: EX, EX-C, EX-L, VSR, JAR na mesma amostra (evidência para o orientador).

**Entregável:** `docs/METRICAS.md` com definições formais + resultados recalculados.

**Dependência:** DuckDB API local (`docker compose up duckdb_api`).

---

## Etapa 2 — Dados de resposta no repositório

**Problema:** o golden tem `query_referencia`, mas não o JSON tabular da resposta esperada.

**Solução (leve e reprodutível):**

```
dados/golden/
  golden_dataset_v1.0.csv          # entrada + SQL + metadados
  reference_results/
    001.json                       # colunas, linhas, row_count, executado_em
    002.json
    ...
  README.md                        # como regenerar
```

**Formato `reference_results/{id}.json`:**

```json
{
  "id_teste": "1",
  "query_referencia": "...",
  "columns": ["ano", "valor"],
  "rows": [{"ano": 2024, "valor": 123.45}],
  "row_count": 6,
  "schema_hash": "sha256 das colunas ordenadas"
}
```

**Para SQL gerada (por experimento):**

```
experimentos/baseline_16k/
  generated_results/
    001.json
  checkpoint.jsonl
  metrics_summary_v2.json
```

**Script:** `scripts/export_result_sets.py --source golden|checkpoint --experiment baseline_16k`

**O que não sobe no Git:**

- `dados/database/*.csv` (gigabytes)
- `.env`, chaves de API
- `lixo/`, `staging/`, caches de embedding
- Prompts com nomes internos (`Póscagada.txt` → renomear para `prompt_sql_compacto.txt` no repo)

**O que sobe:**

- Esquema DDL resumido (`dados/schema/ddl_minificado.sql` ou trecho representativo)
- Instruções de obtenção dos dados SICOM/CGU (links, ETL)
- Golden + resultados derivados (JSON pequenos)
- Checkpoints e traces de avaliação

**Entregável:** pasta `dados/golden/` pronta para commit.

---

## Etapa 3 — Estrutura do repositório (limpa e acadêmica)

**Objetivo:** repositório auditável pela banca, sem ruído.

```
tcc-sistema-de-informacoes-municipais/
  README.md                 # visão geral, citação, como replicar
  LICENSE                   # MIT ou Apache-2.0 (definir com orientador)
  .gitignore
  docs/
    METRICAS.md
    REPLICACAO.md
    PLANO_ETAPAS.md         # opcional: versão pública resumida
  dados/
    golden/
    schema/
  metricas/
    ex/
      README.md
      exemplos/             # 3 casos EX=0 com explicação
    vsr/
    nea/
    tsa/
    chs/
    jar/
  experimentos/
    e1_baseline_compacto/
    e2_contexto_estendido/
    e3_gpt4o_mini/
    e4_gpt4o/
  scripts/
    export_result_sets.py
    recompute_metrics.py
    run_experiment.py       # ou symlink / import de eval/
  artigo/
    main.tex                # template SBC
    refs.bib
    figuras/
```

**Regras de higiene Git:**

- Commits em português, mensagens imperativas curtas (`Adiciona exportação de resultados de referência`).
- Sem arquivos gerados desnecessários (`.aux`, `.log`, PDF no repo: apenas release ou não versionar).
- README por pasta de métrica: definição, script, entrada, saída, exemplo de log.
- Renomear experimentos: E1–E4 com nomes descritivos (sem `poscagada`/`precagada` no remoto).
- Revisar textos: sem travessões longos, sem tom de checklist de IA, sem "neste trabalho apresentamos" em excesso nos READMEs técnicos.

**Entregável:** `git init` + primeiro commit enxuto (README, .gitignore, docs, golden).

---

## Etapa 4 — Evidência por métrica (logs e rastreabilidade)

Para cada pasta em `metricas/`:

1. **README.md** com definição formal (copiar de `docs/METRICAS.md`).
2. **Script** que calcula só aquela métrica a partir de `checkpoint.jsonl` ou `reference_results/`.
3. **Saída exemplo** em `resultados/` (CSV ou JSONL por consulta).
4. **Log textual** opcional: `logs/exemplo_id_012.log` mostrando comparação passo a passo (por que EX=0 e EX-C=1).

**Script unificado de replicação:**

```bash
python scripts/recompute_metrics.py --experiment e1_baseline_compacto
python scripts/export_result_sets.py --source golden
```

Documentar em `docs/REPLICACAO.md`: pré-requisitos, Docker, variáveis de ambiente, ordem dos comandos, tempo estimado.

**Entregável:** banca consegue reproduzir números sem ler todo o código.

---

## Etapa 5 — Reescrever o documento (monografia → artigo SBC)

**Objetivo:** condensar o conteúdo atual no formato de artigo científico SBC.

### 5.1 Escolher template (confirmar com orientador)

| Opção | Uso típico | Compilador |
|-------|------------|------------|
| [SBC Reviews 2025](https://github.com/sbc-reviews/sbc-reviews-template) | Periódico SBC | XeLaTeX ou LuaLaTeX |
| [sbc-template](https://www.overleaf.com/latex/templates/template-for-the-sbc-conferences/khwsbvrwdfxv) | Anais de eventos SBC | pdfLaTeX |

Clonar template em `artigo/` sem alterar o `.cls`.

### 5.2 Mapa de conteúdo (monografia → artigo)

| Monografia (capítulo) | Artigo (seção) |
|----------------------|----------------|
| Introdução | 1. Introdução |
| Referencial (resumo) | 2. Trabalhos relacionados (1–2 páginas) |
| Metodologia + arquitetura | 3. Metodologia |
| Métricas (novo formalismo) | 3.3 Protocolo de avaliação |
| Resultados E1–E4 + EX/EX-C/JAR | 4. Resultados |
| Discussão | 5. Discussão |
| Conclusão | 6. Considerações finais |

**Cortes necessários:** epígrafe, indicadores de impacto extensos, listas de símbolos, capítulos redundantes. Figura de arquitetura permanece.

### 5.3 Destaque da nova narrativa de métricas

O artigo deve:

1. Apresentar EX estrita como referência da literatura.
2. Justificar EX-C (ou variante acordada) para perguntas abertas sem projeção de colunas.
3. Mostrar tabela comparativa EX × EX-C × VSR × JAR.
4. Incluir 2 ou 3 estudos de caso qualitativos (id + pergunta + divergência + veredito do juiz).

**Entregável:** `artigo/main.pdf` compilável, 10–14 páginas (limite depende do template).

---

## Etapa 6 — Publicação no GitHub

**Ordem de pushes (incremental, sem poluir):**

1. Esqueleto + README + .gitignore + LICENSE
2. `dados/golden/` + `docs/METRICAS.md` + `docs/REPLICACAO.md`
3. `metricas/` (módulos + exemplos)
4. `experimentos/` (checkpoints + summaries v2, sem reexecutar API)
5. `artigo/` (fontes LaTeX, sem PDF se preferir)
6. Tag `v1.0-validacao` alinhada à versão citada no artigo

**`.gitignore` mínimo:**

```
.env
__pycache__/
*.aux
*.log
*.out
*.pdf
dados/database/
dados/staging*/
lixo/
node_modules/
```

**Entregável:** URL do repo citável na folha de rosto / agradecimentos do TCC.

---

## Etapa 7 — Validação final com orientador

Checklist antes da defesa:

- [ ] Números do artigo = números do `metrics_summary_v2.json` no repo
- [ ] Tag Git referenciada no artigo
- [ ] Amostra de 5 casos revisada manualmente (EX, EX-C, JAR coerentes)
- [ ] Orientador concorda com métrica principal (EX-C ou composta)
- [ ] Repositório clonável em máquina limpa (teste com colega)

---

## Ordem de execução recomendada

```
Etapa 0 (reunião)
    ↓
Etapa 1 (métricas v2 + recompute)  ← maior impacto na crítica do orientador
    ↓
Etapa 2 (export JSON de respostas)
    ↓
Etapa 3 + 6 (repo estruturado, commits incrementais)
    ↓
Etapa 4 (READMEs por métrica)
    ↓
Etapa 5 (artigo SBC com números finais)
    ↓
Etapa 7 (validação)
```

**Paralelizável:** Etapa 5.1 (montar template SBC) enquanto roda Etapa 1.

---

## Próxima ação imediata

1. Agendar conversa com orientador com a tabela EX / EX-C / EX-L (após implementar recompute em 1 dia de trabalho).
2. Inicializar o repositório local a partir de `D:\tiokk-n8n` com cópia seletiva (não `git add .` na raiz inteira).
3. Implementar `recompute_metrics.py` e `export_result_sets.py` como primeiro código no remoto.

---

*Última atualização: julho/2026*
