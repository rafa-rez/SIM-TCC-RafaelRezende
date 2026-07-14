# Plano LLM-as-Judge — TCC CIC

## Status atual

| Item | Status |
|------|--------|
| EX, VSR, NEA, TSA, CHS | Implementado (`eval/run_experiment.py`) |
| LLM-as-Judge | **Não implementado** |
| Avaliação humana formal | Pendente |

## Objetivo

Complementar EX (métrica estrita) com julgamento semântico: *“esta resposta atende à pergunta do cidadão?”*, inclusive quando a SQL difere da referência mas seria aceitável.

## Fase 1 — Piloto (5 consultas)

1. Criar `eval/prompts/judge_rubric.txt` com rubrica Likert 1–5:
   - adequação semântica
   - correção factual
   - completude
   - qualidade da explicação técnica
2. Criar `eval/run_judge.py`:
   - entrada: `checkpoint.jsonl` de um experimento
   - filtro: `VSR=1 AND EX=0` (casos controversos)
   - saída: `judge_results.jsonl`
3. Rodar em `baseline_16k` com `--limit 5`
4. Revisar manualmente os 5 vereditos

**Custo estimado:** < US$ 0,10

## Fase 2 — Amostra estratificada (30 consultas)

- 10 fáceis + 10 médias + 10 difíceis
- Modelo juiz: `gpt-4.1-mini` (barato) com revalidação de 6 casos em `gpt-4o`
- Métrica principal: **JAR** (Judge Agreement Rate) = % correto + parcialmente correto

**Custo estimado:** US$ 0,15–0,50

## Fase 3 — Calibração humana (10 consultas)

- Autor anota mesma rubrica manualmente
- Calcular concordância (kappa ou % acordo)
- Ajustar prompt do juiz se kappa < 0,6

## Fase 4 — Redação no TCC

- Tabela: EX vs JAR por dificuldade
- 2–3 estudos de caso qualitativos (EX=0, JAR=correto)
- Discussão: quando EX subestima qualidade

## Artefatos a criar

```
eval/
├── prompts/judge_rubric.txt
├── run_judge.py
├── aggregate_judge.py
└── results/<exp>/judge_results.jsonl
```

## Orçamento

Reserva sugerida: **US$ 1,00** dentro do saldo restante (~US$ 8 do teto original).

## Riscos

| Risco | Mitigação |
|-------|-----------|
| Juiz sempre concorda com modelo | Incluir `R_ref` no prompt; temperatura 0 |
| Custo | Amostra fixa 30, não 80 |
| Orientador questionar validade | Subamostra humana obrigatória na Fase 3 |

## Próximo comando (quando implementado)

```powershell
python eval/run_judge.py --experiment baseline_16k --sample 30 --model gpt-4.1-mini
python eval/aggregate_judge.py --experiment baseline_16k
```
