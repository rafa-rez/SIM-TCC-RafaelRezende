# TCC LaTeX — V1 para orientador

## PDF recomendado (template UFLA oficial)

```powershell
cd D:\tiokk-n8n\template-ufla
.\compile.ps1 -UseDocker
```

**Saída:** `template-ufla/templufla_main.pdf` (cópia espelhada em `tcc-latex/main.pdf`)

O arquivo `template-ufla-overleaf` na raiz do repositório é o `main.tex` do Overleaf. O pacote completo (`templufla.cls`, glossários, linguas/) está em `template-ufla/`.

## Compilar rascunho abntex2 (legado)

```powershell
cd D:\tiokk-n8n\tcc-latex
.\compile.ps1 -UseDocker
```

## Judge + traces

Traces completos: `eval/results/baseline_16k/judge_runs/stratified_30/pipeline_trace.jsonl`

## Atualizar tabelas SQL

```powershell
python eval/compare_experiments.py
python eval/update_tcc_results.py
```
