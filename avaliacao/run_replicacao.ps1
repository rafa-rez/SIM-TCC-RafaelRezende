# Replicação offline das métricas v3 (SIM — instância Caeté)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "Instalando dependências..."
pip install -r requirements.txt

Write-Host "Enriquecendo golden (colunas_resposta)..."
python scripts/enrich_golden_colunas.py

$experiments = @(
    "e1_baseline_compacto",
    "e2_contexto_estendido",
    "e3_gpt4o_mini",
    "e4_gpt4o"
)

foreach ($exp in $experiments) {
    Write-Host "Recalculando métricas: $exp"
    python scripts/recompute_metrics.py --experiment $exp
}

Write-Host "Gerando tabela comparativa..."
python scripts/compare_all_experiments.py

Write-Host "Concluído. Ver experimentos/comparison_table.md"
