# Pipeline de métricas — SIM v1.0 (instância Caeté)
# Uso:
#   .\run_metrics_pipeline.ps1              # Text-to-SQL offline (E1–E4)
#   .\run_metrics_pipeline.ps1 -WithRag     # SQL + RAG (Qdrant + OpenAI)
#   .\run_metrics_pipeline.ps1 -SmokeOnly   # só pré-requisitos

param(
    [switch]$WithRag,
    [switch]$SkipSql,
    [switch]$SmokeOnly
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "=== SIM — pipeline de métricas v1.0 ===" -ForegroundColor Cyan

Write-Host "Instalando dependências..."
pip install -r requirements.txt -q

$argsList = @("eval/run_pipeline.py")
if ($WithRag) { $argsList += "--with-rag" }
if ($SkipSql) { $argsList += "--skip-sql" }
if ($SmokeOnly) { $argsList += "--smoke-only" }

python @argsList

if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`nConcluído." -ForegroundColor Green
if (-not $SkipSql -and -not $SmokeOnly) {
    Write-Host "  SQL: experimentos/comparison_table_v3.md"
}
if ($WithRag -and -not $SmokeOnly) {
    Write-Host "  RAG: experimentos/rag_baseline_v1_1/metrics_summary_rag.json"
}
Write-Host "  Relatório: experimentos/pipeline_report.json"
