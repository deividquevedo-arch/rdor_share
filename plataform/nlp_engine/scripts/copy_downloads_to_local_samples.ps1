#Requires -Version 5.1
<#
.SYNOPSIS
  Copia CSVs de referencia da pasta Downloads para plataform/nlp_engine/_local_samples/ (ignorado pelo Git).

.DESCRIPTION
  Nao versiona dados sensiveis: apenas copia ficheiros locais para trabalho offline.
  Copie tambem manualmente para exports/ ou gold/ qualquer outro CSV (ex.: hive_metastore.*.csv) que use localmente.

.NOTES
  Executar a partir de qualquer diretorio. Ajuste $Downloads se a sua pasta for outra.
#>

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$EngineRoot = Split-Path -Parent $ScriptDir
$Base = Join-Path $EngineRoot "_local_samples"
$Dirs = @(
    (Join-Path $Base "exports"),
    (Join-Path $Base "gold"),
    (Join-Path $Base "diamond")
)
foreach ($d in $Dirs) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

$Downloads = Join-Path $env:USERPROFILE "Downloads"
$Files = @(
    "base_teste_local_laudos_validos.csv",
    "replica_sintetica_atibaia.csv",
    "dev_tbl_gold_modelo_hepato_entrada.csv",
    "dev_tb_diamond_mod_colon_entrada.csv",
    "tb_diamond_mod_doencas_biliares_entrada_teste_local.csv"
)

Write-Host "Origem: $Downloads"
Write-Host "Destino exports: $($Dirs[0])"
Write-Host ""

foreach ($name in $Files) {
    $src = Join-Path $Downloads $name
    if (Test-Path -LiteralPath $src) {
        Copy-Item -LiteralPath $src -Destination $Dirs[0] -Force
        Write-Host "OK  $name"
    }
    else {
        Write-Warning "Ficheiro nao encontrado: $src"
    }
}

Write-Host ""
Write-Host "Opcional: mova manualmente de exports\ para gold\ ou diamond\ conforme o tipo de export."
