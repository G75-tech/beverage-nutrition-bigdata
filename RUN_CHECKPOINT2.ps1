param([switch]$SkipEnvironment)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) { throw 'Run setup.ps1 once first.' }
function Invoke-ProjectPython {
    param([string[]]$Arguments)
    & ./.venv/Scripts/python.exe @Arguments
    if ($LASTEXITCODE -ne 0) { throw ('Python command failed: ' + ($Arguments -join ' ')) }
}
if (-not $SkipEnvironment) { Invoke-ProjectPython -Arguments @('-m','src.env_check') }
for ($attempt = 1; $attempt -le 3; $attempt++) {
    Invoke-ProjectPython -Arguments @('-m','src.ingest_file')
}
Invoke-ProjectPython -Arguments @('-m','src.inspect_raw')
