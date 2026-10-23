param([switch]$SkipEnvironment)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
    Write-Host '[LOI] Chua co moi truong Python. Hay chay .\setup.ps1 truoc.' -ForegroundColor Red
    exit 1
}
$runArguments = @('-u', '-m', 'src.checkpoint2')
if ($SkipEnvironment) { $runArguments += '--skip-environment' }
$previousConsoleEncoding = [Console]::OutputEncoding
try {
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    & ./.venv/Scripts/python.exe @runArguments
    $runExitCode = $LASTEXITCODE
} finally {
    [Console]::OutputEncoding = $previousConsoleEncoding
}
exit $runExitCode
