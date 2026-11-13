param(
    [ValidateRange(1, 6)][int]$Task = 6,
    [switch]$Repeat,
    [switch]$List,
    [string]$SnapshotRunId = '',
    [string]$TaskConfig = ''
)
$ErrorActionPreference = 'Stop'
$pipelinePython = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
if (-not (Test-Path -LiteralPath $pipelinePython)) {
    Write-Host '[LOI] Chua co Python cua du an. Hay chay setup.ps1 truoc.' -ForegroundColor Red
    exit 1
}
$pipelineArguments = @('-B', '-u', '-m', 'src.checkpoint3_task', '--task', "$Task")
if ($Repeat) { $pipelineArguments += '--repeat' }
if ($List) { $pipelineArguments += '--list' }
if ($SnapshotRunId) { $pipelineArguments += @('--snapshot-run-id', $SnapshotRunId) }
if ($TaskConfig) { $pipelineArguments += @('--task-config', $TaskConfig) }
$pipelinePreviousEncoding = [Console]::OutputEncoding
$pipelinePreviousUtf8 = $env:PYTHONUTF8
$pipelinePreviousIoEncoding = $env:PYTHONIOENCODING
$pipelineExitCode = 1
Push-Location -LiteralPath $PSScriptRoot
try {
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    $env:PYTHONUTF8 = '1'
    $env:PYTHONIOENCODING = 'utf-8'
    & $pipelinePython @pipelineArguments
    $pipelineExitCode = $LASTEXITCODE
} finally {
    Pop-Location
    [Console]::OutputEncoding = $pipelinePreviousEncoding
    $env:PYTHONUTF8 = $pipelinePreviousUtf8
    $env:PYTHONIOENCODING = $pipelinePreviousIoEncoding
}
exit $pipelineExitCode
