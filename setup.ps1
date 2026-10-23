param([string]$BootstrapPython)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
New-Item -ItemType Directory -Force -Path 'logs/checkpoint2','docs/evidence' | Out-Null
if (-not (Test-Path -LiteralPath '.tools/runtime.json')) {
    if ($BootstrapPython) {
        & $BootstrapPython scripts/bootstrap_runtime.py
    } elseif (Get-Command py -ErrorAction SilentlyContinue) {
        py -3 scripts/bootstrap_runtime.py
    } else {
        throw 'Supply -BootstrapPython with an existing Python 3.11+ interpreter.'
    }
    if ($LASTEXITCODE -ne 0) { throw 'Portable runtime bootstrap failed.' }
}
$runtime = Get-Content -LiteralPath '.tools/runtime.json' -Raw | ConvertFrom-Json
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
    & (Join-Path $PSScriptRoot $runtime.python) -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'venv creation failed.' }
}
& ./.venv/Scripts/python.exe -m pip install --disable-pip-version-check -r requirements.txt 2>&1 | Tee-Object -FilePath logs/checkpoint2/pip_install.log
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
& ./.venv/Scripts/python.exe -m pip check
if ($LASTEXITCODE -ne 0) { throw 'pip check failed.' }
& ./.venv/Scripts/python.exe -m pip freeze | Set-Content -LiteralPath docs/evidence/pip_freeze.txt -Encoding utf8
Write-Output 'Setup complete. Run .\.venv\Scripts\python.exe -m src.env_check'
