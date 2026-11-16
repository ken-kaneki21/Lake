#requires -Version 7.0
param([string]$EvidenceRoot = (Join-Path $PSScriptRoot '../evidence/windows'))
. "$PSScriptRoot/ParityLake.Common.ps1"
foreach ($mode in @('preflight','fault','clean')) {
    if (-not (Test-Path -LiteralPath (Join-Path $EvidenceRoot "$mode/.env"))) { continue }
    Open-Run $EvidenceRoot $mode
    $lock=Lock-Run
    try { Compose @('stop','--timeout','30') 'stop' | Out-Null } finally { $lock.Dispose() }
    Write-Output "Stopped $script:Project. Docker volumes and evidence retained."
}
