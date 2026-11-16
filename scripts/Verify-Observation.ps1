#requires -Version 7.0
param([string]$EvidenceRoot = (Join-Path $PSScriptRoot '../evidence/windows'))
. "$PSScriptRoot/ParityLake.Common.ps1"
$run = Join-Path $EvidenceRoot 'clean'
$state = Read-Json (Join-Path $run 'observation.json')
$report = @{ status='FAIL'; observation_id=$state.id; git_sha=$state.git_sha; verified_utc=(Utc) }

function Read-Lines($file) {
    Get-Content -LiteralPath (Join-Path $run $file) | ForEach-Object { $_ | ConvertFrom-Json -AsHashtable }
}

try {
    if ($state.status -ne 'COLLECTED') { throw "Observation state is $($state.status)" }
    $start = (Parse-Time $state.start_utc)
    $end = (Parse-Time $state.end_utc)
    $report.elapsed_seconds = ($end-$start).TotalSeconds
    if ($report.elapsed_seconds -lt 86400 -or $state.elapsed_monotonic_seconds -lt 86400 -or
        [Math]::Abs($report.elapsed_seconds-$state.elapsed_monotonic_seconds) -gt 15) { throw 'UTC and monotonic time do not prove 24 continuous hours' }
    $power = Read-Json (Join-Path $run 'host-power-events.json')
    if (-not $power.checked -or @($power.events).Count) { throw 'Sleep/resume check failed' }

    $priorUtc = $start
    $priorMonotonic = 0
    $health = @(Read-Lines 'health.jsonl')
    if ($health.Count -lt 480) { throw 'Insufficient service health coverage' }
    foreach ($entry in $health) {
        $time = (Parse-Time $entry.utc)
        $gap = ($time-$priorUtc).TotalSeconds
        $monoGap = ($entry.monotonic_ms-$priorMonotonic)/1000.0
        if ($entry.observation_id -ne $state.id -or $entry.identity -ne $state.identity -or
            $gap -lt 0 -or $gap -gt 180 -or $monoGap -lt 0 -or $monoGap -gt 180 -or
            [Math]::Abs($gap-$monoGap) -gt 15) { throw "Invalid health evidence near $time" }
        $priorUtc=$time
        $priorMonotonic=$entry.monotonic_ms
    }
    if (($end-$priorUtc).TotalSeconds -gt 180 -or $priorUtc -gt $end) { throw 'Health coverage misses the end of the observation' }

    $priorUtc=$start
    $index=0
    $sampleRows=@(Read-Lines 'samples.jsonl')
    $commands=@(Read-Lines 'commands.jsonl')
    if (@($commands | Where-Object { $_.exit_code -ne 0 }).Count) { throw 'A Docker command exited nonzero' }
    foreach ($entry in $sampleRows) {
        $time=(Parse-Time $entry.start_utc)
        $gap=($time-$priorUtc).TotalSeconds
        if ($entry.observation_id -ne $state.id -or $entry.index -ne $index -or $gap -lt 0 -or $gap -gt 420 -or
            $entry.drift_windows -ne 0 -or $entry.duplicate_bronze_offsets -ne 0 -or
            $entry.rows -ne 10 -or $entry.source_count -ne $entry.rows -or
            $entry.bronze_count -ne $entry.rows -or $entry.silver_count -ne $entry.rows -or
            -not $entry.reconciliation_id -or @($entry.latency_seconds).Count -ne $entry.rows) {
            throw "Missing or failed integrity sample at index $index"
        }
        $probeLog = Get-Content -LiteralPath (Join-Path $run ('sample-{0:d4}-parity.log' -f $index))
        $probe = Result-Json @{ lines=$probeLog }
        Assert-Clean $probe $entry.rows
        if ($probe.comparison.run_id -ne $entry.reconciliation_id -or $probe.bronze_count -ne $entry.rows) {
            throw "Spark reconciliation evidence disagrees at sample $index"
        }
        $priorUtc=$time
        $index++
    }
    if ($index -ne 289 -or ($end-$priorUtc).TotalSeconds -gt 420) { throw 'Expected 289 integrity samples covering both endpoints' }
    $report.status='PASS'
    $report.integrity_samples=$index
    $report.health_samples=$health.Count
    $report.false_alerts=0
    $report.cdc_rows=[int](($sampleRows | Measure-Object rows -Sum).Sum)
    $report.latency_seconds=Quantiles @($sampleRows | ForEach-Object { $_.latency_seconds })
} catch {
    $report.status='FAIL'
    $report.error=$_.Exception.Message
    throw
} finally {
    Write-Json (Join-Path $run 'observation-verification.json') $report
}
$report | ConvertTo-Json -Depth 10
