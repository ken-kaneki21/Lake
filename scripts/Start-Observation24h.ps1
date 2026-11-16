#requires -Version 7.0
param([string]$EvidenceRoot = (Join-Path $PSScriptRoot '../evidence/windows'))
. "$PSScriptRoot/ParityLake.Common.ps1"
Open-Run $EvidenceRoot preflight
Require-Pass $script:Evidence preflight preflight.json
Open-Run $EvidenceRoot fault
Require-Pass $script:Evidence fault matrix.json
$faultLock = Lock-Run
try { Compose @('stop') 'before-observation-stop' | Out-Null } finally { $faultLock.Dispose() }
Open-Run $EvidenceRoot clean -Create
$lock = Lock-Run
$statePath = Join-Path $script:Run 'observation.json'
$samples = Join-Path $script:Run 'samples.jsonl'
$beats = Join-Path $script:Run 'health.jsonl'
$continuation = Test-Path -LiteralPath $statePath
$state = if ($continuation) { Read-Json $statePath } else {
    @{ status='INITIALIZING'; git_sha=$script:Sha; id=[guid]::NewGuid().ToString(); dataset='synthetic'; seed=42; cadence_seconds=300; max_health_gap_seconds=180; max_sample_gap_seconds=420; rows_per_sample=10; next_sample=0 }
}
if ($state.git_sha -ne $script:Sha) { $lock.Dispose(); throw 'Observation belongs to another Git SHA' }
if ($state.status -eq 'COLLECTED') { $lock.Dispose(); throw 'Observation collected. Run Verify-Observation.ps1' }
if ($continuation) { $state.status='INVALID'; $state.interrupted_utc=Utc }
Write-Json $statePath $state

function Add-Row($Path, $Row) {
    Add-Content -LiteralPath $Path -Value ($Row | ConvertTo-Json -Depth 32 -Compress)
}

function Add-Heartbeat([string]$Label) {
    $identity = Check-Identity $Label
    $row = @{ observation_id=$state.id; utc=(Utc); monotonic_ms=$timer.ElapsedMilliseconds; identity=$identity }
    Add-Row $beats $row
    if ($identity -ne $state.identity) { throw 'Container identity or restart count changed' }
}

try {
    Start-Stack
    $timer = [Diagnostics.Stopwatch]::StartNew()
    if (-not $continuation) {
        $state.identity = Check-Identity 'clean-initial'
        $state.start_utc = Utc
        $state.status = 'RUNNING'
    } else {
        # Continue collecting separate samples, keeping the original observation invalid.
        $state.identity = Check-Identity 'clean-resume'
    }
    Write-Json $statePath $state
    $targetSeconds = if ($continuation) { 0 } else { 86400 }
    $next = 0
    $beat = 0
    while ($true) {
        Add-Heartbeat "obs-heartbeat-$($state.next_sample)-$beat"
        $beat++
        if ($timer.Elapsed.TotalSeconds -lt $next) {
            Start-Sleep -Seconds ([Math]::Min(30,[Math]::Max(1,$next-$timer.Elapsed.TotalSeconds)))
            continue
        }
        $number = [int]$state.next_sample
        # Reserve each ID range before writing so interruption cannot reuse it.
        $state.next_sample = $number + 1
        Write-Json $statePath $state
        $label = 'sample-{0:d4}' -f $number
        $key = 4000000 + $number * $state.rows_per_sample
        $sampleStart = Utc
        $sampleTimer = [Diagnostics.Stopwatch]::StartNew()
        $before = Offset-State "$label-offset"
        $write = Source $key insert $state.rows_per_sample "$label-source"
        Wait-Offset 'relayed.app.trips' ($before['relayed.app.trips'] + $state.rows_per_sample) "$label-relayed" | Out-Null
        Drain "$label-drain"
        Add-Heartbeat "$label-post-drain"
        $check = Lake $key $state.rows_per_sample "$label-parity"
        Assert-Clean $check $state.rows_per_sample
        if ($check.bronze_count -ne $state.rows_per_sample) { throw 'Unexpected bronze event count' }
        $ids = (Compose (@('ps','-q') + $script:ServerServices) "$label-stats-ids").lines
        Invoke-ParityDocker (@('stats','--no-stream','--format','{{json .}}') + $ids) "$label-resources" | Out-Null
        Add-Row $samples @{
            observation_id=$state.id; index=$number; start_utc=$sampleStart; end_utc=(Utc)
            monotonic_ms=$timer.ElapsedMilliseconds; source=$write; rows=$state.rows_per_sample
            reconciliation_id=$check.comparison.run_id; drift_windows=$check.comparison.drift_windows
            duplicate_bronze_offsets=$check.duplicate_bronze_offsets; bronze_count=$check.bronze_count
            silver_count=$check.silver_count; source_count=$check.source_count
            latency_seconds=$check.latency_seconds; quantiles_seconds=(Quantiles $check.latency_seconds)
            sample_seconds=$sampleTimer.Elapsed.TotalSeconds; resource_artifact="$label-resources.log"
            reconciliation_artifact="$label-parity.log"
        }
        Add-Heartbeat "$label-end"
        if ($next -ge $targetSeconds) { break }
        $next = [Math]::Min($next + $state.cadence_seconds,$targetSeconds)
        if ($timer.Elapsed.TotalSeconds -gt $next + 60) { throw 'A scheduled integrity sample was missed' }
    }
    if (-not $continuation) {
        $state.end_utc = Utc
        $state.elapsed_monotonic_seconds = $timer.Elapsed.TotalSeconds
        # Capture Windows suspend/resume events. Access or query errors fail the observation.
        $queryErrors = @()
        $events = @(Get-WinEvent -FilterHashtable @{
            LogName='System'; StartTime=((Parse-Time $state.start_utc)).LocalDateTime
            EndTime=((Parse-Time $state.end_utc)).LocalDateTime
            Id=@(1,42,107,506,507)
        } -ErrorAction SilentlyContinue -ErrorVariable queryErrors)
        if (@($queryErrors | Where-Object { $_.FullyQualifiedErrorId -notlike 'NoMatchingEventsFound*' }).Count) {
            throw 'Windows System event log could not be checked for sleep/resume'
        }
        $sleep = @($events | Where-Object {
            ($_.ProviderName -eq 'Microsoft-Windows-Power-Troubleshooter' -and $_.Id -eq 1) -or
            ($_.ProviderName -eq 'Microsoft-Windows-Kernel-Power' -and $_.Id -in @(42,107,506,507))
        } | ForEach-Object { @{ id=$_.Id; provider=$_.ProviderName; utc=$_.TimeCreated.ToUniversalTime().ToString('o') } })
        Write-Json (Join-Path $script:Run 'host-power-events.json') @{ checked=$true; events=$sleep }
        if ($sleep.Count) { throw 'Windows System event log reported sleep or resume' }
        $state.status = 'COLLECTED'
    }
} catch {
    $state.status = 'INVALID'
    $state.error = $_.Exception.Message
    throw
} finally {
    $state.last_collected_utc = Utc
    Write-Json $statePath $state
    $lock.Dispose()
}
Write-Output "Observation collection status: $($state.status). Run the independent verifier."
