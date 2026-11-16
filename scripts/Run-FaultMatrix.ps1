#requires -Version 7.0
param([string]$EvidenceRoot = (Join-Path $PSScriptRoot '../evidence/windows'))
. "$PSScriptRoot/ParityLake.Common.ps1"
Open-Run $EvidenceRoot preflight
Require-Pass $script:Evidence preflight preflight.json
$previousLock = Lock-Run
try { Compose @('stop') 'before-fault-stop' | Out-Null } finally { $previousLock.Dispose() }
Open-Run $EvidenceRoot fault -Create
$lock = Lock-Run
$summaryPath = Join-Path $script:Run 'matrix.json'
$summary = @{ status='FAIL'; git_sha=$script:Sha; started_utc=(Utc); dataset='synthetic'; seed=42; trial_files=@(); workload_files=@() }

function Ledger([long]$Id, [string]$Label, [switch]$Release) {
    $commandArgs = @('exec','-T','relay','python','-m','tests.windows_trial','ledger',"$Id")
    if ($Release) { $commandArgs += '--release' }
    Result-Json (Compose $commandArgs $Label)
}

function Assert-Drift($result, [long]$key, [string]$category) {
    $keys = @($result.alerts | ForEach-Object { $_.keys } | Where-Object { $_.trip_id -eq $key -and $_.category -eq $category })
    if ($result.comparison.drift_windows -ne 1 -or $keys.Count -ne 1 -or $result.duplicate_bronze_offsets -ne 0) {
        throw "Expected one $category discrepancy for trip $key"
    }
}

function Save-Resources([string]$Label) {
    $ids = (Compose (@('ps','-q') + $script:ServerServices) "$Label-ids").lines
    Invoke-ParityDocker (@('stats','--no-stream','--format','{{json .}}') + $ids) $Label | Out-Null
}

if (Test-Path -LiteralPath $summaryPath) {
    try { Require-Pass $script:Evidence fault matrix.json } finally { $lock.Dispose() }
    Write-Output "Matrix already passed. Evidence: $script:Run"
    return
}
try {
    Start-Stack
    Invoke-ParityDocker (@('inspect','--format','{{.Name}} {{.Config.Image}} {{.Image}}') + (Compose (@('ps','-q') + $script:ServerServices) 'matrix-initial-ids').lines) 'matrix-images' | Out-Null
    foreach ($size in @(100,1000)) {
        $label = "benchmark-$size"
        $key = 2000000 + $size * 10
        $before = Offset-State "$label-start-offset"
        $timer = [Diagnostics.Stopwatch]::StartNew()
        $write = Source $key insert $size "$label-source"
        Wait-Offset 'relayed.app.trips' ($before['relayed.app.trips'] + $size) "$label-flow" | Out-Null
        $relaySeconds = $timer.Elapsed.TotalSeconds
        Save-Resources "$label-before-drain-resources"
        Drain "$label-drain"
        $drainSeconds = $timer.Elapsed.TotalSeconds
        $check = Lake $key $size "$label-parity"
        Assert-Clean $check $size
        if ($check.bronze_count -ne $size) { throw "Unexpected bronze count for workload $size" }
        $file = "$label.json"
        Write-Json (Join-Path $script:Run $file) @{
            status='PASS'; git_sha=$script:Sha; seed=42; warmup='successful fixture baseline'; workload_size=$size
            started_after_baseline=$true; source=$write; relay_seconds=$relaySeconds
            batch_seconds=$drainSeconds; events_per_second=$size/$drainSeconds
            latency_seconds=(Quantiles $check.latency_seconds); reconciliation=$check.comparison
            environment_record='../preflight/preflight.json'; observation_window_seconds=$timer.Elapsed.TotalSeconds
            resource_sample="$label-before-drain-resources.log"; resource_scope='one service snapshot, excludes Spark job'
        }
        $summary.workload_files += $file
    }
    $modes = @('drop','duplicate','delay','reorder','mutate')
    for ($round=1; $round -le 10; $round++) {
        foreach ($mode in $modes) {
            $number = ($round-1)*$modes.Count + [array]::IndexOf($modes,$mode) + 1
            $key = 3000000 + $number
            $label = "fault-{0:d2}-$mode" -f $number
            $trial = @{ experiment_id=$label; trial=$number; scenario=$mode; key=$key; git_sha=$script:Sha; seed=42; start_utc=(Utc); status='FAIL'; versions_artifact='matrix-images.log' }
            $timer = [Diagnostics.Stopwatch]::StartNew()
            try {
                $ordinary = Offset-State "$label-ordinary-offset"
                Source (3050000+$number) insert 1 "$label-ordinary-traffic" | Out-Null
                Wait-Offset 'relayed.app.trips' ($ordinary['relayed.app.trips']+1) "$label-ordinary-relayed" | Out-Null
                $before = Offset-State "$label-offset"
                $expected = switch ($mode) {
                    drop { 'missing until source update' }
                    duplicate { 'two deliveries and one silver row' }
                    delay { 'missing until held outbox row is released' }
                    reorder { 'later update wins over earlier insert' }
                    mutate { 'changed until source update' }
                }
                $trial.expected = $expected
                $arm = Compose @('exec','-T','relay','python','-m','chaos.relay','--db','/var/lib/paritylake/relay.db','arm','--key',"$key",'--mode',$mode,'--expected',$expected,'--delay-seconds','3600') "$label-arm"
                $id = [long]($arm.lines | Select-Object -Last 1)
                $trial.ledger_id = $id
                $trial.injection = "arm $mode on pg.app.trips key $key, then INSERT"
                $trial.failure_utc = Utc
                $trial.source = Source $key insert 1 "$label-inject"
                Wait-Offset 'pg.app.trips' ($before['pg.app.trips'] + 1) "$label-source-offset" | Out-Null
                $deadline = [Diagnostics.Stopwatch]::StartNew()
                do {
                    $fault = Ledger $id "$label-ledger"
                    if ($fault.fault.injected_at) { break }
                    Start-Sleep -Seconds 1
                } while ($deadline.Elapsed.TotalSeconds -lt 120)
                if (-not $fault.fault.injected_at) { throw 'Relay never recorded injection' }
                $trial.injected_utc = [DateTimeOffset]::FromUnixTimeMilliseconds([long]($fault.fault.injected_at*1000)).ToString('o')
                if ($mode -eq 'reorder') { Source $key update 1 "$label-successor" | Out-Null }
                $forwarded = if ($mode -in @('duplicate','reorder')) { 2 } elseif ($mode -eq 'mutate') { 1 } else { 0 }
                if ($forwarded) { Wait-Offset 'relayed.app.trips' ($before['relayed.app.trips'] + $forwarded) "$label-forwarded" | Out-Null }
                Drain "$label-first-drain"
                $first = Lake $key 1 "$label-first-check"
                $trial.first_observation = $first
                if ($mode -in @('drop','delay','mutate')) {
                    $category = if ($mode -eq 'mutate') { 'changed' } else { 'missing' }
                    Assert-Drift $first $key $category
                    $trial.detection_seconds = ([DateTimeOffset]::UtcNow - (Parse-Time $trial.injected_utc)).TotalSeconds
                    if ($mode -eq 'delay') { Ledger $id "$label-release" -Release | Out-Null }
                    else { Source $key update 1 "$label-correction" | Out-Null }
                    Wait-Offset 'relayed.app.trips' ($before['relayed.app.trips'] + $forwarded + 1) "$label-repaired-offset" | Out-Null
                    Drain "$label-repair-drain"
                    $last = Lake $key 1 "$label-repair-check"
                } else { $last = $first }
                Assert-Clean $last 1
                $expectedBronze = if ($mode -in @('duplicate','reorder','mutate')) { 2 } else { 1 }
                if ($last.bronze_count -ne $expectedBronze) { throw "Expected $expectedBronze bronze events" }
                if ($mode -eq 'duplicate' -and (
                    $last.bronze[0].source_lsn -ne $last.bronze[1].source_lsn -or
                    $last.bronze[0].transaction_order -ne $last.bronze[1].transaction_order
                )) { throw 'Duplicate deliveries do not share the source event version' }
                if ($mode -eq 'reorder' -and ($last.bronze[0].op -ne 'u' -or $last.bronze[1].op -ne 'c')) { throw 'Reordered delivery was not proven' }
                $trial.recovered = $last
                $trial.ledger = Ledger $id "$label-final-ledger"
                $trial.recovery_seconds = ([DateTimeOffset]::UtcNow - (Parse-Time $trial.injected_utc)).TotalSeconds
                $trial.status = 'PASS'
            } catch {
                $trial.error = $_.Exception.Message
                throw
            } finally {
                $trial.end_utc = Utc
                $trial.elapsed_seconds = $timer.Elapsed.TotalSeconds
                $file = "$label.json"
                Write-Json (Join-Path $script:Run $file) $trial
                $summary.trial_files += $file
            }
        }
    }
    $services = @('postgres','connect','kafka','relay','catalog','object-store','spark','delete','schema-evolution')
    for ($i=0; $i -lt $services.Count; $i++) {
        $scenario=$services[$i]
        $key=3100000+$i
        $label="additional-$scenario"
        $trial=@{ experiment_id=$label; scenario=$scenario; key=$key; start_utc=(Utc); status='FAIL'; git_sha=$script:Sha; seed=42; versions_artifact='matrix-images.log'; expected='zero reconciled loss or duplicated offsets after recovery' }
        $timer=[Diagnostics.Stopwatch]::StartNew()
        try {
            $before=Offset-State "$label-offset"
            $expectedEvents=1
            if ($scenario -in $script:ServerServices) {
                $trial.injection="docker compose stop $scenario; restore Compose services; resume the same checkpoint"
                Compose @('stop',$scenario) "$label-stop" | Out-Null
                $stopped=(Compose @('ps','-q',$scenario) "$label-stopped").lines
                if (($stopped -join '').Trim()) { throw "$scenario still running after stop" }
                $trial.failure_utc=Utc
                if ($scenario -eq 'postgres') {
                    $probe=Invoke-ParityDocker ($script:ComposeArgs+@('run','--rm','--no-deps','-T','integration','python','-m','tests.windows_trial','source',"$key",'insert')) "$label-offline-source" -AllowFailure
                    if ($probe.code -eq 0) { throw 'PostgreSQL accepted writes while stopped' }
                    $trial.expected_failure_exit=$probe.code
                } else {
                    $trial.source=Source $key insert 1 "$label-backlog"
                }
                Compose (@('up','-d','--wait','--wait-timeout','240')+$script:ServerServices) "$label-restart" | Out-Null
                Check-Identity "$label-ready" | Out-Null
                if ($scenario -eq 'postgres') { $trial.source=Source $key insert 1 "$label-recovered-source" }
            } elseif ($scenario -eq 'spark') {
                $name="$script:Project-crash-probe"
                $trial.injection="docker kill --signal KILL $name; resume the same checkpoint"
                Compose @('run','--detach','--no-deps','--name',$name,'lake','streaming/consume.py','--prefix','relayed','--checkpoint',$script:Checkpoint) "$label-start" | Out-Null
                $startup=[Diagnostics.Stopwatch]::StartNew()
                do {
                    $sparkLog=(Invoke-ParityDocker @('logs','--tail','150',$name) "$label-startup").lines
                    if (($sparkLog -join " ") -match 'MicroBatchExecution') { break }
                    Start-Sleep -Seconds 2
                } while ($startup.Elapsed.TotalSeconds -lt 120)
                if (($sparkLog -join " ") -notmatch 'MicroBatchExecution') { throw 'Spark streaming startup was not observed' }
                Invoke-ParityDocker @('kill','--signal','KILL',$name) "$label-kill" | Out-Null
                $trial.failure_utc=Utc
                $exit=(Invoke-ParityDocker @('inspect','--format','{{.State.ExitCode}}',$name) "$label-exit").lines -join ''
                if ($exit.Trim() -ne '137') { throw "Expected killed Spark process exit 137, got $exit" }
                $trial.expected_failure_exit=137
                $trial.source=Source $key insert 1 "$label-source"
            } else {
                $trial.source=Source $key insert 1 "$label-source"
                $trial.failure_utc=Utc
                $expectedEvents=2
                if ($scenario -eq 'delete') {
                    $trial.injection='DELETE the new row in PostgreSQL'
                    Source $key delete 1 "$label-delete" | Out-Null
                } else {
                    $trial.injection='ALTER TABLE app.trips ADD COLUMN paritylake_optional text; UPDATE the same key'
                    Source $key evolve 1 "$label-schema" | Out-Null
                    Source $key update 1 "$label-update" | Out-Null
                }
            }
            Wait-Offset 'relayed.app.trips' ($before['relayed.app.trips'] + $expectedEvents) "$label-offset-recovered" | Out-Null
            Drain "$label-drain"
            $check=Lake $key 1 "$label-parity"
            Assert-Clean $check 1
            if ($check.bronze_count -ne $expectedEvents) { throw 'Unexpected bronze count' }
            if ($scenario -eq 'delete' -and (!$check.silver[0].is_deleted -or $check.bronze[1].op -ne 'd')) {
                throw 'Source delete and silver tombstone did not agree'
            }
            if ($scenario -eq 'schema-evolution' -and ($check.bronze[0].schema_id -eq $check.bronze[1].schema_id -or $check.silver[0].fare_amount -ne '11.00')) {
                throw 'New Registry schema and valid silver update were not both observed'
            }
            $trial.recovered=$check
            $trial.recovery_seconds=([DateTimeOffset]::UtcNow-(Parse-Time $trial.failure_utc)).TotalSeconds
            $trial.status='PASS'
        } catch {
            $trial.error=$_.Exception.Message
            throw
        } finally {
            $trial.end_utc=Utc
            $trial.elapsed_seconds=$timer.Elapsed.TotalSeconds
            $file="$label.json"
            Write-Json (Join-Path $script:Run $file) $trial
            $summary.trial_files += $file
        }
    }
    $summary.additional_trials_passed = $services.Count
    $summary.relay_trials_passed = 50
    $detected=@($summary.trial_files | Where-Object { $_ -match '^fault-\d+-(drop|delay|mutate)\.json$' } | ForEach-Object { (Read-Json (Join-Path $script:Run $_)).detection_seconds })
    $summary.detection_seconds=Quantiles $detected
    $summary.status = 'PASS'
} catch {
    $summary.error = $_.Exception.Message
    throw
} finally {
    $summary.ended_utc = Utc
    Write-Json $summaryPath $summary
    $lock.Dispose()
}
Write-Output "Controlled relay matrix PASS. Evidence: $script:Run"
