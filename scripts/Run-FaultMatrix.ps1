#requires -Version 7.0
param(
    [string]$EvidenceRoot = (Join-Path $PSScriptRoot '../evidence/windows'),
    [switch]$ResumePostgresRecovery
)
. "$PSScriptRoot/ParityLake.Common.ps1"
$historicalSha = '594083bada7b77b003f1d2147e132ca146df9ba3'
if ($ResumePostgresRecovery) {
    Open-Run $EvidenceRoot fault
    $lock = Lock-Run
    try {
        $original = $script:Run
        $old = Read-Json (Join-Path $original 'matrix.json')
        $failure = Read-Json (Join-Path $original 'additional-postgres.json')
        $baseline = Read-Json (Join-Path $original 'baseline.json')
        $preflight = Read-Json (Join-Path $script:Evidence 'preflight/preflight.json')
        if ($old.git_sha -ne $historicalSha -or $old.status -ne 'FAIL' -or
            $failure.git_sha -ne $historicalSha -or $failure.scenario -ne 'postgres' -or $failure.status -ne 'FAIL' -or
            $failure.error -notlike 'relayed.app.trips did not reach offset *' -or
            $old.trial_files.Count -ne 51 -or $old.trial_files[-1] -ne 'additional-postgres.json' -or
            $baseline.git_sha -ne $historicalSha -or $baseline.status -ne 'PASS' -or
            $preflight.git_sha -ne $historicalSha -or $preflight.status -ne 'PASS') {
            throw 'Expected the known failed PostgreSQL trial after a passing baseline and preflight.'
        }
        $inputs = @('matrix.json','additional-postgres.json','additional-postgres-offset-recovered.log','baseline.json','matrix-images.log')
        $modes = @('drop','duplicate','delay','reorder','mutate')
        for ($n=1; $n -le 50; $n++) {
            $name = 'fault-{0:d2}-{1}.json' -f $n,$modes[($n-1)%5]
            $saved = Read-Json (Join-Path $original $name)
            if ($old.trial_files[$n-1] -ne $name -or $saved.status -ne 'PASS' -or
                $saved.git_sha -ne $historicalSha -or $saved.trial -ne $n) {
                throw "Original relay trial is not a matching PASS: $name"
            }
            $inputs += $name
        }
        foreach ($size in @(100,1000)) {
            $name = "benchmark-$size.json"
            $saved = Read-Json (Join-Path $original $name)
            if ($name -notin $old.workload_files -or $saved.status -ne 'PASS' -or
                $saved.git_sha -ne $historicalSha -or $saved.workload_size -ne $size) {
                throw "Original benchmark is not a matching PASS: $name"
            }
            $inputs += $name
        }
        $inputs += '../preflight/preflight.json'
        $fingerprints = @{}
        foreach ($name in $inputs) {
            $fingerprints[$name] = (Get-FileHash -LiteralPath (Join-Path $original $name)).Hash
        }
        # Keep the exact Compose project and checkpoints. Redirect all new logs.
        $epoch = [guid]::NewGuid().ToString('N')
        $script:Run = Join-Path $script:Evidence "fault-recovery-$epoch"
        New-Item -ItemType Directory -Path $script:Run | Out-Null
        Write-Host "Recovery evidence: $script:Run"
        $runnerBlob = & git -C $script:Repo hash-object '--' $PSCommandPath
        if ($LASTEXITCODE -ne 0) { throw 'Cannot fingerprint the recovery runner.' }
        Write-Json (Join-Path $script:Run 'historical-inputs.json') @{
            original_sha=$historicalSha; original_matrix_status='FAIL'; original_files_sha256=$fingerprints
            runner_blob=$runnerBlob; checkout_sha=$script:Sha; compose_project=$script:Project
        }
    } catch {
        $lock.Dispose()
        throw
    }
} else {
Open-Run $EvidenceRoot preflight
Require-Pass $script:Evidence preflight preflight.json
$previousLock = Lock-Run
try { Compose @('stop') 'before-fault-stop' | Out-Null } finally { $previousLock.Dispose() }
Open-Run $EvidenceRoot fault -Create
$lock = Lock-Run
}
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

function Wait-PostgresCdc($Before, [string]$Label, [int]$TimeoutSeconds=180, [int]$PollSeconds=2, [int]$RestartCooldownSeconds=15) {
    $baseUrl = 'http://localhost:8083/connectors/pg-app'
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $attempts = 0
    $poll = 0
    $lastRestart = -30
    do {
        $poll++
        $state = Result-Json (Compose @('exec','-T','connect','curl','-fsS','--max-time','5',"$baseUrl/status") "$Label-task-$poll")
        $tasks = @($state.tasks | Where-Object { $_.id -eq 0 })
        if ($state.name -ne 'pg-app' -or $tasks.Count -ne 1) {
            throw 'Kafka Connect did not report exactly one pg-app task 0.'
        }
        $task = $tasks[0].state
        $offsets = Offset-State "$Label-offsets-$poll"
        $bothAdvanced = $true
        foreach ($topic in @('pg.app.trips','relayed.app.trips')) {
            if ($null -eq $offsets[$topic] -or $null -eq $Before[$topic]) { throw "Missing offset for $topic." }
            $delta = [long]$offsets[$topic] - [long]$Before[$topic]
            if ($delta -lt 0 -or $delta -gt 1) { throw "Unexpected offset change for $topic ($delta)." }
            if ($delta -ne 1) { $bothAdvanced = $false }
        }
        if ($state.connector.state -eq 'RUNNING' -and $task -eq 'RUNNING' -and $bothAdvanced) {
            return @{ offsets=$offsets; restart_attempts=$attempts; polls=$poll; seconds=$watch.Elapsed.TotalSeconds }
        }
        if ($task -eq 'FAILED' -and $state.connector.state -eq 'RUNNING' -and
            ($watch.Elapsed.TotalSeconds - $lastRestart) -ge $RestartCooldownSeconds) {
            if ($attempts -ge 3) { throw 'pg-app task 0 remained FAILED after three restart attempts.' }
            $attempts++
            $lastRestart = $watch.Elapsed.TotalSeconds
            $restart = Invoke-ParityDocker ($script:ComposeArgs + @('exec','-T','connect','curl','-fsS','--max-time','5','-X','POST',"$baseUrl/tasks/0/restart")) "$Label-task-restart-$attempts" -AllowFailure
            if ($restart.code -ne 0 -and $attempts -eq 3) { throw "pg-app task restart failed (exit $($restart.code))." }
        } elseif ($task -notin @('FAILED','RUNNING','RESTARTING','UNASSIGNED') -or
                  $state.connector.state -notin @('RUNNING','RESTARTING','UNASSIGNED')) {
            throw "Unexpected pg-app state: connector $($state.connector.state), task $task."
        }
        if ($PollSeconds) { Start-Sleep -Seconds $PollSeconds }
    } while ($watch.Elapsed.TotalSeconds -lt $TimeoutSeconds)
    throw "Timed out waiting for pg-app task 0 and both Kafka offsets. See $Label-task-* and $Label-offsets-*."
}

if (Test-Path -LiteralPath $summaryPath) {
    try { Require-Pass $script:Evidence fault matrix.json } finally { $lock.Dispose() }
    Write-Output "Matrix already passed. Evidence: $script:Run"
    return
}
try {
    if ($ResumePostgresRecovery) {
        $summary.original_git_sha = $historicalSha
        $summary.scope = 'additional scenarios only'
        $summary.runner_blob = $runnerBlob
        Check-Identity 'resume-existing-project' | Out-Null
        # Drain any already forwarded event from the manually recovered trial before the new outage.
        Drain 'resume-drain'
        $previous = Lake ([long]$failure.key) 1 'original-event-reconciliation'
        Assert-Clean $previous 1
        if ($previous.bronze_count -ne 1) { throw 'Original PostgreSQL event must appear once in bronze.' }
        Write-Json (Join-Path $script:Run 'original-event-reconciliation.json') $previous
    } else {
    Start-Stack
    }
    Invoke-ParityDocker (@('inspect','--format','{{.Name}} {{.Config.Image}} {{.Image}}') + (Compose (@('ps','-q') + $script:ServerServices) 'matrix-initial-ids').lines) 'matrix-images' | Out-Null
    if (-not $ResumePostgresRecovery) {
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
    }
    $services = @('postgres','connect','kafka','relay','catalog','object-store','spark','delete','schema-evolution')
    for ($i=0; $i -lt $services.Count; $i++) {
        $scenario=$services[$i]
        $key=3100000+$i
        if ($ResumePostgresRecovery) {
            # A separate key range preserves original rows and lets a new attempt use new keys.
            $key=400000000000L + [Convert]::ToInt64($epoch.Substring(0,9),16)*10 + $i
        }
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
                if ($scenario -eq 'postgres') {
                    $trial.offsets_before = $before
                    $trial.source=Source $key insert 1 "$label-recovered-source"
                    $trial.connect_recovery = Wait-PostgresCdc $before $label
                }
            } elseif ($scenario -eq 'spark') {
                $name="$script:Project-crash-probe"
                if ($ResumePostgresRecovery) { $name += "-" + $epoch.Substring(0,9) }
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
            if ($scenario -ne 'postgres') {
                Wait-Offset 'relayed.app.trips' ($before['relayed.app.trips'] + $expectedEvents) "$label-offset-recovered" | Out-Null
            }
            Drain "$label-drain"
            $check=Lake $key 1 "$label-parity"
            Assert-Clean $check 1
            if ($check.bronze_count -ne $expectedEvents) { throw 'Unexpected bronze count' }
            if ($scenario -eq 'postgres' -and ($check.bronze[0].op -ne 'c' -or
                [long]$check.bronze[0].offset -ne ([long]$trial.connect_recovery.offsets['relayed.app.trips'] - 1))) {
                throw 'The reconciled insert did not match the recovered relay offset.'
            }
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
    if ($ResumePostgresRecovery) {
        foreach ($name in $inputs) {
            if ((Get-FileHash -LiteralPath (Join-Path $original $name)).Hash -ne $fingerprints[$name]) {
                throw "Historical evidence changed: $name"
            }
        }
    } else {
    $summary.relay_trials_passed = 50
    $detected=@($summary.trial_files | Where-Object { $_ -match '^fault-\d+-(drop|delay|mutate)\.json$' } | ForEach-Object { (Read-Json (Join-Path $script:Run $_)).detection_seconds })
    $summary.detection_seconds=Quantiles $detected
    }
    $summary.status = 'PASS'
} catch {
    $summary.error = $_.Exception.Message
    throw
} finally {
    $summary.ended_utc = Utc
    Write-Json $summaryPath $summary
    $lock.Dispose()
}
if ($ResumePostgresRecovery) { Write-Output "Additional recovery scenarios PASS. Evidence: $script:Run" }
else { Write-Output "Controlled relay matrix PASS. Evidence: $script:Run" }
