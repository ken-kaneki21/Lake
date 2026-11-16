#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$script:Repo = Split-Path $PSScriptRoot
$script:ServerServices = @('postgres','kafka','schema-registry','connect','relay','object-store','catalog')
$script:Checkpoint = 's3a://checkpoints/windows-trips'
$script:Credentials = @{}

function Hex([byte[]]$Bytes) { [BitConverter]::ToString($Bytes).Replace('-','').ToLowerInvariant() }
function Utc { [DateTimeOffset]::UtcNow.ToString('o') }
function Parse-Time($Value) {
    if ($Value -is [datetime]) { return [DateTimeOffset]::new($Value) }
    if ($Value -is [DateTimeOffset]) { return $Value }
    [DateTimeOffset]::Parse($Value, [Globalization.CultureInfo]::InvariantCulture)
}

function Write-Json($Path, $Value) {
    $temp = "$Path.tmp"
    $Value | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $temp -Encoding utf8
    Move-Item -LiteralPath $temp -Destination $Path -Force
}

function Read-Json($Path) { Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable }

function Open-Run([string]$Root, [string]$Mode, [switch]$Create) {
    $script:Evidence = [IO.Path]::GetFullPath($Root)
    $script:Run = Join-Path $script:Evidence $Mode
    $hasher = [Security.Cryptography.SHA256]::Create()
    try { $scope = (Hex ($hasher.ComputeHash([Text.Encoding]::UTF8.GetBytes($script:Evidence.ToLowerInvariant())))).Substring(0,8) }
    finally { $hasher.Dispose() }
    $script:Project = "paritylake-$Mode-$scope"
    $script:Sha = (& git -C $script:Repo rev-parse HEAD)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve repository SHA' }
    foreach ($relative in @('docker/object-store.sh','docker/run-dbt.sh')) {
        if ([IO.File]::ReadAllText((Join-Path $script:Repo $relative)).Contains([char]13)) {
            throw "$relative has CRLF. Restore LF for this checkout before starting Docker."
        }
    }
    $script:ComposeArgs = @('compose','--project-directory',(Join-Path $script:Repo 'docker'),'-p',$script:Project)
    foreach ($file in @('compose.source.yml','compose.cdc.yml','compose.lake.yml')) {
        $script:ComposeArgs += @('-f',(Join-Path $script:Repo "docker/$file"))
    }
    $script:ComposeArgs += @('--profile','integration','--profile','lake','--profile','relay')
    if ($Create) { New-Item -ItemType Directory -Path $script:Run -Force | Out-Null }
    if (-not (Test-Path -LiteralPath $script:Run -PathType Container)) { throw "Missing run directory: $script:Run" }
    $secretPath = Join-Path $script:Run '.env'
    $script:Credentials = @{}
    if (-not (Test-Path -LiteralPath $secretPath)) {
        if (-not $Create) { throw 'Project credentials are missing' }
        foreach ($name in @('PGPASSWORD','CDC_PGPASSWORD','MINIO_ROOT_USER','MINIO_ROOT_PASSWORD')) {
            $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
            try {
                $bytes = [byte[]]::new(24)
                $rng.GetBytes($bytes)
                $script:Credentials[$name] = Hex $bytes
            } finally { $rng.Dispose() }
        }
        $script:Credentials.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" } |
            Set-Content -LiteralPath $secretPath -Encoding ascii
    } else {
        foreach ($line in Get-Content -LiteralPath $secretPath) {
            if ($line -notmatch '^(PGPASSWORD|CDC_PGPASSWORD|MINIO_ROOT_USER|MINIO_ROOT_PASSWORD)=([a-f0-9]{48})$') { throw 'Invalid local credentials file' }
            $script:Credentials[$Matches[1]] = $Matches[2]
        }
        if ($script:Credentials.Count -ne 4) { throw 'Incomplete local credentials file' }
    }
}

function Lock-Run {
    $path = Join-Path $script:Run 'operation.lock'
    try {
        [IO.File]::Open($path, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    } catch {
        throw "Another validation process owns $script:Project"
    }
}

function Invoke-ParityDocker([string[]]$Arguments, [string]$Label, [switch]$AllowFailure) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $started = Utc
    $previous = @{}
    try {
        foreach ($name in $script:Credentials.Keys) {
            $previous[$name] = [Environment]::GetEnvironmentVariable($name)
            [Environment]::SetEnvironmentVariable($name, $script:Credentials[$name])
        }
        $raw = @(& docker @Arguments 2>&1)
        $code = $LASTEXITCODE
    } finally {
        foreach ($name in $previous.Keys) { [Environment]::SetEnvironmentVariable($name, $previous[$name]) }
    }
    $lines = @($raw | ForEach-Object {
        $line = [string]$_
        foreach ($secret in $script:Credentials.Values) { $line = $line.Replace($secret, '[REDACTED]') }
        $line
    })
    $log = Join-Path $script:Run "$Label.log"
    $lines | Set-Content -LiteralPath $log -Encoding utf8
    $entry = @{ step=$Label; started_utc=$started; ended_utc=(Utc); seconds=$watch.Elapsed.TotalSeconds; exit_code=$code }
    Add-Content -LiteralPath (Join-Path $script:Run 'commands.jsonl') -Value ($entry | ConvertTo-Json -Compress)
    if ($code -ne 0 -and -not $AllowFailure) { throw "Docker step $Label exited $code. See $log" }
    @{ code=$code; lines=$lines; seconds=$watch.Elapsed.TotalSeconds }
}

function Compose([string[]]$Arguments, [string]$Label) { Invoke-ParityDocker ($script:ComposeArgs + $Arguments) $Label }

function Result-Json($Result) {
    $line = $Result.lines | Where-Object { $_.StartsWith('{') } | Select-Object -Last 1
    if (-not $line) { throw 'Container did not provide JSON result' }
    $line | ConvertFrom-Json -AsHashtable
}

function Source([long]$Key, [string]$Action, [int]$Count, [string]$Label) {
    Result-Json (Compose @('run','--rm','--no-deps','-T','integration','python','-m','tests.windows_trial','source',"$Key",$Action,'--count',"$Count") $Label)
}

function Drain([string]$Label) {
    Compose @('run','--rm','--no-deps','-T','lake','streaming/consume.py','--prefix','relayed','--available-now','--checkpoint',$script:Checkpoint) $Label | Out-Null
}

function Lake([long]$Key, [int]$Count, [string]$Label) {
    Result-Json (Compose @('run','--rm','--no-deps','-T','lake','tests/windows_trial.py','lake',"$Key",'--count',"$Count") $Label)
}

function Offset-State([string]$Label) {
    Result-Json (Compose @('run','--rm','--no-deps','-T','integration','python','-m','tests.windows_trial','offsets') $Label)
}

function Wait-Offset([string]$Topic, [long]$Minimum, [string]$Label, [int]$TimeoutSeconds=120) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    do {
        $offsets = Offset-State $Label
        if ([long]$offsets[$Topic] -ge $Minimum) { return $offsets }
        Start-Sleep -Seconds 2
    } while ($watch.Elapsed.TotalSeconds -lt $TimeoutSeconds)
    throw "$Topic did not reach offset $Minimum. See $Label.log"
}

function Require-Pass([string]$Root, [string]$Mode, [string]$File) {
    $path = Join-Path $Root "$Mode/$File"
    if (-not (Test-Path -LiteralPath $path)) { throw "Run the $Mode stage first. Missing $path" }
    $value = Read-Json $path
    if ($value.status -ne 'PASS' -or $value.git_sha -ne $script:Sha) { throw "$Mode must PASS on this Git SHA" }
}

function Start-Stack {
    $ready = Join-Path $script:Run 'baseline.json'
    if (Test-Path -LiteralPath $ready) {
        $saved = Read-Json $ready
        if ($saved.status -ne 'PASS' -or $saved.git_sha -ne $script:Sha) { throw 'Baseline is incompatible with this checkout' }
        Compose (@('up','-d','--wait','--wait-timeout','240') + $script:ServerServices) 'resume-stack' | Out-Null
        return
    }
    $begin = Join-Path $script:Run 'initializing.json'
    if (Test-Path -LiteralPath $begin) { throw "Initialization was interrupted. Preserve this evidence and use a new validation evidence root after stopping $script:Project." }
    $containers = (Compose @('ps','-a','-q') 'existing-containers').lines
    $volumes = (Invoke-ParityDocker @('volume','ls','-q','--filter',"label=com.docker.compose.project=$script:Project") 'existing-volumes').lines
    if (($containers -join '').Trim() -or ($volumes -join '').Trim()) {
        throw "Docker project $script:Project has existing state. Use its original evidence root."
    }
    Write-Json $begin @{ started_utc=(Utc); git_sha=$script:Sha }
    Compose @('config','--quiet') 'compose-config' | Out-Null
    Compose @('build','integration','connect','lake') 'build' | Out-Null
    Compose @('up','-d','--wait','--wait-timeout','240','postgres','kafka','schema-registry','connect','catalog') 'start' | Out-Null
    foreach ($prefix in @('pg','relayed')) {
        foreach ($entity in @('trips','drivers','zones')) {
            Compose @('exec','-T','kafka','kafka-topics','--bootstrap-server','kafka:29092','--create','--if-not-exists','--partitions','1','--replication-factor','1','--topic',"$prefix.app.$entity") "topic-$prefix-$entity" | Out-Null
        }
    }
    Compose @('run','--rm','--no-deps','-T','integration','python','-m','tests.cdc_probe','initial') 'cdc-initial' | Out-Null
    Compose @('restart','connect') 'connect-restart' | Out-Null
    Compose @('up','-d','--wait','--wait-timeout','180','connect') 'connect-ready' | Out-Null
    Compose @('run','--rm','--no-deps','-T','integration','python','-m','tests.cdc_probe','restart') 'cdc-restart' | Out-Null
    Compose @('up','-d','relay') 'relay-start' | Out-Null
    Compose @('run','--rm','--no-deps','-T','integration','python','-m','tests.cdc_probe','relay-baseline') 'relay-baseline' | Out-Null
    Drain 'baseline-drain'
    $baseline = Result-Json (Compose @('run','--rm','--no-deps','-T','lake','tests/lake_probe.py','baseline') 'baseline-probe')
    Write-Json $ready @{ status='PASS'; git_sha=$script:Sha; ended_utc=(Utc); result=$baseline }
}

function Check-Identity([string]$Label) {
    $ids = (Compose (@('ps','-q') + $script:ServerServices) "$Label-ids").lines
    if (@($ids).Count -ne $script:ServerServices.Count) { throw 'A required service is missing' }
    $format = '{{.Name}} {{.Id}} {{.State.StartedAt}} {{.RestartCount}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}'
    $rows = (Invoke-ParityDocker (@('inspect','--format',$format) + $ids) "$Label-health").lines | Sort-Object
    if (@($rows | Where-Object { $_ -notmatch ' running (healthy|none)$' }).Count) { throw 'A required service is unhealthy' }
    $rows -join "`n"
}

function Assert-Clean($result, [int]$count) {
    if ($result.comparison.drift_windows -ne 0 -or $result.source_count -ne $count -or
        $result.silver_count -ne $count -or $result.silver_unique -ne $count -or $result.duplicate_bronze_offsets -ne 0) {
        throw "Failed independent parity or row identity check: $($result.comparison.run_id)"
    }
}

function Quantiles([double[]]$Values) {
    if (-not $Values.Count) { return @{ samples=0 } }
    if (@($Values | Where-Object { $_ -lt 0 }).Count) { throw 'Negative latency indicates a clock or timestamp issue' }
    $ordered = @($Values | Sort-Object)
    $result = @{ samples=$ordered.Count }
    foreach ($p in @(50,95,99)) { $result["p$p"] = $ordered[[int][Math]::Ceiling($ordered.Count * $p / 100) - 1] }
    $result
}
