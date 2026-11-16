#requires -Version 7.0
param([string]$EvidenceRoot = (Join-Path $PSScriptRoot '../evidence/windows'))
. "$PSScriptRoot/ParityLake.Common.ps1"
$root = [IO.Path]::GetFullPath($EvidenceRoot)
$staging = Join-Path $root ("export-" + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ'))
New-Item -ItemType Directory -Path $staging | Out-Null
$manifest = @{ collected_utc=(Utc); base_ci_run='https://github.com/ken-kaneki21/Lake/actions/runs/43881532134'; stages=@{}; configuration=@{} }
foreach ($file in @('docker/compose.source.yml','docker/compose.cdc.yml','docker/compose.lake.yml','.github/workflows/docker-cdc.yml')) {
    $manifest.configuration[$file] = (Get-FileHash -LiteralPath (Join-Path $script:Repo $file) -Algorithm SHA256).Hash
}
foreach ($mode in @('preflight','fault','clean')) {
    if (-not (Test-Path -LiteralPath (Join-Path $root "$mode/.env"))) { continue }
    Open-Run $root $mode
    $dest = Join-Path $staging $mode
    $lock = $null
    try {
        $lock = Lock-Run
        $logs = Invoke-ParityDocker ($script:ComposeArgs + @('logs','--no-color','--tail','300')) 'collect-compose' -AllowFailure
        Invoke-ParityDocker @('version') 'collect-docker-version' -AllowFailure | Out-Null
        $ids = (Invoke-ParityDocker ($script:ComposeArgs + @('ps','-a','-q')) 'collect-ids' -AllowFailure).lines |
            Where-Object { $_ -match '^[a-f0-9]{12,64}$' }
        if (@($ids).Count) {
            Invoke-ParityDocker (@('inspect','--format','{{.Name}} {{.Config.Image}} {{.Image}}') + $ids) 'collect-images' -AllowFailure | Out-Null
            Invoke-ParityDocker (@('stats','--no-stream','--format','{{json .}}') + $ids) 'collect-stats' -AllowFailure | Out-Null
        }
        $manifest.stages[$mode] = @{ project=$script:Project; git_sha=$script:Sha; log_exit=$logs.code }
    } catch {
        $manifest.stages[$mode] = @{ collector_error=$_.Exception.Message }
    } finally {
        if ($lock) { $lock.Dispose() }
    }
    New-Item -ItemType Directory -Path $dest | Out-Null
    foreach ($file in Get-ChildItem -LiteralPath $script:Run -File | Where-Object Extension -in @('.json','.jsonl','.log')) {
        $body=Get-Content -LiteralPath $file.FullName -Raw
        if ($null -eq $body) { $body='' }
        foreach ($secret in $script:Credentials.Values) { $body=$body.Replace($secret,'[REDACTED]') }
        Set-Content -LiteralPath (Join-Path $dest $file.Name) -Value $body -NoNewline -Encoding utf8
    }
}
$manifest.files=@{}
foreach ($file in Get-ChildItem -LiteralPath $staging -Recurse -File) {
    $relative=[IO.Path]::GetRelativePath($staging,$file.FullName)
    $manifest.files[$relative]=(Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash
}
Write-Json (Join-Path $staging 'evidence-manifest.json') $manifest
$archive="$staging.zip"
Compress-Archive -Path (Join-Path $staging '*') -DestinationPath $archive
Write-Output $archive
