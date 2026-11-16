#requires -Version 7.0
param([string]$EvidenceRoot = (Join-Path $PSScriptRoot '../evidence/windows'))
. "$PSScriptRoot/ParityLake.Common.ps1"
Open-Run $EvidenceRoot preflight -Create
$lock = Lock-Run
$report = @{ status='FAIL'; git_sha=$script:Sha; started_utc=(Utc) }
try {
    $info = (Invoke-ParityDocker @('info','--format','{"OSType":{{json .OSType}},"NCPU":{{json .NCPU}},"MemTotal":{{json .MemTotal}}}') 'docker-info')
    $engine = Result-Json $info
    $drive = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($script:Run))
    $report.resources = @{ docker_os=$engine.OSType; cpus=$engine.NCPU; ram_bytes=$engine.MemTotal; evidence_disk_free_bytes=$drive.AvailableFreeSpace }
    if ($engine.OSType -ne 'linux' -or $engine.NCPU -lt 2 -or $engine.MemTotal -lt 8GB -or $drive.AvailableFreeSpace -lt 15GB) {
        throw 'Preflight needs Linux containers, 2 Docker CPUs, 8 GiB Docker RAM and 15 GiB free on the evidence drive.'
    }
    Invoke-ParityDocker @('system','df') 'docker-disk' | Out-Null
    $disk=(Invoke-ParityDocker @('run','--rm','busybox:1.37.0','df','-Pk','/') 'docker-filesystem').lines |
        Where-Object { $_ -match '\s/\s*$' } | Select-Object -Last 1
    if (-not $disk) { throw 'Unable to measure Docker filesystem free space' }
    $report.resources.docker_disk_free_bytes=[long](($disk.Trim() -split '\s+')[3])*1024
    if ($report.resources.docker_disk_free_bytes -lt 15GB) { throw 'Docker filesystem needs 15 GiB free' }
    Start-Stack
    Check-Identity 'preflight' | Out-Null
    $key = 900002
    Assert-Clean (Lake $key 1 'preflight-independent') 1
    $dbt = @('run','--rm','--no-deps','-T','--entrypoint','/bin/sh','lake','/opt/paritylake/run-dbt.sh')
    Compose ($dbt + @('debug')) 'preflight-dbt-debug' | Out-Null
    Compose ($dbt + @('build','--target-path','/tmp/paritylake-dbt-target','--full-refresh')) 'preflight-dbt-full' | Out-Null
    Compose ($dbt + @('build','--target-path','/tmp/paritylake-dbt-target')) 'preflight-dbt-incremental' | Out-Null
    Compose @('run','--rm','--no-deps','-T','lake','tests/dbt_totals_probe.py') 'preflight-dbt-totals' | Out-Null
    $report.status = 'PASS'
} catch {
    $report.error = $_.Exception.Message
    throw
} finally {
    $report.ended_utc = Utc
    Write-Json (Join-Path $script:Run 'preflight.json') $report
    $lock.Dispose()
}
Write-Output "Windows Docker preflight PASS. Evidence: $script:Run"
