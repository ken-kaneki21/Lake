param([string]$PgBin = (Split-Path (Get-Command psql).Source))
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot
$probe = Join-Path $root ('data/pg-test-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $probe -Force | Out-Null
$pwfile = Join-Path $probe 'bootstrap.secret'
[guid]::NewGuid().ToString('N') | Set-Content -Encoding ascii -LiteralPath $pwfile
$cluster = Join-Path $probe 'cluster'
try {
    & (Join-Path $PgBin 'initdb.exe') -D $cluster -U paritylake --auth=scram-sha-256 --pwfile=$pwfile --encoding=UTF8 --no-locale
    if ($LASTEXITCODE -ne 0) { throw 'initdb failed' }
} finally {
    Remove-Item -LiteralPath $pwfile -Force
}

$schema = Get-Content -Raw -LiteralPath (Join-Path $root 'source_db/schema.sql')
$audit = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'source_audit.sql')
$fixture = @"
INSERT INTO app.trips (trip_id,pickup_ts,fare_amount,tip_amount,total_amount)
VALUES (1,'2024-01-01 00:00+00',12.34,2.00,14.34), (100,'2024-01-01 00:01+00',5.00,0,5.00);
DELETE FROM app.trips WHERE trip_id=100;
"@
$query = (Get-Content -Raw -LiteralPath (Join-Path $root 'recon/source_buckets.sql')).Trim().TrimEnd(';')
$query = $query.Replace('%(window_start)s', "TIMESTAMPTZ '2024-01-01 00:00+00'")
$query = $query.Replace('%(window_end)s', "TIMESTAMPTZ '2024-01-01 00:05+00'")
$out = Join-Path $probe 'buckets.csv'
$copy = "COPY ($query) TO '" + $out.Replace('\','/').Replace("'","''") + "' WITH (FORMAT csv);"
# In single-user mode -j treats a blank line as a command delimiter.
$sql = (($schema, $audit, $fixture, $copy -join "`n") -replace '(?m)^\s*\r?\n', '') + "`n`n"
$result = $sql | & (Join-Path $PgBin 'postgres.exe') --single -j -D $cluster postgres 2>&1
$exitCode = $LASTEXITCODE
$result | ForEach-Object { "$_" }
if ($exitCode -ne 0 -or ($result -match '\b(ERROR|FATAL|PANIC):') -or -not (Test-Path -LiteralPath $out)) {
    throw 'Single-user PostgreSQL assertions failed'
}

$rows = Get-Content -LiteralPath $out
Push-Location $root
try {
    $expected = python -c "from decimal import Decimal as D; from recon.hashing import fingerprint as f; print(f(1,D('12.34'),D('2.00'),False)); print(f(100,D('5.00'),D('0.00'),True))"
    if ($LASTEXITCODE -ne 0 -or $rows.Count -ne 256 -or $rows[196] -ne "c4,1,1234,200,0,$($expected[0])" -or $rows[248] -ne "f8,0,0,0,1,$($expected[1])") {
        throw 'PostgreSQL/Python fingerprint or financial aggregation mismatch'
    }
} finally {
    Pop-Location
}
Write-Output "POSTGRES_BUCKETS=$($rows.Count)"
Write-Output "POSTGRES_LIVE_BUCKET=$($rows[196])"
Write-Output "POSTGRES_DELETE_BUCKET=$($rows[248])"
