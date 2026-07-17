<#
.SYNOPSIS
  QuantVector daily pipeline. Runs the four pipeline stages in order and logs
  EVERYTHING to the lakehouse logs subdirectory. Runs unattended at 02:00
  Central via the "QuantVector Daily Pipeline" scheduled task, which fires
  Tue-Sat (so each run processes a Mon-Fri session; Sun/Mon mornings would have
  no new equity data). On top of that day-of-week schedule, the script skips
  any run whose prior session was an NYSE holiday — see the holiday guard below
  (consults the shared `trading_calendar` table; -Force overrides).

.DESCRIPTION
  Stages (each is best-effort: a failure is logged and the script continues to
  the next stage, then the script exits non-zero so Task Scheduler shows the
  failure). The pipeline is event-driven and resumable, so a stage that finds
  nothing to do is a no-op, and a transient failure is picked up on the next
  run:

    1. downloader   download --schedule daily   (downloads today's incremental
                    files AND emits `vendor_raw_published` per file — the
                    transformer subscribes to those events, so no seed step)
    2. transformer  resume                       (drains vendor_raw_published →
                    bronze parquet + catalog)
    3. silver-refiner resume                     (bronze → silver; also auto-
                    resamples unadjusted 5-min at the end of resume)
    4. gold-calculator resume --all              (silver → gold, every enabled
                    calculator in dependency order)
    5. POST /api/gold/refresh-cache              (best-effort: refresh the web
                    server's in-memory snapshot cache so the UI reflects the new
                    gold data without a manual refresh/restart)

  ALL stdout+stderr from every stage is tee'd into a single timestamped log
  under $LogDir (D:\quantvector\lakehouse\logs). Nothing is discarded.

.PARAMETER Env
  Environment passed to each binary (default 'prod' → reads the prod global.yaml
  via load_env_from_ancestors, which is why the script Push-Location's to the
  repo so the apps find .env).

.PARAMETER SkipDownload
  Skip stage 1 (e.g. to re-drain an already-downloaded day).

.PARAMETER Force
  Run even when the prior session was a non-trading day (bypass the holiday
  guard) — e.g. to process crypto/fx on an equity holiday, or to backfill.

.EXAMPLE
  pwsh -NoProfile -File D:\git\QuantVector\maintenance_scripts\daily_pipeline.ps1
#>
[CmdletBinding()]
param(
    [string]$Env = 'prod',
    [switch]$SkipDownload,
    # Run even if the prior session was a non-trading day (e.g. to process the
    # crypto/fx that DO trade on equity holidays, or to backfill).
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# ─── Paths (current d:\quantvector layout — mirrors maintenance.ps1) ──────────
$Repo        = 'D:\git\QuantVector'
$BinDir      = Join-Path $Repo 'dist\target\release-fast'
$Lakehouse   = 'D:\quantvector\lakehouse'
$LogDir      = Join-Path $Lakehouse 'logs'
$PgContainer = 'quantvector-db'
$ServerUrl   = 'http://localhost:3000'   # web server, for the post-run cache refresh

$DownloaderExe = Join-Path $BinDir 'downloader.exe'
$TransformerExe = Join-Path $BinDir 'transformer-v2.exe'
$SilverExe     = Join-Path $BinDir 'silver-refiner.exe'
$GoldExe       = Join-Path $BinDir 'gold-calculator.exe'

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }
$stamp   = Get-Date -Format 'yyyyMMdd-HHmmss'
$LogFile = Join-Path $LogDir ("daily-pipeline-{0}.log" -f $stamp)
$script:Failures = @()

function Write-Log {
    param([string]$Msg, [string]$Level = 'INFO')
    $line = "{0}  [{1}]  {2}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Msg
    $line | Tee-Object -FilePath $LogFile -Append
}

# Run a pipeline binary, streaming ALL output (stdout+stderr) into the log file.
function Invoke-Stage {
    param([string]$Name, [string]$Exe, [string[]]$AppArgs)
    Write-Log "── $Name ── $Exe $($AppArgs -join ' ')"
    if (-not (Test-Path $Exe)) {
        $script:Failures += $Name
        Write-Log "${Name}: FAILED — binary not found at $Exe" 'ERROR'
        return
    }
    try {
        & $Exe @AppArgs 2>&1 | Tee-Object -FilePath $LogFile -Append
        if ($LASTEXITCODE -ne 0) { throw "$Name exited $LASTEXITCODE" }
        Write-Log "${Name}: OK"
    }
    catch {
        $script:Failures += $Name
        Write-Log "${Name}: FAILED — $($_.Exception.Message)" 'ERROR'
    }
}

Write-Log ("QuantVector daily pipeline start (env={0})" -f $Env)
Write-Log ("log file: {0}" -f $LogFile)

# Ensure the PG container is up (idempotent — no-op if already running). The
# whole pipeline needs it; at 02:00 the host may have just woken.
try {
    docker start $PgContainer 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
} catch {
    Write-Log "docker start $PgContainer reported: $($_.Exception.Message)" 'WARN'
}

# ─── Holiday / non-trading-day guard ──────────────────────────────────────
# The 2 AM run processes the PRIOR session. If that day was not an NYSE (XNYS)
# trading day — weekend or holiday — there is no new equity data, so skip
# (crypto/fx, which DO trade, are picked up on the next run; use -Force to run
# now). Source of truth = the shared `trading_calendar` table. Fail-safe: a
# date the calendar doesn't cover does NOT skip (it warns and runs).
if (-not $Force) {
    $session = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd')
    $q = "SELECT is_trading_day, coalesce(holiday_name,'') FROM trading_calendar WHERE venue='XNYS' AND calendar_date = DATE '$session';"
    $row = docker exec $PgContainer psql -U quantvector -d quantvector -tA -F '|' -c $q 2>&1 |
           Where-Object { $_ -match '\S' } | Select-Object -First 1
    if (-not $row) {
        Write-Log "trading_calendar has no XNYS row for prior session $session — proceeding (extend the calendar)" 'WARN'
    } elseif (($row -split '\|')[0] -eq 'f') {
        $why = ($row -split '\|')[1]; if (-not $why) { $why = 'weekend' }
        Write-Log "prior session $session was not an XNYS trading day ($why) — skipping pipeline (use -Force to run anyway)"
        Write-Log 'DONE — skipped (non-trading day)'
        exit 0
    } else {
        Write-Log "prior session $session is an XNYS trading day — proceeding"
    }
}

Push-Location $Repo   # so the apps find .env via load_env_from_ancestors
try {
    if ($SkipDownload) {
        Write-Log 'stage 1 (download) skipped by -SkipDownload'
    } else {
        Invoke-Stage 'downloader download --schedule daily' $DownloaderExe @('--env', $Env, 'download', '--schedule', 'daily')
    }
    Invoke-Stage 'transformer-v2 resume'    $TransformerExe @('--env', $Env, 'resume')
    Invoke-Stage 'silver-refiner resume'    $SilverExe      @('--env', $Env, 'resume')
    Invoke-Stage 'gold-calculator resume --all' $GoldExe    @('--env', $Env, 'resume', '--all')
}
finally { Pop-Location }

# ─── Stage 5: refresh the web server's in-memory snapshot cache ───────────────
# The server loads gold data into an in-memory cache at startup and only
# refreshes on demand, so without this the running UI keeps serving the previous
# day until someone clicks "Refresh server cache" or restarts. Best-effort: the
# server may not be running, and a failure here must NOT fail the pipeline.
try {
    $resp = Invoke-WebRequest -Uri "$ServerUrl/api/gold/refresh-cache" `
        -Method Post -TimeoutSec 180 -UseBasicParsing
    Write-Log ("server cache refresh: HTTP {0}" -f $resp.StatusCode)
} catch {
    Write-Log "server cache refresh skipped (server not running?): $($_.Exception.Message)" 'WARN'
}

if ($script:Failures.Count) {
    Write-Log ("DONE with FAILURES: {0}" -f ($script:Failures -join ', ')) 'ERROR'
    exit 1
} else {
    Write-Log 'DONE — all stages OK'
    exit 0
}
