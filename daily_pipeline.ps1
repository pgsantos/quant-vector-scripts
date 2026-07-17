<#
.SYNOPSIS
  QuantVector nightly pipeline. Runs the full vendor->raw->bronze->silver->PG->
  gold relay end-to-end and logs EVERYTHING to the lakehouse logs dir. Runs
  unattended at 00:00 Central via the "QuantVector Daily Pipeline" scheduled
  task, which fires Tue-Sat (so each run processes a Mon-Fri session). Skips any
  run whose prior session was an NYSE holiday (holiday guard; -Force overrides).

.DESCRIPTION
  Consolidated from the two cheat-sheet blocks (docs/cheat_sheets/
  quantvector_runs.md: NIGHTLY 7PM main pool + NIGHTLY 11PM options) into one
  midnight run. Midnight Central is ~1h after EODHD finalizes US 5-min intraday
  (~11PM ET) and well past the 00:00 UTC budget reset, so a single run catches
  the current session's 5-min and spends the fresh daily budget.

  Stages, in dependency order. Each is best-effort: a failure is logged and the
  script continues to the next stage, then the script exits non-zero so Task
  Scheduler flags it. The pipeline is event-driven + resumable, so a stage that
  finds nothing to do is a no-op and a transient failure is picked up next run.

    DOWNLOAD (vendor -> raw files on disk + vendor_raw_published events)
      1. downloader download --schedule daily            (universe/eod/splits/divs,
                                                          fundamentals 1/5 rotation,
                                                          calendars, eligible 5min)
      2. downloader download --schedule news_daily        (news shortlist)
      3. downloader download --schedule catalog_enrich --vendor eodhd  (IPO/newcomers)
      4. downloader download --schedule sec_daily --vendor sec         (last-7d filings)
      5. downloader download --schedule options_daily --throttle 200 --concurrency 8
      6. downloader resume --throttle 200 --concurrency 8 (drain budget-deferred items
                                                          from #1-5 + standing backfills;
                                                          a bare resume sweeps ALL pending
                                                          across pools, so the throttle
                                                          rate-limits the options backfill.
                                                          NOTE: it also throttles any
                                                          main-pool mop-up to 200/min)
    TRANSFORM / SILVER (raw -> bronze -> silver; one pass drains equity + options)
      7. transformer-v2 resume
      8. silver-refiner resume                            (auto-resamples unadj 5min)
    METADATA PROJECTIONS (raw metadata/fundamentals/filings -> PG tables)
      9. meta-manager resume
     10. meta-manager sync-catalog                        (catalog -> symbols lifecycle)
     11. meta-manager produce-factors                     (split/div -> read-time factors)
    SQL ROLLUP
     12. sec_filing_signals_rollup.sql                    (needs filing_events from #9)
    SCORERS (GPU = this PC's RTX 4090; annotation-tier, best-effort, -SkipScorers)
     13. news_scorer.py --aggregate                       (-> news_signal_daily; feeds
                                                          the selection news-veto in #16)
     14. sec_text_metrics.py                              (-> filing_text_metrics)
    GOLD (one command: resume --all incremental + the 4 fundamentals calcs; the
          options silver landed above is revived here as options_vol_metrics)
     15. gold-calculator nightly                          ($env:RUST_MIN_STACK raised;
                                                          --with-prev-month auto-added
                                                          in the first days of a month)
    SELECTION ("Today's Longs")
     16. selection_rollup.sql                             (needs crv2 + regime +
                                                          realized_volatility fresh)
    CACHE
     17. POST /api/gold/refresh-cache                     (best-effort UI snapshot refresh)

  ALL stdout+stderr from every stage is tee'd into a single timestamped log
  under $LogDir (D:\quantvector\lakehouse\logs). Nothing is discarded.

.PARAMETER Env
  Environment passed to each Rust binary (default 'prod'). The script also
  Push-Location's to the repo so the apps resolve config via .env /
  load_env_from_ancestors.

.PARAMETER SkipDownload
  Skip the whole download phase (stages 1-6) — e.g. to re-drain an
  already-downloaded day.

.PARAMETER SkipScorers
  Skip the GPU Python scorers (stages 13-14) — e.g. an unattended host with no
  GPU, or to keep the run pure-Rust/SQL.

.PARAMETER Force
  Run even when the prior session was a non-trading day (bypass the holiday
  guard) — e.g. to process crypto/fx on an equity holiday, or to backfill.

.EXAMPLE
  pwsh -NoProfile -File D:\quantvector\lakehouse\scripts\daily_pipeline.ps1
#>
[CmdletBinding()]
param(
    [string]$Env = 'prod',
    [switch]$SkipDownload,
    [switch]$SkipScorers,
    # Run even if the prior session was a non-trading day (e.g. to process the
    # crypto/fx that DO trade on equity holidays, or to backfill).
    [switch]$Force
)

$ErrorActionPreference = 'Stop'

# ─── Paths (current d:\quantvector layout) ────────────────────────────────────
$Repo        = 'D:\git\QuantVector'
$BinDir      = Join-Path $Repo 'dist\target\release-fast'
$SqlDir      = Join-Path $Repo 'maintenance_scripts_sql'
$Lakehouse   = 'D:\quantvector\lakehouse'
$LogDir      = Join-Path $Lakehouse 'logs'
$PgContainer = 'quantvector-db'
$ServerUrl   = 'http://localhost:3000'   # web server, for the post-run cache refresh

$DownloaderExe  = Join-Path $BinDir 'downloader.exe'
$TransformerExe = Join-Path $BinDir 'transformer-v2.exe'
$SilverExe      = Join-Path $BinDir 'silver-refiner.exe'
$MetaExe        = Join-Path $BinDir 'meta-manager.exe'
$GoldExe        = Join-Path $BinDir 'gold-calculator.exe'

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

# Run a .sql file through the PG container's psql over stdin. Best-effort.
# -StopOnError adds ON_ERROR_STOP=1 (use for rollups that must be all-or-nothing;
# selection_rollup deliberately runs WITHOUT it — it \sets its own defaults).
function Invoke-Sql {
    param([string]$Name, [string]$SqlFile, [switch]$StopOnError)
    Write-Log "── $Name ── psql < $SqlFile"
    if (-not (Test-Path $SqlFile)) {
        $script:Failures += $Name
        Write-Log "${Name}: FAILED — sql not found at $SqlFile" 'ERROR'
        return
    }
    try {
        $psqlArgs = @('exec', '-i', $PgContainer, 'psql', '-U', 'quantvector', '-d', 'quantvector')
        if ($StopOnError) { $psqlArgs += @('-v', 'ON_ERROR_STOP=1') }
        Get-Content $SqlFile -Raw | docker @psqlArgs 2>&1 | Tee-Object -FilePath $LogFile -Append
        if ($LASTEXITCODE -ne 0) { throw "$Name exited $LASTEXITCODE" }
        Write-Log "${Name}: OK"
    }
    catch {
        $script:Failures += $Name
        Write-Log "${Name}: FAILED — $($_.Exception.Message)" 'ERROR'
    }
}

# Run a `py -3.13` scorer. Annotation-tier (feeds the briefing / news-veto, NOT
# gold rankings), so it is best-effort and NEVER blocks the pipeline.
function Invoke-Py {
    param([string]$Name, [string[]]$PyArgs)
    Write-Log "── $Name ── py $($PyArgs -join ' ')"
    $py = Get-Command py -ErrorAction SilentlyContinue
    if (-not $py) {
        $script:Failures += $Name
        Write-Log "${Name}: FAILED — 'py' launcher not found on PATH" 'ERROR'
        return
    }
    try {
        & $py.Source @PyArgs 2>&1 | Tee-Object -FilePath $LogFile -Append
        if ($LASTEXITCODE -ne 0) { throw "$Name exited $LASTEXITCODE" }
        Write-Log "${Name}: OK"
    }
    catch {
        $script:Failures += $Name
        Write-Log "${Name}: FAILED — $($_.Exception.Message)" 'ERROR'
    }
}

Write-Log ("QuantVector nightly pipeline start (env={0})" -f $Env)
Write-Log ("log file: {0}" -f $LogFile)

# Ensure the PG container is up (idempotent — no-op if already running). The
# whole pipeline needs it; at 00:00 the host may have just woken.
try {
    docker start $PgContainer 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
} catch {
    Write-Log "docker start $PgContainer reported: $($_.Exception.Message)" 'WARN'
}

# ─── Holiday / non-trading-day guard ──────────────────────────────────────
# The midnight run processes the PRIOR session. If that day was not an NYSE
# (XNYS) trading day — weekend or holiday — there is no new equity data, so skip
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
    # ── DOWNLOAD ─────────────────────────────────────────────────────────────
    if ($SkipDownload) {
        Write-Log 'download phase (stages 1-6) skipped by -SkipDownload'
    } else {
        Invoke-Stage 'download daily'          $DownloaderExe @('--env', $Env, 'download', '--schedule', 'daily')
        Invoke-Stage 'download news_daily'     $DownloaderExe @('--env', $Env, 'download', '--schedule', 'news_daily')
        Invoke-Stage 'download catalog_enrich' $DownloaderExe @('--env', $Env, 'download', '--schedule', 'catalog_enrich', '--vendor', 'eodhd')
        Invoke-Stage 'download sec_daily'      $DownloaderExe @('--env', $Env, 'download', '--schedule', 'sec_daily', '--vendor', 'sec')
        Invoke-Stage 'download options_daily'  $DownloaderExe @('--env', $Env, 'download', '--schedule', 'options_daily', '--throttle', '200', '--concurrency', '8')
        # Bare resume drains ALL pending across pools; --throttle/--concurrency
        # rate-limit the options (marketplace-pool) backfill. Applies to the
        # whole eodhd resume, so main-pool mop-up is also capped at 200/min.
        Invoke-Stage 'downloader resume' $DownloaderExe @('--env', $Env, 'resume', '--throttle', '200', '--concurrency', '8')
    }

    # ── TRANSFORM / SILVER ───────────────────────────────────────────────────
    Invoke-Stage 'transformer-v2 resume' $TransformerExe @('--env', $Env, 'resume')
    Invoke-Stage 'silver-refiner resume' $SilverExe      @('--env', $Env, 'resume')

    # ── METADATA PROJECTIONS ─────────────────────────────────────────────────
    Invoke-Stage 'meta-manager resume'          $MetaExe @('--env', $Env, 'resume')
    Invoke-Stage 'meta-manager sync-catalog'    $MetaExe @('--env', $Env, 'sync-catalog')
    Invoke-Stage 'meta-manager produce-factors' $MetaExe @('--env', $Env, 'produce-factors')

    # ── SQL ROLLUP (SEC filing signals) — needs filing_events from meta resume ─
    Invoke-Sql 'sec_filing_signals_rollup' (Join-Path $SqlDir 'sec_filing_signals_rollup.sql') -StopOnError

    # ── SCORERS (GPU; annotation-tier, best-effort) ──────────────────────────
    if ($SkipScorers) {
        Write-Log 'scorers (stages 13-14) skipped by -SkipScorers'
    } else {
        Invoke-Py 'news_scorer'      @('-3.13', 'scripts/news_scorer.py', '--aggregate')
        Invoke-Py 'sec_text_metrics' @('-3.13', 'scripts/sec_text_metrics.py')
    }

    # ── GOLD (one command: resume --all + fundamentals four; options_vol_metrics
    #    is revived here off the options silver landed above) ──────────────────
    $env:RUST_MIN_STACK = '67108864'
    $goldArgs = @('--env', $Env, 'nightly')
    if ((Get-Date).Day -le 3) {
        $goldArgs += '--with-prev-month'
        Write-Log 'gold: first days of month — adding --with-prev-month (late data lands in prior month)'
    }
    Invoke-Stage 'gold-calculator nightly' $GoldExe $goldArgs

    # ── SELECTION ("Today's Longs") — needs crv2 + regime + realized_volatility ─
    Invoke-Sql 'selection_rollup' (Join-Path $SqlDir 'selection_rollup.sql')
}
finally { Pop-Location }

# ─── Stage 17: refresh the web server's in-memory snapshot cache ──────────────
# The server loads gold data into an in-memory cache at startup and only
# refreshes on demand, so without this the running UI keeps serving the previous
# day until someone clicks "Refresh server cache" or restarts. Best-effort: the
# server may not be running, and a failure here must NOT fail the pipeline.
try {
    $resp = Invoke-WebRequest -Uri "$ServerUrl/api/gold/refresh-cache" `
        -Method Post -TimeoutSec 300 -UseBasicParsing
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
