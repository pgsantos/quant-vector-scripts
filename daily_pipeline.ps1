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
      5x. downloader download --schedule capacity_weekly --vendor capacity
          WEEKLY (Saturday). FRED monthly + EIA weekly series behind the
          capital_cycle capacity leg; nightly would re-fetch identical bytes.
      5w. downloader download --schedule rename_reconcile --vendor eodhd
          WEEKLY (Saturday run only — post-Friday close). Full-history rename
          pull; the only one once the nightly job carries lookback_days: 60.
          All WEEKLY stages gate on $IsWeekly — Saturday, NOT Sunday: this
          pipeline skips non-trading days, so Sun/Mon 00:00 never run.
      (no blanket `downloader resume` — deliberately. Each schedule above drains
       its own throughput-sized plan; standing backfills are drained MANUALLY
       after inspecting what's pending. See the note at the stage site below.)
    TRANSFORM / SILVER (raw -> bronze -> silver; one pass drains equity + options)
      7. transformer-v2 resume
      8. silver-refiner resume                            (auto-resamples unadj 5min)
    METADATA PROJECTIONS (raw metadata/fundamentals/filings -> PG tables)
      9. meta-manager resume
     10. meta-manager sync-catalog                        (catalog -> symbols lifecycle)
     11. meta-manager produce-factors                     (split/div -> read-time factors)
    SQL ROLLUP
     12. sec_filing_signals_rollup.sql                    (needs filing_events from #9)
    SCORER (GPU = this PC's RTX 4090; annotation-tier, best-effort, -SkipScorers)
     13. news_scorer.py --aggregate                       (-> news_signal_daily; feeds
                                                          the selection news-veto in #16)
          (sec_text_metrics.py is intentionally NOT run — the 2026-07 change_score
           study found its signal not worth the nightly GPU cost; on-demand only)
    GOLD (resume --all: event-driven + incremental. The fundamentals chain fires
          via events — fundamental_features off meta-manager's fundamentals_published,
          then rating -> group_strength -> composite_rating_v2 off gold_published;
          options_vol_metrics is revived off the options silver landed above)
     14. gold-calculator resume --all                     (the binary sets its own 256 MB
                                                          stack, no env var needed)
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
  Skip the GPU news scorer (stage 13) — e.g. an unattended host with no GPU, or
  to keep the run pure-Rust/SQL.

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
    [switch]$Force,
    # Run the WEEKLY stages regardless of weekday (they normally fire only on
    # the Saturday 00:00 run). Use to catch up a missed week, or to test.
    [switch]$ForceWeekly
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

# ─── Weekly gate ──────────────────────────────────────────────────────────
# WEEKLY stages run on the Saturday 00:00 run — the first run after Friday's
# close, so a full trading week is in the books.
#
# 🚨 NOT Sunday. This pipeline processes the PRIOR session and the guard above
# exits on a non-trading day, so Sunday 00:00 (prior session = Saturday) and
# Monday 00:00 (prior session = Sunday) never run. The effective cadence is
# Tue-Sat; a Sunday-gated stage would silently never fire.
$IsWeekly = $ForceWeekly -or ((Get-Date).DayOfWeek -eq 'Saturday')
if ($IsWeekly) {
    $why = if ($ForceWeekly) { '-ForceWeekly' } else { 'Saturday run (post-Friday close)' }
    Write-Log "weekly stages ENABLED this run — $why"
} else {
    Write-Log "weekly stages skipped (not the Saturday run; use -ForceWeekly to override)"
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

        # ── WEEKLY downloads (Saturday run only — see the weekly gate above) ──
        if ($IsWeekly) {
            # FULL rename reconcile: /symbol-change-history from 2000 -> today.
            # 1 API call/week, and the ONLY full-history pull once the nightly
            # `symbol_changes` job carries `lookback_days: 60`.
            #
            # LOAD-BEARING, not insurance. A rename only enters
            # symbol_change_events once its OLD ticker exists in `symbols` (the
            # insert gate). A ticker can join our universe YEARS after its
            # rename — 9 such renames surfaced at once on 2026-07-21 with
            # effective dates spanning 2022-2025. Those records sit far outside
            # a 60-day window, so without this weekly full re-process the
            # nightly would never see them again. This bounds that latency to
            # <= 1 week, which is fine for historical renames (they land in a
            # review inbox, not a trading path).
            #
            # Placed in the download phase on purpose: `meta-manager resume`
            # (stage 9 below) parses the landed file in the SAME run.
            Invoke-Stage 'download rename_reconcile' $DownloaderExe @('--env', $Env, 'download', '--schedule', 'rename_reconcile', '--vendor', 'eodhd')

            # Macro capacity utilization (FRED + EIA) -> macro_capacity, the
            # capital_cycle capacity leg. WEEKLY on purpose: the FRED series are
            # MONTHLY and the EIA one WEEKLY, so a nightly pull would re-fetch
            # byte-identical payloads six days in seven. Free APIs, 4 requests,
            # no EODHD budget.
            #
            # Without this the overlay silently FREEZES at whatever vintage was
            # last landed: capital_cycle keeps scoring, the capacity leg keeps
            # returning a value, and nothing reports that the value has stopped
            # moving. Same shape as every other defect this feature found.
            #
            # Placed in the download phase on purpose, like rename_reconcile:
            # `meta-manager resume` (stage 9) parses the landed files in the
            # SAME run, so a Saturday night lands data AND projects it.
            Invoke-Stage 'download capacity_weekly' $DownloaderExe @('--env', $Env, 'download', '--schedule', 'capacity_weekly', '--vendor', 'capacity')
        }

        # NOTE: a blanket `downloader resume` is intentionally NOT run nightly.
        # A bare resume sweeps ALL pending across pools unattended (the options
        # marketplace backfill, catalog_enrich leftovers, any budget-deferred
        # daily items). Each schedule above already drains its own
        # throughput-sized plan, so standing backfills are drained MANUALLY when
        # you choose — inspect first, then scope the drain:
        #   # what's pending, by run:
        #   docker exec quantvector-db psql -U quantvector -d quantvector -c \
        #     "SELECT run_id, count(*) FROM plan_items \
        #      WHERE planning_status='ok' AND download_status='pending' \
        #      GROUP BY run_id ORDER BY count(*) DESC;"
        #   # then drain a specific run (options at 200/min, 8 concurrent):
        #   downloader --env prod resume --run-id <run_id> --throttle 200 --concurrency 8
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

    # ── SCORER (GPU; annotation-tier, best-effort) ───────────────────────────
    if ($SkipScorers) {
        Write-Log 'news_scorer skipped by -SkipScorers'
    } else {
        Invoke-Py 'news_scorer' @('-3.13', 'scripts/news_scorer.py', '--aggregate')
        # sec_text_metrics is intentionally NOT run: the 2026-07 change_score
        # event study found the SEC-filing text signal weak/short-lived and not
        # worth the nightly GPU cost. Run on demand only if revisiting the study.
        # Invoke-Py 'sec_text_metrics' @('-3.13', 'scripts/sec_text_metrics.py')
    }

    # ── GOLD (resume --all: event-driven, incremental. Walks the DAG in phase
    #    order, so the fundamentals chain fires via events — fundamental_features
    #    off meta-manager's `fundamentals_published` (stage 9), then rating ->
    #    industry_group_strength -> composite_rating_v2 off `gold_published`. Late
    #    prior-month fundamentals revive their own as-of month (the event carries
    #    it), so no --with-prev-month heuristic is needed. options_vol_metrics is
    #    revived off the options silver landed above. The binary sets its own
    #    256 MB stack (build.rs /STACK + tokio thread_stack_size + main.rs
    #    RUST_MIN_STACK override), so no $env:RUST_MIN_STACK is needed. ──────────
    # ⚠️ 2026-07-24: switched resume -> calculate for the weekend full rebuild
    # (fold in the earnings sentinel + VIX fixes). `calculate --all` bypasses the
    # event log and unconditionally recomputes EVERY enabled calculator (~heavy,
    # full-history cross-sectional). REVERT to `resume --all` after the weekend
    # rebuild lands — leaving this makes every weekday nightly a full rebuild.
    Invoke-Stage 'gold-calculator calculate --all' $GoldExe @('--env', $Env, 'calculate', '--all')

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

# ─── Stage 18: completion sentinel for downstream consumers ──────────────────
# The Claude quant-briefing scheduled task gates on pipeline completion: it will
# not build a briefing until the nightly has actually landed, so a slow run (e.g.
# a `calculate --all` full rebuild) no longer produces a briefing on stale data.
# The briefing checks the database first; this sentinel adds the one thing the DB
# cannot tell it - whether the run SUCCEEDED or logged failures. Best-effort: a
# failure here must NOT fail the pipeline.
try {
    $sentinelPath = Join-Path $Lakehouse '.pipeline-complete.json'
    [pscustomobject]@{
        completed_utc = (Get-Date).ToUniversalTime().ToString('o')
        status        = if ($script:Failures.Count) { 'FAILURES' } else { 'OK' }
        failures      = @($script:Failures)
        log_file      = $LogFile
        run_stamp     = $stamp
    } | ConvertTo-Json -Depth 4 | Set-Content -Path $sentinelPath -Encoding UTF8
    Write-Log ("completion sentinel written: {0}" -f $sentinelPath)
} catch {
    Write-Log "sentinel write skipped: $($_.Exception.Message)" 'WARN'
}

if ($script:Failures.Count) {
    Write-Log ("DONE with FAILURES: {0}" -f ($script:Failures -join ', ')) 'ERROR'
    exit 1
} else {
    Write-Log 'DONE — all stages OK'
    exit 0
}
