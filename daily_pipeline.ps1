<#
.SYNOPSIS
  QuantVector nightly pipeline. Runs the full vendor->raw->bronze->silver->PG->
  gold relay end-to-end and logs EVERYTHING to the lakehouse logs dir. Runs
  unattended at 22:00 local via the "QuantVector Daily Pipeline" scheduled task,
  which fires Mon-Fri.

  The run DERIVES the session it is processing rather than assuming one from the
  trigger time: the most recent session whose 16:00 ET close precedes the run
  start, computed in Eastern time. At a 22:00 local start that is TODAY; at a
  00:00 start it would be yesterday. Skips any run whose derived session was not
  an XNYS trading day (weekend or holiday; -Force overrides).

.DESCRIPTION
  Consolidated from the two cheat-sheet blocks (docs/cheat_sheets/
  quantvector_runs.md: NIGHTLY 7PM main pool + NIGHTLY 11PM options) into one
  run. 22:00 local (23:00 ET) is after EODHD finalizes US 5-min intraday
  (~11PM ET), so a single run catches the current session's 5-min.

  🚨 The 22:00 start is deliberate and the session derivation exists to serve
  it. Between 2026-08-29 and 2026-09-06 the trigger fired at 22:00 while the
  code still assumed a 00:00 "prior session" start — so a run downloaded TODAY's
  session but evaluated YESTERDAY's calendar entry. Mon 2026-08-31 was never
  fetched at all (Monday was not in the trigger set) and Tue 2026-09-08 would
  have skipped on the Labor-Day entry for 09-07, losing the Tuesday session.

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
          WEEKLY (see $IsWeekly). FRED monthly + EIA weekly series behind the
          capital_cycle capacity leg; nightly would re-fetch identical bytes.
      5w. downloader download --schedule rename_reconcile --vendor eodhd
          WEEKLY (first run at/after the week's last trading day). Full-history
          rename pull; the only one once the nightly carries lookback_days: 60.
          All WEEKLY stages gate on $IsWeekly, which keys on the SESSION's
          week — the first run at or after the week's last trading day — not on
          the run's weekday. A missed Friday is caught up by the next run.
      (no blanket `downloader resume` — deliberately. Each schedule above drains
       its own throughput-sized plan; standing backfills are drained MANUALLY
       after inspecting what's pending. See the note at the stage site below.)
    TRANSFORM / SILVER (raw -> bronze -> silver; one pass drains equity + options)
      7. transformer-v2 resume
      8. silver-refiner resume                            (auto-resamples unadj 5min)
    METADATA PROJECTIONS (raw metadata/fundamentals/filings -> PG tables)
      9. meta-manager resume
          (Form 4 -> insider_transactions is projected INSIDE #9, not a separate stage)
     10. meta-manager sync-catalog                        (catalog -> symbols lifecycle)
     10b. meta-manager sync-corporate-actions             (delist/listing bridge; breaker-capped)
     10c. meta-manager extract-deal-terms                 (8-K M&A/spinoff facts; converging drain)
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
     14a. refresh_symbols_first_bar_date.sql              (symbols.first_bar_date for names
                                                           that just gained a first bar;
                                                           reads daily_returns, so AFTER 14)
     14b. special_situations calculate                    (explicit; feeds the thesis log's
                                                           special_situations funnel)
     14c. theme_map calculate                             (explicit; Funnel 3, emits BOTH
                                                           theme_membership + theme_bottleneck;
                                                           activated 2026-08-05 once task #21
                                                           cleared)
     15. thesis_event_outcomes calculate                  (explicit + unfiltered: no event
                                                          feeds it, and it rewrites every
                                                          prior month as horizons mature)
    SELECTION ("Today's Longs")
     15b. thesis_assembler calculate                      (explicit; AFTER selection_rollup —
                                                           stamps the verification block —
                                                           merges the 3 funnels into
                                                           thesis_candidates for briefing §3b)
     16. selection_rollup.sql                             (needs crv2 + regime +
                                                          realized_volatility fresh)
    CACHE
     17. POST /api/gold/refresh-cache                     (best-effort UI snapshot refresh)

  ALL stdout+stderr from every stage is tee'd into a single timestamped log
  under $LogDir (D:\quantvector\lakehouse\logs, or lakehouse-test\logs under
  -Env test). Nothing is discarded.

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
  Run even when the derived session was a non-trading day (bypass the holiday
  guard) — e.g. to process crypto/fx on an equity holiday, or to backfill.

.EXAMPLE
  pwsh -NoProfile -File D:\quantvector\lakehouse\scripts\daily_pipeline.ps1
#>
[CmdletBinding()]
param(
    # 🚨 ValidateSet, not a free string. Every SQL stage picks its database from
    # this value, so a typo used to be the difference between "ran nothing" and
    # "wrote to prod". `-Env prd` must fail at the parameter binder, loudly,
    # rather than fall through a default.
    [ValidateSet('prod', 'test')]
    [string]$Env = 'prod',
    [switch]$SkipDownload,
    [switch]$SkipScorers,
    # Run even if the derived session was a non-trading day (e.g. to process the
    # crypto/fx that DO trade on equity holidays, or to backfill).
    [switch]$Force,
    # Run the WEEKLY stages regardless of the gate (they normally fire on the
    # first run at/after the week's last trading day). Catch-up, or to test.
    [switch]$ForceWeekly
)

$ErrorActionPreference = 'Stop'

# ─── Paths (current d:\quantvector layout) ────────────────────────────────────
$Repo        = 'D:\git\QuantVector'
$BinDir      = Join-Path $Repo 'dist\target\release-fast'
$SqlDir      = Join-Path $Repo 'maintenance_scripts_sql'
# 🚨 Follows -Env, same reason as $PgDatabase below. This is not only about
# where logs land: line ~500 writes `.pipeline-complete.json` here, and while
# this path was hardcoded a `-Env test` run stamped the PROD completion
# sentinel — so anything reading it would have believed the live pipeline had
# finished when only a rehearsal had. The two env trees are siblings by
# convention (`lakehouse` / `lakehouse-test`), matching the config layout the
# Rust binaries resolve from --env.
$Lakehouse   = if ($Env -eq 'test') { 'D:\quantvector\lakehouse-test' } else { 'D:\quantvector\lakehouse' }
$LogDir      = Join-Path $Lakehouse 'logs'
$PgContainer = 'quantvector-db'
# 🚨 The DATABASE follows -Env; the CONTAINER does not. One Postgres instance
# serves both databases, so the container name is genuinely constant while the
# database is not.
#
# Until 2026-07-31 every psql call here hardcoded `-d quantvector`, so
# `-Env test` sent the Rust stages at the test lakehouse (they honour --env)
# while every SQL stage — selection_rollup, sec_filing_signals_rollup and the
# holiday guard that decides whether the run happens at all — read and WROTE
# PROD. A run believed to be a rehearsal would have silently rewritten the live
# selection snapshot. Never reintroduce a literal database name below; use this.
$PgDatabase  = if ($Env -eq 'test') { 'quantvector_test' } else { 'quantvector' }
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
        $psqlArgs = @('exec', '-i', $PgContainer, 'psql', '-U', 'quantvector', '-d', $PgDatabase)
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

Write-Log ("QuantVector nightly pipeline start (env={0}, db={1})" -f $Env, $PgDatabase)
Write-Log ("log file: {0}" -f $LogFile)

# Ensure the PG container is up (idempotent — no-op if already running). The
# whole pipeline needs it; at 00:00 the host may have just woken.
try {
    docker start $PgContainer 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
} catch {
    Write-Log "docker start $PgContainer reported: $($_.Exception.Message)" 'WARN'
}

# ─── Session under processing ─────────────────────────────────────────────
# The run processes the most recent session whose close precedes the run start.
#
# 🚨 DERIVED, never assumed from the trigger time. This used to be a hardcoded
# `(Get-Date).AddDays(-1)` written for a 00:00 trigger. The trigger moved to
# 22:00 on 2026-08-29 and the assumption silently went a day out: a 22:00 run
# downloads TODAY's session but the guard was still evaluating YESTERDAY. That
# is why Tue 2026-09-08 would have skipped on the Labor-Day calendar entry and
# lost the Tuesday session, exactly as Mon 08-31 was lost.
#
# 🚨 Derived in EXCHANGE time, not host-local time. The session is a property of
# the NYSE clock, and both the host's zone and the trigger hour have moved
# before. This host is PACIFIC, so the 22:00 local trigger fires at 01:00 ET the
# NEXT calendar day — 9h after the 16:00 ET close, not 7h. Deriving in ET makes
# that irrelevant: the rule is "the last close that precedes now", whatever the
# host clock says. A local-hour rule would need the right hour per zone (13 for
# PT, 15 for CT) and silently misfires for any run between the true close and a
# wrong configured hour. The close is 16:00 ET year-round, so no DST branch.
#
# Defined UNCONDITIONALLY — a -Force run needs $session too. Only the calendar
# CHECK below is gated on -Force.
$EtZone = try   { [TimeZoneInfo]::FindSystemTimeZoneById('Eastern Standard Time') }
          catch { [TimeZoneInfo]::FindSystemTimeZoneById('America/New_York') }
$Et = [TimeZoneInfo]::ConvertTime((Get-Date), $EtZone)
$SessionDate = if ($Et.TimeOfDay -ge [timespan]'16:00') { $Et.Date } else { $Et.Date.AddDays(-1) }
$session = $SessionDate.ToString('yyyy-MM-dd')
Write-Log ("session={0} (run start {1} local / {2} ET; NYSE close 16:00 ET)" -f `
    $session, (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Et.ToString('yyyy-MM-dd HH:mm:ss'))

# ─── Holiday / non-trading-day guard ──────────────────────────────────────
# If the derived session was not an NYSE (XNYS) trading day — weekend or holiday
# — there is no new equity data, so skip (crypto/fx, which DO trade, are picked
# up on the next run; use -Force to run now). Source of truth = the shared
# `trading_calendar` table. Fail-safe: a date the calendar doesn't cover does
# NOT skip (it warns and runs).
if (-not $Force) {
    $q = "SELECT is_trading_day, coalesce(holiday_name,'') FROM trading_calendar WHERE venue='XNYS' AND calendar_date = DATE '$session';"
    $row = docker exec $PgContainer psql -U quantvector -d $PgDatabase -tA -F '|' -c $q 2>&1 |
           Where-Object { $_ -match '\S' } | Select-Object -First 1
    if (-not $row) {
        Write-Log "trading_calendar has no XNYS row for session $session — proceeding (extend the calendar)" 'WARN'
    } elseif (($row -split '\|')[0] -eq 'f') {
        $why = ($row -split '\|')[1]; if (-not $why) { $why = 'weekend' }
        Write-Log "session $session was not an XNYS trading day ($why) — skipping pipeline (use -Force to run anyway)"
        Write-Log 'DONE — skipped (non-trading day)'
        exit 0
    } else {
        Write-Log "session $session is an XNYS trading day — proceeding"
    }
}

# ─── Weekly gate ──────────────────────────────────────────────────────────
# WEEKLY stages run once per trading week, on the first run at or after that
# week's LAST trading day ("the week closer" — normally Friday).
#
# 🚨 KEYED ON STATE, not on a weekday. It used to be `DayOfWeek -eq 'Saturday'`,
# which assumes the schedule fired. Once the Saturday trigger is dropped there
# is no second chance: one missed or failed Friday run and the whole week's
# weekly stages are skipped silently, forever. Instead we compare the week
# closer against `last_weekly_completed_session` from the completion sentinel,
# so a missed Friday is picked up by the NEXT run (Monday's) rather than never.
#
# 🚨 The closer is "the last XNYS trading day of its ISO week", read from
# `trading_calendar` — NOT literal Friday. Good Friday is a market holiday every
# year, and a literal-Friday rule would skip that entire week's weekly stages.
#
# Bootstrap: an ABSENT `last_weekly_completed_session` means "never" and the
# weekly stages FIRE. The first run after this change reads a sentinel written
# by the old code, which has no such field. Firing is the safe default — do not
# "fix" this into a skip.
$WeeklyStageNames = @('download rename_reconcile', 'download capacity_weekly')
$weekCloser = $null
$lastWeekly = $null
try {
    $qc = @"
WITH td AS (
    SELECT calendar_date::date AS d, date_trunc('week', calendar_date)::date AS wk
    FROM trading_calendar WHERE venue='XNYS' AND is_trading_day
), closers AS (
    SELECT wk, max(d) AS d FROM td GROUP BY wk
)
SELECT max(d) FROM closers WHERE d <= DATE '$session';
"@
    $weekCloser = (docker exec $PgContainer psql -U quantvector -d $PgDatabase -tA -c $qc 2>&1 |
                   Where-Object { $_ -match '^\d{4}-\d{2}-\d{2}$' } | Select-Object -First 1)
} catch {
    Write-Log "week-closer lookup failed: $($_.Exception.Message)" 'WARN'
}
$sentinelPathForRead = Join-Path $Lakehouse '.pipeline-complete.json'
if (Test-Path $sentinelPathForRead) {
    try {
        $prev = Get-Content $sentinelPathForRead -Raw | ConvertFrom-Json
        if ($prev.PSObject.Properties.Name -contains 'last_weekly_completed_session') {
            $lastWeekly = $prev.last_weekly_completed_session
        }
    } catch {
        Write-Log "could not read prior sentinel for the weekly gate: $($_.Exception.Message)" 'WARN'
    }
}
# Fail-safe: if the closer can't be determined, do NOT fire weekly off a guess.
$IsWeekly = $ForceWeekly -or ($weekCloser -and (-not $lastWeekly -or $lastWeekly -lt $weekCloser))
if ($IsWeekly) {
    $why = if ($ForceWeekly) { '-ForceWeekly' }
           elseif (-not $lastWeekly) { "no prior weekly recorded (bootstrap); week closer $weekCloser" }
           else { "week closer $weekCloser is newer than last weekly $lastWeekly" }
    Write-Log "weekly stages ENABLED this run — $why"
} elseif (-not $weekCloser) {
    Write-Log 'weekly stages skipped — could not resolve the week closer from trading_calendar' 'WARN'
} else {
    Write-Log "weekly stages skipped (already ran for week closer $weekCloser; use -ForceWeekly to override)"
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

        # ── WEEKLY downloads (gated on $IsWeekly — see the weekly gate above) ──
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
            # SAME run, so the week-closing night lands data AND projects it.
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
    # NOTE: no `ingest-form4` stage. Form 4 -> insider_transactions is projected
    # by `meta-manager resume` above, inside the sec_filing event consumer, as
    # each filing's partition is published (2026-08-07). A separate scanning
    # stage here would be both redundant and WRONG: it enumerated sec_filings on
    # a `filing_date` window, so a 2023 Form 4 downloaded by a backfill today
    # fell outside the window forever while the stage reported healthy row
    # counts. `ingest-form4 --backfill` survives as a manual catch-up/repair verb
    # for filings whose partitions were consumed before the projection existed.
    # Bridge catalog delistings/listings into corporate_actions (Phase 2 step
    # 1b). AFTER sync-catalog on purpose: that is what stamps the derived
    # delist dates this reads. Idempotent, cheap, and the historical backfill
    # already ran (2026-07-27, 32,483 rows) — this is the daily trickle.
    # Listing circuit breaker: a burst beyond the default cap inserts NOTHING
    # and says why (a vendor-side catalog import, not a listing wave).
    Invoke-Stage 'meta-manager sync-corporate-actions' $MetaExe @('--env', $Env, 'sync-corporate-actions')
    # Extract M&A deal terms + spinoffs from newly landed 8-Ks (Phase 2 steps
    # 2+3, PHASE2 doc §18-§19). AFTER meta resume on purpose: that is what
    # lands the raw filings and classifies filing_events.items this walks.
    # 🚨 No window. Invoked with no arguments, which IS the drain: work is
    # selected by NOT EXISTS (a COMPLETED projection partition), so it converges
    # to zero and stays there. The old 10-day window left 17,695 of 17,802
    # candidates permanently outside it while this stage reported healthy counts.
    # Idempotent (identity-deduped both tables); historical corpus drained
    # 2026-08-08 (17,747 evaluated -> 2 new deal rows).
    # Uncertain classification = NO row; skips are named in the stage log.
    Invoke-Stage 'meta-manager extract-deal-terms' $MetaExe @('--env', $Env, 'extract-deal-terms')
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
    # 2026-07-28: REVERTED to `resume --all` (the 07-24 calculate-mode switch for
    # the weekend rebuild was left in place and made the 07-28 weekday nightly a
    # full rebuild — killed mid new_highs, ~6h in, 4 of 24 calculators done).
    # If a full rebuild is ever needed again: run `calculate --all` MANUALLY,
    # never by editing this stage — an edit here outlives the weekend it was
    # made for, which is exactly what happened.
    Invoke-Stage 'gold-calculator resume --all' $GoldExe @('--env', $Env, 'resume', '--all')

    # ── symbols.first_bar_date (2026-07-31) ─────────────────────────────────────
    # The pipeline's OWN earliest observed bar per (instrument, symbol) — the
    # only listing-date leg not sourced from EODHD, and so the only one that can
    # contradict `ipo_calendar` and `company_profiles.ipo_date` when they agree
    # with each other and are both wrong.
    #
    # AFTER stage 14: it reads daily_returns, so a name that first traded today
    # needs today's gold to have landed. Seeded once by
    # migrations/2026-07-31b_listing_provenance.sql (2m48s, 33,340 rows); this
    # only fills names that have gained a first bar since.
    #
    # 🚨 -StopOnError deliberately. Without ON_ERROR_STOP psql can exit 0 with a
    # failed statement, and this stage would report OK forever while
    # first_bar_date silently froze — the wired-but-blind shape (DQ-17).
    #
    # Bounded by design, and the obvious formulations are NOT: the seed's full
    # GROUP BY costs 168s per run even when it updates nothing, and a
    # per-symbol probe over NULL rows costs 52s because 49,662 symbols have no
    # bars at all and get re-probed nightly forever. The shipped form filters on
    # trade_date first (chunk exclusion) and measures 0.14s on prod.
    Invoke-Sql 'refresh symbols.first_bar_date' (Join-Path $SqlDir 'refresh_symbols_first_bar_date.sql') -StopOnError

    # ── SPECIAL SITUATIONS (PHASE2 §21-§24) ─────────────────────────────────────
    # Daily snapshot of structural triggers (spinoffs, live M&A targets from
    # ma_deal_terms_live, insider clusters, buybacks). Explicit like the thesis
    # stage: no pub/sub event feeds it (its sources are PG tables the ledger
    # does not track), so claim-driven paths never schedule it. Same-day
    # idempotent; minutes. Runs AFTER 10c (extract-deal-terms feeds the live
    # view) and BEFORE the thesis stage, which ingests its month-end rows as
    # funnel='special_situations'. Refuses to run if the live view's staleness
    # constant drifts from config (pg_get_viewdef pin).
    Invoke-Stage 'special_situations calculate' $GoldExe @('--env', $Env, 'calculate', '--calculator', 'special_situations', '--instrument', 'stock')

    # ── THEME MAPPER (PHASE 3 ACTIVATION, 2026-08-05) ───────────────────────────
    # 14c. Funnel 3. Explicit like 14b/15 — `theme_map` is explicit_only (every
    # source is a PG table the ledger does not track), so no event ever claims
    # it and it appears in resume's phase list without ever being scheduled.
    # That is why Phase 3 sat built-but-dormant: nothing was wrong, nothing was
    # running it.
    #
    # Activation was gated on task #21 (correlation_matrix), closed 2026-08-04.
    #
    # 🚨 Emits BOTH datasets from ONE run (theme_membership + theme_bottleneck).
    # The spec (§1.4) wanted membership weekly and bottleneck nightly; the
    # calculator cannot split them, so both refresh nightly. A stale-membership
    # day is indistinguishable from a fresh one — ETF holdings only move on the
    # vendor's own cadence — so daily is the honest shape, not a compromise.
    #
    # POSITION IS LOAD-BEARING IN BOTH DIRECTIONS: after the gold resume (its
    # dependencies fundamental_features + universe_eligibility complete there)
    # and BEFORE stage 15 — thesis_event_outcomes ingests theme_bottleneck as
    # funnel='theme_bottleneck', and 15b's assembler reads it as a third funnel.
    # Running it after either would feed them yesterday's themes.
    Invoke-Stage 'theme_map calculate' $GoldExe @('--env', $Env, 'calculate', '--calculator', 'theme_map', '--instrument', 'stock')

    # ── THESIS OUTCOME LOG (upstream thesis, SPEC-thesis-event-outcomes) ────────
    # Explicit and UNFILTERED, deliberately — this cannot ride resume/calculate
    # --all like the others:
    #   * no pub/sub event feeds it (it is a RECORD, not an input), so the
    #     claim-driven paths never schedule it;
    #   * claims are month-scoped, and this calculator must rewrite EVERY prior
    #     month each run — forward horizons (63-504 trading days) mature long
    #     after their snapshot was written, and a month filter would silently
    #     freeze old partitions' maturation.
    # Daily and cheap; runs after the gold stage so capital_cycle_name and
    # daily_returns are fresh. Refuses to write (exit 1) if a thesis is somehow
    # invalidated on its own entry date — that is a producer bug, not data.
    Invoke-Stage 'thesis_event_outcomes calculate' $GoldExe @('--env', $Env, 'calculate', '--calculator', 'thesis_event_outcomes', '--instrument', 'stock')

    # ── SELECTION ("Today's Longs") — needs crv2 + regime + realized_volatility ─
    # 🚨 -StopOnError (psql ON_ERROR_STOP=1) is LOAD-BEARING here, not tidiness.
    # This file is transactional (BEGIN / DELETE / INSERT / COMMIT). Without the
    # flag, psql continues past a failed statement and EXITS 0, so a rejected
    # INSERT rolls the transaction back, leaves selection_daily at yesterday's
    # date, and this stage still logs "selection_rollup: OK" — the sentinel
    # reads healthy and the next briefing gates on stale data.
    # Found 2026-09-07: a column-count drift between the INSERT list and the
    # SELECT would have failed exactly this way on the Tue 09-08 run.
    Invoke-Sql 'selection_rollup' (Join-Path $SqlDir 'selection_rollup.sql') -StopOnError

    # ── THESIS ASSEMBLER (Phase 4, SPEC-phase4-thesis-assembler.md) ─────────────
    # 15b. Merges the three funnels (special_situations, capital_cycle_name,
    # theme_bottleneck) into thesis_candidates — the briefing §3b hand-off.
    # Explicit like 14b/15: no pub/sub event feeds it (PG sources the ledger
    # does not track). Position is load-bearing on BOTH sides: AFTER
    # selection_rollup because every candidate is stamped with the selection
    # verification block (quant_verdict), and BEFORE the paper portfolio
    # because daily_review is its next consumer. Refuses (exit 1) if the
    # special_situations anchor is stale — an assembler on yesterday's
    # situations republishes dead forced-flow windows; slower feeds carry
    # forward under the per-feed windows in gold-calculator.yaml.
    Invoke-Stage 'thesis_assembler calculate' $GoldExe @('--env', $Env, 'calculate', '--calculator', 'thesis_assembler', '--instrument', 'stock')

    # -- PAPER PORTFOLIO -- books pending decisions, marks, reviews, post-mortems --
    # HERE rather than on its own clock. It must run AFTER selection_rollup
    # (daily_review.py reads selection_daily) and after the gold load, and
    # guessing a time is exactly what produced the 2026-07-29 thin day folder:
    # the update fired before the session was in the DB, logged
    # "! fill session ... not in DB yet -- deferring", skipped the mark, and
    # left a folder containing nothing but signals.md.
    #
    # BEST-EFFORT, like stages 17-18, and that is deliberate. daily_update.py
    # now exits 1 when decisions.csv fails its shape check -- a real and useful
    # refusal, but a PAPER-LEDGER problem. Routing it through Invoke-Stage would
    # add it to $Failures, flip the completion sentinel to FAILURES, and through
    # that stop the next quant-briefing from being built. The lakehouse's health
    # and the paper book's ledger are different things; neither should gate the
    # other. Failures are logged loudly here and in days\<session>\run.log.
    #
    # Via the .bat, not python directly: it already sets QV_PORTFOLIO_DATA and
    # cd's, and it appends to daily_update.log -- which until now held a single
    # line, because nothing ever called it. Idempotent: an already-marked
    # session is skipped, so a re-run or a double fire costs nothing.
    #
    # Prod only -- the book exists once and there is no test paper portfolio.
    if ($Env -eq 'prod') {
        $PortfolioBat = 'D:\quantvector\claude_briefing\portfolio\run_daily_update.bat'
        Write-Log '-- portfolio daily_update --'
        try {
            & $PortfolioBat 2>&1 | Tee-Object -FilePath $LogFile -Append
            if ($LASTEXITCODE -ne 0) {
                Write-Log "portfolio daily_update: exit $LASTEXITCODE -- see days\<session>\run.log (ledger shape check?)" 'ERROR'
            } else {
                Write-Log 'portfolio daily_update: OK'
            }
        } catch {
            Write-Log "portfolio daily_update skipped: $($_.Exception.Message)" 'WARN'
        }
    }

    # -- BASE-RATE AUDIT -- regenerates the table the briefing's section 4 quotes --
    # BRIEFING_RULES section 4.4 says "regenerate, never retype", and until
    # 2026-08-07 nothing regenerated it: the briefing cited a dated markdown
    # artifact because scripts/base_rate_audit.py needed Python >= 3.12 for one
    # f-string and the briefing sandbox has 3.10. That line is fixed, so the only
    # thing missing was a producer. This is it.
    #
    # NOT via Invoke-Py, deliberately. Invoke-Py appends to $script:Failures, which
    # flips the Stage 18 sentinel to FAILURES, which stops the NEXT MORNING'S
    # BRIEFING FROM BEING BUILT AT ALL. That trade is wrong here: this artifact is
    # annotation-tier, and a stale base-rate table should degrade section 4 to a
    # loud caveat, never suppress the whole briefing. Same reasoning the portfolio
    # block above records for itself. (Note Invoke-Py's own comment claims it
    # "NEVER blocks the pipeline" while adding to $Failures -- worth reconciling,
    # but not from here.)
    #
    # --check 3.2 only: that is the effective-N / cluster-robust table section 4
    # publishes. The full nine-check audit takes far longer and its other verdicts
    # are documentation questions, not daily inputs.
    $BaseRateJson = 'D:\quantvector\claude_briefing\base-rates-latest.json'
    Write-Log '-- base_rate_audit (section 3.2) --'
    try {
        $py = Get-Command py -ErrorAction SilentlyContinue
        if (-not $py) {
            Write-Log 'base_rate_audit skipped: py launcher not on PATH' 'WARN'
        } else {
            & $py.Source -3.13 'scripts/base_rate_audit.py' '--check' '3.2' `
                '--json' $BaseRateJson 2>&1 | Tee-Object -FilePath $LogFile -Append
            # Exit code is NOT the health signal here. The audit exits 0 while
            # reporting FAIL verdicts -- that is its job, and section 3.2 currently
            # FAILs by design because the PUBLISHED table upstream still carries no
            # effective N. What matters to the briefing is only whether the artifact
            # was written, so that is what gets checked and logged.
            if (Test-Path $BaseRateJson) {
                $age = (Get-Item $BaseRateJson).LastWriteTime
                Write-Log ("base_rate_audit: OK -> {0} (written {1:yyyy-MM-dd HH:mm})" -f $BaseRateJson, $age)
            } else {
                Write-Log "base_rate_audit: ran but wrote no artifact -- section 4 will caveat" 'WARN'
            }
        }
    } catch {
        Write-Log "base_rate_audit skipped: $($_.Exception.Message)" 'WARN'
    }
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

    # ── gold_gap_sessions ────────────────────────────────────────────────────
    # How many trading sessions gold is behind the market, measured by the
    # gold-calculator against trading_calendar + ingested bellwether bars and
    # dropped in `.gold-freshness.json`.
    #
    # 🚨 READ, never recompute. The old in-calculator check measured staleness
    # by counting rows in `daily_returns` -- a gold table written by the run
    # being verified -- so it read zero exactly when the pipeline was behind and
    # reported `status: OK` over a two-day-old universe on 2026-08-05. The rule
    # now has one implementation; a second query here would be free to drift
    # from it and would relearn the same lesson.
    #
    # $null (not 0) when the file is missing or unreadable: "not measured" and
    # "no lag" are different facts, and a consumer must be able to tell them
    # apart. Reporting 0 here would rebuild the very blindness this fixes.
    # ── ingest_lag_sessions ──────────────────────────────────────────────────
    # 🚨 gold_gap_sessions ALONE IS NOT A FRESHNESS CHECK (2026-09-04).
    # It measures gold against min(latest_session, latest_ingested), so when
    # INGEST stalls the reference falls with it and the gap reads 0 at exactly
    # the moment the universe is behind. On 2026-09-04 it read 0 with
    # latest_ingested=2026-09-02 against latest_session=2026-09-04 - two
    # completed sessions missing, because 09-03's bulk_eod files downloaded but
    # failed to register in the ledger. This run only avoided reporting OK
    # because an unrelated stage happened to fail.
    #
    # The calculator now also emits ingest_lag_sessions (uncapped) and
    # ingest_stale. Same rule as above: READ, never recompute.
    $goldGap = $null
    $goldFreshness = $null
    $ingestLag = $null
    $ingestStale = $false
    $maxIngestLag = $null
    # Distinguishes "the calculator does not emit this yet" (an older binary)
    # from "the calculator emitted it as null" (measured, and unknown). Only the
    # SECOND is a reason to downgrade status - treating the first as UNKNOWN
    # would make every night read UNKNOWN until the gold-calculator is rebuilt,
    # which is a false alarm, and a guard that cries wolf nightly is one nobody
    # reads. See the status expression below.
    $ingestFieldPresent = $false
    try {
        $freshnessPath = Join-Path $Lakehouse '.gold-freshness.json'
        if (Test-Path $freshnessPath) {
            $f = Get-Content $freshnessPath -Raw | ConvertFrom-Json
            $goldGap = $f.gold_gap_sessions
            $goldFreshness = $f.scopes
            $ingestFieldPresent = ($f.PSObject.Properties.Name -contains 'ingest_lag_sessions')
            $ingestLag = $f.ingest_lag_sessions
            $maxIngestLag = $f.max_ingest_lag_sessions
            # -eq $true so a missing property (an older calculator build) reads
            # as false rather than throwing.
            $ingestStale = ($f.ingest_stale -eq $true)
            if ($goldGap -gt 0) {
                Write-Log ("gold is {0} session(s) behind the market" -f $goldGap) 'WARN'
            }
            if ($ingestStale) {
                Write-Log ("INGEST IS STALE: {0} session(s) behind the trading calendar (tolerance {1}). Bars for a completed session never landed - check `download daily` failures and `downloader reregister-orphans --dry-run`." -f $ingestLag, $maxIngestLag) 'ERROR'
            } elseif (-not $ingestFieldPresent) {
                Write-Log 'gold-calculator predates the ingest-lag measure (no ingest_lag_sessions field) - rebuild it; until then gold_gap_sessions is BLIND to an ingest stall' 'WARN'
            } elseif ($null -eq $ingestLag) {
                Write-Log 'ingest lag not measured - freshness is UNKNOWN, not healthy' 'WARN'
            }
        } else {
            Write-Log 'gold freshness file absent - gold_gap_sessions reported as null' 'WARN'
        }
    } catch {
        Write-Log "gold freshness read skipped: $($_.Exception.Message)" 'WARN'
    }

    # ── consecutive_failures ─────────────────────────────────────────────────
    # 🚨 The sentinel is overwritten every run, so it has no memory. That is why
    # `meta-manager sync-corporate-actions` could fail EIGHT nights running
    # while each morning's file looked like a fresh single-night blip - there
    # was nothing to compare against, and the only cross-night evidence was the
    # per-run logs.
    #
    # Carry a per-stage streak forward: read the PREVIOUS sentinel before
    # overwriting it, and for each stage failing now, increment what it carried.
    # A stage that succeeds drops out of the map, so a streak only ever counts
    # CONSECUTIVE nights.
    $consecutiveFailures = @{}
    try {
        if (Test-Path $sentinelPath) {
            $prev = Get-Content $sentinelPath -Raw | ConvertFrom-Json
            $prevStreaks = @{}
            if ($prev.PSObject.Properties.Name -contains 'consecutive_failures' -and $prev.consecutive_failures) {
                foreach ($p in $prev.consecutive_failures.PSObject.Properties) {
                    $prevStreaks[$p.Name] = [int]$p.Value
                }
            }
            foreach ($stage in $script:Failures) {
                $consecutiveFailures[$stage] = 1 + $(if ($prevStreaks.ContainsKey($stage)) { $prevStreaks[$stage] } else { 0 })
            }
        } else {
            foreach ($stage in $script:Failures) { $consecutiveFailures[$stage] = 1 }
        }
    } catch {
        Write-Log "previous sentinel unreadable, streaks restart at 1: $($_.Exception.Message)" 'WARN'
        foreach ($stage in $script:Failures) { $consecutiveFailures[$stage] = 1 }
    }
    # Surface a repeat failure loudly. A stage failing two nights running is not
    # a transient and will not fix itself.
    $repeatFailures = @($consecutiveFailures.Keys | Where-Object { $consecutiveFailures[$_] -ge 2 })
    foreach ($stage in $repeatFailures) {
        Write-Log ("REPEAT FAILURE: '{0}' has now failed {1} nights running - this is not transient" -f $stage, $consecutiveFailures[$stage]) 'ERROR'
    }

    # ── status ───────────────────────────────────────────────────────────────
    # THREE values, not two (Cowork ruling 2026-08-06):
    #   FAILURES - a stage failed
    #   STALE    - every stage succeeded but gold is behind the market
    #   OK       - succeeded AND current
    #
    # 🚨 A consumer asking "can I trust this run?" reads ONE field. With only
    # OK/FAILURES, a run that succeeded over a two-session-old universe answers
    # "OK" and is only corrected if the consumer also knows to read
    # gold_gap_sessions - which is the same trust-it-has-not-earned problem the
    # freshness check was written to fix, moved one layer up.
    #
    # This is NOT a re-tune: the bail threshold, the comparison and the
    # tolerance are all untouched in the calculator. It only stops a
    # within-tolerance lag from presenting as clean. A gap beyond tolerance
    # never reaches here - the calculator refuses and the stage fails.
    #
    # $null gap means NOT MEASURED, which must not read as OK either.
    #
    # 🚨 ingest_stale is checked BEFORE the gold gap and outranks it. A stalled
    # ingest presents as gold_gap_sessions = 0 (see above), so ordering it after
    # the gap check would leave it unreachable - the exact blindness this is
    # here to close. An unmeasurable ingest lag is UNKNOWN, never OK.
    #
    # The `$ingestFieldPresent -and` guard is deliberate: an older
    # gold-calculator emits no ingest_lag_sessions at all, and downgrading every
    # such night to UNKNOWN would be a nightly false alarm. That case is a loud
    # WARN in the log instead (see above) and leaves status on the old rules --
    # which means it is BLIND to an ingest stall until the calculator is
    # rebuilt. Once the field is emitted, a null value is a real "cannot
    # measure" and does downgrade.
    $status = if ($script:Failures.Count) { 'FAILURES' }
              elseif ($ingestStale)       { 'STALE' }
              elseif ($null -eq $goldGap) { 'UNKNOWN' }
              elseif ($ingestFieldPresent -and $null -eq $ingestLag) { 'UNKNOWN' }
              elseif ($goldGap -gt 0)     { 'STALE' }
              else                        { 'OK' }

    $weeklyFailed = @($script:Failures | Where-Object { $WeeklyStageNames -contains $_ }).Count -gt 0
    $weeklyStamp = if ($IsWeekly -and -not $weeklyFailed -and $weekCloser) { $weekCloser } else { $lastWeekly }
    if ($IsWeekly -and $weeklyFailed) {
        Write-Log 'weekly stages FAILED this run — not advancing last_weekly_completed_session (will retry next run)' 'WARN'
    }

    [pscustomobject]@{
        completed_utc           = (Get-Date).ToUniversalTime().ToString('o')
        status                  = $status
        failures                = @($script:Failures)
        # Per-stage consecutive-night streaks; a stage that succeeds drops out.
        consecutive_failures    = $consecutiveFailures
        # Stages failing >= 2 nights running: the "this is not transient" set.
        repeat_failures         = @($repeatFailures)
        gold_gap_sessions       = $goldGap
        # 🚨 Read BOTH. gold_gap_sessions is blind to an ingest stall by
        # construction; ingest_lag_sessions is what catches a missing session.
        ingest_lag_sessions     = $ingestLag
        max_ingest_lag_sessions = $maxIngestLag
        ingest_stale            = $ingestStale
        gold_freshness          = $goldFreshness
        # Drives the weekly gate on the NEXT run (see "Weekly gate" above).
        # Advanced only when this run actually ran the weekly stages AND none of
        # them failed — a failed weekly stage must be retried next run, not
        # marked done. Otherwise the prior value is carried forward unchanged;
        # writing $null here would re-fire weekly every single night.
        last_weekly_completed_session = $weeklyStamp
        log_file                = $LogFile
        run_stamp               = $stamp
    } | ConvertTo-Json -Depth 5 | Set-Content -Path $sentinelPath -Encoding UTF8
    Write-Log ("completion sentinel written: {0} (status={1} gold_gap_sessions={2} ingest_lag_sessions={3})" -f `
        $sentinelPath, $status, $(if ($null -eq $goldGap) { 'null' } else { $goldGap }), `
        $(if ($null -eq $ingestLag) { 'null' } else { $ingestLag }))
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
