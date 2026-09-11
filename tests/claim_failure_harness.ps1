# Harness for daily_pipeline.ps1's gold claim-failure handling (spec
# IMPLEMENTATION-SPEC-duckdb-claim-dispatch-desync section 4, option C).
#
# Parses the REAL script (or a candidate copy), loads its functions and
# constants, executes the REAL Stage 18 block against throwaway lakehouse
# directories, and executes the REAL claim-failure report step against real
# binaries in the TEST env. Never touches the prod lakehouse.
#
# Run it from a directory OUTSIDE the repo, as the scheduled task is: the report
# step depends on the working directory, and a harness run from the repo root
# once passed over exactly that bug.
#
#   pwsh -NoProfile -File tests\claim_failure_harness.ps1 `
#       -OldExe <a gold-calculator without `claim-failures`> `
#       -NewExe <a gold-calculator with it>
#
# Set $env:CF_DEBUG=1 to dump each case's raw sentinel.
[CmdletBinding()]
param(
    [string]$ScriptPath = (Join-Path (Split-Path $PSScriptRoot -Parent) 'daily_pipeline.ps1'),
    [string]$Scratch = $env:TEMP,
    # Real binaries for the S1/S2 cases. Omitting them is a FAIL, not a skip: a
    # harness that reads green over cases it never ran is the exact failure this
    # feature exists to catch.
    [string]$OldExe,
    [string]$NewExe,
    # Where the TEST env's gold-calculator writes its report (S2 reads it back).
    [string]$TestLakehouse = 'D:\quantvector\lakehouse-test'
)
$ErrorActionPreference = 'Stop'
$script:results = [System.Collections.Generic.List[string]]::new()
function Check([string]$Name, [bool]$Cond, [string]$Detail = '') {
    $script:results.Add($(if ($Cond) { "PASS  $Name" } else { "FAIL  $Name  $Detail" }))
}

$src = Get-Content $ScriptPath -Raw
$tokens = $null; $errs = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($ScriptPath, [ref]$tokens, [ref]$errs)
Check 'script parses with zero errors' ($errs.Count -eq 0) (($errs | ForEach-Object Message) -join '; ')

foreach ($f in $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true)) {
    . ([scriptblock]::Create($f.Extent.Text))
}
foreach ($n in 'Read-ClaimFailureReport', 'Get-ClaimFailureStreaks', 'Read-SentinelStreaks') {
    Check "function $n exists" ([bool](Get-Command $n -ErrorAction SilentlyContinue))
}
function Get-Assignment([string]$Name) {
    $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq "`$$Name" }, $true) | Select-Object -First 1
}
foreach ($v in 'ClaimFailureEscalateNights', 'ClaimFailuresFile') {
    $a = Get-Assignment $v
    Check "constant `$$v defined" ($null -ne $a)
    if ($a) { . ([scriptblock]::Create($a.Extent.Text)) }
}
$repoAssign = Get-Assignment 'Repo'
if ($repoAssign) { . ([scriptblock]::Create($repoAssign.Extent.Text)) }
if (-not $ClaimFailuresFile) { $ClaimFailuresFile = '.gold-claim-failures.json' }
Check 'harness is NOT running from inside the repo' (-not ($Repo -and (Get-Location).Path -like "$Repo*")) "cwd=$((Get-Location).Path)"

# Select the SMALLEST matching try block. The pipeline's outer `try` also
# contains these strings, and executing it would run the whole nightly.
function Smallest($nodes) { $nodes | Sort-Object { $_.Extent.Text.Length } | Select-Object -First 1 }
$stage18 = Smallest ($ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.TryStatementAst] -and $n.Body.Extent.Text -match "'\.pipeline-complete\.json'" }, $true))
Check 'stage 18 block found' ($null -ne $stage18)
$reportStep = Smallest ($ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.TryStatementAst] -and $n.Body.Extent.Text -match 'claim-failures' }, $true))
Check 'claim-failure report step found' ($null -ne $reportStep)
$reportSafe = $false
$reportInside = $false
if ($reportStep) {
    Check 'report step is NOT routed through Invoke-Stage' ($reportStep.Extent.Text -notmatch 'Invoke-Stage')
    Check 'report step passes --env' ($reportStep.Extent.Text -match '--env \$Env')
    $reportSafe = $reportStep.Extent.Text.Length -lt 4000
    Check 'report step is the small inner try (safe to execute)' $reportSafe "length=$($reportStep.Extent.Text.Length)"
    # The invariant the first install broke: the binaries resolve .env from their
    # working directory, which is the repo only inside Push-Location $Repo.
    $pushAt = $src.IndexOf('Push-Location $Repo')
    $popAt = $src.IndexOf('finally { Pop-Location }')
    $reportInside = ($pushAt -ge 0 -and $popAt -gt $pushAt -and $reportStep.Extent.StartOffset -gt $pushAt -and $reportStep.Extent.EndOffset -lt $popAt)
    Check 'report step runs INSIDE the Push-Location $Repo block' $reportInside "push=$pushAt pop=$popAt start=$($reportStep.Extent.StartOffset) end=$($reportStep.Extent.EndOffset)"
}

$vc = [ordered]@{ dataset = 'volume_conviction'; failed_partitions = 18; instruments = @('etf', 'stock')
    oldest_failed_at = '2026-09-11T06:52:44Z'; latest_failed_at = '2026-09-11T06:52:44Z'; max_failure_count = 1
    latest_error = 'run_claimed_partition_duckdb: "volume_conviction" is not a DuckDB-only calc' }
function New-Report([string]$Dir, [datetime]$WrittenUtc, [object[]]$Datasets) {
    $total = 0; foreach ($d in $Datasets) { $total += $d.failed_partitions }
    [ordered]@{ written_utc = $WrittenUtc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'"); failed_partitions = $total; datasets = @($Datasets) } |
        ConvertTo-Json -Depth 5 | Set-Content (Join-Path $Dir $ClaimFailuresFile)
}
function New-PrevSentinel([string]$Dir, [hashtable]$Streaks, [hashtable]$Consecutive = @{}) {
    [ordered]@{ status = 'OK'; failures = @(); consecutive_failures = $Consecutive; claim_failure_streaks = $Streaks } |
        ConvertTo-Json -Depth 5 | Set-Content (Join-Path $Dir '.pipeline-complete.json')
}
function Invoke-Stage18Case([string]$Label, [scriptblock]$Arrange) {
    $Lakehouse = Join-Path $Scratch ('cf-harness-' + [guid]::NewGuid())
    New-Item -ItemType Directory -Path $Lakehouse | Out-Null
    $LogFile = Join-Path $Lakehouse 'harness.log'
    $stamp = 'harness'
    $runStartUtc = (Get-Date).ToUniversalTime().AddMinutes(-30)
    $WeeklyStageNames = @('download rename_reconcile', 'download capacity_weekly')
    $IsWeekly = $false; $weekCloser = $null; $lastWeekly = '2026-09-04'
    $script:Failures = @()
    [ordered]@{ written_utc = (Get-Date).ToUniversalTime().ToString('o'); gold_gap_sessions = 0; ingest_lag_sessions = 1
        max_ingest_lag_sessions = 1; ingest_stale = $false; scopes = @() } | ConvertTo-Json | Set-Content (Join-Path $Lakehouse '.gold-freshness.json')
    # Discard pipeline output: Write-Log tees every line to the pipeline, and
    # anything uncaptured here would become part of this function's return
    # value, turning $s into an array that member-enumerates empty lists to $null.
    $null = & $Arrange $Lakehouse $runStartUtc
    if ($stage18) { $null = . ([scriptblock]::Create($stage18.Extent.Text)) }
    $out = $null
    $raw = $null
    try { $raw = Get-Content (Join-Path $Lakehouse '.pipeline-complete.json') -Raw; $out = $raw | ConvertFrom-Json } catch { }
    if ($env:CF_DEBUG) { Write-Host "---- $Label raw sentinel ----`n$raw" }
    Remove-Item -Recurse -Force $Lakehouse
    return $out
}
function Streak($s, [string]$ds) { if ($s -and $s.claim_failure_streaks) { $s.claim_failure_streaks.$ds } else { $null } }
function StreakCount($s) { if ($s -and $s.PSObject.Properties.Name -contains 'claim_failure_streaks' -and $null -ne $s.claim_failure_streaks) { @($s.claim_failure_streaks.PSObject.Properties).Count } else { -1 } }
# Null-safe list count: in PowerShell @($null).Count is 1, not 0.
function ListCount($x) { if ($null -eq $x) { -1 } else { @($x).Count } }
$esc = 'gold claim failures: volume_conviction'

# C1 - no report, no previous sentinel: not measured, nothing escalated.
$s = Invoke-Stage18Case 'C1' { param($d, $t) }
Check 'C1 sentinel written' ($null -ne $s)
Check 'C1 claim_failures field present and null' ($s -and ($s.PSObject.Properties.Name -contains 'claim_failures') -and $null -eq $s.claim_failures)
Check 'C1 status OK' ($s.status -eq 'OK') "status=$($s.status)"
Check 'C1 no streaks' ((StreakCount $s) -eq 0) "count=$(StreakCount $s)"

# C2 - fresh report with a failure, first night: banner only, status untouched.
$s = Invoke-Stage18Case 'C2' { param($d, $t) New-Report $d (Get-Date).ToUniversalTime() @($vc) }
Check 'C2 claim_failures carries the dataset' ($s -and (ListCount $s.claim_failures) -eq 1 -and $s.claim_failures[0].dataset -eq 'volume_conviction' -and $s.claim_failures[0].failed_partitions -eq 18)
Check 'C2 streak = 1' ((Streak $s 'volume_conviction') -eq 1)
Check 'C2 NOT escalated, status OK' ($s.status -eq 'OK' -and (ListCount $s.failures) -eq 0) "status=$($s.status) failures=$(ListCount $s.failures)"

# C3 - second consecutive night: escalated into failures[], status and consecutive_failures.
$s = Invoke-Stage18Case 'C3' { param($d, $t) New-Report $d (Get-Date).ToUniversalTime() @($vc); New-PrevSentinel $d @{ volume_conviction = 1 } }
Check 'C3 streak = 2' ((Streak $s 'volume_conviction') -eq 2)
Check 'C3 escalated into failures[]' ($s -and @($s.failures) -contains $esc) "failures=$($s.failures -join ',')"
Check 'C3 status FAILURES' ($s.status -eq 'FAILURES') "status=$($s.status)"
Check 'C3 escalation counted in consecutive_failures' ($s -and $s.consecutive_failures.$esc -eq 1) "value=$($s.consecutive_failures.$esc)"

# C4 - fresh EMPTY report: measured and healthy; streaks clear.
$s = Invoke-Stage18Case 'C4' { param($d, $t) New-Report $d (Get-Date).ToUniversalTime() @(); New-PrevSentinel $d @{ volume_conviction = 3 } }
Check 'C4 claim_failures is an empty list, not null' ($s -and (ListCount $s.claim_failures) -eq 0) "count=$(ListCount $s.claim_failures)"
Check 'C4 streaks cleared' ((StreakCount $s) -eq 0) "count=$(StreakCount $s)"
Check 'C4 status OK' ($s.status -eq 'OK') "status=$($s.status)"

# C5 - report left over from an earlier night: not measured, streaks carried.
$s = Invoke-Stage18Case 'C5' { param($d, $t) New-Report $d $t.AddDays(-1) @($vc); New-PrevSentinel $d @{ volume_conviction = 1 } }
Check 'C5 stale report reads as not measured' ($s -and $null -eq $s.claim_failures)
Check 'C5 streak carried unchanged' ((Streak $s 'volume_conviction') -eq 1)
Check 'C5 not escalated' ($s.status -eq 'OK' -and (ListCount $s.failures) -eq 0) "status=$($s.status) failures=$(ListCount $s.failures)"

# C6 - malformed report: the sentinel is still written.
$s = Invoke-Stage18Case 'C6' { param($d, $t) Set-Content (Join-Path $d $ClaimFailuresFile) '{ not json'; New-PrevSentinel $d @{ volume_conviction = 1 } }
Check 'C6 sentinel still written' ($null -ne $s)
Check 'C6 malformed reads as not measured, streak carried' ($s -and $null -eq $s.claim_failures -and (Streak $s 'volume_conviction') -eq 1)

# C7 - unmeasured night after an escalation: the alarm is held, not cleared.
$s = Invoke-Stage18Case 'C7' { param($d, $t) New-PrevSentinel $d @{ volume_conviction = 2 } }
Check 'C7 escalation held on an unmeasured night' ($s -and @($s.failures) -contains $esc -and $s.status -eq 'FAILURES') "status=$($s.status)"

# C8 - an unrelated real stage failure is untouched by an empty report.
$s = Invoke-Stage18Case 'C8' { param($d, $t) $script:Failures = @('meta-manager x'); New-Report $d (Get-Date).ToUniversalTime() @() }
Check 'C8 real failure preserved' ($s -and @($s.failures) -contains 'meta-manager x' -and $s.status -eq 'FAILURES')

# C9 - REGRESSION: consecutive_failures still carries a real stage's streak.
$s = Invoke-Stage18Case 'C9' { param($d, $t) $script:Failures = @('meta-manager x'); New-PrevSentinel $d @{} @{ 'meta-manager x' = 2 } }
Check 'C9 consecutive_failures carried (2 -> 3)' ($s -and $s.consecutive_failures.'meta-manager x' -eq 3) "value=$($s.consecutive_failures.'meta-manager x')"
Check 'C9 repeat_failures lists the stage' ($s -and @($s.repeat_failures) -contains 'meta-manager x')

# F1 - streak carry does not alias the previous hashtable.
if (Get-Command Get-ClaimFailureStreaks -ErrorAction SilentlyContinue) {
    $prev = @{ volume_conviction = 1 }
    $next = Get-ClaimFailureStreaks -Report $null -PreviousStreaks $prev
    $next['x'] = 9
    Check 'F1 carried streaks are a copy' (-not $prev.ContainsKey('x'))
}

# S1/S2 - the REAL report step, run against REAL binaries in the TEST env, inside
# `Push-Location $Repo` exactly as production runs it (placement asserted above).
# The old-binary path (S1) is what a nightly takes until the binary is rebuilt.
if ($reportStep -and $reportSafe -and $reportInside -and $Repo -and $OldExe -and $NewExe) {
    function Invoke-ReportStep([string]$Exe) {
        $LogFile = Join-Path $Scratch ('report-step-' + [guid]::NewGuid() + '.log')
        $GoldExe = $Exe
        $Env = 'test'
        $script:Failures = @()
        $threw = $false
        Push-Location $Repo
        try { $null = . ([scriptblock]::Create($reportStep.Extent.Text)) } catch { $threw = $true }
        finally { Pop-Location }
        $log = if (Test-Path $LogFile) { Get-Content $LogFile -Raw } else { '' }
        Remove-Item -Force $LogFile -ErrorAction SilentlyContinue
        return [pscustomobject]@{ Failures = @($script:Failures); Log = $log; Threw = $threw }
    }
    $r = Invoke-ReportStep $OldExe
    Check 'S1 old binary: step does not throw' (-not $r.Threw)
    Check 'S1 old binary: nothing added to $Failures' (@($r.Failures).Count -eq 0) "failures=$($r.Failures -join ',')"
    Check 'S1 old binary: logged as a WARN, not a failure' ($r.Log -match 'gold claim-failure report: (exit|skipped)') ($r.Log -split "`n" | Select-Object -Last 3)

    $caseStart = (Get-Date).ToUniversalTime().AddSeconds(-2)
    $r = Invoke-ReportStep $NewExe
    Check 'S2 new binary: nothing added to $Failures' (@($r.Failures).Count -eq 0)
    Check 'S2 new binary: report written' ($r.Log -match 'gold claim-failure report: written') ($r.Log -split "`n" | Select-Object -Last 3)
    $real = $null
    try { $real = Read-ClaimFailureReport -Path (Join-Path $TestLakehouse $ClaimFailuresFile) -RunStartUtc $caseStart } catch { }
    Check 'S2 real Rust report parses through the real reader as THIS run' ($null -ne $real -and $null -ne $real.datasets)
} else {
    Check 'S1/S2 report-step cases ran' $false "step=$([bool]$reportStep) safe=$reportSafe inside=$reportInside Repo='$Repo' OldExe='$OldExe' NewExe='$NewExe'"
}

$script:results | ForEach-Object { $_ }
$fails = @($script:results | Where-Object { $_ -like 'FAIL*' }).Count
"`n$($script:results.Count - $fails) passed, $fails failed"
exit $fails
