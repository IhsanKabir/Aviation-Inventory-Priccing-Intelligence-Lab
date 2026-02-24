param(
    [string]$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [double]$StaleMinutes = 15,
    [string[]]$TaskNames = @("AirlineIntel_Ingestion4H", "AirlineIntel_Ingestion4H_User")
)

$ErrorActionPreference = "SilentlyContinue"

function Get-HeartbeatTimestampText {
    param([hashtable]$Payload)
    foreach ($k in @(
        "accumulation_written_at_utc",
        "written_at_utc",
        "accumulation_last_query_at_utc",
        "last_query_at_utc",
        "accumulation_started_at_utc",
        "started_at_utc"
    )) {
        if ($Payload.ContainsKey($k) -and $Payload[$k]) { return [string]$Payload[$k] }
    }
    return $null
}

function Get-ExecutionTimeLimitInfo {
    param($Task)
    $raw = $null
    try { $raw = $Task.Settings.ExecutionTimeLimit } catch {}

    # Handle both TimeSpan and XML duration string (e.g., PT3H, PT0S)
    if ($raw -is [TimeSpan]) {
        if ($raw -eq [TimeSpan]::Zero) {
            return @{ Raw = $raw; Text = "NoLimit"; Risk = $false; Hours = 0.0 }
        }
        return @{ Raw = $raw; Text = $raw.ToString(); Risk = ($raw.TotalHours -le 3.0); Hours = $raw.TotalHours }
    }

    $rawText = [string]$raw
    if ([string]::IsNullOrWhiteSpace($rawText)) {
        return @{ Raw = $raw; Text = "Unknown"; Risk = $false; Hours = $null }
    }

    if ($rawText -eq "PT0S") {
        return @{ Raw = $raw; Text = "NoLimit"; Risk = $false; Hours = 0.0 }
    }

    # Rough parser for common ISO-8601 duration patterns used by Task Scheduler
    $hours = 0.0
    if ($rawText -match "PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?") {
        $h = if ($Matches[1]) { [double]$Matches[1] } else { 0 }
        $m = if ($Matches[2]) { [double]$Matches[2] } else { 0 }
        $s = if ($Matches[3]) { [double]$Matches[3] } else { 0 }
        $hours = $h + ($m / 60.0) + ($s / 3600.0)
    }
    return @{ Raw = $raw; Text = $rawText; Risk = ($hours -gt 0 -and $hours -le 3.0); Hours = $hours }
}

function ConvertTo-Hashtable {
    param($Obj)
    $ht = @{}
    if ($null -eq $Obj) { return $ht }
    $Obj.PSObject.Properties | ForEach-Object {
        $ht[$_.Name] = $_.Value
    }
    return $ht
}

$Reports = Join-Path $Root "output\reports"
$HeartbeatCandidates = @(
    (Join-Path $Reports "run_all_accumulation_status_latest.json"),
    (Join-Path $Reports "run_all_status_latest.json")
)

Write-Host "=== Scheduler Tasks ==="
$TaskSummary = @()
foreach ($tn in $TaskNames) {
    $t = Get-ScheduledTask -TaskName $tn
    if (-not $t) { continue }
    $ti = Get-ScheduledTaskInfo -TaskName $tn
    $limitInfo = Get-ExecutionTimeLimitInfo -Task $t

    [pscustomobject]@{
        TaskName           = $tn
        State              = [string]$t.State
        LastRunTime        = $ti.LastRunTime
        NextRunTime        = $ti.NextRunTime
        LastTaskResult     = $ti.LastTaskResult
        ExecutionTimeLimit = $limitInfo.Text
    } | Format-List

    if ($limitInfo.Risk) {
        Write-Host "Execution time limit risk on $tn ($($limitInfo.Text))" -ForegroundColor Yellow
    }

    $TaskSummary += [pscustomobject]@{
        TaskName = $tn
        State = [string]$t.State
        LastTaskResult = $ti.LastTaskResult
        ExecutionTimeLimit = $limitInfo.Text
        ExecutionLimitRisk = [bool]$limitInfo.Risk
    }
}

Write-Host "`n=== Active Accumulation Processes ==="
$procs = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -match 'python' -and $_.CommandLine -match 'run_pipeline\.py|run_all\.py|generate_reports\.py'
} | Select-Object ProcessId, ParentProcessId, Name, CommandLine

if ($procs) {
    $procs | Format-List
} else {
    Write-Host "No accumulation processes running."
}

Write-Host "`n=== Heartbeat Freshness ==="
$hbPath = $HeartbeatCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
$HeartbeatSummary = $null
if (-not $hbPath) {
    Write-Host "Heartbeat file not found."
    $HeartbeatSummary = [pscustomobject]@{
        HeartbeatFile = $null
        State = $null
        AccumulationRunId = $null
        HeartbeatAgeMinutes = $null
        HeartbeatStatus = "MISSING"
        Progress = $null
        Current = $null
    }
} else {
    $raw = Get-Content $hbPath -Raw
    $obj = $raw | ConvertFrom-Json
    $p = ConvertTo-Hashtable -Obj $obj

    $tsText = Get-HeartbeatTimestampText -Payload $p
    $ageMin = $null
    if ($tsText) {
        try {
            $ts = [DateTimeOffset]::Parse([string]$tsText)
            $ageMin = [math]::Round(((Get-Date).ToUniversalTime() - $ts.UtcDateTime).TotalMinutes, 2)
        } catch {}
    }

    $state = if ($p.ContainsKey("state")) { $p["state"] } else { $null }
    $accId = $null
    if ($p.ContainsKey("accumulation_run_id") -and $p["accumulation_run_id"]) { $accId = $p["accumulation_run_id"] }
    elseif ($p.ContainsKey("scrape_id")) { $accId = $p["scrape_id"] }

    $currentAirline = if ($p.ContainsKey("current_airline")) { $p["current_airline"] } else { "?" }
    $currentOrigin = if ($p.ContainsKey("current_origin")) { $p["current_origin"] } else { "?" }
    $currentDest = if ($p.ContainsKey("current_destination")) { $p["current_destination"] } else { "?" }
    $currentDate = if ($p.ContainsKey("current_date")) { $p["current_date"] } else { "?" }
    $currentCabin = if ($p.ContainsKey("current_cabin")) { $p["current_cabin"] } else { "?" }
    $route = "{0} {1}->{2} {3} {4}" -f $currentAirline, $currentOrigin, $currentDest, $currentDate, $currentCabin

    $done = if ($p.ContainsKey("overall_query_completed")) { $p["overall_query_completed"] } else { "?" }
    $total = if ($p.ContainsKey("overall_query_total")) { $p["overall_query_total"] } else { "?" }
    $ageFlag = if ($ageMin -ne $null -and $ageMin -gt $StaleMinutes) { "STALE" } else { "OK" }

    $HeartbeatSummary = [pscustomobject]@{
        HeartbeatFile = $hbPath
        State = $state
        AccumulationRunId = $accId
        HeartbeatAgeMinutes = $ageMin
        HeartbeatStatus = $ageFlag
        Progress = "$done/$total"
        Current = $route
    }
    $HeartbeatSummary | Format-List
}

Write-Host "`n=== Quick Verdict ==="
$running = [bool]$procs
if ($running) {
    Write-Host "Accumulation is RUNNING now." -ForegroundColor Green
} else {
    Write-Host "No accumulation process is running now." -ForegroundColor Yellow
}

$TaskRisk = $TaskSummary | Where-Object { $_.TaskName -eq "AirlineIntel_Ingestion4H" -and $_.ExecutionLimitRisk }
if ($TaskRisk) {
    Write-Host "Scheduler execution time limit is risky (<= 3h). Consider removing it." -ForegroundColor Yellow
}
if ($HeartbeatSummary -and $HeartbeatSummary.HeartbeatStatus -eq "STALE") {
    Write-Host "Heartbeat is STALE. Recovery pulse should be allowed to relaunch when no active process exists." -ForegroundColor Yellow
}

Write-Host "`n=== Machine-Readable Summary (JSON) ==="
$summary = [pscustomobject]@{
    checked_at_local = (Get-Date).ToString("o")
    root = $Root
    task_summary = $TaskSummary
    active_accumulation_process_count = if ($procs) { @($procs).Count } else { 0 }
    heartbeat = $HeartbeatSummary
}
$summary | ConvertTo-Json -Depth 6

