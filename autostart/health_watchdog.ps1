# =============================================================================
# ALPHAWAVE INDICATOR SIGNALS HEALTH WATCHDOG
# Ensures dashboard & telegram scanner on http://127.0.0.1:8800 are ALWAYS running.
# Runs silently every 1 minute via Task Scheduler ("Indicator_Signals_Watchdog").
# =============================================================================

$ErrorActionPreference = "Continue"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$Root = Split-Path -Parent $ScriptDir
if (-not $Root -or -not (Test-Path "$Root\app.py")) {
    $Root = "C:\Projects\trading\ChartSignalGenerator"
}

$Port = 8800
$HealthUrl = "http://127.0.0.1:$Port/healthz"
$SignalsUrl = "http://127.0.0.1:$Port/api/signals"

$PyExe = "C:\Users\rkmat\AppData\Local\Programs\Python\Python311\python.exe"
if (-not (Test-Path $PyExe)) {
    $PyCmd = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($PyCmd) { $PyExe = $PyCmd.Source } else { $PyExe = "python.exe" }
}

$LogDir = Join-Path $Root "data\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$OutLog = Join-Path $LogDir "server.log"
$ErrLog = Join-Path $LogDir "server.err.log"
$WatchdogLog = Join-Path $LogDir "watchdog.log"
$LockFile = Join-Path $LogDir "server.start.lock"
$StrikeFile = Join-Path $LogDir "healthz.strikes"
$StrikesToRestart = 2

function Write-Log([string]$msg) {
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $WatchdogLog -Value "$stamp | $msg"
}

function Get-DashboardProcesses {
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -and
        $_.CommandLine -like "*uvicorn*" -and
        $_.CommandLine -like "*app:app*" -and
        $_.CommandLine -like "*8800*"
    }
}

function Test-Health {
    # Check healthz first, fallback to signals endpoint
    for ($i = 1; $i -le 2; $i++) {
        try {
            $resp = Invoke-WebRequest -Uri $HealthUrl -UseBasicParsing -TimeoutSec 4
            if ($resp.StatusCode -eq 200) { return $true }
        } catch { }

        try {
            $resp2 = Invoke-WebRequest -Uri $SignalsUrl -UseBasicParsing -TimeoutSec 4
            if ($resp2.StatusCode -eq 200) { return $true }
        } catch { }

        if ($i -lt 2) { Start-Sleep -Seconds 1 }
    }
    return $false
}

function Get-Strikes {
    if (-not (Test-Path $StrikeFile)) { return 0 }
    try { return [int](Get-Content $StrikeFile -ErrorAction Stop | Select-Object -First 1) }
    catch { return 0 }
}

function Set-Strikes([int]$n) {
    Set-Content -Path $StrikeFile -Value $n -Encoding ascii
}

function Clear-Strikes {
    if ((Get-Strikes) -ne 0) { Set-Strikes 0 }
}

function Test-RecentLock {
    if (-not (Test-Path $LockFile)) { return $false }
    $age = (Get-Date) - (Get-Item $LockFile).LastWriteTime
    return ($age.TotalSeconds -lt 45)
}

function Stop-Dashboard {
    $procs = @(Get-DashboardProcesses)
    foreach ($p in $procs) {
        Write-Log "Stopping server PID $($p.ProcessId)"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
}

function Start-Dashboard {
    if (Test-RecentLock) {
        Write-Log "Start skipped - recently started within last 45s"
        return
    }
    Set-Content -Path $LockFile -Value (Get-Date -Format o) -Encoding ascii
    Write-Log "Starting AlphaWave Dashboard on port $Port..."
    
    Start-Process -FilePath $PyExe `
        -ArgumentList "-m uvicorn app:app --host 127.0.0.1 --port $Port" `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $OutLog `
        -RedirectStandardError $ErrLog
}

# --- Execution Logic ---
$procs = @(Get-DashboardProcesses)
$isAlive = $procs.Count -gt 0
$isHealthy = Test-Health

# 1. Clean up duplicate instances if multiple exist
if ($procs.Count -gt 1) {
    if ($isHealthy) {
        $keep = $procs | Sort-Object ProcessId | Select-Object -First 1
        Write-Log "Multiple instances detected. Keeping healthy PID $($keep.ProcessId), terminating extras."
        $procs | Where-Object { $_.ProcessId -ne $keep.ProcessId } | ForEach-Object {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        }
        exit 0
    } else {
        Write-Log "Multiple unhealthy instances detected. Performing full restart."
        Stop-Dashboard
        Start-Dashboard
        exit 0
    }
}

# 2. Healthy and running
if ($isAlive -and $isHealthy) {
    Clear-Strikes
    # Periodic silent verification
    exit 0
}

# 3. Process exists but port not responding
if ($isAlive -and -not $isHealthy) {
    $strikes = (Get-Strikes) + 1
    Set-Strikes $strikes
    if ($strikes -lt $StrikesToRestart) {
        Write-Log "Port $Port unresponsive (Strike $strikes/$StrikesToRestart). PID $($procs[0].ProcessId) still alive, waiting for next check."
        exit 0
    }
    Write-Log "Port $Port unresponsive for $strikes consecutive checks. Restarting server PID $($procs[0].ProcessId)..."
    Set-Strikes 0
    Stop-Dashboard
    Start-Dashboard
    exit 0
}

# 4. Process is dead / down: immediately revive
Clear-Strikes
Write-Log "Dashboard server is DOWN. Reviving immediately..."
Start-Dashboard
