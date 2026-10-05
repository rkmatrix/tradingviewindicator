# =============================================================================
# ALPHAWAVE INDICATOR SIGNALS AUTO-LAUNCHER
# Starts background server (if not already running) and opens dashboard in browser
# =============================================================================

$ErrorActionPreference = "Continue"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptDir

$Port = 8800
$Url = "http://127.0.0.1:$Port/"
$LogDir = Join-Path $ScriptDir "data\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$LogFile = Join-Path $LogDir "autolaunch.log"

function Write-Log([string]$msg) {
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $LogFile -Value "$stamp | $msg"
}

Write-Log "Checking if Indicator Signals server is running on port $Port..."

# 1. Check if server is already responding
$running = $false
try {
    $res = Invoke-WebRequest -Uri "$Url" -TimeoutSec 3 -UseBasicParsing
    if ($res.StatusCode -eq 200) {
        $running = $true
        Write-Log "Server already active on port $Port."
    }
} catch {
    $running = $false
}

# 2. If not running, start server in background using pythonw / hidden window
if (-not $running) {
    Write-Log "Starting server in background..."
    $vbs = Join-Path $ScriptDir "run_hidden.vbs"
    if (Test-Path $vbs) {
        Start-Process "wscript.exe" -ArgumentList "`"$vbs`"" -WindowStyle Hidden
    } else {
        Start-Process "python.exe" -ArgumentList "-m uvicorn app:app --host 127.0.0.1 --port $Port" -WorkingDirectory "$ScriptDir" -WindowStyle Hidden
    }

    # Wait for server to become healthy (up to 30s)
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 1
        try {
            $check = Invoke-WebRequest -Uri "$Url" -TimeoutSec 2 -UseBasicParsing
            if ($check.StatusCode -eq 200) {
                $running = $true
                Write-Log "Server became healthy."
                break
            }
        } catch { }
    }
}

# 3. Open Dashboard in default browser
Write-Log "Opening dashboard in browser: $Url"
try {
    Start-Process $Url
} catch {
    Write-Log "Error launching browser: $($_.Exception.Message)"
}
