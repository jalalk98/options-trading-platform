# Daily B2 -> laptop sync wrapper
# Called by Windows Task Scheduler at 15:20
#
# This wrapper provides a Telegram failsafe: if daily_sync.py is killed
# externally (sleep, lid close, force-quit) before its own failure branch
# runs, the wrapper notifies Telegram instead. This catches OS-level
# terminations like STATUS_CONTROL_C_EXIT (0xC000013A).

$ProjectDir = "D:\projects\options-trading-platform"
$PythonExe  = "C:\Users\Jalal\anaconda3\envs\trading-platform\python.exe"
$LogFile    = "D:\tickdata\sync_stdout.log"
$EnvFile    = "$ProjectDir\.env"

# Ensure log directory exists
$LogDir = Split-Path $LogFile -Parent
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

Set-Location $ProjectDir

# --- Read Telegram credentials from .env ---
function Get-EnvVar {
    param([string]$Name)
    if (-not (Test-Path $EnvFile)) { return $null }
    $line = Get-Content $EnvFile | Where-Object { $_ -match "^$Name=" } | Select-Object -First 1
    if ($line) { return ($line -split '=', 2)[1].Trim() }
    return $null
}

$TgToken  = Get-EnvVar -Name "TELEGRAM_BOT_TOKEN"
$TgChatId = Get-EnvVar -Name "TELEGRAM_CHAT_ID"

function Send-TelegramAlert {
    param([string]$Message)
    if (-not $TgToken -or -not $TgChatId) {
        Add-Content -Path $LogFile -Value "[wrapper] Telegram credentials missing, alert suppressed"
        return
    }
    try {
        $url = "https://api.telegram.org/bot$TgToken/sendMessage"
        Invoke-RestMethod -Uri $url -Method Post -Body @{
            chat_id = $TgChatId
            text    = $Message
        } -TimeoutSec 10 | Out-Null
        Add-Content -Path $LogFile -Value "[wrapper] Telegram failsafe alert sent"
    } catch {
        Add-Content -Path $LogFile -Value "[wrapper] Telegram failsafe FAILED: $_"
    }
}

# --- Timestamp & start log ---
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content -Path $LogFile -Value ""
Add-Content -Path $LogFile -Value "======================================"
Add-Content -Path $LogFile -Value "[$Timestamp] Task Scheduler triggered sync"
Add-Content -Path $LogFile -Value "======================================"

# --- Run the sync ---
$ExitCode = 1
try {
    & $PythonExe "scripts\daily_sync.py" *>> $LogFile
    $ExitCode = $LASTEXITCODE
} catch {
    Add-Content -Path $LogFile -Value "[wrapper] Exception running Python: $_"
    $ExitCode = 99
}

$EndTs = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content -Path $LogFile -Value "[$EndTs] Exit code: $ExitCode"

# --- Failsafe Telegram on non-zero exit ---
# Python's daily_sync.py sends Telegram on graceful failure (returncode 1)
# But on OS-level kill (e.g. 3221225786 = STATUS_CONTROL_C_EXIT) Python
# never reaches its notification code. The wrapper covers that case.
#
# Heuristic: if exit code is NOT 0 AND NOT 1 (graceful failure already
# handled by Python), we send a wrapper-level alert.
if ($ExitCode -ne 0 -and $ExitCode -ne 1) {
    $hexCode = "0x{0:X}" -f [int64]$ExitCode
    $alertMsg = "B2DailySync wrapper alert`nPython exited with code $ExitCode ($hexCode)`nThis usually means the process was killed externally (sleep, shutdown, force-quit).`nTime: $EndTs`nRecommendation: next run will self-heal via integrity-validated date detection."
    Send-TelegramAlert -Message $alertMsg
}

exit $ExitCode
