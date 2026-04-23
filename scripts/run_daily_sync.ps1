# Daily B2 -> laptop sync
# Called by Windows Task Scheduler at 3:20 PM

$ProjectDir = "D:\projects\options-trading-platform"
$PythonExe = "C:\Users\Jalal\anaconda3\envs\trading-platform\python.exe"
$LogFile = "D:\tickdata\sync_stdout.log"

# Ensure log directory exists
$LogDir = Split-Path $LogFile -Parent
if (-not (Test-Path $LogDir)) {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

# Change to project directory
Set-Location $ProjectDir

# Timestamp for log
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content -Path $LogFile -Value ""
Add-Content -Path $LogFile -Value "======================================"
Add-Content -Path $LogFile -Value "[$Timestamp] Task Scheduler triggered sync"
Add-Content -Path $LogFile -Value "======================================"

# Run the sync, capture all output
& $PythonExe "scripts\daily_sync.py" *>> $LogFile
$ExitCode = $LASTEXITCODE

$EndTs = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Add-Content -Path $LogFile -Value "[$EndTs] Exit code: $ExitCode"

exit $ExitCode
