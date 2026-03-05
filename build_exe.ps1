param(
    [string]$Name = "DiskIdleShutdownGUI",
    [string]$Entry = ".\disk_idle_shutdown_gui.py"
)

$ErrorActionPreference = "Stop"

Write-Host "Building self-contained executable..."
python -m PyInstaller `
  --noconfirm `
  --clean `
  --onefile `
  --windowed `
  --name $Name `
  $Entry

Write-Host ""
Write-Host "Build complete:"
Write-Host "  .\dist\$Name.exe"
