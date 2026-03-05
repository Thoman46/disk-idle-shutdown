# Disk Idle Shutdown (Python)

This app monitors disk throughput and triggers a system shutdown when activity
stays below a setpoint for a sustained period.

You now have two entry points:
- CLI: `disk_idle_shutdown.py`
- GUI: `disk_idle_shutdown_gui.py`

It includes:
- Configurable disk speed setpoint (`--threshold-mbps`)
- Sustained low-activity timer (`--sustained-seconds`)
- Logging to file + console
- Safe test toggle (`--test-mode`) that never shuts down your machine
- Optional self-test trigger (`--self-test-seconds`) to verify end-to-end behavior
- Optional CPU and network idle gates (GUI)
- Optional process-exit gate (GUI)
- Shutdown cancel action (`shutdown /a` on Windows) in GUI

## Requirements

- Python 3.9+
- `psutil`

Install dependency:

```powershell
pip install psutil
```

Or:

```powershell
pip install -r .\requirements.txt
```

## Usage

### GUI

Launch:

```powershell
python .\disk_idle_shutdown_gui.py
```

### Build Executable (Windows)

Self-contained executable generated:

```text
.\dist\DiskIdleShutdownGUI.exe
```

Build/rebuild with:

```powershell
python -m pip install pyinstaller
.\build_exe.ps1
```

GUI controls:
- Start/Stop monitoring
- One-click presets (Balanced / Conservative / Aggressive)
- Test mode toggle (safe)
- Ultra-light mode toggle (reduced polling, reduced UI/log churn)
- Trigger Test button (always safe)
- Cancel Pending Shutdown button
- Dashboard with live disk/cpu/network/uptime metrics
- Gate status panel (Disk/CPU/Network/Process)
- Sustained-idle progress bar + grace countdown
- Live log panel with auto-scroll toggle
- Memory-safe in-memory log view cap (old lines auto-trimmed)
- CPU/Network idle gate toggles
- Process-exit gate toggle (comma-separated process names)
- Settings auto-save on close and auto-load on next launch

For lowest resource usage in GUI:
- Keep `Ultra-light mode` enabled
- Use a longer `Sample interval` (for example `5`)
- Disable `Use CPU gate` and `Use network gate` unless needed

### CLI

List available per-disk keys (for `--drives`):

```powershell
python .\disk_idle_shutdown.py --list-drives
```

Run monitoring all disks (safe test mode):

```powershell
python .\disk_idle_shutdown.py --test-mode --threshold-mbps 1.0 --sustained-seconds 120 --grace-seconds 60
```

Very low-overhead CLI run (throttled sample logging):

```powershell
python .\disk_idle_shutdown.py --test-mode --sample-log-seconds 60
```

Run real shutdown mode:

```powershell
python .\disk_idle_shutdown.py --threshold-mbps 1.0 --sustained-seconds 120 --grace-seconds 60 --shutdown-delay-seconds 30
```

Monitor only specific disk keys:

```powershell
python .\disk_idle_shutdown.py --test-mode --drives PhysicalDrive0 PhysicalDrive1
```

Force a quick self-test path after 15 seconds (still safe with `--test-mode`):

```powershell
python .\disk_idle_shutdown.py --test-mode --self-test-seconds 15
```

## Logging

Default log file:

```text
.\disk_idle_shutdown.log
```

Override log location:

```powershell
python .\disk_idle_shutdown.py --test-mode --log-file .\logs\disk-monitor.log
```

## GUI Settings Persistence

When the GUI closes, it saves current settings to:

```text
.\disk_idle_shutdown_gui.settings.json
```

On next launch, settings are loaded automatically.

## Safety Notes

- Always validate settings with `--test-mode` first.
- A longer `--sustained-seconds` helps avoid false triggers from brief idle dips.
- On Windows, the app uses `shutdown /s /t N`.
- Use `Cancel Pending Shutdown` in GUI if you need to abort a scheduled shutdown.
