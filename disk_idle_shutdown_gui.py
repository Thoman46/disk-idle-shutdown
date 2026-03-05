#!/usr/bin/env python3
"""
GUI app for disk-idle shutdown monitoring.

This revision keeps all existing monitoring behavior and adds a more structured
dashboard UI with:
- clearer status badges
- presets
- live signal panels
- per-gate pass/wait indicators
- sustained-idle progress
"""

from __future__ import annotations

import json
import logging
import os
import platform
import queue
import subprocess
import threading
import time
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

from disk_idle_shutdown import (
    BYTES_PER_MEGABYTE,
    current_total_bytes,
    issue_shutdown,
    list_available_disks,
)

SETTINGS_FILE_NAME = "disk_idle_shutdown_gui.settings.json"
MAX_EVENT_QUEUE_ITEMS = 64
MAX_LOG_QUEUE_ITEMS = 256
MAX_GUI_LOG_LINES = 400
LOG_TRIM_BATCH_LINES = 120
MAX_LOGS_PER_DRAIN = 80
MAX_EVENTS_PER_DRAIN = 80
UI_DRAIN_INTERVAL_MS = 750


@dataclass
class GuiConfig:
    threshold_mbps: float
    interval_seconds: float
    sustained_seconds: float
    grace_seconds: float
    drives: list[str]
    test_mode: bool
    shutdown_delay_seconds: int
    self_test_seconds: float | None
    use_cpu_gate: bool
    cpu_threshold_percent: float
    use_network_gate: bool
    network_threshold_mbps: float
    require_process_exit: bool
    process_names: list[str]
    ultra_light: bool


def parse_drive_list(raw: str) -> list[str]:
    cleaned = raw.replace(",", " ")
    return [item.strip() for item in cleaned.split() if item.strip()]


def parse_process_list(raw: str) -> list[str]:
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


def speed_mbps(prev_bytes: int, curr_bytes: int, dt_seconds: float) -> float:
    if dt_seconds <= 0:
        return 0.0
    return (curr_bytes - prev_bytes) / dt_seconds / BYTES_PER_MEGABYTE


def cancel_pending_shutdown() -> None:
    system = platform.system().lower()
    if system.startswith("win"):
        command = ["shutdown", "/a"]
    elif system == "linux":
        command = ["shutdown", "-c"]
    else:
        raise RuntimeError(f"Shutdown cancel is not supported on this OS: {system}")
    subprocess.run(command, check=True)


def put_queue_bounded(target_queue: queue.Queue[Any], item: Any) -> None:
    """
    Insert item without allowing unbounded growth.

    If the queue is full, drop the oldest item and retry once.
    """
    try:
        target_queue.put_nowait(item)
        return
    except queue.Full:
        pass

    try:
        target_queue.get_nowait()
    except queue.Empty:
        pass

    try:
        target_queue.put_nowait(item)
    except queue.Full:
        pass


class QueueLogHandler(logging.Handler):
    def __init__(self, output_queue: queue.Queue[str]) -> None:
        super().__init__()
        self.output_queue = output_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            put_queue_bounded(self.output_queue, self.format(record))
        except Exception:
            self.handleError(record)


class MonitorWorker(threading.Thread):
    def __init__(
        self,
        config: GuiConfig,
        stop_event: threading.Event,
        event_queue: queue.Queue[tuple[str, Any]],
        logger: logging.Logger,
    ) -> None:
        super().__init__(daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.event_queue = event_queue
        self.logger = logger
        self.last_process_state: set[str] | None = None

    def _emit_event(self, event_name: str, payload: Any) -> None:
        put_queue_bounded(self.event_queue, (event_name, payload))

    def _running_target_processes(self, psutil_module) -> set[str]:
        targets = set(self.config.process_names)
        if not targets:
            return set()

        active: set[str] = set()
        for proc in psutil_module.process_iter(["name", "exe"]):
            try:
                proc_name = (proc.info.get("name") or "").lower()
                exe_name = os.path.basename(proc.info.get("exe") or "").lower()
                for target in targets:
                    if proc_name == target or exe_name == target:
                        active.add(target)
            except (psutil_module.NoSuchProcess, psutil_module.AccessDenied):
                continue
        return active

    def _log_process_state_change(self, running: set[str]) -> None:
        if self.last_process_state == running:
            return
        if running:
            joined = ", ".join(sorted(running))
            self.logger.info("Process gate waiting: still running -> %s", joined)
        else:
            self.logger.info("Process gate satisfied: tracked processes are no longer running.")
        self.last_process_state = running

    def _publish_sample(
        self,
        *,
        elapsed_seconds: float,
        disk_mbps: float,
        cpu_percent: float,
        net_mbps: float,
        disk_ok: bool,
        cpu_ok: bool,
        net_ok: bool,
        process_gate_ok: bool,
        cpu_gate_enabled: bool,
        net_gate_enabled: bool,
        process_gate_enabled: bool,
        all_conditions_met: bool,
        idle_duration: float,
        grace_remaining: float,
        running_targets: list[str],
    ) -> None:
        self._emit_event(
            "sample",
            {
                "elapsed_seconds": elapsed_seconds,
                "disk_mbps": disk_mbps,
                "cpu_percent": cpu_percent,
                "net_mbps": net_mbps,
                "disk_ok": disk_ok,
                "cpu_ok": cpu_ok,
                "net_ok": net_ok,
                "process_gate_ok": process_gate_ok,
                "cpu_gate_enabled": cpu_gate_enabled,
                "net_gate_enabled": net_gate_enabled,
                "process_gate_enabled": process_gate_enabled,
                "all_conditions_met": all_conditions_met,
                "idle_duration": idle_duration,
                "sustained_seconds": self.config.sustained_seconds,
                "grace_remaining": grace_remaining,
                "running_targets": running_targets,
            },
        )

    def run(self) -> None:
        try:
            self._run_monitor_loop()
        except Exception as exc:
            self.logger.exception("Monitor failed.")
            self._emit_event("error", str(exc))

    def _run_monitor_loop(self) -> None:
        from disk_idle_shutdown import get_psutil

        psutil = get_psutil()
        start_time = time.monotonic()
        below_since: float | None = None
        sample_counter = 0
        last_time = time.monotonic()
        last_disk_bytes = current_total_bytes(self.config.drives)
        last_net_bytes = 0
        if self.config.use_network_gate:
            net = psutil.net_io_counters()
            last_net_bytes = net.bytes_sent + net.bytes_recv

        self.logger.info(
            "Monitoring started | disk<%.3f MB/s for %.1fs | interval=%.1fs | "
            "grace=%.1fs | drives=%s | test_mode=%s",
            self.config.threshold_mbps,
            self.config.sustained_seconds,
            self.config.interval_seconds,
            self.config.grace_seconds,
            ",".join(self.config.drives) if self.config.drives else "ALL",
            self.config.test_mode,
        )

        while not self.stop_event.wait(self.config.interval_seconds):
            now = time.monotonic()
            elapsed = now - start_time
            dt = now - last_time

            current_disk_bytes = current_total_bytes(self.config.drives)
            disk_mbps = speed_mbps(last_disk_bytes, current_disk_bytes, dt)

            if self.config.use_network_gate:
                net = psutil.net_io_counters()
                current_net_bytes = net.bytes_sent + net.bytes_recv
                net_mbps = speed_mbps(last_net_bytes, current_net_bytes, dt)
            else:
                current_net_bytes = last_net_bytes
                net_mbps = 0.0

            cpu_percent = 0.0
            if self.config.use_cpu_gate:
                cpu_percent = psutil.cpu_percent(interval=None)

            disk_ok = disk_mbps < self.config.threshold_mbps
            if (not self.config.ultra_light) and self.logger.isEnabledFor(logging.DEBUG):
                threshold_cmp = "<" if disk_ok else ">="
                self.logger.debug(
                    "Signals | disk=%.3f MB/s | cpu=%.1f%% | net=%.3f MB/s | threshold=%.3f (%s)",
                    disk_mbps,
                    cpu_percent,
                    net_mbps,
                    self.config.threshold_mbps,
                    threshold_cmp,
                )

            if self.config.self_test_seconds is not None and elapsed >= self.config.self_test_seconds:
                self.logger.warning("Self-test timer fired after %.1fs.", self.config.self_test_seconds)
                self._publish_sample(
                    elapsed_seconds=elapsed,
                    disk_mbps=disk_mbps,
                    cpu_percent=cpu_percent,
                    net_mbps=net_mbps,
                    disk_ok=disk_ok,
                    cpu_ok=True,
                    net_ok=True,
                    process_gate_ok=True,
                    cpu_gate_enabled=self.config.use_cpu_gate,
                    net_gate_enabled=self.config.use_network_gate,
                    process_gate_enabled=self.config.require_process_exit,
                    all_conditions_met=False,
                    idle_duration=0.0,
                    grace_remaining=0.0,
                    running_targets=[],
                )
                self._trigger_shutdown("self-test timer condition met")
                return

            process_gate_enabled = self.config.require_process_exit
            running_targets: set[str] = set()
            process_gate_ok = True
            if process_gate_enabled:
                running_targets = self._running_target_processes(psutil)
                self._log_process_state_change(running_targets)
                process_gate_ok = len(running_targets) == 0

            cpu_gate_enabled = self.config.use_cpu_gate
            net_gate_enabled = self.config.use_network_gate
            cpu_ok = (not cpu_gate_enabled) or (cpu_percent <= self.config.cpu_threshold_percent)
            net_ok = (not net_gate_enabled) or (net_mbps <= self.config.network_threshold_mbps)
            all_conditions_met = disk_ok and cpu_ok and net_ok and process_gate_ok

            grace_remaining = max(0.0, self.config.grace_seconds - elapsed)
            idle_duration = 0.0

            sample_counter += 1
            publish_sample = True
            if self.config.ultra_light:
                publish_sample = (
                    sample_counter % 2 == 0
                    or grace_remaining > 0
                    or all_conditions_met
                    or below_since is not None
                )

            if grace_remaining > 0:
                below_since = None
                if publish_sample:
                    self._publish_sample(
                        elapsed_seconds=elapsed,
                        disk_mbps=disk_mbps,
                        cpu_percent=cpu_percent,
                        net_mbps=net_mbps,
                        disk_ok=disk_ok,
                        cpu_ok=cpu_ok,
                        net_ok=net_ok,
                        process_gate_ok=process_gate_ok,
                        cpu_gate_enabled=cpu_gate_enabled,
                        net_gate_enabled=net_gate_enabled,
                        process_gate_enabled=process_gate_enabled,
                        all_conditions_met=False,
                        idle_duration=0.0,
                        grace_remaining=grace_remaining,
                        running_targets=sorted(running_targets),
                    )
                last_time = now
                last_disk_bytes = current_disk_bytes
                last_net_bytes = current_net_bytes
                continue

            if all_conditions_met:
                if below_since is None:
                    below_since = now
                    self.logger.info("All enabled idle conditions are now true.")
                idle_duration = now - below_since
                self.logger.debug("Idle duration: %.1fs", idle_duration)
                if idle_duration >= self.config.sustained_seconds:
                    self.logger.warning(
                        "Idle conditions stayed true for %.1fs. Triggering shutdown.",
                        idle_duration,
                    )
                    self._publish_sample(
                        elapsed_seconds=elapsed,
                        disk_mbps=disk_mbps,
                        cpu_percent=cpu_percent,
                        net_mbps=net_mbps,
                        disk_ok=disk_ok,
                        cpu_ok=cpu_ok,
                        net_ok=net_ok,
                        process_gate_ok=process_gate_ok,
                        cpu_gate_enabled=cpu_gate_enabled,
                        net_gate_enabled=net_gate_enabled,
                        process_gate_enabled=process_gate_enabled,
                        all_conditions_met=True,
                        idle_duration=idle_duration,
                        grace_remaining=0.0,
                        running_targets=sorted(running_targets),
                    )
                    self._trigger_shutdown("sustained idle conditions met")
                    return
            else:
                if below_since is not None:
                    self.logger.info("Idle conditions reset before sustained timer completed.")
                below_since = None
                idle_duration = 0.0

            if publish_sample:
                self._publish_sample(
                    elapsed_seconds=elapsed,
                    disk_mbps=disk_mbps,
                    cpu_percent=cpu_percent,
                    net_mbps=net_mbps,
                    disk_ok=disk_ok,
                    cpu_ok=cpu_ok,
                    net_ok=net_ok,
                    process_gate_ok=process_gate_ok,
                    cpu_gate_enabled=cpu_gate_enabled,
                    net_gate_enabled=net_gate_enabled,
                    process_gate_enabled=process_gate_enabled,
                    all_conditions_met=all_conditions_met,
                    idle_duration=idle_duration,
                    grace_remaining=0.0,
                    running_targets=sorted(running_targets),
                )

            last_time = now
            last_disk_bytes = current_disk_bytes
            last_net_bytes = current_net_bytes

        self.logger.info("Monitoring stopped by user.")
        self._emit_event("stopped", "User requested stop.")

    def _trigger_shutdown(self, reason: str) -> None:
        try:
            if self.config.test_mode:
                self.logger.warning(
                    "Trigger verification | %s | TEST MODE is ON, shutdown would be triggered.",
                    reason,
                )
            else:
                self.logger.warning(
                    "Trigger verification | %s | TEST MODE is OFF, shutdown will be triggered.",
                    reason,
                )
            issue_shutdown(
                test_mode=self.config.test_mode,
                delay_seconds=self.config.shutdown_delay_seconds,
            )
            if self.config.test_mode:
                self._emit_event("test_triggered", "Test-mode shutdown path executed.")
            else:
                self._emit_event(
                    "shutdown_scheduled",
                    f"Shutdown command sent with {self.config.shutdown_delay_seconds}s delay.",
                )
        except Exception as exc:
            self._emit_event("error", str(exc))


class DiskIdleGuiApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Disk Idle Shutdown")
        self.geometry("1120x800")
        self.minsize(980, 740)

        self.monitor_worker: MonitorWorker | None = None
        self.stop_event = threading.Event()
        self.event_queue: queue.Queue[tuple[str, Any]] = queue.Queue(
            maxsize=MAX_EVENT_QUEUE_ITEMS
        )
        self.log_queue: queue.Queue[str] = queue.Queue(maxsize=MAX_LOG_QUEUE_ITEMS)
        self.logger = logging.getLogger("disk_idle_shutdown_gui")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        self.input_widgets: list[ttk.Widget] = []
        self.preset_buttons: list[ttk.Button] = []
        self.is_running = False
        self.log_line_count = 0

        self._build_variables()
        self._load_settings()
        self._configure_style()
        self._build_layout()
        self._sync_idle_target_label()
        self._update_mode_badge()
        self._set_running_ui_state(False)

        self.test_mode_var.trace_add("write", self._on_test_mode_changed)
        self.sustained_var.trace_add("write", self._on_sustained_changed)

        self.after(UI_DRAIN_INTERVAL_MS, self._drain_queues)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_variables(self) -> None:
        self.threshold_var = tk.StringVar(value="1.0")
        self.interval_var = tk.StringVar(value="3.0")
        self.sustained_var = tk.StringVar(value="120")
        self.grace_var = tk.StringVar(value="60")
        self.shutdown_delay_var = tk.StringVar(value="30")
        self.drives_var = tk.StringVar(value="")
        self.log_file_var = tk.StringVar(value="disk_idle_shutdown.log")
        self.self_test_var = tk.StringVar(value="")

        self.test_mode_var = tk.BooleanVar(value=True)
        self.use_cpu_gate_var = tk.BooleanVar(value=True)
        self.cpu_threshold_var = tk.StringVar(value="20")
        self.use_network_gate_var = tk.BooleanVar(value=True)
        self.network_threshold_var = tk.StringVar(value="1.0")
        self.require_process_exit_var = tk.BooleanVar(value=False)
        self.process_names_var = tk.StringVar(value="msiexec.exe, setup.exe")
        self.ultra_light_var = tk.BooleanVar(value=True)

        self.autoscroll_var = tk.BooleanVar(value=True)

        self.status_var = tk.StringVar(value="Status: Idle")
        self.status_badge_var = tk.StringVar(value="IDLE")
        self.mode_badge_var = tk.StringVar(value="TEST MODE")

        self.disk_live_var = tk.StringVar(value="0.000 MB/s")
        self.cpu_live_var = tk.StringVar(value="0.0%")
        self.net_live_var = tk.StringVar(value="0.000 MB/s")
        self.idle_live_var = tk.StringVar(value="0.0 / 120.0 s")
        self.uptime_live_var = tk.StringVar(value="0.0 s")
        self.grace_live_var = tk.StringVar(value="0.0 s")
        self.process_live_var = tk.StringVar(value="None")

    def _settings_path(self) -> Path:
        return Path(__file__).resolve().with_name(SETTINGS_FILE_NAME)

    def _set_stringvar_from_settings(self, var: tk.StringVar, raw: Any) -> None:
        if isinstance(raw, (str, int, float)):
            var.set(str(raw))

    def _set_boolvar_from_settings(self, var: tk.BooleanVar, raw: Any) -> None:
        if isinstance(raw, bool):
            var.set(raw)
        elif isinstance(raw, str):
            lowered = raw.strip().lower()
            if lowered in {"true", "1", "yes", "on"}:
                var.set(True)
            elif lowered in {"false", "0", "no", "off"}:
                var.set(False)

    def _load_settings(self) -> None:
        path = self._settings_path()
        if not path.exists():
            return

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return

        if not isinstance(payload, dict):
            return

        self._set_stringvar_from_settings(self.threshold_var, payload.get("threshold_mbps"))
        self._set_stringvar_from_settings(self.interval_var, payload.get("interval_seconds"))
        self._set_stringvar_from_settings(self.sustained_var, payload.get("sustained_seconds"))
        self._set_stringvar_from_settings(self.grace_var, payload.get("grace_seconds"))
        self._set_stringvar_from_settings(
            self.shutdown_delay_var, payload.get("shutdown_delay_seconds")
        )
        self._set_stringvar_from_settings(self.drives_var, payload.get("drives"))
        self._set_stringvar_from_settings(self.log_file_var, payload.get("log_file"))
        self._set_stringvar_from_settings(self.self_test_var, payload.get("self_test_seconds"))
        self._set_boolvar_from_settings(self.test_mode_var, payload.get("test_mode"))
        self._set_boolvar_from_settings(self.use_cpu_gate_var, payload.get("use_cpu_gate"))
        self._set_stringvar_from_settings(self.cpu_threshold_var, payload.get("cpu_threshold_percent"))
        self._set_boolvar_from_settings(self.use_network_gate_var, payload.get("use_network_gate"))
        self._set_stringvar_from_settings(
            self.network_threshold_var, payload.get("network_threshold_mbps")
        )
        self._set_boolvar_from_settings(
            self.require_process_exit_var, payload.get("require_process_exit")
        )
        self._set_stringvar_from_settings(self.process_names_var, payload.get("process_names"))
        self._set_boolvar_from_settings(self.autoscroll_var, payload.get("autoscroll"))
        self._set_boolvar_from_settings(self.ultra_light_var, payload.get("ultra_light"))

        geometry = payload.get("window_geometry")
        if isinstance(geometry, str) and geometry.strip():
            try:
                self.geometry(geometry)
            except tk.TclError:
                pass

    def _save_settings(self) -> None:
        payload: dict[str, Any] = {
            "threshold_mbps": self.threshold_var.get(),
            "interval_seconds": self.interval_var.get(),
            "sustained_seconds": self.sustained_var.get(),
            "grace_seconds": self.grace_var.get(),
            "shutdown_delay_seconds": self.shutdown_delay_var.get(),
            "drives": self.drives_var.get(),
            "log_file": self.log_file_var.get(),
            "self_test_seconds": self.self_test_var.get(),
            "test_mode": self.test_mode_var.get(),
            "use_cpu_gate": self.use_cpu_gate_var.get(),
            "cpu_threshold_percent": self.cpu_threshold_var.get(),
            "use_network_gate": self.use_network_gate_var.get(),
            "network_threshold_mbps": self.network_threshold_var.get(),
            "require_process_exit": self.require_process_exit_var.get(),
            "process_names": self.process_names_var.get(),
            "autoscroll": self.autoscroll_var.get(),
            "ultra_light": self.ultra_light_var.get(),
            "window_geometry": self.geometry(),
        }
        self._settings_path().write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _configure_style(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("Title.TLabel", font=("Segoe UI", 16, "bold"))
        style.configure("Subtitle.TLabel", foreground="#475569")
        style.configure("MetricName.TLabel", foreground="#334155")
        style.configure("MetricValue.TLabel", font=("Segoe UI", 12, "bold"))
        style.configure("GateName.TLabel", foreground="#334155")

    def _build_layout(self) -> None:
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        container = ttk.Frame(self, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=1)

        header = ttk.Frame(container, padding=(12, 10))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        ttk.Label(header, text="Disk Idle Shutdown", style="Title.TLabel").grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(
            header,
            text="Monitor install completion with disk/cpu/network/process gates.",
            style="Subtitle.TLabel",
        ).grid(row=1, column=0, sticky="w")

        self.status_badge_label = tk.Label(
            header,
            textvariable=self.status_badge_var,
            bg="#e2e8f0",
            fg="#0f172a",
            padx=10,
            pady=5,
            font=("Segoe UI", 9, "bold"),
        )
        self.status_badge_label.grid(row=0, column=1, padx=(10, 0), sticky="e")

        self.mode_badge_label = tk.Label(
            header,
            textvariable=self.mode_badge_var,
            bg="#fef3c7",
            fg="#92400e",
            padx=10,
            pady=5,
            font=("Segoe UI", 9, "bold"),
        )
        self.mode_badge_label.grid(row=0, column=2, padx=(8, 0), sticky="e")

        actions = ttk.Frame(container)
        actions.grid(row=1, column=0, sticky="ew", pady=(8, 8))
        for i in range(9):
            actions.columnconfigure(i, weight=1)

        self.start_button = ttk.Button(actions, text="Start", command=self.start_monitoring)
        self.start_button.grid(row=0, column=0, sticky="ew", padx=3)

        self.stop_button = ttk.Button(actions, text="Stop", command=self.stop_monitoring)
        self.stop_button.grid(row=0, column=1, sticky="ew", padx=3)

        self.trigger_button = ttk.Button(actions, text="Trigger Test", command=self.trigger_test)
        self.trigger_button.grid(row=0, column=2, sticky="ew", padx=3)

        self.cancel_button = ttk.Button(
            actions, text="Cancel Shutdown", command=self.cancel_shutdown
        )
        self.cancel_button.grid(row=0, column=3, sticky="ew", padx=3)

        self.drives_button = ttk.Button(actions, text="List Drives", command=self.show_drives)
        self.drives_button.grid(row=0, column=4, sticky="ew", padx=3)

        self.clear_log_button = ttk.Button(actions, text="Clear Log", command=self.clear_log_view)
        self.clear_log_button.grid(row=0, column=5, sticky="ew", padx=3)

        preset_bal = ttk.Button(
            actions, text="Preset: Balanced", command=lambda: self._apply_preset("balanced")
        )
        preset_bal.grid(row=0, column=6, sticky="ew", padx=3)
        self.preset_buttons.append(preset_bal)

        preset_cons = ttk.Button(
            actions, text="Preset: Conservative", command=lambda: self._apply_preset("conservative")
        )
        preset_cons.grid(row=0, column=7, sticky="ew", padx=3)
        self.preset_buttons.append(preset_cons)

        preset_aggr = ttk.Button(
            actions, text="Preset: Aggressive", command=lambda: self._apply_preset("aggressive")
        )
        preset_aggr.grid(row=0, column=8, sticky="ew", padx=3)
        self.preset_buttons.append(preset_aggr)

        body = ttk.Panedwindow(container, orient="horizontal")
        body.grid(row=2, column=0, sticky="nsew")

        left = ttk.Frame(body, padding=(0, 0, 8, 0))
        right = ttk.Frame(body, padding=(8, 0, 0, 0))
        body.add(left)
        body.add(right)

        left.columnconfigure(0, weight=1)
        left.rowconfigure(1, weight=1)
        right.columnconfigure(0, weight=1)
        right.rowconfigure(2, weight=1)

        profile_frame = ttk.LabelFrame(left, text="Profile", padding=10)
        profile_frame.grid(row=0, column=0, sticky="ew")
        profile_frame.columnconfigure(0, weight=1)
        profile_frame.columnconfigure(1, weight=1)
        profile_frame.columnconfigure(2, weight=1)
        profile_frame.columnconfigure(3, weight=1)

        self._add_checkbutton(profile_frame, "Test mode (safe)", self.test_mode_var, 0, 0)
        self._add_checkbutton(profile_frame, "Use CPU gate", self.use_cpu_gate_var, 0, 1)
        self._add_checkbutton(
            profile_frame, "Use network gate", self.use_network_gate_var, 0, 2
        )
        self._add_checkbutton(profile_frame, "Ultra-light mode", self.ultra_light_var, 0, 3)

        tabs = ttk.Notebook(left)
        tabs.grid(row=1, column=0, sticky="nsew", pady=(8, 0))

        tab_core = ttk.Frame(tabs, padding=10)
        tab_gates = ttk.Frame(tabs, padding=10)
        tabs.add(tab_core, text="Core Settings")
        tabs.add(tab_gates, text="Advanced Gates")

        for i in range(2):
            tab_core.columnconfigure(i, weight=1)
            tab_gates.columnconfigure(i, weight=1)

        self._add_labeled_entry(tab_core, "Disk threshold (MB/s)", self.threshold_var, 0, 0)
        self._add_labeled_entry(tab_core, "Sample interval (s)", self.interval_var, 0, 1)
        self._add_labeled_entry(tab_core, "Sustained idle (s)", self.sustained_var, 1, 0)
        self._add_labeled_entry(tab_core, "Startup grace (s)", self.grace_var, 1, 1)
        self._add_labeled_entry(tab_core, "Shutdown delay (s)", self.shutdown_delay_var, 2, 0)
        self._add_labeled_entry(tab_core, "Self-test timer (s, optional)", self.self_test_var, 2, 1)
        self._add_labeled_entry(tab_core, "Drives (optional)", self.drives_var, 3, 0, span=2)
        self._add_labeled_entry(tab_core, "Log file", self.log_file_var, 4, 0, span=2)

        self._add_labeled_entry(tab_gates, "CPU max (%)", self.cpu_threshold_var, 0, 0)
        self._add_labeled_entry(tab_gates, "Network max (MB/s)", self.network_threshold_var, 0, 1)
        self._add_checkbutton(
            tab_gates, "Require process exit", self.require_process_exit_var, 1, 0
        )
        self._add_labeled_entry(
            tab_gates,
            "Process names (comma-separated)",
            self.process_names_var,
            2,
            0,
            span=2,
        )

        live_frame = ttk.LabelFrame(right, text="Live Metrics", padding=10)
        live_frame.grid(row=0, column=0, sticky="ew")
        for i in range(4):
            live_frame.columnconfigure(i, weight=1)

        ttk.Label(live_frame, text="Disk", style="MetricName.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(live_frame, text="CPU", style="MetricName.TLabel").grid(row=0, column=1, sticky="w")
        ttk.Label(live_frame, text="Network", style="MetricName.TLabel").grid(
            row=0, column=2, sticky="w"
        )
        ttk.Label(live_frame, text="Uptime", style="MetricName.TLabel").grid(row=0, column=3, sticky="w")

        ttk.Label(live_frame, textvariable=self.disk_live_var, style="MetricValue.TLabel").grid(
            row=1, column=0, sticky="w"
        )
        ttk.Label(live_frame, textvariable=self.cpu_live_var, style="MetricValue.TLabel").grid(
            row=1, column=1, sticky="w"
        )
        ttk.Label(live_frame, textvariable=self.net_live_var, style="MetricValue.TLabel").grid(
            row=1, column=2, sticky="w"
        )
        ttk.Label(live_frame, textvariable=self.uptime_live_var, style="MetricValue.TLabel").grid(
            row=1, column=3, sticky="w"
        )

        ttk.Label(live_frame, text="Idle progress", style="MetricName.TLabel").grid(
            row=2, column=0, sticky="w", pady=(8, 0)
        )
        ttk.Label(live_frame, textvariable=self.idle_live_var, style="MetricValue.TLabel").grid(
            row=2, column=1, columnspan=2, sticky="w", pady=(8, 0)
        )
        ttk.Label(live_frame, textvariable=self.grace_live_var, style="MetricValue.TLabel").grid(
            row=2, column=3, sticky="w", pady=(8, 0)
        )

        self.idle_progress = ttk.Progressbar(
            live_frame, orient="horizontal", mode="determinate", maximum=100
        )
        self.idle_progress.grid(row=3, column=0, columnspan=4, sticky="ew", pady=(6, 0))

        gate_frame = ttk.LabelFrame(right, text="Gate Status", padding=10)
        gate_frame.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        for i in range(4):
            gate_frame.columnconfigure(i, weight=1)

        ttk.Label(gate_frame, text="Disk", style="GateName.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(gate_frame, text="CPU", style="GateName.TLabel").grid(row=0, column=1, sticky="w")
        ttk.Label(gate_frame, text="Network", style="GateName.TLabel").grid(row=0, column=2, sticky="w")
        ttk.Label(gate_frame, text="Process", style="GateName.TLabel").grid(row=0, column=3, sticky="w")

        self.disk_gate_label = tk.Label(gate_frame, text="WAIT", fg="#b91c1c")
        self.cpu_gate_label = tk.Label(gate_frame, text="PASS", fg="#15803d")
        self.net_gate_label = tk.Label(gate_frame, text="PASS", fg="#15803d")
        self.process_gate_label = tk.Label(gate_frame, text="DISABLED", fg="#64748b")
        self.disk_gate_label.grid(row=1, column=0, sticky="w")
        self.cpu_gate_label.grid(row=1, column=1, sticky="w")
        self.net_gate_label.grid(row=1, column=2, sticky="w")
        self.process_gate_label.grid(row=1, column=3, sticky="w")

        ttk.Label(gate_frame, text="Running targets:", style="GateName.TLabel").grid(
            row=2, column=0, sticky="w", pady=(8, 0)
        )
        ttk.Label(gate_frame, textvariable=self.process_live_var).grid(
            row=2, column=1, columnspan=3, sticky="w", pady=(8, 0)
        )

        log_frame = ttk.LabelFrame(right, text="Live Log", padding=8)
        log_frame.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(1, weight=1)

        ttk.Checkbutton(log_frame, text="Auto-scroll", variable=self.autoscroll_var).grid(
            row=0, column=0, sticky="w", pady=(0, 4)
        )

        self.log_text = tk.Text(
            log_frame,
            height=20,
            wrap="none",
            state="disabled",
            bg="#0f172a",
            fg="#e2e8f0",
            insertbackground="#e2e8f0",
            relief="flat",
            padx=8,
            pady=8,
        )
        self.log_text.grid(row=1, column=0, sticky="nsew")

        scroll_y = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scroll_y.grid(row=1, column=1, sticky="ns")

        scroll_x = ttk.Scrollbar(log_frame, orient="horizontal", command=self.log_text.xview)
        scroll_x.grid(row=2, column=0, sticky="ew")

        self.log_text.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

        ttk.Label(container, textvariable=self.status_var).grid(
            row=3, column=0, sticky="w", pady=(8, 0)
        )

    def _add_labeled_entry(
        self,
        parent: ttk.Widget,
        label: str,
        variable: tk.StringVar,
        row: int,
        column: int,
        span: int = 1,
        track_input: bool = True,
    ) -> ttk.Entry:
        frame = ttk.Frame(parent)
        frame.grid(row=row, column=column, columnspan=span, sticky="ew", padx=4, pady=3)
        frame.columnconfigure(0, weight=1)
        ttk.Label(frame, text=label).grid(row=0, column=0, sticky="w")
        entry = ttk.Entry(frame, textvariable=variable)
        entry.grid(row=1, column=0, sticky="ew")
        if track_input:
            self.input_widgets.append(entry)
        return entry

    def _add_checkbutton(
        self,
        parent: ttk.Widget,
        text: str,
        variable: tk.BooleanVar,
        row: int,
        column: int,
        span: int = 1,
        track_input: bool = True,
    ) -> ttk.Checkbutton:
        widget = ttk.Checkbutton(parent, text=text, variable=variable)
        widget.grid(row=row, column=column, columnspan=span, sticky="w", padx=4, pady=3)
        if track_input:
            self.input_widgets.append(widget)
        return widget

    def _setup_logger(self, log_path: Path, ultra_light: bool) -> None:
        log_path.parent.mkdir(parents=True, exist_ok=True)

        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)
            handler.close()

        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        file_level = logging.WARNING if ultra_light else logging.INFO
        self.logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)

        queue_handler = QueueLogHandler(self.log_queue)
        queue_handler.setLevel(logging.INFO)
        queue_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(queue_handler)

    def _parse_positive_float(
        self, value: str, label: str, allow_blank: bool = False
    ) -> float | None:
        text = value.strip()
        if allow_blank and text == "":
            return None
        try:
            parsed = float(text)
        except ValueError as exc:
            raise ValueError(f"{label} must be a number.") from exc
        if parsed <= 0:
            raise ValueError(f"{label} must be greater than 0.")
        return parsed

    def _parse_nonnegative_float(self, value: str, label: str) -> float:
        try:
            parsed = float(value.strip())
        except ValueError as exc:
            raise ValueError(f"{label} must be a number.") from exc
        if parsed < 0:
            raise ValueError(f"{label} must be >= 0.")
        return parsed

    def _parse_nonnegative_int(self, value: str, label: str) -> int:
        try:
            parsed = int(value.strip())
        except ValueError as exc:
            raise ValueError(f"{label} must be an integer.") from exc
        if parsed < 0:
            raise ValueError(f"{label} must be >= 0.")
        return parsed

    def _build_config(self) -> GuiConfig:
        threshold = self._parse_nonnegative_float(self.threshold_var.get(), "Disk threshold")
        interval = self._parse_positive_float(self.interval_var.get(), "Sample interval")
        sustained = self._parse_positive_float(self.sustained_var.get(), "Sustained idle")
        grace = self._parse_nonnegative_float(self.grace_var.get(), "Startup grace")
        delay = self._parse_nonnegative_int(self.shutdown_delay_var.get(), "Shutdown delay")
        self_test = self._parse_positive_float(
            self.self_test_var.get(), "Self-test timer", allow_blank=True
        )
        cpu_threshold = self._parse_nonnegative_float(self.cpu_threshold_var.get(), "CPU max")
        net_threshold = self._parse_nonnegative_float(
            self.network_threshold_var.get(), "Network max"
        )
        drives = parse_drive_list(self.drives_var.get())
        process_names = parse_process_list(self.process_names_var.get())

        return GuiConfig(
            threshold_mbps=threshold,
            interval_seconds=interval if interval is not None else 3.0,
            sustained_seconds=sustained if sustained is not None else 120.0,
            grace_seconds=grace,
            drives=drives,
            test_mode=self.test_mode_var.get(),
            shutdown_delay_seconds=delay,
            self_test_seconds=self_test,
            use_cpu_gate=self.use_cpu_gate_var.get(),
            cpu_threshold_percent=cpu_threshold,
            use_network_gate=self.use_network_gate_var.get(),
            network_threshold_mbps=net_threshold,
            require_process_exit=self.require_process_exit_var.get(),
            process_names=process_names,
            ultra_light=self.ultra_light_var.get(),
        )

    def _apply_preset(self, name: str) -> None:
        if self.is_running:
            messagebox.showinfo("Preset Locked", "Stop monitoring before applying a preset.")
            return

        if name == "balanced":
            self.threshold_var.set("1.0")
            self.interval_var.set("3.0")
            self.sustained_var.set("120")
            self.grace_var.set("60")
            self.cpu_threshold_var.set("20")
            self.network_threshold_var.set("1.0")
            self.use_cpu_gate_var.set(True)
            self.use_network_gate_var.set(True)
            self.require_process_exit_var.set(False)
            self.ultra_light_var.set(True)
        elif name == "conservative":
            self.threshold_var.set("0.5")
            self.interval_var.set("5.0")
            self.sustained_var.set("240")
            self.grace_var.set("120")
            self.cpu_threshold_var.set("12")
            self.network_threshold_var.set("0.5")
            self.use_cpu_gate_var.set(True)
            self.use_network_gate_var.set(True)
            self.require_process_exit_var.set(True)
            self.ultra_light_var.set(True)
        elif name == "aggressive":
            self.threshold_var.set("2.0")
            self.interval_var.set("2.0")
            self.sustained_var.set("45")
            self.grace_var.set("20")
            self.cpu_threshold_var.set("35")
            self.network_threshold_var.set("2.0")
            self.use_cpu_gate_var.set(True)
            self.use_network_gate_var.set(False)
            self.require_process_exit_var.set(False)
            self.ultra_light_var.set(False)

        self._sync_idle_target_label()
        self.status_var.set(f"Status: Preset applied ({name})")

    def _set_running_ui_state(self, running: bool) -> None:
        self.is_running = running

        if running:
            self.start_button.state(["disabled"])
            self.stop_button.state(["!disabled"])
        else:
            self.start_button.state(["!disabled"])
            self.stop_button.state(["disabled"])

        for widget in self.input_widgets:
            try:
                if running:
                    widget.state(["disabled"])
                else:
                    widget.state(["!disabled"])
            except tk.TclError:
                pass

        for button in self.preset_buttons:
            if running:
                button.state(["disabled"])
            else:
                button.state(["!disabled"])

    def _set_status_badge(self, label: str) -> None:
        palette = {
            "IDLE": ("#e2e8f0", "#0f172a"),
            "MONITORING": ("#dcfce7", "#166534"),
            "STOPPING": ("#fef9c3", "#854d0e"),
            "STOPPED": ("#e2e8f0", "#0f172a"),
            "TEST TRIGGERED": ("#fef3c7", "#92400e"),
            "SHUTDOWN SCHEDULED": ("#fee2e2", "#991b1b"),
            "ERROR": ("#fee2e2", "#991b1b"),
        }
        key = label.upper()
        bg, fg = palette.get(key, ("#e2e8f0", "#0f172a"))
        self.status_badge_var.set(key)
        self.status_badge_label.configure(bg=bg, fg=fg)

    def _update_mode_badge(self) -> None:
        if not hasattr(self, "mode_badge_label"):
            return
        if self.test_mode_var.get():
            self.mode_badge_var.set("TEST MODE")
            self.mode_badge_label.configure(bg="#fef3c7", fg="#92400e")
        else:
            self.mode_badge_var.set("LIVE MODE")
            self.mode_badge_label.configure(bg="#fee2e2", fg="#991b1b")

    def _sync_idle_target_label(self) -> None:
        try:
            target = float(self.sustained_var.get().strip())
        except ValueError:
            target = 0.0
        if not self.is_running:
            self.idle_live_var.set(f"0.0 / {target:.1f} s")
            self.idle_progress["value"] = 0.0

    def _on_test_mode_changed(self, *_: Any) -> None:
        self._update_mode_badge()

    def _on_sustained_changed(self, *_: Any) -> None:
        self._sync_idle_target_label()

    def start_monitoring(self) -> None:
        if self.monitor_worker is not None and self.monitor_worker.is_alive():
            messagebox.showinfo("Monitor Running", "Monitoring is already running.")
            return

        try:
            config = self._build_config()
            self._setup_logger(
                Path(self.log_file_var.get().strip() or "disk_idle_shutdown.log"),
                config.ultra_light,
            )
        except ValueError as exc:
            messagebox.showerror("Invalid Settings", str(exc))
            return
        except OSError as exc:
            messagebox.showerror("Log File Error", str(exc))
            return

        self.stop_event = threading.Event()
        self.monitor_worker = MonitorWorker(config, self.stop_event, self.event_queue, self.logger)
        self.monitor_worker.start()

        self._set_running_ui_state(True)
        self.status_var.set("Status: Monitoring")
        self._set_status_badge("Monitoring")
        self.logger.info("GUI Start requested.")

    def stop_monitoring(self) -> None:
        worker = self.monitor_worker
        if worker is None or not worker.is_alive():
            self.status_var.set("Status: Idle")
            self._set_status_badge("Idle")
            self._set_running_ui_state(False)
            return
        self.stop_event.set()
        self.status_var.set("Status: Stopping...")
        self._set_status_badge("Stopping")
        self.logger.info("GUI Stop requested.")

    def trigger_test(self) -> None:
        try:
            delay = self._parse_nonnegative_int(self.shutdown_delay_var.get(), "Shutdown delay")
            issue_shutdown(test_mode=True, delay_seconds=delay)
            self._append_log_line("TEST MODE: manual trigger executed.")
        except Exception as exc:
            messagebox.showerror("Test Trigger Failed", str(exc))

    def cancel_shutdown(self) -> None:
        try:
            cancel_pending_shutdown()
            self._append_log_line("Pending shutdown canceled.")
        except Exception as exc:
            messagebox.showerror("Cancel Failed", str(exc))

    def show_drives(self) -> None:
        try:
            drives = list_available_disks()
        except Exception as exc:
            messagebox.showerror("Drive List Error", str(exc))
            return

        if not drives:
            messagebox.showinfo("Available Drives", "No per-disk counters were found.")
            return

        messagebox.showinfo("Available Drives", "\n".join(drives))

    def clear_log_view(self) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")
        self.log_line_count = 0

    def _append_log_line(self, line: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, f"{line}\n")
        self.log_line_count += 1

        if self.log_line_count > MAX_GUI_LOG_LINES:
            trim_count = max(LOG_TRIM_BATCH_LINES, self.log_line_count - MAX_GUI_LOG_LINES)
            self.log_text.delete("1.0", f"{trim_count + 1}.0")
            self.log_line_count = max(0, self.log_line_count - trim_count)

        if self.autoscroll_var.get():
            self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _set_gate_state(self, label: tk.Label, enabled: bool, passed: bool) -> None:
        if not enabled:
            label.configure(text="DISABLED", fg="#64748b")
            return
        if passed:
            label.configure(text="PASS", fg="#15803d")
            return
        label.configure(text="WAIT", fg="#b91c1c")

    def _update_live_metrics(self, payload: dict[str, Any]) -> None:
        disk_mbps = float(payload.get("disk_mbps", 0.0))
        cpu_percent = float(payload.get("cpu_percent", 0.0))
        net_mbps = float(payload.get("net_mbps", 0.0))
        elapsed = float(payload.get("elapsed_seconds", 0.0))
        idle_duration = float(payload.get("idle_duration", 0.0))
        sustained_seconds = float(payload.get("sustained_seconds", 0.0))
        grace_remaining = float(payload.get("grace_remaining", 0.0))
        running_targets = payload.get("running_targets", [])
        if not isinstance(running_targets, list):
            running_targets = []

        self.disk_live_var.set(f"{disk_mbps:.3f} MB/s")
        self.cpu_live_var.set(f"{cpu_percent:.1f}%")
        self.net_live_var.set(f"{net_mbps:.3f} MB/s")
        self.uptime_live_var.set(f"{elapsed:.1f} s")
        self.idle_live_var.set(f"{idle_duration:.1f} / {sustained_seconds:.1f} s")

        if grace_remaining > 0:
            self.grace_live_var.set(f"Grace: {grace_remaining:.1f}s")
        else:
            self.grace_live_var.set("Grace: complete")

        progress = 0.0
        if sustained_seconds > 0:
            progress = min(100.0, max(0.0, idle_duration / sustained_seconds * 100.0))
        self.idle_progress["value"] = progress

        self._set_gate_state(self.disk_gate_label, True, bool(payload.get("disk_ok", False)))
        self._set_gate_state(
            self.cpu_gate_label,
            bool(payload.get("cpu_gate_enabled", False)),
            bool(payload.get("cpu_ok", False)),
        )
        self._set_gate_state(
            self.net_gate_label,
            bool(payload.get("net_gate_enabled", False)),
            bool(payload.get("net_ok", False)),
        )
        self._set_gate_state(
            self.process_gate_label,
            bool(payload.get("process_gate_enabled", False)),
            bool(payload.get("process_gate_ok", False)),
        )

        self.process_live_var.set(", ".join(running_targets) if running_targets else "None")

        if grace_remaining > 0:
            self.status_var.set(f"Status: Monitoring | Grace active ({grace_remaining:.1f}s remaining)")
        elif bool(payload.get("all_conditions_met", False)):
            self.status_var.set("Status: Monitoring | Idle conditions met, timer accumulating")
        else:
            self.status_var.set("Status: Monitoring | Waiting for all enabled gates")

    def _handle_worker_event(self, event_name: str, payload: Any) -> None:
        if event_name == "sample":
            if isinstance(payload, dict):
                self._update_live_metrics(payload)
            return

        if event_name == "stopped":
            self.status_var.set("Status: Stopped")
            self._set_status_badge("Stopped")
            self._set_running_ui_state(False)
            return

        if event_name == "test_triggered":
            self.status_var.set("Status: Test Triggered (no shutdown)")
            self._set_status_badge("Test Triggered")
            self._set_running_ui_state(False)
            return

        if event_name == "shutdown_scheduled":
            self.status_var.set("Status: Shutdown Scheduled")
            self._set_status_badge("Shutdown Scheduled")
            self._set_running_ui_state(False)
            return

        if event_name == "error":
            self.status_var.set("Status: Error")
            self._set_status_badge("Error")
            self._set_running_ui_state(False)
            messagebox.showerror("Monitor Error", str(payload))
            return

        self.status_var.set(f"Status: {event_name}")

    def _drain_queues(self) -> None:
        for _ in range(MAX_LOGS_PER_DRAIN):
            try:
                self._append_log_line(self.log_queue.get_nowait())
            except queue.Empty:
                break

        for _ in range(MAX_EVENTS_PER_DRAIN):
            try:
                event_name, payload = self.event_queue.get_nowait()
            except queue.Empty:
                break
            self._handle_worker_event(event_name, payload)

        worker = self.monitor_worker
        if self.is_running and (worker is None or not worker.is_alive()):
            self._set_running_ui_state(False)
            if self.status_badge_var.get() == "MONITORING":
                self.status_var.set("Status: Idle")
                self._set_status_badge("Idle")

        self.after(UI_DRAIN_INTERVAL_MS, self._drain_queues)

    def _on_close(self) -> None:
        try:
            self._save_settings()
        except OSError as exc:
            messagebox.showerror("Save Settings Error", str(exc))

        worker = self.monitor_worker
        if worker is not None and worker.is_alive():
            self.stop_event.set()
            worker.join(timeout=2.0)
        self.destroy()


def main() -> None:
    app = DiskIdleGuiApp()
    app.mainloop()


if __name__ == "__main__":
    main()
