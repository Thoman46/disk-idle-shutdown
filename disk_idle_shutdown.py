#!/usr/bin/env python3
"""
Monitor disk throughput and shut down the machine when it stays below a setpoint.

Designed for long-running install/update sessions where disk activity eventually
drops near idle when work is complete.
"""

from __future__ import annotations

import argparse
import logging
import platform
import subprocess
import sys
import time
from functools import lru_cache
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


BYTES_PER_MEGABYTE = 1024 * 1024


@dataclass
class MonitorConfig:
    threshold_mbps: float
    interval_seconds: float
    sustained_seconds: float
    grace_seconds: float
    drives: list[str]
    test_mode: bool
    shutdown_delay: int
    dry_trigger_after: float | None
    sample_log_seconds: float


@lru_cache(maxsize=1)
def get_psutil():
    try:
        import psutil  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency: psutil. Install it with: pip install psutil"
        ) from exc
    return psutil


def configure_logging(log_file: Path, verbose: bool) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    handlers: list[logging.Handler] = [
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=handlers,
    )


def list_available_disks() -> list[str]:
    psutil = get_psutil()
    counters = psutil.disk_io_counters(perdisk=True) or {}
    return sorted(counters.keys())


def _sum_counters_for_drives(drives: Iterable[str]) -> int:
    psutil = get_psutil()
    counters = psutil.disk_io_counters(perdisk=True) or {}
    total = 0
    for drive in drives:
        if drive not in counters:
            raise ValueError(
                f"Drive '{drive}' not found in psutil counters. "
                "Run with --list-drives to see valid names."
            )
        total += counters[drive].read_bytes + counters[drive].write_bytes
    return total


def current_total_bytes(drives: list[str]) -> int:
    psutil = get_psutil()
    if drives:
        return _sum_counters_for_drives(drives)
    counters = psutil.disk_io_counters(perdisk=False)
    if counters is None:
        raise RuntimeError("Unable to read disk counters from this system.")
    return counters.read_bytes + counters.write_bytes


def speed_mbps(prev_bytes: int, curr_bytes: int, dt_seconds: float) -> float:
    if dt_seconds <= 0:
        return 0.0
    return (curr_bytes - prev_bytes) / dt_seconds / BYTES_PER_MEGABYTE


def issue_shutdown(test_mode: bool, delay_seconds: int) -> None:
    system = platform.system().lower()
    if system.startswith("win"):
        command = [
            "shutdown",
            "/s",
            "/t",
            str(max(0, delay_seconds)),
            "/c",
            "Disk activity stayed below threshold.",
        ]
    elif system == "linux":
        command = ["shutdown", "-h", f"+{max(0, delay_seconds) // 60}"]
    elif system == "darwin":
        command = ["shutdown", "-h", f"+{max(0, delay_seconds) // 60}"]
    else:
        raise RuntimeError(f"Unsupported OS for shutdown command: {system}")

    if test_mode:
        logging.warning("TEST MODE: would run shutdown command: %s", " ".join(command))
        return

    logging.warning("Executing shutdown command: %s", " ".join(command))
    subprocess.run(command, check=True)


def monitor(config: MonitorConfig) -> None:
    start_time = time.monotonic()
    below_since: float | None = None
    last_sample_log_elapsed = -10**9
    last_threshold_state: bool | None = None
    last_bytes = current_total_bytes(config.drives)
    last_time = time.monotonic()
    logging.info(
        "Monitoring started | threshold=%.3f MB/s | interval=%.1fs | sustained=%.1fs | "
        "grace=%.1fs | drives=%s | test_mode=%s",
        config.threshold_mbps,
        config.interval_seconds,
        config.sustained_seconds,
        config.grace_seconds,
        ",".join(config.drives) if config.drives else "ALL",
        config.test_mode,
    )

    while True:
        time.sleep(config.interval_seconds)
        now = time.monotonic()
        curr_bytes = current_total_bytes(config.drives)
        mbps = speed_mbps(last_bytes, curr_bytes, now - last_time)
        elapsed = now - start_time

        is_below_threshold = mbps < config.threshold_mbps
        threshold_state_changed = (
            last_threshold_state is None or is_below_threshold != last_threshold_state
        )
        should_log_sample = (
            threshold_state_changed
            or elapsed - last_sample_log_elapsed >= config.sample_log_seconds
        )
        if should_log_sample:
            threshold_cmp = "<" if is_below_threshold else ">="
            logging.info("Disk speed: %.3f MB/s", mbps)
            logging.info(
                "Threshold check: %.3f MB/s %s %.3f MB/s.",
                mbps,
                threshold_cmp,
                config.threshold_mbps,
            )
            last_sample_log_elapsed = elapsed
        last_threshold_state = is_below_threshold

        if config.dry_trigger_after is not None and elapsed >= config.dry_trigger_after:
            logging.warning(
                "Self-test trigger fired after %.1fs. Running shutdown path.",
                config.dry_trigger_after,
            )
            if config.test_mode:
                logging.warning(
                    "Trigger verification: self-test condition met. "
                    "TEST MODE is ON, shutdown would be triggered."
                )
            else:
                logging.warning(
                    "Trigger verification: self-test condition met. "
                    "TEST MODE is OFF, shutdown will be triggered."
                )
            issue_shutdown(config.test_mode, config.shutdown_delay)
            return

        if elapsed < config.grace_seconds:
            remaining = config.grace_seconds - elapsed
            logging.debug("Grace period active (%.1fs remaining).", remaining)
            last_bytes = curr_bytes
            last_time = now
            continue

        if is_below_threshold:
            if below_since is None:
                below_since = now
                logging.info(
                    "Speed dropped below threshold (%.3f < %.3f).",
                    mbps,
                    config.threshold_mbps,
                )
            below_duration = now - below_since
            logging.debug("Below-threshold duration: %.1fs", below_duration)
            if below_duration >= config.sustained_seconds:
                logging.warning(
                    "Disk speed stayed below threshold for %.1fs. Triggering shutdown.",
                    below_duration,
                )
                if config.test_mode:
                    logging.warning(
                        "Trigger verification: sustained below-threshold condition met. "
                        "TEST MODE is ON, shutdown would be triggered."
                    )
                else:
                    logging.warning(
                        "Trigger verification: sustained below-threshold condition met. "
                        "TEST MODE is OFF, shutdown will be triggered."
                    )
                issue_shutdown(config.test_mode, config.shutdown_delay)
                return
        else:
            if below_since is not None:
                logging.info("Disk speed recovered above threshold.")
            below_since = None

        last_bytes = curr_bytes
        last_time = now


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Watch disk throughput and shut down when sustained speed stays below a threshold."
        )
    )
    parser.add_argument(
        "--threshold-mbps",
        type=float,
        default=1.0,
        help="Shutdown trigger threshold in MB/s (default: 1.0).",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=2.0,
        help="Sampling interval in seconds (default: 2.0).",
    )
    parser.add_argument(
        "--sustained-seconds",
        type=float,
        default=120.0,
        help="How long speed must stay below threshold before triggering (default: 120).",
    )
    parser.add_argument(
        "--grace-seconds",
        type=float,
        default=60.0,
        help="Startup grace period before evaluating threshold (default: 60).",
    )
    parser.add_argument(
        "--drives",
        nargs="+",
        default=[],
        help=(
            "Specific psutil disk keys to monitor (space-separated). "
            "If omitted, monitors all disks."
        ),
    )
    parser.add_argument(
        "--list-drives",
        action="store_true",
        help="List available disk keys and exit.",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Toggle safe mode: logs the shutdown command instead of executing it.",
    )
    parser.add_argument(
        "--self-test-seconds",
        type=float,
        default=None,
        help=(
            "Optional self-test timer. When set, triggers shutdown path after N seconds "
            "regardless of disk speed."
        ),
    )
    parser.add_argument(
        "--shutdown-delay-seconds",
        type=int,
        default=30,
        help="Delay passed to OS shutdown command (default: 30).",
    )
    parser.add_argument(
        "--sample-log-seconds",
        type=float,
        default=30.0,
        help=(
            "How often to emit sample speed logs in steady state (default: 30). "
            "State changes are always logged immediately."
        ),
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("disk_idle_shutdown.log"),
        help="Path to log file (default: ./disk_idle_shutdown.log).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.threshold_mbps < 0:
        raise ValueError("--threshold-mbps must be >= 0.")
    if args.interval_seconds <= 0:
        raise ValueError("--interval-seconds must be > 0.")
    if args.sustained_seconds <= 0:
        raise ValueError("--sustained-seconds must be > 0.")
    if args.grace_seconds < 0:
        raise ValueError("--grace-seconds must be >= 0.")
    if args.shutdown_delay_seconds < 0:
        raise ValueError("--shutdown-delay-seconds must be >= 0.")
    if args.sample_log_seconds <= 0:
        raise ValueError("--sample-log-seconds must be > 0.")
    if args.self_test_seconds is not None and args.self_test_seconds <= 0:
        raise ValueError("--self-test-seconds must be > 0 when provided.")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.list_drives:
        try:
            drives = list_available_disks()
        except RuntimeError as exc:
            parser.exit(2, f"{exc}\n")
        if not drives:
            print("No per-disk counters found.")
            return 0
        print("Available disk keys:")
        for drive in drives:
            print(f"  {drive}")
        return 0

    try:
        validate_args(args)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        get_psutil()
    except RuntimeError as exc:
        parser.exit(2, f"{exc}\n")

    configure_logging(args.log_file, args.verbose)

    config = MonitorConfig(
        threshold_mbps=args.threshold_mbps,
        interval_seconds=args.interval_seconds,
        sustained_seconds=args.sustained_seconds,
        grace_seconds=args.grace_seconds,
        drives=args.drives,
        test_mode=args.test_mode,
        shutdown_delay=args.shutdown_delay_seconds,
        dry_trigger_after=args.self_test_seconds,
        sample_log_seconds=args.sample_log_seconds,
    )

    try:
        monitor(config)
    except KeyboardInterrupt:
        logging.info("Interrupted by user. Exiting.")
        return 0
    except Exception:
        logging.exception("Fatal error while monitoring disk speed.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
