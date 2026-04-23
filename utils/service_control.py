"""Small wrapper for managed macOS LaunchAgent restarts."""

from __future__ import annotations

import os
import shutil
import subprocess
from typing import Any


def _launchctl_target(label: str) -> str:
    """Return the per-user launchctl target for one LaunchAgent label."""
    return f"gui/{os.getuid()}/{label}"


def inspect_managed_services(settings: Any, targets: list[str]) -> dict[str, Any]:
    """Inspect configured managed services through launchctl."""
    launchctl_path = shutil.which("launchctl")
    if not launchctl_path:
        return {
            "status": "failed",
            "message": "launchctl is not available on this system.",
            "targets": targets,
            "results": [],
        }

    configured_targets = {
        "backend": settings.backend_service_label,
        "gui": settings.gui_service_label,
    }
    results: list[dict[str, Any]] = []
    overall_status = "healthy"
    for target in targets:
        label = configured_targets.get(target, "").strip()
        if not label:
            results.append(
                {
                    "target": target,
                    "label": "",
                    "status": "failed",
                    "summary": "No service label configured.",
                    "stdout": "",
                    "stderr": "",
                }
            )
            overall_status = "failed"
            continue

        completed = subprocess.run(
            [launchctl_path, "print", _launchctl_target(label)],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        if completed.returncode != 0:
            status = "failed"
            summary = stderr or stdout or "Service is not loaded."
            overall_status = "failed"
        else:
            status = "healthy"
            summary = "Service is loaded."
            if "state = running" in stdout:
                summary = "Service is running."
            elif "state = waiting" in stdout:
                summary = "Service is loaded and waiting."
        results.append(
            {
                "target": target,
                "label": label,
                "status": status,
                "summary": summary,
                "returncode": completed.returncode,
                "stdout": stdout,
                "stderr": stderr,
            }
        )

    return {
        "status": overall_status,
        "message": "Managed services are healthy." if overall_status == "healthy" else "One or more managed services are unhealthy.",
        "targets": targets,
        "results": results,
    }


def restart_managed_services(settings: Any, targets: list[str]) -> dict[str, Any]:
    """Restart configured managed services through launchctl kickstart."""
    if not settings.enable_service_restart:
        return {
            "status": "disabled",
            "message": "Managed service restart is disabled.",
            "targets": targets,
            "results": [],
        }

    launchctl_path = shutil.which("launchctl")
    if not launchctl_path:
        return {
            "status": "failed",
            "message": "launchctl is not available on this system.",
            "targets": targets,
            "results": [],
        }

    configured_targets = {
        "backend": settings.backend_service_label,
        "gui": settings.gui_service_label,
    }
    results: list[dict[str, Any]] = []
    overall_status = "success"

    for target in targets:
        label = configured_targets.get(target, "").strip()
        if not label:
            results.append(
                {
                    "target": target,
                    "label": "",
                    "status": "skipped",
                    "stdout": "",
                    "stderr": "No service label configured.",
                }
            )
            if overall_status == "success":
                overall_status = "partial"
            continue

        completed = subprocess.run(
            [launchctl_path, "kickstart", "-k", _launchctl_target(label)],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        status = "success" if completed.returncode == 0 else "failed"
        if status == "failed":
            overall_status = "failed"
        results.append(
            {
                "target": target,
                "label": label,
                "status": status,
                "returncode": completed.returncode,
                "stdout": completed.stdout.strip(),
                "stderr": completed.stderr.strip(),
            }
        )

    message = "Managed services restarted successfully."
    if overall_status == "partial":
        message = "Managed service restart was only partially applied."
    elif overall_status == "failed":
        message = "Managed service restart failed."

    return {
        "status": overall_status,
        "message": message,
        "targets": targets,
        "results": results,
    }
