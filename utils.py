# utils.py
import os
import platform
import subprocess
import sys
from datetime import datetime
import qtawesome as qta
from PySide6.QtGui import QIcon

def get_icon(name, fallback_name, color=None):
    """
    Gets a native SF Symbol on macOS if available, otherwise a Font Awesome icon.
    The color parameter is only applied to the Font Awesome fallback.
    """
    if sys.platform == "darwin":
        # QIcon.fromTheme() automatically finds SF Symbols by their name
        # when the Info.plist key is set during the PyInstaller build.
        return QIcon.fromTheme(name)
    else:
        # Provide a fallback for non-macOS platforms
        return qta.icon(fallback_name, color=color)

def get_icon_for_path(path):
    is_mount = os.path.ismount(path)
    if is_mount:
        return get_icon("externaldrive.fill", "fa5s.hdd", color="silver")
    else:
        return get_icon("folder.fill", "fa5s.folder", color="#ff9f0a") # Use orange for consistency

def format_bytes(byte_count):
    if byte_count is None: return "N/A"
    if byte_count == 0: return "0.00 B"
    power = 1024; n = 0
    power_labels = {0: '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    while byte_count >= power and n < len(power_labels) -1 :
        byte_count /= power; n += 1
    return f"{byte_count:.2f} {power_labels[n]}"

def format_eta(seconds):
    if seconds is None or seconds < 0: return "N/A"
    if seconds == 0: return "Done"
    mins, secs = divmod(int(seconds), 60)
    hours, mins = divmod(mins, 60)
    if hours > 0: return f"{hours}h {mins}m"
    if mins > 0: return f"{mins}m {secs}s"
    return f"{secs}s"

def check_command(cmd_path):
    try:
        creationflags = subprocess.CREATE_NO_WINDOW if platform.system() == "Windows" else 0
        subprocess.run([cmd_path, "-version"], capture_output=True, check=True, creationflags=creationflags)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError): return False

def resolve_path_template(template, user_tokens, card_num, source_name):
    now = datetime.now()
    path = template.replace("{date_yyyy-mm-dd}", now.strftime("%Y-%m-%d"))
    path = path.replace("{date_yyyymmdd}", now.strftime("%Y%m%d"))
    path = path.replace("{date_yy-mm-dd}", now.strftime("%y-%m-%d"))
    path = path.replace("{project_name}", user_tokens.get("project_name", "Project"))
    path = path.replace("{camera_id}", user_tokens.get("camera_id", "CAM"))
    path = path.replace("{card_num}", f"{card_num:03d}")
    path = path.replace("{source_name}", source_name)
    return path

# Colour and icon for each job status. Matched on the exact status, so
# "Completed with errors" can never be drawn as a plain green "Completed".
_OK, _WARN, _BAD = "#4CAF50", "#FF9800", "#F44336"
_JOB_STATUS_STYLES = {
    "Processed": (_OK, "checkmark.seal.fill", "fa5s.check-double"),
    "Completed": (_OK, "checkmark.circle.fill", "fa5s.check-circle"),
    "Post-processing": ("#9C27B0", "film.fill", "fa5s.film"),
    "Running": ("#00BCD4", "gearshape.2.fill", "fa5s.cogs"),
    "Scanning": ("#00BCD4", "gearshape.2.fill", "fa5s.cogs"),
    "Queued": ("gray", "clock.fill", "fa5s.clock"),
    "Cancelled": (_WARN, "xmark.octagon.fill", "fa5s.ban"),
}
_PROBLEM_STYLE = (_BAD, "exclamationmark.triangle.fill", "fa5s.exclamation-triangle")

def job_status_style(status):
    """Returns (colour, SF Symbol name, Font Awesome name) for a job status.
    Anything that is not a known good or in-progress state is shown as a problem."""
    return _JOB_STATUS_STYLES.get(status, _PROBLEM_STYLE)
