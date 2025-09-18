# utils.py
import os
import platform
import subprocess
from datetime import datetime
import psutil
import qtawesome as qta

def get_icon_for_path(path):
    if os.path.ismount(path): return qta.icon("fa5s.hdd", color="silver")
    return qta.icon("fa5s.folder", color="orange")

def format_bytes(byte_count):
    if byte_count is None: return "N/A"
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