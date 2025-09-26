# config.py
import os
import sys

APP_NAME = "Slate"
APP_VERSION = "1.0.1 (Shippable)" 
PROJECTS_BASE_DIR = os.path.join(os.path.expanduser("~"), "DIT_Projects")

# --- START MODIFICATION ---

def get_resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:

        base_path = sys._MEIPASS
    except Exception:

        return relative_path

    return os.path.join(base_path, relative_path)


FFPROBE_PATH = get_resource_path("ffprobe")
FFMPEG_PATH = get_resource_path("ffmpeg")

