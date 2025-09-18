# config.py
import os
import sys

APP_NAME = "Slate"
APP_VERSION = "1.0.1 (Shippable)" # Let's bump the version
PROJECTS_BASE_DIR = os.path.join(os.path.expanduser("~"), "DIT_Projects")

# --- START MODIFICATION ---

def get_resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        # In a development environment, we assume the binaries are on the system PATH
        # For a more robust dev setup, you could place them in a local 'bin' folder
        # and use os.path.dirname(__file__) to find them.
        # For now, we'll rely on the system PATH for development.
        return relative_path

    return os.path.join(base_path, relative_path)

# Use the function to define paths. PyInstaller will bundle these.
FFPROBE_PATH = get_resource_path("ffprobe")
FFMPEG_PATH = get_resource_path("ffmpeg")

# --- END MODIFICATION ---