# hook-PySide6.py
# A robust hook to manually locate and bundle Qt platform plugins for PySide6 on macOS.
import os
from PySide6 import QtCore

# The 'datas' variable is what PyInstaller looks for in a hook file.
# It's a list of tuples, where each tuple is (source_path, destination_in_bundle).
datas = []

# Get the directory where PySide6 plugins are stored
plugin_path = os.path.join(os.path.dirname(QtCore.__file__), "plugins")

# We absolutely need the 'platforms' plugin for the GUI to launch on macOS
platforms_path = os.path.join(plugin_path, "platforms")
if os.path.isdir(platforms_path):
    datas.append((platforms_path, "PySide6/plugins/platforms"))

# It's also good practice to include style plugins
styles_path = os.path.join(plugin_path, "styles")
if os.path.isdir(styles_path):
    datas.append((styles_path, "PySide6/plugins/styles"))

# You could add other plugin types here if needed, e.g., 'imageformats'
# imageformats_path = os.path.join(plugin_path, "imageformats")
# if os.path.isdir(imageformats_path):
#     datas.append((imageformats_path, "PySide6/plugins/imageformats"))