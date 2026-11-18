# hook-ascmhl.py
# PyInstaller hook for the ASC MHL library.
# ascmhl looks up its own version number when it is imported. In a packaged
# app that lookup fails unless the package's metadata is bundled too, and
# Slate could then not write manifests. This bundles it.
from PyInstaller.utils.hooks import copy_metadata

datas = copy_metadata("ascmhl")
