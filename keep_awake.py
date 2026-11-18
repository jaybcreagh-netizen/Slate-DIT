# keep_awake.py
"""
Stops the Mac going to sleep while a copy is running.

Uses macOS's own `caffeinate` tool. It holds off idle sleep and disk sleep,
and system sleep while on mains power. Closing the lid on battery still
sleeps the machine; nothing an app does can prevent that.

On other systems this does nothing.
"""
import os
import subprocess
import sys

from app_log import log


class KeepAwake:
    def __init__(self):
        self._process = None

    @property
    def active(self):
        return self._process is not None and self._process.poll() is None

    def start(self):
        if self.active or sys.platform != "darwin":
            return
        try:
            # -w ties it to Slate: if Slate dies, the Mac is allowed to sleep again.
            self._process = subprocess.Popen(
                ["caffeinate", "-i", "-m", "-s", "-w", str(os.getpid())],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except OSError as e:
            self._process = None
            log.warning("Could not stop the computer from sleeping during the copy: %s", e)

    def stop(self):
        if self._process is None:
            return
        try:
            self._process.terminate()
            self._process.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            pass
        self._process = None
