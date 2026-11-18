# app_log.py
"""
Slate's log file.

Everything that goes wrong, and a summary of every job, is written to
~/DIT_Projects/logs/slate.log so there is something to look at after the
fact. The file is capped at about 10 MB; older entries roll into
slate.log.1, slate.log.2 and so on.
"""
import logging
import os
import sys
import threading
from logging.handlers import RotatingFileHandler

from config import APP_NAME, APP_VERSION, PROJECTS_BASE_DIR

log = logging.getLogger("slate")
LOG_DIR = os.path.join(PROJECTS_BASE_DIR, "logs")
LOG_PATH = os.path.join(LOG_DIR, "slate.log")


def setup_logging(log_dir=LOG_DIR):
    """Starts writing the log file. Safe to call more than once."""
    if log.handlers:
        return LOG_PATH
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s")
    try:
        os.makedirs(log_dir, exist_ok=True)
        handler = RotatingFileHandler(os.path.join(log_dir, "slate.log"), maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    except OSError:
        handler = logging.StreamHandler()  # no log folder: at least show it in the terminal
    handler.setFormatter(formatter)
    log.addHandler(handler)

    def on_crash(exc_type, exc, tb):
        log.critical("Unexpected error", exc_info=(exc_type, exc, tb))
        sys.__excepthook__(exc_type, exc, tb)

    def on_thread_crash(args):
        log.critical("Unexpected error in background thread %s", args.thread.name if args.thread else "?",
                     exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

    sys.excepthook = on_crash
    threading.excepthook = on_thread_crash
    log.info("%s %s started", APP_NAME, APP_VERSION)
    return LOG_PATH
