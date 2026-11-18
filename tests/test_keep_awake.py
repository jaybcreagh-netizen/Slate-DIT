import logging
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import app_log
import keep_awake


class TestKeepAwake(unittest.TestCase):
    def test_on_a_mac_it_holds_off_sleep_for_as_long_as_slate_lives(self):
        process = MagicMock()
        process.poll.return_value = None
        with patch.object(keep_awake.sys, "platform", "darwin"), \
                patch.object(keep_awake.subprocess, "Popen", return_value=process) as popen:
            awake = keep_awake.KeepAwake()
            awake.start()
            awake.start()  # a second start must not launch a second helper
            self.assertEqual(popen.call_count, 1)
            command = popen.call_args[0][0]
            self.assertEqual(command[0], "caffeinate")
            self.assertEqual(command[-2:], ["-w", str(os.getpid())])
            self.assertTrue(awake.active)
            awake.stop()
            process.terminate.assert_called_once()
            self.assertFalse(awake.active)

    def test_elsewhere_it_does_nothing(self):
        with patch.object(keep_awake.sys, "platform", "linux"), patch.object(keep_awake.subprocess, "Popen") as popen:
            awake = keep_awake.KeepAwake()
            awake.start()
            awake.stop()
            popen.assert_not_called()

    def test_a_missing_helper_does_not_stop_the_copy(self):
        with patch.object(keep_awake.sys, "platform", "darwin"), \
                patch.object(keep_awake.subprocess, "Popen", side_effect=FileNotFoundError("caffeinate")):
            awake = keep_awake.KeepAwake()
            with self.assertLogs("slate", level="WARNING"):
                awake.start()
            self.assertFalse(awake.active)


class TestLogFile(unittest.TestCase):
    def test_messages_reach_the_file(self):
        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder, True)
        saved_handlers, saved_hook = app_log.log.handlers[:], app_log.sys.excepthook
        app_log.log.handlers.clear()

        def restore():
            for handler in app_log.log.handlers:
                handler.close()
            app_log.log.handlers[:] = saved_handlers
            app_log.sys.excepthook = saved_hook
        self.addCleanup(restore)

        app_log.setup_logging(os.path.join(folder, "logs"))
        app_log.log.error("C001.mov was NOT copied safely")
        logging.shutdown()
        with open(os.path.join(folder, "logs", "slate.log")) as f:
            text = f.read()
        self.assertIn("started", text)
        self.assertIn("C001.mov was NOT copied safely", text)


if __name__ == "__main__":
    unittest.main()
