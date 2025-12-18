import unittest
import queue
import time
import os
import shutil
from unittest.mock import MagicMock
from workers import ScanWorker

class TestScanWorker(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.src_file = os.path.join(self.test_dir, "test.txt")
        with open(self.src_file, "w") as f:
            f.write("test content")

        self.dest_dir = os.path.join(self.test_dir, "dest")
        os.makedirs(self.dest_dir)

        self.file_queue = queue.Queue()
        self.job_params = {
            "sources": [self.test_dir],
            "destinations": [self.dest_dir],
            "has_template": False,
            "create_source_folder": False,
            "naming_preset": {},
            "card_counter": 1
        }

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_scan_pushes_to_queue(self):
        worker = ScanWorker(self.job_params, self.file_queue)
        worker.scan_progress = MagicMock()
        worker.scan_finished = MagicMock()

        worker.run()

        # Expect 1 file task and 1 sentinel (None)
        self.assertEqual(self.file_queue.qsize(), 2)

        task = self.file_queue.get()
        self.assertEqual(task['source'], self.src_file)
        self.assertEqual(task['size'], len("test content"))

        sentinel = self.file_queue.get()
        self.assertIsNone(sentinel)

        worker.scan_finished.emit.assert_called()

import tempfile
if __name__ == '__main__':
    unittest.main()
