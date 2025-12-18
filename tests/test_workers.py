import unittest
import tempfile
import os
import shutil
import queue
from unittest.mock import MagicMock
from workers import TransferWorker

class TestTransferWorker(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.src_file = os.path.join(self.test_dir, "source.txt")
        self.dest_dir_1 = os.path.join(self.test_dir, "dest1")
        self.dest_dir_2 = os.path.join(self.test_dir, "dest2")

        # Create 1MB source file
        with open(self.src_file, "wb") as f:
            f.write(os.urandom(1024 * 1024))

        self.file_queue = queue.Queue()

        # Push file task to queue
        task = {
            'source': self.src_file,
            'destinations': [
                os.path.join(self.dest_dir_1, "source.txt"),
                os.path.join(self.dest_dir_2, "source.txt")
            ],
            'size': 1024 * 1024,
            'base_source_path': self.test_dir
        }
        self.file_queue.put(task)
        self.file_queue.put(None) # Sentinel

        self.job = {
            "id": "job_1",
            "sources": [self.test_dir],
            "destinations": [self.dest_dir_1, self.dest_dir_2],
            "resolved_dests": {}, # Unused in new logic
            "checksum_method": "xxHash (Fast)",
            "report": {"total_size": 1024*1024},
            "metadata": {},
            "file_queue": self.file_queue
        }

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_parallel_write(self):
        worker = TransferWorker(self.job, self.test_dir)
        # Mock signals to avoid QThread issues in headless test
        worker.progress = MagicMock()
        worker.file_progress = MagicMock()
        worker.job_finished = MagicMock()

        # Run copy logic directly
        worker.run()

        # Verify files exist and match size
        dest1_file = os.path.join(self.dest_dir_1, "source.txt")
        dest2_file = os.path.join(self.dest_dir_2, "source.txt")

        self.assertTrue(os.path.exists(dest1_file))
        self.assertTrue(os.path.exists(dest2_file))
        self.assertEqual(os.path.getsize(dest1_file), os.path.getsize(self.src_file))
        self.assertEqual(os.path.getsize(dest2_file), os.path.getsize(self.src_file))

        # Verify job_finished signal was called
        worker.job_finished.emit.assert_called()
        report = worker.job_finished.emit.call_args[0][0]
        self.assertEqual(report['status'], 'Completed')

if __name__ == '__main__':
    unittest.main()
