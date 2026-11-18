"""
Breaks copies on purpose and checks that Slate says so.

Each test damages a copy in a way that happens on set (bad drive, full
drive, drive unplugged, card unplugged, cancel half way, file already
there) and then checks three things:
  1. the job does not finish as a plain "Completed",
  2. no damaged or half-written file is left under its real name,
  3. good copies on other drives are still kept.
"""
import errno
import os
import queue
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import copy_engine
from workers import ScanWorker, TransferWorker

SMALL_CHUNK = 64 * 1024
FILE_SIZE = 10 * SMALL_CHUNK + 123  # several chunks plus an odd tail


class FaultyFile:
    """Stands in for a destination file on a drive that misbehaves."""

    def __init__(self, real, fail_after=None, error_code=None, corrupt=False):
        self.real = real
        self.fail_after = fail_after
        self.error_code = error_code
        self.corrupt = corrupt
        self.written = 0

    def write(self, buf):
        if self.fail_after is not None and self.written + len(buf) > self.fail_after:
            raise OSError(self.error_code, os.strerror(self.error_code))
        if self.corrupt and self.written <= SMALL_CHUNK < self.written + len(buf):
            damaged = bytearray(buf)
            damaged[10] ^= 0x01  # one flipped bit, as a bad cable or drive would do
            buf = bytes(damaged)
        self.written += len(buf)
        return self.real.write(buf)

    def flush(self):
        self.real.flush()

    def fileno(self):
        return self.real.fileno()

    def close(self):
        self.real.close()


class FaultyTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.card = os.path.join(self.tmp, "A001")
        self.drive_a = os.path.join(self.tmp, "DriveA")
        self.drive_b = os.path.join(self.tmp, "DriveB")
        os.makedirs(os.path.join(self.card, "CLIPS"))
        os.makedirs(self.drive_a)
        os.makedirs(self.drive_b)
        self.clips = []
        for name in ("C001.mov", "C002.mov"):
            path = os.path.join(self.card, "CLIPS", name)
            with open(path, "wb") as f:
                f.write(os.urandom(FILE_SIZE))
            self.clips.append(path)
        chunk = patch.object(copy_engine, "CHUNK_SIZE", SMALL_CHUNK)
        chunk.start()
        self.addCleanup(chunk.stop)
        self.addCleanup(shutil.rmtree, self.tmp, True)

    # --- helpers -----------------------------------------------------
    def dest(self, drive, clip):
        return os.path.join(drive, "A001", os.path.relpath(clip, self.card))

    def make_job(self, sources=None, **extra):
        job = {
            "id": "job_1",
            "sources": sources or [self.card],
            "destinations": [self.drive_a, self.drive_b],
            "checksum_method": "xxHash (Fast)",
            "verification_mode": "full",
            "metadata": {},
            "file_queue": queue.Queue(),
        }
        job.update(extra)
        return job

    def scan(self, job):
        params = {
            "sources": job["sources"], "destinations": job["destinations"],
            "has_template": False, "create_source_folder": True,
            "naming_preset": {}, "card_counter": 1,
        }
        scanner = ScanWorker(params, job["file_queue"])
        scanner.scan_progress = MagicMock()
        scanner.scan_finished = MagicMock()
        scanner.run()

    def run_job(self, job=None, before_run=None):
        job = job or self.make_job()
        self.scan(job)
        worker = TransferWorker(job, self.tmp)
        worker.progress = MagicMock()
        worker.file_progress = MagicMock()
        worker.job_finished = MagicMock()
        worker.error = MagicMock()
        if before_run:
            before_run(worker)
        worker.run()
        self.worker = worker
        return worker.job_finished.emit.call_args[0][0]

    def break_drive(self, drive, **fault):
        """Every file written to `drive` goes through a FaultyFile."""
        real_open = copy_engine._open_dest

        def faulty_open(path):
            f = real_open(path)
            return FaultyFile(f, **fault) if path.startswith(drive) else f

        p = patch.object(copy_engine, "_open_dest", faulty_open)
        p.start()
        self.addCleanup(p.stop)

    def all_files(self, folder):
        found = []
        for root, _, files in os.walk(folder):
            if os.path.basename(root) == "ascmhl":
                continue
            found += [os.path.join(root, f) for f in files if f != "ascmhl_chain.xml"]
        return sorted(found)

    def assertSameContent(self, a, b):
        with open(a, "rb") as fa, open(b, "rb") as fb:
            self.assertEqual(fa.read(), fb.read())

    def assertJobFailed(self, report):
        self.assertEqual(report["status"], "Completed with errors")
        self.assertTrue(report["errors"])
        self.worker.error.emit.assert_called()

    def assertDriveClean(self, drive):
        """Nothing half-written, and nothing damaged under a real name."""
        for path in self.all_files(drive):
            self.assertFalse(path.endswith(copy_engine.PARTIAL_SUFFIX), f"left behind: {path}")
            source = os.path.join(self.card, os.path.relpath(path, os.path.join(drive, "A001")))
            self.assertSameContent(source, path)


class TestHealthyCopy(FaultyTestCase):
    def test_good_copy_is_completed_and_identical(self):
        report = self.run_job()
        self.assertEqual(report["status"], "Completed")
        self.assertEqual(report["errors"], [])
        self.worker.error.emit.assert_not_called()
        for clip in self.clips:
            for drive in (self.drive_a, self.drive_b):
                self.assertSameContent(clip, self.dest(drive, clip))
        self.assertTrue(all(f["status"] == "Verified" for f in report["files"]))

    def test_timestamps_are_kept(self):
        os.utime(self.clips[0], (1_600_000_000, 1_600_000_000))
        self.run_job()
        self.assertEqual(int(os.path.getmtime(self.dest(self.drive_a, self.clips[0]))), 1_600_000_000)


class TestDamagedCopies(FaultyTestCase):
    def test_flipped_bit_on_one_drive(self):
        self.break_drive(self.drive_b, corrupt=True)
        report = self.run_job()
        self.assertJobFailed(report)
        self.assertEqual(self.all_files(self.drive_b), [])  # the bad copies were not kept
        self.assertEqual(len(self.all_files(self.drive_a)), 2)  # the good drive is complete
        self.assertDriveClean(self.drive_a)
        for f in report["files"]:
            self.assertEqual(f["status"], "Failed")
            a, b = f["destinations"]
            self.assertTrue(a["verified"])
            self.assertFalse(b["verified"])
            self.assertIn("checksum mismatch", b["status"])

    def test_drive_full(self):
        self.break_drive(self.drive_b, fail_after=3 * SMALL_CHUNK, error_code=errno.ENOSPC)
        report = self.run_job()
        self.assertJobFailed(report)
        self.assertIn("No space left", " ".join(report["errors"]))
        self.assertEqual(self.all_files(self.drive_b), [])
        self.assertEqual(len(self.all_files(self.drive_a)), 2)
        self.assertDriveClean(self.drive_a)

    def test_drive_unplugged_mid_file(self):
        self.break_drive(self.drive_a, fail_after=5 * SMALL_CHUNK, error_code=errno.EIO)
        report = self.run_job()
        self.assertJobFailed(report)
        self.assertEqual(self.all_files(self.drive_a), [])
        self.assertEqual(len(self.all_files(self.drive_b)), 2)
        self.assertDriveClean(self.drive_b)

    def test_every_drive_fails(self):
        self.break_drive(self.tmp, fail_after=0, error_code=errno.EIO)
        report = self.run_job()
        self.assertJobFailed(report)
        self.assertEqual(self.all_files(self.drive_a) + self.all_files(self.drive_b), [])

    def test_card_unplugged_mid_file(self):
        real_open = copy_engine._open_source

        class DyingCard:
            def __init__(self, f):
                self.f, self.reads = f, 0

            def read(self, n):
                self.reads += 1
                if self.reads > 3:
                    raise OSError(errno.EIO, os.strerror(errno.EIO))
                return self.f.read(n)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                self.f.close()

        with patch.object(copy_engine, "_open_source", lambda p: DyingCard(real_open(p))):
            report = self.run_job()
        self.assertJobFailed(report)
        self.assertEqual(self.all_files(self.drive_a) + self.all_files(self.drive_b), [])
        self.assertEqual(report["manifests"], [])


class TestCancel(FaultyTestCase):
    def test_cancel_mid_file_leaves_nothing_half_written(self):
        def cancel_when_copying(worker):
            def on_file_progress(percent, text, path, speed):
                if percent >= 30:
                    worker.cancel()
            worker.file_progress.emit.side_effect = on_file_progress

        report = self.run_job(before_run=cancel_when_copying)
        self.assertEqual(report["status"], "Cancelled")
        self.assertEqual(self.all_files(self.drive_a) + self.all_files(self.drive_b), [])


class TestFilesAlreadyThere(FaultyTestCase):
    def test_identical_file_is_checked_not_rewritten(self):
        existing = self.dest(self.drive_a, self.clips[0])
        os.makedirs(os.path.dirname(existing))
        shutil.copyfile(self.clips[0], existing)
        before = os.stat(existing)
        report = self.run_job()
        self.assertEqual(report["status"], "Completed")
        after = os.stat(existing)
        self.assertEqual((before.st_ino, before.st_mtime_ns), (after.st_ino, after.st_mtime_ns))
        statuses = [d["status"] for f in report["files"] for d in f["destinations"]]
        self.assertIn("Verified (already at destination)", statuses)

    def test_different_file_is_never_overwritten(self):
        existing = self.dest(self.drive_a, self.clips[0])
        os.makedirs(os.path.dirname(existing))
        with open(existing, "wb") as f:
            f.write(b"footage from another day that must not be lost")
        report = self.run_job()
        self.assertJobFailed(report)
        with open(existing, "rb") as f:
            self.assertEqual(f.read(), b"footage from another day that must not be lost")
        self.assertIn("already exists", " ".join(report["errors"]))

    def test_same_size_but_different_content_is_caught(self):
        existing = self.dest(self.drive_a, self.clips[0])
        os.makedirs(os.path.dirname(existing))
        with open(existing, "wb") as f:
            f.write(os.urandom(FILE_SIZE))
        report = self.run_job()
        self.assertJobFailed(report)

    def test_leftover_from_an_interrupted_copy_is_redone(self):
        leftover = self.dest(self.drive_a, self.clips[0]) + copy_engine.PARTIAL_SUFFIX
        os.makedirs(os.path.dirname(leftover))
        with open(leftover, "wb") as f:
            f.write(b"half a file")
        report = self.run_job()
        self.assertEqual(report["status"], "Completed")
        self.assertDriveClean(self.drive_a)

    def test_running_the_same_copy_twice_is_safe(self):
        self.run_job()
        report = self.run_job()
        self.assertEqual(report["status"], "Completed")
        self.assertDriveClean(self.drive_a)
        self.assertDriveClean(self.drive_b)


class TestNothingToCopy(FaultyTestCase):
    def test_empty_card_is_not_a_success(self):
        shutil.rmtree(os.path.join(self.card, "CLIPS"))
        report = self.run_job()
        self.assertJobFailed(report)
        self.assertIn("No files were found", report["errors"][0])

    def test_card_missing_at_start_is_not_a_success(self):
        report = self.run_job(self.make_job(sources=[os.path.join(self.tmp, "NOT_MOUNTED")]))
        self.assertJobFailed(report)

    def test_unreadable_file_on_card_is_reported(self):
        real_getsize = os.path.getsize

        def getsize(path):
            if path == self.clips[1]:
                raise PermissionError(errno.EACCES, os.strerror(errno.EACCES))
            return real_getsize(path)

        job = self.make_job()
        with patch("workers.os.path.getsize", getsize):
            self.scan(job)
        with patch.object(self, "scan", lambda job: None):
            report = self.run_job(job)
        self.assertJobFailed(report)
        self.assertIn("C002.mov", " ".join(report["errors"]))
        self.assertEqual(len(self.all_files(self.drive_a)), 1)  # the readable clip was still copied


class TestEject(FaultyTestCase):
    def test_card_is_only_offered_for_eject_after_a_clean_copy(self):
        with patch("workers.os.path.ismount", lambda p: True):
            good = self.run_job(self.make_job(eject_on_completion=True))
        self.assertEqual(good["ejectable_sources_on_success"], [self.card])

        shutil.rmtree(self.drive_b)
        os.makedirs(self.drive_b)
        self.break_drive(self.drive_b, corrupt=True)
        with patch("workers.os.path.ismount", lambda p: True):
            bad = self.run_job(self.make_job(eject_on_completion=True))
        self.assertNotIn("ejectable_sources_on_success", bad)


class TestFolderChoices(unittest.TestCase):
    def test_destination_inside_source_is_refused(self):
        self.assertIsNotNone(copy_engine.find_path_conflict(["/Volumes/A001"], ["/Volumes/A001/backup"]))
        self.assertIsNotNone(copy_engine.find_path_conflict(["/Volumes/A001"], ["/Volumes/A001"]))
        self.assertIsNotNone(copy_engine.find_path_conflict(["/Volumes/RAID/A001"], ["/Volumes/RAID"]))

    def test_separate_folders_are_fine(self):
        self.assertIsNone(copy_engine.find_path_conflict(["/Volumes/A001"], ["/Volumes/A001_backup", "/Volumes/RAID"]))


if __name__ == "__main__":
    unittest.main()
