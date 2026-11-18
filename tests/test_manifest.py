"""
Checks the proof Slate writes next to each copy, using the ASC's own
reference tool as the judge rather than Slate's own code.
"""
import glob
import os
import unittest

from test_faults import FaultyTestCase

try:
    from ascmhl import commands as mhl_commands, errors as mhl_errors
except ImportError:  # pragma: no cover
    mhl_commands = None


def official_verify(folder):
    """Runs the reference tool's verify on a folder. Raises if anything is wrong."""
    mhl_commands.verify_entire_folder(folder, False, None, None)


@unittest.skipIf(mhl_commands is None, "the ascmhl package is not installed")
class TestManifest(FaultyTestCase):
    def root(self, drive):
        return os.path.join(drive, "A001")

    def generations(self, drive):
        return sorted(glob.glob(os.path.join(self.root(drive), "ascmhl", "*.mhl")))

    def test_manifest_is_written_beside_every_copy_and_passes(self):
        report = self.run_job()
        self.assertEqual(len(report["manifests"]), 2)
        for drive in (self.drive_a, self.drive_b):
            self.assertEqual(len(self.generations(drive)), 1)
            official_verify(self.root(drive))

    def test_md5_manifest_passes(self):
        self.run_job(self.make_job(checksum_method="MD5 (Compatible)"))
        official_verify(self.root(self.drive_a))
        with open(self.generations(self.drive_a)[0]) as f:
            self.assertIn("<md5 ", f.read())

    def test_reference_tool_catches_damage_after_the_copy(self):
        self.run_job()
        with open(self.dest(self.drive_a, self.clips[0]), "r+b") as f:
            f.seek(1000)
            f.write(b"\x00\xff\x00\xff")
        with self.assertRaises(mhl_errors.VerificationFailedException):
            official_verify(self.root(self.drive_a))

    def test_reference_tool_catches_a_missing_file(self):
        self.run_job()
        os.remove(self.dest(self.drive_a, self.clips[0]))
        with self.assertRaises(mhl_errors.CompletenessCheckFailedException):
            official_verify(self.root(self.drive_a))

    def test_failed_files_are_left_out_and_good_drive_still_passes(self):
        self.break_drive(self.drive_b, corrupt=True)
        report = self.run_job()
        self.assertEqual(report["status"], "Completed with errors")
        official_verify(self.root(self.drive_a))
        self.assertEqual(self.generations(self.drive_b), [])  # nothing good there, so nothing to vouch for

    def test_second_run_adds_a_generation_instead_of_replacing_the_first(self):
        self.run_job()
        report = self.run_job()
        self.assertEqual(report["status"], "Completed")
        self.assertEqual(len(self.generations(self.drive_a)), 2)
        with open(self.generations(self.drive_a)[1]) as f:
            self.assertIn('action="verified"', f.read())
        official_verify(self.root(self.drive_a))

    def test_no_manifest_when_checksums_are_switched_off(self):
        report = self.run_job(self.make_job(verification_mode="size"))
        self.assertEqual(report["status"], "Completed")
        self.assertEqual(report["manifests"], [])


if __name__ == "__main__":
    unittest.main()
