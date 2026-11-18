"""
Checks Slate's "MHL Verify" against manifests from other tools as well as its own.
"""
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock

import xxhash

from workers import MHLVerifyWorker, parse_mhl

DATA = b"camera original " * 4096
BIG_ENDIAN = xxhash.xxh64(DATA).hexdigest()
LITTLE_ENDIAN = bytes.fromhex(BIG_ENDIAN)[::-1].hex()


def legacy_mhl(tag, value, name="C001.mov"):
    return (f'<?xml version="1.0" encoding="UTF-8"?>\n<hashlist version="1.1">\n'
            f'  <hash><file>{name}</file><size>{len(DATA)}</size><{tag}>{value}</{tag}></hash>\n</hashlist>\n')


class TestMHLVerify(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        with open(os.path.join(self.tmp, "C001.mov"), "wb") as f:
            f.write(DATA)

    def verify(self, mhl_text):
        mhl_path = os.path.join(self.tmp, "check.mhl")
        with open(mhl_path, "w") as f:
            f.write(mhl_text)
        worker = MHLVerifyWorker({"id": "v1", "mhl_file": mhl_path, "target_dir": self.tmp})
        for signal in ("progress", "file_progress", "job_finished", "error"):
            setattr(worker, signal, MagicMock())
        worker.run()
        return worker.job_finished.emit.call_args[0][0]

    def test_xxhash64be_from_other_tools_is_checked(self):
        report = self.verify(legacy_mhl("xxhash64be", BIG_ENDIAN))
        self.assertEqual((report["status"], report["verified_count"]), ("Completed", 1))

    def test_old_xxhash64_tag_is_checked_in_either_byte_order(self):
        for value in (LITTLE_ENDIAN, BIG_ENDIAN):
            report = self.verify(legacy_mhl("xxhash64", value))
            self.assertEqual((report["status"], report["verified_count"]), ("Completed", 1))

    def test_wrong_checksum_fails(self):
        report = self.verify(legacy_mhl("xxhash64be", "0" * 16))
        self.assertEqual((report["status"], report["failed_count"]), ("Completed with issues", 1))

    def test_missing_file_is_reported(self):
        report = self.verify(legacy_mhl("xxhash64be", BIG_ENDIAN, name="C999.mov"))
        self.assertEqual((report["status"], report["missing_count"]), ("Completed with issues", 1))

    def test_a_manifest_that_lists_nothing_is_not_a_pass(self):
        report = self.verify('<?xml version="1.0"?>\n<hashlist version="1.1"></hashlist>\n')
        self.assertEqual(report["status"], "Failed")
        self.assertEqual(report["verified_count"], 0)

    def test_checksum_type_slate_cannot_check_is_not_a_pass(self):
        report = self.verify(legacy_mhl("c4", "c4" + "1" * 88))
        self.assertEqual((report["status"], report["failed_count"]), ("Completed with issues", 1))

    def test_asc_mhl_format_is_read(self):
        text = ('<?xml version="1.0" encoding="UTF-8"?>\n<hashlist version="2.0" xmlns="urn:ASC:MHL:v2.0">\n<hashes>\n'
                f'<hash><path size="{len(DATA)}">C001.mov</path><xxh64 action="original">{BIG_ENDIAN}</xxh64></hash>\n'
                '</hashes>\n</hashlist>\n')
        self.assertEqual(parse_mhl(self._write(text)), [("C001.mov", BIG_ENDIAN, "xxh64")])
        report = self.verify(text)
        self.assertEqual((report["status"], report["verified_count"]), ("Completed", 1))

    def _write(self, text):
        path = os.path.join(self.tmp, "parse.mhl")
        with open(path, "w") as f:
            f.write(text)
        return path


if __name__ == "__main__":
    unittest.main()
