import unittest
from utils import format_bytes, format_eta, resolve_path_template, job_status_style

class TestUtils(unittest.TestCase):
    def test_format_bytes(self):
        self.assertEqual(format_bytes(0), "0.00 B")
        self.assertEqual(format_bytes(1024), "1.00 KB")
        self.assertEqual(format_bytes(1024 * 1024), "1.00 MB")
        self.assertEqual(format_bytes(1024 * 1024 * 1024), "1.00 GB")

    def test_format_eta(self):
        self.assertEqual(format_eta(0), "Done")
        self.assertEqual(format_eta(30), "30s")
        self.assertEqual(format_eta(65), "1m 5s")
        self.assertEqual(format_eta(3665), "1h 1m")

    def test_resolve_path_template(self):
        template = "{project_name}/{camera_id}/{card_num}"
        tokens = {"project_name": "MyProject", "camera_id": "A"}
        result = resolve_path_template(template, tokens, 1, "ignored")
        self.assertEqual(result, "MyProject/A/001")

    def test_only_a_clean_job_is_drawn_green(self):
        green = job_status_style("Completed")[0]
        self.assertEqual(job_status_style("Processed")[0], green)
        for status in ("Completed with errors", "Completed with issues", "Failed", "Something unexpected"):
            self.assertNotEqual(job_status_style(status)[0], green, status)

if __name__ == '__main__':
    unittest.main()
