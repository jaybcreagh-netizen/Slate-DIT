import unittest
from models import Job, JobStatus

class TestModels(unittest.TestCase):
    def test_job_defaults(self):
        job = Job()
        self.assertEqual(job.status, JobStatus.QUEUED)
        self.assertEqual(job.job_type, "copy")
        self.assertTrue(job.id.startswith("Job_"))

    def test_job_serialization(self):
        job = Job(id="test_id", sources=["/src"], destinations=["/dst"])
        data = job.to_dict()
        self.assertEqual(data["id"], "test_id")
        self.assertEqual(data["sources"], ["/src"])
        self.assertEqual(data["status"], "QUEUED")

    def test_job_deserialization(self):
        data = {
            "id": "test_id",
            "sources": ["/src"],
            "status": "COMPLETED"
        }
        job = Job.from_dict(data)
        self.assertEqual(job.id, "test_id")
        self.assertEqual(job.status, JobStatus.COMPLETED)
        self.assertEqual(job.sources, ["/src"])

if __name__ == '__main__':
    unittest.main()
