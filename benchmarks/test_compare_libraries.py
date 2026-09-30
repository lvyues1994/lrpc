"""Independent comparison-validator checks, without network activity."""

from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from compare_libraries import validate


class ComparisonValidation(unittest.TestCase):
    def trial(self, rows="0,100,1100,0\n1,1200,3200,0\n", **changes):
        config = {"bytes": 64, "inflight": 1}
        summary = dict(framework="brpc", bytes=64, body_bytes=62, inflight=1, iterations=2,
                       warmup=1, errors=0, elapsed_ns=3300, p99_ns=2000, client_cpu_seconds=.001)
        summary.update(changes)
        args = SimpleNamespace(iterations=2, warmup=1)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "samples.csv"
            path.write_text("sequence,started_ns,completed_ns,status\n" + rows)
            return validate(summary, path, "brpc", config, args)

    def test_nearest_rank_and_cpu(self):
        result = self.trial()
        self.assertEqual(result["p50_us"], 1)
        self.assertEqual(result["p99_us"], 2)
        self.assertEqual(result["maximum_api_inflight"], 1)
        self.assertEqual(result["client_cpu_ns_per_call"], 500000)

    def test_rejects_unfilled_sample(self):
        with self.assertRaises(RuntimeError):
            self.trial("0,100,1100,0\n1,0,0,0\n")

    def test_rejects_errors(self):
        with self.assertRaises(RuntimeError):
            self.trial("0,100,1100,0\n1,1200,3200,1\n")

    def test_rejects_overlap_beyond_limit(self):
        with self.assertRaises(RuntimeError):
            self.trial("0,100,1100,0\n1,1000,3000,0\n")

    def test_rejects_wrong_quantile(self):
        with self.assertRaises(RuntimeError):
            self.trial(p99_ns=1000)

    def test_rejects_count_size_sequence_and_elapsed(self):
        for changes in (dict(iterations=3), dict(body_bytes=64), dict(elapsed_ns=2000)):
            with self.assertRaises(RuntimeError):
                self.trial(**changes)
        with self.assertRaises(RuntimeError):
            self.trial("1,100,1100,0\n1,1200,3200,0\n")


if __name__ == "__main__":
    unittest.main()
