"""Regression checks for the independent sample validator; no sockets required."""

from pathlib import Path
import tempfile
import unittest

from run_suite import validate


class SampleValidation(unittest.TestCase):
    def trial(self, *, lag=100000, code=0, drop=False, rate=300000, diagnostic=False,
              faithful=True, eligible=True):
        counts = [0] * 17
        if not drop:
            counts[code] = 1
        def distribution(ns):
            return dict(p50_us=ns / 1000, p99_us=ns / 1000, max_us=ns / 1000)
        summary = dict(receive_buffer_bytes=65536, offered=1, started=int(not drop),
                       success=counts[0], rejected=counts[8], timeout=counts[4], other_errors=0,
                       status_counts=counts, generator_drop=int(drop), target_rate=rate, burst=1,
                       max_schedule_lag_us=100, allocation_counting=diagnostic,
                       queue_instrumentation=False, sanitizers=False, diagnostic_only=diagnostic,
                       load_faithful=faithful, eligible_for_zero_error_p99=eligible,
                       offer_lag=distribution(lag), schedule_lag=None if drop else distribution(lag),
                       success_scheduled=distribution(lag + 2000) if code == 0 and not drop else None,
                       success_api=None, rejected_api=None, timeout_api=None, other_error_api=None)
        if not drop:
            summary[{0: "success_api", 8: "rejected_api", 4: "timeout_api"}[code]] = distribution(2000)
        row = f"0,0,{lag},{-1 if drop else lag},{lag if drop else lag + 2000},{-1 if drop else code},{int(drop)}\n"
        return summary, row

    def check_trial(self, summary, row, *, window=65536):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "samples.csv"
            path.write_text("id,scheduled_ns,offered_ns,started_ns,completed_ns,status,generator_drop\n" + row)
            validate(summary, path, {"receive-buffer-bytes": window})

    def test_inclusive_lateness_boundary(self):
        self.check_trial(*self.trial())
        summary, row = self.trial(lag=100001, faithful=False, eligible=False)
        self.check_trial(summary, row)
        summary["load_faithful"] = True
        with self.assertRaises(AssertionError):
            self.check_trial(summary, row)

    def test_drop_rejection_and_timeout_cannot_qualify(self):
        for options in (dict(drop=True, faithful=False), dict(code=8), dict(code=4)):
            summary, row = self.trial(**options, eligible=False)
            self.check_trial(summary, row)
            summary["eligible_for_zero_error_p99"] = True
            with self.assertRaises(AssertionError):
                self.check_trial(summary, row)

    def test_closed_loop_has_no_offered_load_gate(self):
        self.check_trial(*self.trial(lag=200000, rate=0, faithful=None))

    def test_diagnostics_cannot_qualify(self):
        summary, row = self.trial(diagnostic=True, eligible=False)
        self.check_trial(summary, row)
        summary["eligible_for_zero_error_p99"] = True
        with self.assertRaises(AssertionError):
            self.check_trial(summary, row)

    def test_metadata_and_counts_must_match(self):
        summary, row = self.trial()
        with self.assertRaises(AssertionError):
            self.check_trial(summary, row, window=8208)
        wrong = summary.copy()
        wrong["success"] = 0
        with self.assertRaises(AssertionError):
            self.check_trial(wrong, row)


if __name__ == "__main__":
    unittest.main()
