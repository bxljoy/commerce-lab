"""Fast regression tests for evidence parsing, not claims of process crashes."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid

spec = importlib.util.spec_from_file_location("proof", Path(__file__).with_name("outbox-proof.py"))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


class EvidenceTest(unittest.TestCase):
    line = 'Partition:1\tOffset:4\torder-id\t{"eventId":"selected","orderId":"order-id"}'

    def test_repeated_reads_are_not_duplicates(self):
        self.assertEqual(len(proof.parse_records(self.line + "\n" + self.line, "selected")), 1)

    def test_distinct_offsets_are_distinct_publications(self):
        self.assertEqual(len(proof.parse_records(self.line + "\n" + self.line.replace("Offset:4", "Offset:5"), "selected")), 2)

    def test_unrelated_events_and_cli_diagnostics_are_ignored(self):
        self.assertEqual(proof.parse_records("Processed a total of 1 messages\n" + self.line, "other"), [])

    def test_payload_bytes_preserved(self):
        record = proof.parse_records(self.line, "selected")[0]
        self.assertEqual(record, {"partition": 1, "offset": 4, "key": "order-id",
                                 "value": '{"eventId":"selected","orderId":"order-id"}'})

    def test_marker_requires_exact_structured_event(self):
        self.assertFalse(proof.ack_boundary('ACK_BOUNDARY selected', 'selected'))
        self.assertFalse(proof.ack_boundary('{"marker":"ACK_BOUNDARY","eventId":"other"}', 'selected'))
        self.assertTrue(proof.ack_boundary('{"marker":"ACK_BOUNDARY","eventId":"selected"}', 'selected'))

    def test_kill_requires_marker_record_and_undelivered_row(self):
        for marker, records, delivered in ((False, [1], False), (True, [], False), (True, [1], True)):
            with self.assertRaises(AssertionError):
                proof.require_crash_boundary(marker, records, delivered)
        proof.require_crash_boundary(True, [1], False)


class DeadlineTest(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.clock = patch.object(proof.runtime.time, "monotonic", side_effect=lambda: self.now)
        self.clock.start()
        self.addCleanup(self.clock.stop)

    def deadline(self, seconds):
        return proof.runtime.Deadline(seconds, "test budget")

    def test_reused_helpers_always_bound_the_docker_subprocess(self):
        def stuck(command, **kwargs):
            self.assertGreater(kwargs.get("timeout", 0), 0)
            self.assertLessEqual(kwargs["timeout"], 120)
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        with patch.object(proof.runtime.subprocess, "check_output", side_effect=stuck):
            for action in (proof.runtime.order_url, lambda: proof.runtime.assert_services([]),
                           lambda: proof.runtime.compose("exec", "-T", "postgres", "psql")):
                with self.subTest(action=action), self.assertRaises(subprocess.TimeoutExpired):
                    action()

    def test_row_poll_clamps_sql_to_remaining_fractional_budget(self):
        deadline = self.deadline(180)
        self.now = 179.75
        def stuck(command, **kwargs):
            self.assertEqual(kwargs["timeout"], 0.25)
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        with patch.object(proof.runtime.subprocess, "check_output", side_effect=stuck):
            with self.assertRaises(subprocess.TimeoutExpired):
                proof.await_row(str(uuid.uuid4()), lambda row: True, deadline=deadline)

    def test_expired_marker_wait_does_not_spawn_docker(self):
        deadline = self.deadline(180)
        self.now = 180
        with patch.object(proof.runtime.subprocess, "check_output") as run:
            with self.assertRaises(TimeoutError):
                proof.await_marker("event", deadline=deadline)
            run.assert_not_called()

    def test_marker_poll_clamps_logs_and_sleep_to_remaining_budget(self):
        deadline = self.deadline(180)
        self.now = 179.75
        def logs(command, **kwargs):
            self.assertEqual(kwargs["timeout"], 0.25)
            self.now += 0.125
            return ""
        def sleep(seconds):
            self.assertEqual(seconds, 0.125)
            self.now += seconds
        with patch.object(proof.runtime.subprocess, "check_output", side_effect=logs) as run, \
                patch.object(proof.runtime.time, "sleep", side_effect=sleep):
            with self.assertRaises(TimeoutError):
                proof.await_marker("event", deadline=deadline)
            self.assertEqual(run.call_count, 1)

    def test_consumer_and_delivery_share_one_observation_budget(self):
        deadline = self.deadline(180)
        self.now = 179.5
        def run(command, **kwargs):
            if "kafka" in command:
                self.assertEqual(kwargs["timeout"], 0.5)
                self.now += 0.25
                return EvidenceTest.line
            self.assertEqual(kwargs["timeout"], 0.25)
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        with patch.object(proof.runtime.subprocess, "check_output", side_effect=run):
            proof.observe("selected", 1, deadline=deadline)
            with self.assertRaises(subprocess.TimeoutExpired):
                proof.await_row(str(uuid.uuid4()), lambda row: True, deadline=deadline)

    def test_recreation_shares_startup_budget_with_inspect_and_port(self):
        budgets = []
        def run(command, **kwargs):
            budgets.append(kwargs["timeout"])
            if "up" in command:
                self.now = 119
                return ""
            self.now += 0.25
            if "inspect" in command:
                return "[]"
            return "container" if "ps" in command else "127.0.0.1:1234"
        with patch.dict(os.environ), patch.object(proof.runtime.subprocess, "check_output", side_effect=run):
            self.assertEqual(proof.recreate(True), "http://127.0.0.1:1234")
        self.assertEqual(budgets, [120, 1, 0.75, 0.5])

    def test_broker_and_topic_setup_share_startup_budget(self):
        budgets = []
        def run(command, **kwargs):
            budgets.append(kwargs["timeout"])
            if "up" in command:
                self.now = 110 if "kafka" in command else 119.5
                return ""
            if "ps" in command:
                self.now = 119.75
                return "container"
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        with patch.object(proof.runtime.subprocess, "check_output", side_effect=run):
            with self.assertRaises(subprocess.TimeoutExpired):
                proof.start_broker()
        self.assertEqual(budgets, [120, 10, 0.5, 0.25])

    def test_late_success_is_not_accepted_after_deadline(self):
        deadline = self.deadline(1)
        def late(command, **kwargs):
            self.now = 1.01
            return "success"
        with patch.object(proof.runtime.subprocess, "check_output", side_effect=late):
            with self.assertRaises(TimeoutError):
                proof.runtime.compose("ps", deadline=deadline)


class CleanupTimeoutTest(unittest.TestCase):
    def test_fake_docker_timeout_still_runs_cleanup_even_if_diagnostics_timeout(self):
        root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory(prefix="proof-timeout-test-") as directory:
            directory = Path(directory)
            fake = directory / "docker"
            log = directory / "calls.jsonl"
            fake.write_text("#!/usr/bin/env python3\n"
                            "import json, os, sys, time\n"
                            "with open(os.environ['FAKE_DOCKER_LOG'], 'a') as log:\n"
                            "    log.write(json.dumps(sys.argv[1:]) + '\\n')\n"
                            "if sys.argv[1:3] in (['compose', 'port'], ['compose', 'logs']):\n"
                            "    time.sleep(10)\n")
            fake.chmod(0o755)
            environment = {**os.environ, "PATH": str(directory) + os.pathsep + os.environ["PATH"],
                           "FAKE_DOCKER_LOG": str(log), "PYTHONDONTWRITEBYTECODE": "1"}
            result = subprocess.run(["bash", "-c", """
proof_kind=timeout-test
source scripts/verify-compose-common.sh
# Accelerate only this fake test's cleanup budgets; real defaults remain unchanged.
proof_docker() { shift; proof_command 0.2 docker "$@"; }
proof_command 0.2 docker compose port order-service 8080
"""], cwd=root, env=environment, text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 124, result.stdout + result.stderr)
            self.assertIn("Command deadline exceeded", result.stderr)
            for resource in ("container", "network", "volume"):
                self.assertIn(resource + "=0", result.stdout)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertIn(["compose", "down", "-v", "--remove-orphans", "--timeout", "10"], calls)
            self.assertIn(["compose", "logs", "--tail=80"], calls)
            self.assertEqual(sum(call[:2] == ["container", "ls"] for call in calls), 1)


if __name__ == "__main__":
    unittest.main()
