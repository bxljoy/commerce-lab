"""Fast regression tests for evidence parsing, not claims of process crashes."""
import importlib.util
from pathlib import Path
import unittest

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


if __name__ == "__main__":
    unittest.main()
