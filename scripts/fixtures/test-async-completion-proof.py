"""Harness unit tests, not process-crash evidence."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("proof", Path(__file__).with_name("async-completion-proof.py"))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)
EVENT = "12345678-1234-4234-8234-123456789012"


class ProofTests(unittest.TestCase):
    def test_outage_cold_starts_both_enabled_services_and_never_restarts_for_recovery(self):
        pending = {"event_id": EVENT, "order_id": EVENT, "event_type": "InventoryReserved",
                   "schema_version": 1, "topic": "commerce.inventory.v1", "message_key": EVENT,
                   "payload": "{}", "created_at": "now", "delivered_at": None, "attempt_count": 0}
        failed = dict(pending, attempt_count=1, last_error_code="BROKER")
        deadline = proof.Deadline(60, "outage")
        with patch.object(proof, "compose") as command, patch.object(proof, "recreate") as recreate, \
                patch.object(proof, "event_row", return_value=failed), \
                patch.object(proof, "await_event", return_value=dict(failed, delivered_at="later")), \
                patch.object(proof, "terminal") as terminal, patch.object(proof, "check_effect"), \
                patch.object(proof, "assert_dns_absent"), \
                patch.object(proof, "wait_container_healthy") as wait_healthy, \
                patch.object(proof, "container_identity", return_value={"id": "stable", "restartCount": 0}), \
                patch.object(proof, "cold_http_work", return_value=(EVENT, pending)):
            terminal.side_effect = lambda *args: self.assertEqual(recreate.call_count, 2)
            proof.recover_outage(EVENT, pending, deadline)
            self.assertEqual(recreate.call_args_list[0].args, ("inventory", deadline))
            self.assertEqual(recreate.call_args_list[0].kwargs, {})
            self.assertEqual(recreate.call_args_list[1].args, ("order", deadline))
            self.assertEqual(recreate.call_args_list[1].kwargs, {})
            self.assertEqual(recreate.call_count, 2)
            self.assertEqual(command.call_args_list[0].args[:3], ("stop", "-t", "10"))
            self.assertEqual(command.call_args_list[1].args, ("start", "kafka"))
            wait_healthy.assert_called_once_with("kafka", deadline)

    def test_all_stock_rows_are_checked_not_only_requested_sku(self):
        proof.require_stock({"SKU-APPLE": 8, "SKU-BANANA": 5}, 8)
        for stock in ({"SKU-APPLE": 8}, {"SKU-APPLE": 8, "SKU-BANANA": 4},
                      {"SKU-APPLE": 7, "SKU-BANANA": 5}):
            with self.assertRaises(AssertionError):
                proof.require_stock(stock, 8)

    def test_container_health_wait_uses_inspection_without_compose_start_wait_flags(self):
        deadline = proof.Deadline(60, "broker health")
        states = [
            json.dumps({"Running": True, "Health": {"Status": "starting"}}),
            json.dumps({"Running": True, "Health": {"Status": "healthy"}}),
        ]
        with patch.object(proof, "compose", return_value="container-id") as compose, \
                patch.object(proof, "docker", side_effect=states) as inspect, \
                patch.object(deadline, "sleep") as sleep:
            state = proof.wait_container_healthy("kafka", deadline)
        self.assertEqual(state["Health"]["Status"], "healthy")
        self.assertEqual(compose.call_count, 2)
        self.assertEqual(inspect.call_count, 2)
        sleep.assert_called_once_with()
        self.assertTrue(all(call.kwargs["deadline"] is deadline for call in compose.call_args_list))
        self.assertTrue(all(call.kwargs["deadline"] is deadline for call in inspect.call_args_list))

    def test_final_totals_reject_extra_or_missing_business_effects(self):
        expected = {"orders": 6, "placed": 6, "orderInbox": 6, "accepted": 6,
                    "inventoryInbox": 6, "attempts": 7, "reservations": 6, "lines": 6, "results": 6}
        proof.require_totals(expected)
        for key in expected:
            with self.assertRaises(AssertionError):
                proof.require_totals(dict(expected, **{key: expected[key] + 1}))

    def test_marker_requires_exact_identity_and_outcome(self):
        marker = f"DB_COMMIT_BOUNDARY eventId={EVENT} outcome=APPLIED"
        self.assertTrue(proof.boundary(marker, EVENT))
        for text in (marker + "extra", "prefix " + marker, marker.replace(EVENT, "other"),
                     marker.replace("APPLIED", "DUPLICATE")):
            self.assertFalse(proof.boundary(text, EVENT))

    def test_offsets_parse_only_selected_group_topic_partition(self):
        output = "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\n" \
                 "g topic 0 12 13 1\ng topic 1 - 4 -\nother topic 0 99 99 0"
        self.assertEqual(proof.parse_offset(output, "g", "topic", 0), 12)
        self.assertIsNone(proof.parse_offset(output, "g", "topic", 1))
        with self.assertRaises(AssertionError):
            proof.parse_offset(output, "g", "topic", 2)
        with self.assertRaises(ValueError):
            proof.parse_offset("g topic 0 garbage 1 1", "g", "topic", 0)

    def test_duplicate_evidence_requires_same_identity_and_distinct_offsets(self):
        record = {"partition": 1, "offset": 4, "key": "k", "value": json.dumps({"eventId": EVENT})}
        proof.require_duplicates([record, dict(record, offset=5)])
        for records in ([record, record], [record, dict(record, key="other", offset=5)],
                        [record, dict(record, value="{}", offset=5)]):
            with self.assertRaises(AssertionError):
                proof.require_duplicates(records)

    def test_group_offset_waits_for_selected_row_including_explicit_uncommitted(self):
        group, topic = proof.GROUPS["inventory"], proof.TOPICS["inventory"]
        for text, expected in (("12", 12), ("-", None)):
            with self.subTest(offset=text), \
                    patch.object(proof, "compose", side_effect=[
                        "Warning: Consumer group is rebalancing.\n",
                        f"{group} {topic} 0 {text} 13 1"]) as command, \
                    patch.object(proof.Deadline, "sleep"):
                deadline = proof.Deadline(60, "offset row")
                self.assertEqual(proof.group_offset("inventory", {"partition": 0}, deadline), expected)
                self.assertEqual(command.call_count, 2)
                self.assertTrue(all(call.kwargs["deadline"] is deadline for call in command.call_args_list))

    def test_missing_offset_row_exhausts_deadline_instead_of_proving_no_commit(self):
        deadline = proof.Deadline(60, "offset row")
        with patch.object(proof, "compose", return_value=""), \
                patch.object(deadline, "sleep", side_effect=TimeoutError("offset row deadline")):
            with self.assertRaises(TimeoutError):
                proof.group_offset("inventory", {"partition": 0}, deadline)

    def test_group_offset_does_not_retry_ambiguous_or_malformed_selected_rows(self):
        group, topic = proof.GROUPS["inventory"], proof.TOPICS["inventory"]
        row = f"{group} {topic} 0 12 13 1"
        for text, error in ((row + "\n" + row, AssertionError),
                            (f"{group} {topic} 0 garbage 13 1", ValueError)):
            with self.subTest(output=text), patch.object(proof, "compose", return_value=text), \
                    patch.object(proof.Deadline, "sleep") as sleep:
                with self.assertRaises(error):
                    proof.group_offset("inventory", {"partition": 0}, proof.Deadline(60, "offset row"))
                sleep.assert_not_called()

    def test_nested_commands_cannot_reset_aggregate_deadline(self):
        deadline = proof.Deadline(60, "aggregate")
        with patch.object(proof.runtime.time, "monotonic", return_value=deadline.end - 0.25), \
                patch.object(proof.runtime.subprocess, "check_output", return_value="ok") as command:
            proof.compose("ps", deadline=deadline, timeout=120)
            self.assertLessEqual(command.call_args.kwargs["timeout"], 0.25)
        with patch.object(proof.runtime.time, "monotonic", return_value=deadline.end + 1), \
                patch.object(proof.runtime.subprocess, "check_output") as command:
            with self.assertRaises(TimeoutError):
                proof.compose("ps", deadline=deadline)
            command.assert_not_called()

    def test_sigkill_requires_stopped_exit137(self):
        proof.require_killed({"Running": False, "ExitCode": 137})
        for state in ({"Running": True, "ExitCode": 137}, {"Running": False, "ExitCode": 0}):
            with self.assertRaises(AssertionError):
                proof.require_killed(state)

    def test_normal_configuration_rejects_even_disabled_proof_settings(self):
        proof.require_normal(["ORDER_EVENTS_ENABLED=true"])
        for setting in ("CONSUMER_PROOF_ENABLED=false", "CONSUMER_PROOF_EVENT_ID=",
                        "SPRING_PROFILES_ACTIVE=default", "ORDER_OUTBOX_PROOF_ENABLED=false"):
            with self.assertRaises(AssertionError):
                proof.require_normal([setting])


if __name__ == "__main__":
    unittest.main()
