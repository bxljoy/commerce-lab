"""Deployment regression guard; accepts `docker compose config --format json`."""
import json
import sys

config = json.load(sys.stdin)
services = config["services"]
for name in ("order-service", "inventory-service"):
    assert "service-network" in services[name]["networks"], name
for name in ("postgres", "inventory-postgres"):
    assert "service-network" not in services[name]["networks"], name
assert set(services["order-service"]["depends_on"]) == {"postgres"}
env = services["order-service"]["environment"]
assert env["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:9092"
assert not any(key.startswith(("INVENTORY_", "ORDER_RECOVERY_")) for key in env)
kafka = services["kafka"]
assert kafka["image"] == "apache/kafka:3.7.1"
assert set(kafka["networks"]) == {"service-network"}
assert not kafka.get("ports")
expected = {
    "KAFKA_PROCESS_ROLES": "broker,controller", "KAFKA_NODE_ID": "1",
    "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
    "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092",
    "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@kafka:9093",
    "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    "KAFKA_INTER_BROKER_LISTENER_NAME": "PLAINTEXT",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1", "KAFKA_MIN_INSYNC_REPLICAS": "1",
    "KAFKA_DEFAULT_REPLICATION_FACTOR": "1", "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
    "KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",
}
assert all(str(kafka["environment"].get(k)) == v for k, v in expected.items()), kafka["environment"]
assert any(v["source"] == "kafka-data" and v["target"] == expected["KAFKA_LOG_DIRS"]
           for v in kafka["volumes"])
assert "kafka-data" in config["volumes"]
assert 0 < kafka["healthcheck"]["retries"] <= 20
assert kafka["healthcheck"]["timeout"] and kafka["healthcheck"]["interval"]
init = services["topic-init"]
assert set(init["networks"]) == {"service-network"} and not init.get("ports")
assert init["depends_on"]["kafka"]["condition"] == "service_healthy"
command = " ".join(init["command"])
for part in ("--bootstrap-server kafka:9092", "--partitions 3", "--replication-factor 1",
             "--config retention.ms=604800000", "--config cleanup.policy=delete", "commerce.orders.v1"):
    assert part in command, part
if "--proof" in sys.argv:
    assert services["order-service"]["restart"] == "no"
    assert env["SPRING_PROFILES_ACTIVE"] in ("default", "outbox-proof")
    assert "ORDER_OUTBOX_PROOF_ENABLED" in env and "ORDER_OUTBOX_PROOF_EVENT_ID" in env
    for name in ("postgres", "inventory-postgres", "order-service", "inventory-service"):
        assert all(p["host_ip"] == "127.0.0.1" and str(p["published"]) == "0" for p in services[name]["ports"])
print("Compose Kafka topology, durability, topic contract and independent order startup: PASS")
