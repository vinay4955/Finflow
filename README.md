# FinFlow — Kafka Docker Stack

**Group 1 · AIDA2 — Data Platform Architectures**
**Platform:** Apache Kafka 3-broker KRaft cluster + Kafka UI

---

## Stack Overview

FinFlow processes 10M payment transactions/day for 340 European banks. This Docker stack runs the core real-time event backbone: a **3-broker Kafka cluster in KRaft mode** (no ZooKeeper) with Kafka UI for live inspection.

| Service     | Image                             | Port  | Purpose                          |
|-------------|-----------------------------------|-------|----------------------------------|
| `broker-1`  | `confluentinc/cp-kafka:7.6.0`    | 9092  | KRaft broker + controller node 1 |
| `broker-2`  | `confluentinc/cp-kafka:7.6.0`    | 9094  | KRaft broker + controller node 2 |
| `broker-3`  | `confluentinc/cp-kafka:7.6.0`    | 9095  | KRaft broker + controller node 3 |
| `kafka-ui`  | `provectuslabs/kafka-ui:v0.7.2`  | 8080  | Web UI — topics, messages, brokers |

**Key config:**
- Replication Factor: **3** — every topic replicated across all brokers
- Min In-Sync Replicas: **2** — cluster stays writable with 1 broker down
- KRaft quorum: `1@broker-1:9093,2@broker-2:9093,3@broker-3:9093`
- Resource limits: **1 CPU / 1.5 GB per broker**, 0.5 CPU / 1 GB for the UI
- Healthchecks on **all four** services; `kafka-ui` starts only once all 3 brokers are healthy

---

## Prerequisites

- Docker running (Docker Desktop, or Colima on macOS)
- Ports **9092, 9094, 9095, 8080** free
- **~5.5 GB of memory available to Docker.** Three JVM brokers at 1.5 GB each plus the UI
  will not fit in a 2 GB VM. On Colima: `colima start --cpu 4 --memory 8`

---

## Start the Stack

Commands are given for **macOS / Linux (zsh, bash)** and **Windows CMD**. They are identical
apart from the comment syntax, because everything runs inside containers.

**Step 1 — Stop any old stack first**
```bash
docker-compose down -v
```

**Step 2 — Pull all images**
```bash
docker-compose pull
```

**Step 3 — Start the full 3-broker cluster**
```bash
docker-compose up -d
```

**Step 4 — Check all 4 services are healthy**
```bash
docker-compose ps
```
All four containers should show `(healthy)`. Compose waits for the three brokers before it
starts `kafka-ui`, so if the UI is up at all, the KRaft quorum has already formed.
Cold start is roughly **20–30 seconds** on a warm image cache.

**Step 5 — Seed demo data (macOS / Linux)**
```bash
./seed-payments.sh
```
Windows CMD has no bash; either run it from Git Bash / WSL, or produce manually using the
keyed command in the *Demo Commands* section below.

**Step 6 — Open Kafka UI**
```
http://localhost:8080
```
You should see the **finflow-kafka** cluster with **3 brokers online** and the
`payment-events` topic holding **18 messages, 6 per partition**.

---

## Demo Commands

Three distinct domain operations, in the order to present them.

### Verify all 3 brokers are reachable
```bash
docker exec broker-1 kafka-topics --list --bootstrap-server localhost:29092
```

### Inspect the KRaft quorum
```bash
docker exec broker-1 kafka-metadata-quorum --bootstrap-server localhost:29092 describe --status
```
`CurrentVoters: [1,2,3]` confirms all three nodes are controllers — no ZooKeeper anywhere.

### Create the payment-events topic (3 partitions, RF=3)
```bash
docker exec broker-1 kafka-topics --create --topic payment-events --bootstrap-server localhost:29092 --partitions 3 --replication-factor 3
```

### Describe the topic — partition distribution across brokers
```bash
docker exec broker-1 kafka-topics --describe --topic payment-events --bootstrap-server localhost:29092
```
Each partition has a different **Leader**, and **Replicas** spread across brokers 1, 2, 3:
```
Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
Partition: 1   Leader: 2   Replicas: 2,3,1   Isr: 2,3,1
Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
```

### Produce payment messages — **with a key**

> **The key matters.** Kafka picks the partition by hashing the message key. Producing
> without a key sends every message to a single partition and disproves the partitioning
> design described below. Always pass `parse.key=true`.

```bash
docker exec -it broker-1 kafka-console-producer --bootstrap-server localhost:29092 --topic payment-events --property "parse.key=true" --property "key.separator=|"
```
Paste these (format is `card_token|json`), then `Ctrl+C`:
```
tok_visa_4242|{"transaction_id":"TXN-001","card_token":"tok_visa_4242","merchant_id":"MCH-Berlin-001","amount":149.99,"currency":"EUR","country":"DE","mcc_code":"5411"}
tok_mc_5555|{"transaction_id":"TXN-002","card_token":"tok_mc_5555","merchant_id":"MCH-Amsterdam-007","amount":89.00,"currency":"EUR","country":"NL","mcc_code":"7011"}
tok_visa_4242|{"transaction_id":"TXN-003","card_token":"tok_visa_4242","merchant_id":"MCH-Bucharest-023","amount":2500.00,"currency":"EUR","country":"RO","mcc_code":"5411"}
tok_amex_3782|{"transaction_id":"TXN-004","card_token":"tok_amex_3782","merchant_id":"MCH-Paris-044","amount":12.50,"currency":"EUR","country":"FR","mcc_code":"5812"}
tok_mc_5555|{"transaction_id":"TXN-005","card_token":"tok_mc_5555","merchant_id":"MCH-Berlin-001","amount":340.00,"currency":"EUR","country":"DE","mcc_code":"5311"}
```

### Consume and read messages — showing key and partition
```bash
docker exec broker-1 kafka-console-consumer --bootstrap-server localhost:29092 --topic payment-events --from-beginning --max-messages 18 --property print.key=true --property print.partition=true
```
Every transaction for one `card_token` appears on the **same partition** — that is the
ordering guarantee the payments domain needs.

---

## Failover Demo — proving RF=3 / min-ISR=2

This is the demo that proves the availability claim rather than just asserting it. It takes
about 60 seconds.

**1. Show the healthy baseline — ISR lists all three replicas**
```bash
docker exec broker-1 kafka-topics --describe --topic payment-events --bootstrap-server localhost:29092
```

**2. Kill a broker**
```bash
docker-compose stop broker-2
```

**3. Show the cluster reacting** (wait ~10s)
```bash
docker exec broker-1 kafka-topics --describe --topic payment-events --bootstrap-server localhost:29092
```
Two things happen: **ISR shrinks from 3 to 2**, and the partition that broker-2 was leading
**elects a new leader**:
```
Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,3
Partition: 1   Leader: 3   Replicas: 2,3,1   Isr: 3,1      <-- leader moved 2 -> 3
Partition: 2   Leader: 3   Replicas: 3,1,2   Isr: 3,1
```

**4. Prove the cluster is still writable** — `acks=all` requires `min.insync.replicas=2`,
and 2 replicas are still alive, so the write is accepted:
```bash
echo 'tok_visa_4242|{"transaction_id":"TXN-999","card_token":"tok_visa_4242","amount":500.00,"currency":"EUR","note":"written during broker-2 outage"}' | docker exec -i broker-1 kafka-console-producer --bootstrap-server localhost:29092 --topic payment-events --property "parse.key=true" --property "key.separator=|" --producer-property acks=all
```

**5. Bring the broker back and watch it heal**
```bash
docker-compose start broker-2
sleep 10
docker exec broker-1 kafka-topics --describe --topic payment-events --bootstrap-server localhost:29092
```
ISR returns to all three replicas within a few seconds, and no messages were lost.

> **Expect this question:** *"Why didn't partition 1's leadership move back to broker-2?"*
> Preferred-leader rebalancing is not immediate — `auto.leader.rebalance.enable` is on by
> default but only runs every 5 minutes. You can force it with
> `kafka-leader-election --election-type PREFERRED --all-topic-partitions`.

---

## Useful Commands

```bash
docker-compose ps                                        # container + health status
docker-compose logs -f broker-1 broker-2 broker-3        # live logs from all brokers
docker stats --no-stream                                 # verify resource limits in effect
docker-compose down                                      # stop; data kept in named volumes
docker-compose down -v                                   # stop and wipe all data
```

Windows CMD uses `REM` for comments instead of `#`; the commands themselves are unchanged.

---

## Architecture Decisions

### Why 3 Brokers?
FinFlow requires **99.99% availability** — planned maintenance must not interrupt payment processing. With RF=3 and min ISR=2, the cluster continues to accept writes even when 1 broker is completely down.

| RF | Min ISR | Can survive | Write available |
|----|---------|-------------|-----------------|
| 1  | 1       | 0 failures  | Only if leader alive |
| 3  | 2       | 1 failure   | Yes (2 replicas confirm) |

Demonstrated live in the *Failover Demo* above.

### Why KRaft (no ZooKeeper)?
Kafka 3.x+ manages its own metadata via the **KRaft** consensus protocol (Raft-based). Each broker acts as both broker and controller. This eliminates ZooKeeper as a dependency — fewer containers, faster failover, simpler ops.

### Why Two Listeners Per Broker?
Each broker exposes:
- `PLAINTEXT_INTERNAL` on port 29092 — used by kafka-ui and other brokers (docker bridge network, resolved by container name)
- `PLAINTEXT_EXTERNAL` on ports 9092/9094/9095 — used by your terminal on the host machine
- `CONTROLLER` on port 9093 — the KRaft metadata quorum, never exposed to the host

A single listener cannot serve both: `broker-1:29092` is unresolvable from your laptop, and `localhost:9092` means the wrong machine inside a container.

### Why Partitions = 3?
3 partitions across 3 brokers means **each broker leads exactly 1 partition**. Kafka selects the partition as `murmur2(key) % partitions`, so messages with the same `card_token` always land on the same partition — one card's transaction history stays strictly ordered, while different cards process in parallel across all three brokers.

The seed data uses six card tokens chosen to spread **evenly, 2 tokens and 6 messages per partition**, so the balance is visible in the UI rather than just claimed.

### Why Resource Limits?
Each broker is capped at **1 CPU / 1.5 GB** and the UI at **0.5 CPU / 1 GB**. Without limits, one container can starve the others on a laptop — the classic cause of a demo dying mid-presentation. The JVM heap is pinned to `-Xmx1G -Xms1G`: equal min and max means the heap never resizes, so there is no resize GC pause, and 1 GB of heap inside a 1.5 GB container leaves headroom for metaspace, direct buffers and thread stacks.

### Why Healthchecks?
`depends_on` alone only waits for a container to *start*, not to be *ready*. Each broker's healthcheck runs `kafka-topics --list`, and `kafka-ui` uses `condition: service_healthy` on all three — so the UI can never come up pointing at a cluster that has not formed a quorum. The UI's own healthcheck hits Spring Boot's `/actuator/health` with `wget` (the image ships no `curl`).

---

## Configuration

All host-facing settings live in `.env`. Every variable there is referenced by
`docker-compose.yml` — there are no decorative entries.

```
CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
BROKER_1_PORT=9092
BROKER_2_PORT=9094
BROKER_3_PORT=9095
KAFKA_UI_PORT=8080
```

Changing a `BROKER_n_PORT` updates the published port, the listener and the advertised
address together, so a port conflict is a one-line fix.

**Broker node IDs are deliberately not configurable.** They are structural — each broker
needs a fixed identity and the KRaft quorum string hardcodes all three. A node ID is not a
setting.

`.env` is gitignored; `.env.example` is the committed template.

---

## References

- Confluent Platform Docker Images → https://hub.docker.com/r/confluentinc/cp-kafka
- Kafka UI → https://github.com/provectus/kafka-ui
- KRaft Mode → https://docs.confluent.io/platform/current/kafka/kraft.html
- Kleppmann (2017), *Designing Data-Intensive Applications*, Chapter 5: Replication
