# FinFlow — Kafka Docker Stack

**Group 1 · AIDA2 — Data Platform Architectures**
**Platform:** Apache Kafka 3-broker KRaft cluster + Kafka UI
**Lecture 5 Presentation Stack**

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

---

## Prerequisites

- Docker Desktop installed and running
- Ports **9092, 9094, 9095, 8080** free on your machine

---

## Start the Stack (Windows CMD)

**Step 1 — Stop any old single-broker stack first**
```cmd
docker-compose down -v
```

**Step 2 — Pull all images**
```cmd
docker-compose pull
```

**Step 3 — Start the full 3-broker cluster**
```cmd
docker-compose up -d
```

**Step 4 — Wait ~45 seconds, then check all 3 brokers are healthy**
```cmd
docker-compose ps
```
All 4 containers should show `running`. The 3 brokers should show `(healthy)`.

**Step 5 — Open Kafka UI**
```
http://localhost:8080
```
You should see **finflow-kafka** cluster with **3 brokers online**.

---

## Demo Commands (Windows CMD — Single Line)

### Verify all 3 brokers are reachable
```cmd
docker exec broker-1 kafka-topics --list --bootstrap-server localhost:29092
```

### Create the payment-events topic (3 partitions, RF=3)
```cmd
docker exec broker-1 kafka-topics --create --topic payment-events --bootstrap-server localhost:29092 --partitions 3 --replication-factor 3
```

### Describe the topic — shows partition distribution across brokers
```cmd
docker exec broker-1 kafka-topics --describe --topic payment-events --bootstrap-server localhost:29092
```
This shows each partition's **Leader broker** and **Replicas** spread across brokers 1, 2, 3.

### Produce payment messages (type each line, press Enter, Ctrl+C to stop)
```cmd
docker exec -it broker-1 kafka-console-producer --broker-list localhost:29092 --topic payment-events
```
Paste these one by one:
```
{"transaction_id":"TXN-001","card_token":"tok_visa_4242","merchant_id":"MCH-Berlin-001","amount":149.99,"currency":"EUR","country":"DE","mcc_code":"5411"}
{"transaction_id":"TXN-002","card_token":"tok_mc_5555","merchant_id":"MCH-Amsterdam-007","amount":89.00,"currency":"EUR","country":"NL","mcc_code":"7011"}
{"transaction_id":"TXN-003","card_token":"tok_visa_4242","merchant_id":"MCH-Bucharest-023","amount":2500.00,"currency":"EUR","country":"RO","mcc_code":"5411"}
{"transaction_id":"TXN-004","card_token":"tok_amex_3782","merchant_id":"MCH-Paris-044","amount":12.50,"currency":"EUR","country":"FR","mcc_code":"5812"}
{"transaction_id":"TXN-005","card_token":"tok_mc_5555","merchant_id":"MCH-Berlin-001","amount":340.00,"currency":"EUR","country":"DE","mcc_code":"5311"}
```
Press `Ctrl+C` to exit.

### Consume and read messages
```cmd
docker exec broker-1 kafka-console-consumer --bootstrap-server localhost:29092 --topic payment-events --from-beginning --max-messages 5
```

---

## Useful Commands

```cmd
REM Check running containers
docker-compose ps

REM Live logs from all brokers
docker-compose logs -f broker-1 broker-2 broker-3

REM Stop stack — data preserved in named volumes
docker-compose down

REM Stop and wipe all data
docker-compose down -v
```

---

## Architecture Decisions

### Why 3 Brokers?
FinFlow requires **99.99% availability** — planned maintenance must not interrupt payment processing. With RF=3 and min ISR=2, the cluster continues to accept writes even when 1 broker is completely down.

| RF | Min ISR | Can survive | Write available |
|----|---------|-------------|-----------------|
| 1  | 1       | 0 failures  | Only if leader alive |
| 3  | 2       | 1 failure   | Yes (2 replicas confirm) |

### Why KRaft (no ZooKeeper)?
Kafka 3.x+ manages its own metadata via the **KRaft** consensus protocol (Raft-based). Each broker acts as both broker and controller. This eliminates ZooKeeper as a dependency — fewer containers, faster failover, simpler ops.

### Why Two Listeners Per Broker?
Each broker exposes:
- `PLAINTEXT_INTERNAL` on port 29092 — used by kafka-ui and other brokers (docker bridge network, resolved by container name)
- `PLAINTEXT_EXTERNAL` on ports 9092/9094/9095 — used by your terminal on the host machine

### Why Partitions = 3?
3 partitions across 3 brokers means **each broker leads exactly 1 partition**. Messages with the same `card_token` go to the same partition (same key → same partition), keeping one card's transaction history ordered while parallelising across cards.

---

## Configuration

Edit `.env` to change ports if there are conflicts:
```
CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk
KAFKA_UI_PORT=8080
```

---

## References

- Confluent Platform Docker Images → https://hub.docker.com/r/confluentinc/cp-kafka
- Kafka UI → https://github.com/provectus/kafka-ui
- KRaft Mode → https://docs.confluent.io/platform/current/kafka/kraft.html
- Kleppmann (2017), *Designing Data-Intensive Applications*, Chapter 5: Replication
