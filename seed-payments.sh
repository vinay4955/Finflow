#!/usr/bin/env bash
#
# FinFlow — seed the payment-events topic with realistic transaction data.
#
# Why this script exists:
#   The messages are produced WITH A KEY (the card_token). That is the whole point.
#   Kafka hashes the key to pick a partition, so every transaction for a given card
#   lands on the same partition and stays in order relative to that card's history,
#   while different cards spread across all 3 brokers in parallel.
#
#   Producing without a key (the naive `kafka-console-producer --topic X`) sends
#   everything to ONE partition and quietly disproves the architecture you are
#   presenting. Run this, then open the Messages tab and show the Key column.
#
# Usage:  ./seed-payments.sh [topic]
set -euo pipefail

TOPIC="${1:-payment-events}"
BROKER="${BROKER:-broker-1}"
BOOTSTRAP="localhost:29092"

if ! docker exec "$BROKER" true 2>/dev/null; then
  echo "error: container '$BROKER' is not running. Start the stack first:" >&2
  echo "       docker-compose up -d" >&2
  exit 1
fi

# Create the topic if it is not there yet (idempotent — safe to re-run).
if ! docker exec "$BROKER" kafka-topics --list --bootstrap-server "$BOOTSTRAP" \
     | grep -qx "$TOPIC"; then
  echo "creating topic $TOPIC (3 partitions, RF=3)..."
  docker exec "$BROKER" kafka-topics --create \
    --topic "$TOPIC" \
    --bootstrap-server "$BOOTSTRAP" \
    --partitions 3 \
    --replication-factor 3
fi

echo "seeding $TOPIC with keyed payment events (key = card_token)..."

# Format:  <card_token>|<json payload>
# The key is everything before the first '|'.
docker exec -i "$BROKER" kafka-console-producer \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --property "parse.key=true" \
  --property "key.separator=|" <<'EOF'
tok_visa_4242|{"transaction_id":"TXN-001","card_token":"tok_visa_4242","merchant_id":"MCH-Berlin-001","amount":149.99,"currency":"EUR","country":"DE","mcc_code":"5411"}
tok_mc_5555|{"transaction_id":"TXN-002","card_token":"tok_mc_5555","merchant_id":"MCH-Amsterdam-007","amount":89.00,"currency":"EUR","country":"NL","mcc_code":"7011"}
tok_visa_4242|{"transaction_id":"TXN-003","card_token":"tok_visa_4242","merchant_id":"MCH-Bucharest-023","amount":2500.00,"currency":"EUR","country":"RO","mcc_code":"5411"}
tok_amex_3782|{"transaction_id":"TXN-004","card_token":"tok_amex_3782","merchant_id":"MCH-Paris-044","amount":12.50,"currency":"EUR","country":"FR","mcc_code":"5812"}
tok_mc_5555|{"transaction_id":"TXN-005","card_token":"tok_mc_5555","merchant_id":"MCH-Berlin-001","amount":340.00,"currency":"EUR","country":"DE","mcc_code":"5311"}
tok_visa_4242|{"transaction_id":"TXN-006","card_token":"tok_visa_4242","merchant_id":"MCH-Madrid-012","amount":75.20,"currency":"EUR","country":"ES","mcc_code":"5812"}
tok_mc_5555|{"transaction_id":"TXN-007","card_token":"tok_mc_5555","merchant_id":"MCH-Warsaw-055","amount":18.90,"currency":"EUR","country":"PL","mcc_code":"5814"}
tok_amex_3782|{"transaction_id":"TXN-008","card_token":"tok_amex_3782","merchant_id":"MCH-Vienna-002","amount":58.40,"currency":"EUR","country":"AT","mcc_code":"5411"}
tok_amex_3782|{"transaction_id":"TXN-009","card_token":"tok_amex_3782","merchant_id":"MCH-Paris-044","amount":210.00,"currency":"EUR","country":"FR","mcc_code":"5812"}
tok_visa_4111|{"transaction_id":"TXN-010","card_token":"tok_visa_4111","merchant_id":"MCH-Milan-088","amount":1899.00,"currency":"EUR","country":"IT","mcc_code":"5732"}
tok_visa_4111|{"transaction_id":"TXN-011","card_token":"tok_visa_4111","merchant_id":"MCH-Dublin-019","amount":430.75,"currency":"EUR","country":"IE","mcc_code":"7011"}
tok_visa_4111|{"transaction_id":"TXN-012","card_token":"tok_visa_4111","merchant_id":"MCH-Prague-064","amount":64.30,"currency":"EUR","country":"CZ","mcc_code":"5411"}
tok_visa_4556|{"transaction_id":"TXN-013","card_token":"tok_visa_4556","merchant_id":"MCH-Lisbon-031","amount":22.00,"currency":"EUR","country":"PT","mcc_code":"5814"}
tok_visa_4556|{"transaction_id":"TXN-014","card_token":"tok_visa_4556","merchant_id":"MCH-Athens-076","amount":312.00,"currency":"EUR","country":"GR","mcc_code":"4511"}
tok_visa_4556|{"transaction_id":"TXN-015","card_token":"tok_visa_4556","merchant_id":"MCH-Helsinki-090","amount":1150.00,"currency":"EUR","country":"FI","mcc_code":"5732"}
tok_mc_5454|{"transaction_id":"TXN-016","card_token":"tok_mc_5454","merchant_id":"MCH-Brussels-038","amount":880.00,"currency":"EUR","country":"BE","mcc_code":"5311"}
tok_mc_5454|{"transaction_id":"TXN-017","card_token":"tok_mc_5454","merchant_id":"MCH-Copenhagen-047","amount":47.60,"currency":"EUR","country":"DK","mcc_code":"5499"}
tok_mc_5454|{"transaction_id":"TXN-018","card_token":"tok_mc_5454","merchant_id":"MCH-Stockholm-061","amount":9.99,"currency":"EUR","country":"SE","mcc_code":"5968"}
EOF

echo
echo "done. Partition distribution by key:"
docker exec "$BROKER" kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" 2>/dev/null \
  | awk -F: '{printf "  partition %s: %s messages\n", $2, $3}'
