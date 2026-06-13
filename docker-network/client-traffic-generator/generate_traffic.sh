#!/bin/sh

set -eu

API_URL="${API_URL:-http://api-service:8080}"

echo "[Traffic generator] API: $API_URL"

while true; do
  curl -s "$API_URL/" > /dev/null
  curl -s "$API_URL/health" > /dev/null
  curl -s "$API_URL/api/data?id=$(date +%s)" > /dev/null
  curl -s "$API_URL/api/random-db" > /dev/null

  curl -s -X POST "$API_URL/api/events" \
    -H "Content-Type: application/json" \
    -d "{\"eventType\":\"client_event\",\"source\":\"client-traffic-generator\",\"message\":\"normal traffic\"}" \
    > /dev/null

  sleep 0.2
done