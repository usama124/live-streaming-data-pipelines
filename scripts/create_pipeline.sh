#!/usr/bin/env bash
# Create a live pipeline reading the mock OPC UA server (docker compose --profile stratahub).
set -euo pipefail

PIPELINE_ID="${1:-opcua-line-1}"

curl -X POST "http://localhost:8000/pipelines" \
  -H "Content-Type: application/json" \
  -d "{
    \"pipeline_id\": \"${PIPELINE_ID}\",
    \"pipeline_type\": \"live\",
    \"source_type\": \"opcua\",
    \"topic\": \"pipeline.${PIPELINE_ID}.events\",
    \"batch_size\": 500,
    \"flush_interval_seconds\": 60,
    \"source_options\": {
      \"endpoint\": \"opc.tcp://opcua-mock:4840/stratahub/server/\",
      \"node_ids\": [\"ns=2;i=2\", \"ns=2;i=3\", \"ns=2;i=4\", \"ns=2;i=5\", \"ns=2;i=6\"],
      \"node_names\": {
        \"ns=2;i=2\": \"Temperature\",
        \"ns=2;i=3\": \"Pressure\",
        \"ns=2;i=4\": \"Vibration\",
        \"ns=2;i=5\": \"FlowRate\",
        \"ns=2;i=6\": \"MachineStatus\"
      },
      \"publishing_interval_ms\": 500
    }
  }"
