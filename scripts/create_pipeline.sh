#!/usr/bin/env bash
set -euo pipefail

PIPELINE_ID="${1:-opcua-line-1}"

curl -X POST "http://localhost:8000/pipelines" \
  -H "Content-Type: application/json" \
  -d "{
    \"pipeline_id\": \"${PIPELINE_ID}\",
    \"pipeline_type\": \"live\",
    \"source_type\": \"mock\",
    \"topic\": \"pipeline.${PIPELINE_ID}.events\",
    \"batch_size\": 500,
    \"flush_interval_seconds\": 60,
    \"source_options\": {
      \"min_delay_ms\": 100,
      \"max_delay_ms\": 500
    }
  }"
