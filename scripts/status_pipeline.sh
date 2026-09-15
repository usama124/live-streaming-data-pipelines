#!/usr/bin/env bash
set -euo pipefail
PIPELINE_ID="${1:-opcua-line-1}"
curl "http://localhost:8000/pipelines/${PIPELINE_ID}/status"
