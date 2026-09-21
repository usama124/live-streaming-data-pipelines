"""Registry #2 — connector emits well-formed records against a healthy source."""

from __future__ import annotations

import subprocess
import sys

from tests.phase1.conftest import REPO, connector_env, read_json_lines

REQUIRED_KEYS = {"pipeline_id", "event_time", "sequence", "source", "sensor", "value", "quality"}


def test_connector_emits_well_formed_json_lines(mock_server) -> None:
    proc = subprocess.Popen(
        [sys.executable, "-m", "connectors.opcua.connector"],
        cwd=REPO,
        env=connector_env(mock_server.endpoint, pipeline_id="pipe-emit"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        records = read_json_lines(proc, 5)
    finally:
        proc.terminate()

    for record in records:
        assert REQUIRED_KEYS <= record.keys(), f"missing keys: {REQUIRED_KEYS - record.keys()}"
        assert record["pipeline_id"] == "pipe-emit"
        assert record["source"] == "opcua"
        assert record["quality"] in {"good", "uncertain", "bad"}
        # node_names mapping must be applied — raw NodeIDs are not useful downstream
        assert record["sensor"] in {"temperature", "pressure", "vibration", "flowrate", "machinestatus"}

    # sequence must actually increment, not be a constant
    sequences = [r["sequence"] for r in records]
    assert sequences == sorted(sequences) and len(set(sequences)) == len(sequences), sequences
