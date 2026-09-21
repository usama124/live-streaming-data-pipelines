from __future__ import annotations

"""Render a per-pipeline Telegraf config from a PipelineConfig.

Replaces the env-var soup the old producer container was launched with. One
rendered config per pipeline; the template lives in telegraf/templates/.

Quoting note: every substituted value goes through _toml(), which uses
json.dumps. TOML basic strings and JSON strings share escaping rules for
everything that appears here, so this correctly handles the case that actually
breaks — a JSON node-name map nested inside a TOML string.
"""

import json
from pathlib import Path
from string import Template
from typing import Any

from common.app_common.models import PipelineConfig

def _template_dirs() -> list[Path]:
    """Where to look for templates: the image path first, then a source checkout.

    Resolved lazily and defensively — this module sits four directories below the
    repo root in a checkout but only two below /app in the image, so indexing
    .parents at import time crashed the container on startup.
    """
    dirs = [Path("/opt/telegraf/templates")]
    here = Path(__file__).resolve()
    dirs += [
        parent / "telegraf" / "templates"
        for parent in here.parents
        if (parent / "telegraf" / "templates").is_dir()
    ]
    return dirs

CONNECTOR_DIR = "/opt/connectors"
METRICS_PORT = 9273  # Telegraf's conventional Prometheus port
# Go reference-time layout for RFC3339, which is what the connector emits.
_TIME_FORMAT = "2006-01-02T15:04:05.999999999Z07:00"


def _toml(value: Any) -> str:
    """Quote a Python value as a TOML scalar or array."""
    if isinstance(value, list):
        return "[" + ", ".join(_toml(v) for v in value) + "]"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value))


def _template(source_type: str) -> Template:
    searched = _template_dirs()
    for directory in searched:
        path = directory / f"{source_type}.conf.tmpl"
        if path.is_file():
            return Template(path.read_text())
    raise FileNotFoundError(
        f"no Telegraf template for source_type={source_type!r} "
        f"(looked in {[str(d) for d in searched]})"
    )


def _opcua_environment(config: PipelineConfig) -> list[str]:
    opts = config.source_options
    env = {
        "PIPELINE_ID": config.pipeline_id,
        "OPCUA_ENDPOINT": opts.get("endpoint", ""),
        "OPCUA_NODE_IDS": ",".join(opts.get("node_ids", [])),
        "OPCUA_NODE_NAMES_JSON": json.dumps(opts.get("node_names", {})),
        "OPCUA_PUBLISHING_INTERVAL_MS": str(opts.get("publishing_interval_ms", 500)),
    }
    if "connect_timeout_s" in opts:
        env["OPCUA_CONNECT_TIMEOUT_S"] = str(opts["connect_timeout_s"])
    return [f"{k}={v}" for k, v in env.items()]


def render_telegraf_config(
    config: PipelineConfig,
    *,
    kafka_brokers: str,
    connector_dir: str = CONNECTOR_DIR,
    restart_delay: str = "10s",
) -> str:
    """Return the full Telegraf config text for one pipeline."""
    if config.source_type != "opcua":
        raise ValueError(
            f"no Telegraf template for source_type={config.source_type!r}. "
            "MQTT uses Telegraf's native input; see docs/ONBOARDING.md §5."
        )

    command = ["python3", f"{connector_dir}/opcua/connector.py"]

    return _template(config.source_type).substitute(
        interval="10s",
        flush_interval="5s",
        metric_batch_size=config.batch_size,
        metric_buffer_limit=max(config.batch_size * 20, 10_000),
        command=_toml(command),
        environment=_toml(_opcua_environment(config)),
        restart_delay=restart_delay,
        time_format=_TIME_FORMAT,
        measurement=config.topic,
        brokers=_toml([b.strip() for b in kafka_brokers.split(",") if b.strip()]),
        topic=_toml(config.topic),
        METRICS_PORT=METRICS_PORT,
    )
