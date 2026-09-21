# Producer = Telegraf + our thin connector, per docs/ARCHITECTURE.md §3.1.
# Telegraf owns batching, the Kafka producer, retry/backoff and metrics; the
# connector only speaks OPC UA and writes JSON lines to stdout.
#
# Build from the project root:
#   docker build -f services/producer_service/Dockerfile -t data-platform-producer:latest .
FROM telegraf:1.35

# Upstream's image ships no LICENSE file, so this derived image carries the
# notice MIT requires. Do not drop it — see THIRD-PARTY-NOTICES.md.
COPY THIRD-PARTY-NOTICES.md /usr/share/doc/THIRD-PARTY-NOTICES.md

# python3 for the connector; a venv because Debian's python is externally managed.
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv \
 && python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY services/producer_service/requirements.txt /tmp/requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

# Puts the venv's python3 first, so the rendered config's plain "python3" command
# resolves to the interpreter that has asyncua.
ENV PATH=/opt/venv/bin:$PATH

COPY connectors /opt/connectors
COPY telegraf/templates /opt/telegraf/templates
COPY services/producer_service/entrypoint.sh /usr/local/bin/producer-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/producer-entrypoint.sh"]
