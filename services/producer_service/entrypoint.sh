#!/bin/sh
# The per-pipeline config is rendered by task_manager and passed in as
# TELEGRAF_CONFIG. Passing the text avoids bind-mounting a file: task_manager
# talks to the host Docker daemon, so any host path it names would have to exist
# on the host rather than in its own container.
set -e

if [ -n "$TELEGRAF_CONFIG" ]; then
    printf '%s' "$TELEGRAF_CONFIG" > /etc/telegraf/telegraf.conf
fi

exec telegraf --config /etc/telegraf/telegraf.conf "$@"
