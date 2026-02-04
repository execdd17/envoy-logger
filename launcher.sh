#!/usr/bin/env bash
set -eou pipefail
cd "$(dirname "${0}")"

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

exec poetry run python3 -m envoy_logger "${@}"
