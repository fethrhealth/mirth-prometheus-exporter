#!/usr/bin/env bash
# Serve fixtures/metrics-sample.txt at /metrics so the S0-01 checks can run
# without a Mirth or a running exporter. Prints the URL and blocks until killed.
#
#   test/serve-fixture.sh [port]
set -euo pipefail

port="${1:-9001}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
served="$(mktemp -d)"
trap 'rm -rf "${served}"' EXIT

cp "${here}/fixtures/metrics-sample.txt" "${served}/metrics"
echo "serving the fixture at http://localhost:${port}/metrics"
echo "  MIRTH_EXPORTER_URL=http://localhost:${port}"
exec python3 -m http.server "${port}" --directory "${served}" --bind 127.0.0.1
