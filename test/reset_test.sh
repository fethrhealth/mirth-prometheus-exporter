#!/usr/bin/env bash
# S0-01 slot 3, the negative check: a Mirth statistics clear must never produce a
# negative rate in any derived series.
#
# Mirth's message totals reset to zero when an operator clears statistics. If the
# exporter published them as gauges, every rate() over the reset would go
# negative and a queue-growth rule would read a drain that never happened. They
# are published as counters so Prometheus detects the reset instead.
#
#   test/reset_test.sh
#
# Environment: MIRTH_EXPORTER_URL, default http://localhost:9001
set -euo pipefail

exporter_url="${MIRTH_EXPORTER_URL:-http://localhost:9001}"
endpoint="${exporter_url%/}/metrics"

scrape="$(mktemp)"
trap 'rm -f "${scrape}"' EXIT

if ! curl --silent --show-error --fail --max-time 30 "${endpoint}" > "${scrape}"; then
    echo "FAIL: could not scrape ${endpoint}" >&2
    exit 1
fi

python3 -I - "${scrape}" <<'PY'
import re, sys

scrape_path = sys.argv[1]
text = open(scrape_path, encoding="utf-8").read()

# Totals that reset when statistics are cleared. Each must be a counter. Prometheus
# names a counter's family without the _total suffix and declares its TYPE with it,
# so each name is looked up as exposed (<name>_total) first, then as given.
MUST_BE_COUNTER = [
    "mirth_messages_received", "mirth_messages_sent",
    "mirth_messages_filtered", "mirth_messages_errored",
    "mirth_destination_messages_received", "mirth_destination_messages_sent",
    "mirth_destination_messages_filtered", "mirth_destination_messages_errored",
    "mirth_queue_age_lookup_errors",
]

declared = dict(re.findall(r"^# TYPE (\S+) (\S+)$", text, re.M))
failures = []

for metric in MUST_BE_COUNTER:
    exposed = metric + "_total" if metric + "_total" in declared else metric
    kind = declared.get(exposed)
    if kind is None:
        failures.append("%s is not exported at all — a reset-prone total that Prometheus "
                        "cannot see cannot be rate()d safely" % metric)
    elif kind != "counter":
        failures.append("%s is exported as %s, not counter. A statistics clear would make "
                        "rate() negative and a drain would be invented." % (metric, kind))

# No sample may be negative. The exporter derives nothing it cannot ground, so a
# negative anywhere means a subtraction it should not have made.
negative = []
for line in text.splitlines():
    if line.startswith("#") or not line.strip():
        continue
    m = re.match(r"^(mirth_\S*?)(?:\{[^}]*\})?\s+(-[0-9.eE+]+)\s*$", line)
    if m:
        negative.append("%s = %s" % (m.group(1), m.group(2)))

for line in failures:
    print("FAIL: " + line, file=sys.stderr)
for line in negative:
    print("FAIL negative sample: " + line, file=sys.stderr)

if failures or negative:
    print("\n%d type problem(s), %d negative sample(s)" % (len(failures), len(negative)),
          file=sys.stderr)
    sys.exit(1)

print("ok: all %d reset-prone totals are counters, and no mirth_* sample is negative"
      % len(MUST_BE_COUNTER))
PY
