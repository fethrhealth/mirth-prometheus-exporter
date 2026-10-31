#!/usr/bin/env bash
# S0-01 slot 5. The bounded queue-age lookup must return a plausible age for a
# destination that has a known queue.
#
# §13 item 2 left this to be checked on the deployed server, and the backlog says
# halt if it fails there: queue age comes from the exporter's bounded background
# lookup (decision item 50), and without it the oldest-message-age rules in
# stage 1 have no source at all.
#
#   test/queue_age_probe.sh [--max-age 86400]
#
# Environment: MIRTH_EXPORTER_URL, default http://localhost:9001
set -euo pipefail

exporter_url="${MIRTH_EXPORTER_URL:-http://localhost:9001}"
max_age=86400

while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-age) max_age="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

endpoint="${exporter_url%/}/metrics"
scrape="$(mktemp)"
trap 'rm -f "${scrape}"' EXIT

if ! curl --silent --show-error --fail --max-time 30 "${endpoint}" > "${scrape}"; then
    echo "FAIL: could not scrape ${endpoint}" >&2
    exit 1
fi

python3 -I - "${scrape}" "${max_age}" <<'PY'
import re, sys

scrape_path, max_age = sys.argv[1], float(sys.argv[2])
text = open(scrape_path, encoding="utf-8").read()


def samples(metric):
    out = {}
    for m in re.finditer(r"^%s\{([^}]*)\}\s+([0-9.eE+-]+)\s*$" % re.escape(metric), text, re.M):
        labels = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', m.group(1)))
        key = (labels.get("channelName", "?"), labels.get("destination", labels.get("metaDataId", "?")))
        out[key] = float(m.group(2))
    return out


queued = samples("mirth_destination_messages_queued")
ages = samples("mirth_destination_oldest_queued_age_seconds")

if not queued:
    print("FAIL: no mirth_destination_messages_queued series — nothing to probe. The exporter "
          "reached no destination, so this says nothing about queue age.", file=sys.stderr)
    sys.exit(1)

with_queue = {k: v for k, v in queued.items() if v > 0}
print("  %d destination(s), %d with a non-empty queue, %d reporting an age"
      % (len(queued), len(with_queue), len(ages)))

if not with_queue:
    print("FAIL: every destination's queue is empty, so the bounded lookup was never exercised. "
          "This check cannot pass on an idle server — queue a message on a destination whose "
          "channel storage mode is DEVELOPMENT or PRODUCTION and run it again.", file=sys.stderr)
    sys.exit(1)

failures = []
for key, depth in sorted(with_queue.items()):
    label = "%s/%s" % key
    age = ages.get(key)
    if age is None:
        failures.append("%s has %d queued message(s) but reports no age. The lookup is bounded, "
                        "so absence means not-yet-observed — but on a destination with a standing "
                        "queue it means the lookup is not working (§13 item 2: halt)."
                        % (label, int(depth)))
    elif age <= 0:
        failures.append("%s has %d queued message(s) and reports age %.1f. Zero means an empty "
                        "queue; reporting it for a non-empty one is a fabricated value."
                        % (label, int(depth), age))
    elif age > max_age:
        failures.append("%s reports age %.0fs, beyond the %.0fs plausibility bound — more likely "
                        "a clock or parsing fault than a real queue." % (label, age, max_age))
    else:
        print("    %s: %d queued, oldest %.0fs" % (label, int(depth), age))

# _total because prometheus_client suffixes a counter's sample names; the bare
# spelling is accepted too so the note survives either exposition.
errors = re.search(r"^mirth_queue_age_lookup_errors(?:_total)?(?:\{[^}]*\})?\s+([0-9.eE+-]+)\s*$",
                   text, re.M)
if errors and float(errors.group(1)) > 0:
    print("  note: mirth_queue_age_lookup_errors is %s — the lookup has failed before"
          % errors.group(1))

for line in failures:
    print("FAIL: " + line, file=sys.stderr)
if failures:
    sys.exit(1)
print("ok: every destination with a queue reports a plausible oldest-message age")
PY
