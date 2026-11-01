#!/usr/bin/env bash
# S4-04, exporter side. The queue-age series is published only where a queue-age
# source can exist, and nowhere else.
#
#   test/queue_age_source_test.sh
#
# Environment:
#   MIRTH_EXPORTER_URL   default http://localhost:9001
#   MIRTH_STORAGE_MODES  required, "channelId=PRODUCTION,otherId=METADATA".
#                        Unset means every channel is UNKNOWN, which suppresses the
#                        storage-mode verdict and makes this check pass vacuously —
#                        so it is refused rather than defaulted.
#
# This is the companion to queue_age_probe.sh and asks a different question. The
# probe asks whether the bounded lookup produces a plausible age for a destination
# that has a queue, which only a real server with a standing queue can answer (it
# is @nightly for that reason). This asks whether the exporter ever offers an age
# where no source exists — a destination that does not queue, or a channel whose
# storage mode cannot retain a destination queue. That question is answerable from
# any scrape, including the recorded fixture, because the failure is the presence
# of a series rather than the absence of a queue.
#
# Why it matters on this side as well as in the alerting repo: S4-04's rule is that
# a value without a source reports `unavailable`, never a number, and the consuming
# half can only hold that line for series it is given. A `0.0` published for a
# destination whose storage mode cannot hold a queue reads as "the queue is empty"
# rather than "there is no queue", and a consumer has no way to tell the two apart
# once the number exists. The exporter stopped publishing that 0 (see test/README.md,
# "Queue age is absent where there is no queue"); this is the check that keeps it so.
set -euo pipefail

exporter_url="${MIRTH_EXPORTER_URL:-http://localhost:9001}"
endpoint="${exporter_url%/}/metrics"

if [[ -z "${MIRTH_STORAGE_MODES:-}" ]]; then
    echo "FAIL: MIRTH_STORAGE_MODES is unset. Every channel would be recorded UNKNOWN, which" >&2
    echo "      suppresses the storage-mode verdict and lets this check pass without checking" >&2
    echo "      anything. Supply the modes read off the server, e.g." >&2
    echo "      MIRTH_STORAGE_MODES=\"c1=PRODUCTION,c2=METADATA\" test/queue_age_source_test.sh" >&2
    exit 2
fi

scrape="$(mktemp)"
trap 'rm -f "${scrape}"' EXIT

if ! curl --silent --show-error --fail --max-time 30 "${endpoint}" > "${scrape}"; then
    echo "FAIL: could not scrape ${endpoint}" >&2
    echo "      start the exporter first, or serve the fixture: test/serve-fixture.sh 9001" >&2
    exit 1
fi

python3 -I - "${scrape}" "${MIRTH_STORAGE_MODES}" <<'PY'
import re, sys

scrape_path, modes_arg = sys.argv[1], sys.argv[2]
text = open(scrape_path, encoding="utf-8").read()

# MessageStorageMode in orchestration-engine: only these retain a destination queue.
# The same two the capability matrix contract's queue_age_storage_modes names.
QUEUE_AGE_MODES = {"DEVELOPMENT", "PRODUCTION"}
QUEUE_AGE = "mirth_destination_oldest_queued_age_seconds"

modes = {}
for pair in modes_arg.split(","):
    if "=" in pair:
        channel, mode = pair.split("=", 1)
        modes[channel.strip()] = mode.strip().upper()


def samples(metric):
    out = {}
    for m in re.finditer(r"^%s\{([^}]*)\}\s+([0-9.eE+-]+)\s*$" % re.escape(metric), text, re.M):
        labels = dict(re.findall(r'(\w+)="((?:[^"\\]|\\.)*)"', m.group(1)))
        key = (labels.get("channelId", "?"), labels.get("metaDataId", "?"))
        out[key] = (float(m.group(2)), labels)
    return out


queue_enabled = samples("mirth_destination_queue_enabled")
ages = samples(QUEUE_AGE)

if not queue_enabled:
    print("FAIL: no mirth_destination_queue_enabled series — the exporter reached no destination, "
          "so this scrape says nothing about where a queue-age source exists.", file=sys.stderr)
    sys.exit(1)

failures, covered, without_series = [], 0, 0
for key, (enabled, labels) in sorted(queue_enabled.items()):
    channel_id, meta_data_id = key
    label = "%s/%s" % (labels.get("channelName", channel_id), labels.get("destination", meta_data_id))
    mode = modes.get(channel_id, "UNKNOWN")
    age = ages.get(key)
    if mode != "UNKNOWN":
        covered += 1
    if age is None:
        without_series += 1
        continue

    offending = False
    if enabled != 1:
        offending = True
        failures.append("%s reports %s = %.1f with queueing disabled. There is no queue to age, so "
                        "the value is not a measurement of anything a consumer can act on."
                        % (label, QUEUE_AGE, age[0]))
    if mode != "UNKNOWN" and mode not in QUEUE_AGE_MODES:
        offending = True
        failures.append("%s reports %s = %.1f, but its channel's storage mode %s cannot retain a "
                        "destination queue. The server decides this, not the exporter (§13 item 2)."
                        % (label, QUEUE_AGE, age[0], mode))
    if not offending:
        print("    %s: storage mode %s, queueing on, age %.0fs" % (label, mode, age[0]))

print("  %d destination(s), %d with a storage mode supplied, %d publishing an age, %d not"
      % (len(queue_enabled), covered, len(ages), without_series))

if covered == 0:
    failures.append("no destination's channel had a storage mode supplied, so every storage-mode "
                    "verdict degraded to UNKNOWN and nothing above was actually checked. "
                    "MIRTH_STORAGE_MODES must name the channels in this scrape.")

if without_series == 0:
    failures.append("every destination publishes an age, so this scrape cannot show the series being "
                    "withheld where no source exists. A scrape with no such destination proves "
                    "nothing: include one whose queueing is off or whose storage mode cannot hold a "
                    "queue.")

for line in failures:
    print("FAIL: " + line, file=sys.stderr)
if failures:
    sys.exit(1)
print("ok: the queue-age series appears only on destinations that queue, on channels whose storage "
      "mode retains a destination queue")
PY
