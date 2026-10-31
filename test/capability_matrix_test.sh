#!/usr/bin/env bash
# S0-01 slot 1. Build the capability matrix: for every sampled channel and every
# one of its destinations, what the exporter actually produces.
#
#   test/capability_matrix_test.sh
#
# Environment:
#   MIRTH_EXPORTER_URL       default http://localhost:9001
#   MIRTH_CAPABILITY_MATRIX  where to write, default build/S0-01/capability-matrix.json
#   MIRTH_STORAGE_MODES      optional "channelId=PRODUCTION,otherId=METADATA".
#                            A channel not listed is recorded UNKNOWN, which is a
#                            legitimate answer — §13 item 2 settled that storage
#                            mode is read from the server, never assumed.
#
# Asserts coverage only: every sampled channel reaches the matrix with a status
# for each metric. Whether a metric is available is a finding about the
# deployment, and the contract check in the alerting repo reports those.
set -euo pipefail

exporter_url="${MIRTH_EXPORTER_URL:-http://localhost:9001}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${here}/.." && pwd)"
matrix="${MIRTH_CAPABILITY_MATRIX:-${repo_root}/build/S0-01/capability-matrix.json}"
endpoint="${exporter_url%/}/metrics"

mkdir -p "$(dirname "${matrix}")"
scrape="$(mktemp)"
trap 'rm -f "${scrape}"' EXIT

if ! curl --silent --show-error --fail --max-time 30 "${endpoint}" > "${scrape}"; then
    echo "FAIL: could not scrape ${endpoint}" >&2
    echo "      start the exporter first, or set MIRTH_EXPORTER_URL" >&2
    exit 1
fi

python3 -I - "${scrape}" "${matrix}" "${endpoint}" "${MIRTH_STORAGE_MODES:-}" <<'PY'
import datetime, json, os, re, sys

scrape_path, matrix_path, endpoint, modes_arg = sys.argv[1:5]

CHANNEL_METRICS = ["mirth_messages_received", "mirth_messages_sent",
                   "mirth_messages_filtered", "mirth_messages_errored",
                   "mirth_messages_queued", "mirth_channel_started"]
DESTINATION_METRICS = ["mirth_destination_messages_received", "mirth_destination_messages_sent",
                       "mirth_destination_messages_filtered", "mirth_destination_messages_errored",
                       "mirth_destination_messages_queued", "mirth_destination_started",
                       "mirth_destination_queue_enabled",
                       "mirth_destination_oldest_queued_age_seconds"]
# MessageStorageMode in orchestration-engine: only these retain a destination queue.
QUEUE_AGE_MODES = {"DEVELOPMENT", "PRODUCTION"}
QUEUE_AGE = "mirth_destination_oldest_queued_age_seconds"

KNOWN_METRICS = set(CHANNEL_METRICS) | set(DESTINATION_METRICS)


def canonical(name):
    """The family name a metric is keyed by here, given the name on the wire.

    prometheus_client suffixes a counter's samples with _total, so the family the
    exporter builds as mirth_messages_received arrives as
    mirth_messages_received_total. The matrix keys metrics by the un-suffixed name
    because contracts/capability-matrix.v1.json is frozen on those, so resolve the
    wire name back to it once, here, rather than at every comparison below.
    """
    if name not in KNOWN_METRICS and name.endswith("_total"):
        base = name[: -len("_total")]
        if base in KNOWN_METRICS:
            return base
    return name

storage_modes = {}
for pair in modes_arg.split(","):
    if "=" in pair:
        cid, mode = pair.split("=", 1)
        storage_modes[cid.strip()] = mode.strip().upper()

sample_re = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}\s+([0-9.eE+-]+)\s*$")
bare_re = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)\s+([0-9.eE+-]+)\s*$")


def labels_of(text):
    out = {}
    for m in re.finditer(r'(\w+)="((?:[^"\\]|\\.)*)"', text):
        out[m.group(1)] = m.group(2).replace('\\"', '"')
    return out


instance = None
# (channelId, metric) -> True ; (channelId, metaDataId, metric) -> True
channel_seen, dest_seen = {}, {}
channels, destinations = {}, {}

for line in open(scrape_path, encoding="utf-8"):
    if line.startswith("#") or not line.strip():
        continue
    m = sample_re.match(line)
    if not m:
        b = bare_re.match(line)
        continue
    name, lbl, _value = m.group(1), labels_of(m.group(2)), m.group(3)
    if not name.startswith("mirth_"):
        continue
    name = canonical(name)
    instance = instance or lbl.get("mirth_instance")
    cid = lbl.get("channelId")
    if cid is None:
        continue
    channels.setdefault(cid, {"channel_name": lbl.get("channelName", cid)})
    if "metaDataId" in lbl:
        key = (cid, lbl["metaDataId"])
        d = destinations.setdefault(key, {"destination": lbl.get("destination", lbl["metaDataId"]),
                                          "meta_data_id": lbl["metaDataId"],
                                          "queue_enabled": False})
        if name == "mirth_destination_queue_enabled":
            d["queue_enabled"] = float(_value) == 1.0
        if name == "mirth_destination_state":
            if float(_value) == 1.0 and lbl.get("state"):
                d["state"] = lbl["state"]
        dest_seen[(cid, lbl["metaDataId"], name)] = True
    else:
        if name == "mirth_channel_state" and float(_value) == 1.0 and lbl.get("state"):
            channels[cid]["state"] = lbl["state"]
        channel_seen[(cid, name)] = True

out_channels = []
for cid in sorted(channels):
    mode = storage_modes.get(cid, "UNKNOWN")
    entry = {
        "channel_id": cid,
        "channel_name": channels[cid]["channel_name"],
        "storage_mode": mode,
        "metrics": [{"metric": metric,
                     "status": "available" if channel_seen.get((cid, metric)) else "absent"}
                    for metric in CHANNEL_METRICS],
        "destinations": [],
    }
    if "state" in channels[cid]:
        entry["state"] = channels[cid]["state"]
    for (ccid, mid), d in sorted(destinations.items()):
        if ccid != cid:
            continue
        metrics = []
        for metric in DESTINATION_METRICS:
            if dest_seen.get((cid, mid, metric)) and metric == QUEUE_AGE and mode == "UNKNOWN":
                # Observed, but the storage mode that would make it meaningful is not known:
                # claim nothing until the owner supplies the mode.
                status = "unverified"
            elif dest_seen.get((cid, mid, metric)):
                status = "available"
            elif metric == QUEUE_AGE and mode not in QUEUE_AGE_MODES and mode != "UNKNOWN":
                # Not a gap to chase: this storage mode cannot retain the queue.
                status = "unavailable_by_storage_mode"
            else:
                status = "absent"
            metrics.append({"metric": metric, "status": status})
        dest = {"destination": d["destination"], "meta_data_id": mid,
                "queue_enabled": d["queue_enabled"], "metrics": metrics}
        if "state" in d:
            dest["state"] = d["state"]
        entry["destinations"].append(dest)
    out_channels.append(entry)

matrix = {
    "schema_version": 1,
    "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "endpoint": endpoint,
    "mirth_instance": instance or "unknown",
    "channels": out_channels,
}
os.makedirs(os.path.dirname(matrix_path), exist_ok=True)
with open(matrix_path, "w", encoding="utf-8") as fh:
    json.dump(matrix, fh, indent=2)
    fh.write("\n")

total_dest = sum(len(c["destinations"]) for c in out_channels)
print("wrote %s" % matrix_path)
print("  %d channel(s), %d destination(s)" % (len(out_channels), total_dest))
unknown = [c["channel_name"] for c in out_channels if c["storage_mode"] == "UNKNOWN"]
if unknown:
    print("  storage mode UNKNOWN for %d channel(s): %s" % (len(unknown), ", ".join(unknown[:5])))
    print("  set MIRTH_STORAGE_MODES to record them (§13 item 2: read from the server, never assumed)")

if not out_channels:
    print("FAIL: no channels in the scrape. A Mirth with no deployed channels, or an "
          "exporter that could not reach it, produces an empty matrix, and an empty "
          "matrix cannot gate anything.", file=sys.stderr)
    sys.exit(1)

missing_dest = [c["channel_name"] for c in out_channels if not c["destinations"]]
if missing_dest:
    print("FAIL: %d channel(s) reached the matrix with no destination rows: %s. Slot 1 requires "
          "every sampled channel x destination to be covered."
          % (len(missing_dest), ", ".join(missing_dest)), file=sys.stderr)
    sys.exit(1)
PY
echo "next: validate it from the alerting repo —"
echo "  MIRTH_CAPABILITY_MATRIX=${matrix} evals/S0-01/validate-matrix.sh"
