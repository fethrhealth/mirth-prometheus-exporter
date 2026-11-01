#!/usr/bin/env bash
# S0-01 fixture: create and deploy the two sampled channels on a Mirth that has
# none, so the live checks have something to observe.
#
# A Mirth with no channels produces an empty capability matrix, and an empty
# matrix cannot gate anything — capability_matrix_test.sh and the alerting
# repo's validate-matrix.sh both fail on it. A freshly built QA stack comes up
# with empty volumes and therefore no channels, so without this the two checks
# depend on channels someone created by hand and did not write down.
#
#   test/deploy-channels.sh
#
# Environment:
#   MIRTH_API_URL     default https://localhost:8443/api
#   MIRTH_USERNAME    default admin
#   MIRTH_PASSWORD    default admin
#   MIRTH_CONTAINER   default hl7-engine. Mirth's HTTPS port is usually not
#                     published to the host, so when MIRTH_API_URL is not
#                     reachable directly the calls are run inside this
#                     container instead.
#   DOCKER_BIN        default docker, falling back to ~/.docker/bin/docker
#
# Idempotent: the channel ids are fixed in the XML, so a re-run updates the two
# channels in place and redeploys them. Exits non-zero unless both reach STARTED.
set -euo pipefail

api_url="${MIRTH_API_URL:-https://localhost:8443/api}"
username="${MIRTH_USERNAME:-admin}"
password="${MIRTH_PASSWORD:-admin}"
container="${MIRTH_CONTAINER:-hl7-engine}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker_bin="${DOCKER_BIN:-docker}"
if ! command -v "${docker_bin}" > /dev/null 2>&1 && [ -x "${HOME}/.docker/bin/docker" ]; then
    docker_bin="${HOME}/.docker/bin/docker"
fi

# How to reach the API: straight from here if the port is published, otherwise
# from inside the Mirth container.
transport=""
probe=(--silent --show-error --fail --insecure --max-time 10
       --user "${username}:${password}" -H "X-Requested-With: curl" -H "Accept: text/plain")
if curl "${probe[@]}" "${api_url}/server/id" > /dev/null 2>&1; then
    transport="direct"
elif command -v "${docker_bin}" > /dev/null 2>&1 \
        && "${docker_bin}" exec "${container}" sh -lc "curl ${probe[*]@Q} '${api_url}/server/id'" > /dev/null 2>&1; then
    transport="container"
else
    echo "FAIL: no route to the Mirth API at ${api_url}" >&2
    echo "      tried this host directly and 'docker exec ${container}'." >&2
    echo "      Is the stack up? Compose project fethr-qa." >&2
    exit 1
fi

# curl against the API, with a body read from a local file when one is given.
mirth_api() {
    local method="$1" path="$2" accept="$3" body_file="${4:-}"
    local args=(--silent --show-error --insecure --max-time 60
                --user "${username}:${password}"
                -H "X-Requested-With: curl" -H "Accept: ${accept}"
                -w '\n%{http_code}' -X "${method}")
    if [ -n "${body_file}" ]; then
        args+=(-H "Content-Type: application/xml")
    fi

    if [ "${transport}" = "direct" ]; then
        [ -n "${body_file}" ] && args+=(--data-binary "@${body_file}")
        curl "${args[@]}" "${api_url}${path}"
    else
        local remote=""
        if [ -n "${body_file}" ]; then
            remote="/tmp/s0-01-$(basename "${body_file}")"
            "${docker_bin}" cp "${body_file}" "${container}:${remote}" > /dev/null
            args+=(--data-binary "@${remote}")
        fi
        "${docker_bin}" exec "${container}" sh -lc "curl ${args[*]@Q} '${api_url}${path}'"
    fi
}

# Everything below reads the trailing status line that -w appended.
http_status() { printf '%s' "$1" | tail -n 1; }
http_body()   { printf '%s' "$1" | sed '$d'; }

existing="$(mirth_api GET /channels application/json)"
if [ "$(http_status "${existing}")" != "200" ]; then
    echo "FAIL: could not list channels (HTTP $(http_status "${existing}"))" >&2
    exit 1
fi
existing_ids="$(http_body "${existing}" | python3 -I -c '
import json, sys

try:
    doc = json.load(sys.stdin)
except json.JSONDecodeError:
    sys.exit(0)

listed = (doc.get("list") or {}).get("channel") or []
if isinstance(listed, dict):
    listed = [listed]
for channel in listed:
    print(channel.get("id", ""))
')"

deployed=()
for xml in "${here}"/channels/*.xml; do
    name="$(basename "${xml}" .xml)"
    id="$(sed -n 's:.*<id>\(.*\)</id>.*:\1:p' "${xml}" | head -n 1)"
    if [ -z "${id}" ]; then
        echo "FAIL: ${xml} carries no <id>" >&2
        exit 1
    fi

    if printf '%s\n' "${existing_ids}" | grep -qx "${id}"; then
        response="$(mirth_api PUT "/channels/${id}?override=true" text/plain "${xml}")"
        verb="updated"
    else
        response="$(mirth_api POST /channels text/plain "${xml}")"
        verb="created"
    fi
    if [ "$(http_status "${response}")" != "200" ]; then
        echo "FAIL: could not ${verb%d} ${name} (HTTP $(http_status "${response}"))" >&2
        http_body "${response}" >&2
        exit 1
    fi

    # The deploy endpoint returns void: it answers 406 to Accept: text/plain,
    # and its 204 says the task was accepted, not that the channel started.
    response="$(mirth_api POST "/channels/${id}/_deploy" '*/*')"
    if [ "$(http_status "${response}")" != "204" ]; then
        echo "FAIL: could not deploy ${name} (HTTP $(http_status "${response}"))" >&2
        http_body "${response}" >&2
        exit 1
    fi
    echo "${verb} and deployed ${name} (${id})"
    deployed+=("${name}")
done

# Deployment is asynchronous and a failure is reported only here, so poll.
for _ in $(seq 1 30); do
    statuses="$(http_body "$(mirth_api GET /channels/statuses application/json)")"
    started="$(printf '%s' "${statuses}" | python3 -I -c '
import json, sys

try:
    doc = json.load(sys.stdin)
except json.JSONDecodeError:
    sys.exit(0)

listed = (doc.get("list") or {}).get("dashboardStatus") or []
if isinstance(listed, dict):
    listed = [listed]
for channel in listed:
    if channel.get("state") == "STARTED":
        print(channel.get("name", ""))
')"
    missing=()
    for name in "${deployed[@]}"; do
        printf '%s\n' "${started}" | grep -qx "${name}" || missing+=("${name}")
    done
    [ "${#missing[@]}" -eq 0 ] && break
    sleep 2
done

if [ "${#missing[@]}" -ne 0 ]; then
    echo "FAIL: deployed but not STARTED: ${missing[*]}" >&2
    echo "      Mirth reports a deploy failure in its own log, not in the API response:" >&2
    echo "      ${docker_bin} exec ${container} grep -i DeployException /opt/connect/logs/mirth.log" >&2
    echo "      A METADATA channel whose destination has queueing enabled fails this way." >&2
    exit 1
fi

echo "ok: ${#deployed[@]} channel(s) STARTED — ${deployed[*]}"
echo
echo "Storage modes are an input to the matrix, not something /metrics carries:"
echo "  export MIRTH_STORAGE_MODES=\"$(for xml in "${here}"/channels/*.xml; do
        id="$(sed -n 's:.*<id>\(.*\)</id>.*:\1:p' "${xml}" | head -n 1)"
        mode="$(sed -n 's:.*<messageStorageMode>\(.*\)</messageStorageMode>.*:\1:p' "${xml}" | head -n 1)"
        printf '%s=%s,' "${id}" "${mode}"
    done | sed 's/,$//')\""
echo
echo "Both channels are PRODUCTION, which is also MIRTH_DEFAULT_STORAGE_MODE, so an"
echo "unset MIRTH_STORAGE_MODES records the right mode for this deployment and the"
echo "storage-mode verdict still carries — that is why control/bin/regress can run"
echo "the closure with nothing set. Supply the modes anyway when the deployment has"
echo "a channel this script did not create. queue_age_source_test.sh refuses to run"
echo "without them. See test/README.md."
