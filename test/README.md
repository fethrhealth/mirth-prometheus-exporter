# S0-01 — Mirth exporter capability matrix

Three checks, run against a running exporter:

```bash
export MIRTH_EXPORTER_URL=http://localhost:9001

test/capability_matrix_test.sh      # slot 1
test/reset_test.sh                  # slot 3
test/queue_age_probe.sh             # slot 5
```

Slot 2 lives in the alerting repo and validates the matrix slot 1 writes:

```bash
MIRTH_CAPABILITY_MATRIX=<path printed by capability_matrix_test.sh> \
  evals/S0-01/validate-matrix.sh
```

The split is deliberate: this repo must not need a checkout of the alerting
control repo to run its own checks, so slot 1 asserts coverage only — every
sampled channel reaching the matrix with a destination row and a status per
metric. Whether a metric is *available* is a finding, and the contract check
reports those.

## A Mirth with no channels

A freshly built QA stack comes up with empty volumes, so its Mirth has no
channels at all. The matrix is then empty, and an empty matrix cannot gate
anything — slot 1 fails on `0 channel(s), 0 destination(s)` and the alerting
repo's `validate-matrix.sh` on `$.channels: expected at least 1 item(s)`.

```bash
test/deploy-channels.sh
```

creates and deploys the two sampled channels from `test/channels/*.xml`:
`qa-s0-01-production` (`PRODUCTION` storage, destination queueing on) and
`qa-s0-01-metadata` (`METADATA`, queueing off). They differ in exactly the
property the matrix reports on, which is why there are two. The script is
idempotent — the channel ids are fixed in the XML, so a re-run updates them in
place — and it polls `/channels/statuses` rather than trusting the deploy
call, which returns 204 for a channel that then fails to start.

`METADATA` with destination queueing enabled is one such failure:

```
DeployException: ... one or more destination connectors have queueing
enabled, but the current storage settings do not support queueing on
destination connectors.
```

Mirth reports it only in `/opt/connect/logs/mirth.log`, never in the API
response. That contradiction is the same one slot 2 checks for, arriving from
the server instead.

### Queue age is absent where there is no queue

Supplying the modes used to fail slot 2:

```
FAIL contradiction: destination qa-s0-01-metadata/To MLLP claims
mirth_destination_oldest_queued_age_seconds available, but the channel's
storage mode METADATA cannot retain a destination queue
```

The exporter published `mirth_destination_oldest_queued_age_seconds 0.0` for
every destination, including ones whose storage mode cannot hold a queue, and a
`0` there says "the queue is empty" rather than "there is no queue" — a rule
reading it cannot tell a drained queue from one that cannot exist. It now
publishes the series only for a destination whose `queueEnabled` is true, which
is what the family's "absent when not observed" already promised.

With the modes supplied the three destinations without a queue are reported as
gaps rather than as a contradiction, and only the one real queue carries an age:

```
3 capability gap(s) recorded — findings about the deployment, not failures:
  - qa-s0-01-metadata/Source:  unavailable_by_storage_mode (storage mode METADATA)
  - qa-s0-01-metadata/To MLLP: unavailable_by_storage_mode (storage mode METADATA)
  - qa-s0-01-production/Source: absent (storage mode PRODUCTION)
```

Supply the modes. Left unset everything degrades to `UNKNOWN`, which suppresses
the storage-mode verdict and hides exactly this kind of disagreement.

## Storage modes are an input, not an observation

`/metrics` does not carry a channel's Mirth storage mode, and it must not be
guessed: §13 item 2 settled that it is read from the server. Pass it:

```bash
MIRTH_STORAGE_MODES="c1=PRODUCTION,c2=METADATA" test/capability_matrix_test.sh
```

A channel not listed is recorded `UNKNOWN`. **`UNKNOWN` suppresses the
`unavailable_by_storage_mode` verdict**, so on a real server every storage-mode
finding degrades to plain `absent` unless the modes are supplied — and `absent`
reads as "chase this down" rather than "this is impossible". Only `DEVELOPMENT`
and `PRODUCTION` retain a destination queue, so only those can report queue age.

## Without a Mirth

```bash
test/serve-fixture.sh 9001 &
export MIRTH_EXPORTER_URL=http://localhost:9001

MIRTH_STORAGE_MODES="c1=PRODUCTION,c2=METADATA" test/capability_matrix_test.sh
test/reset_test.sh
test/queue_age_probe.sh
```

This proves the scripts, the matrix and the contract agree. It proves nothing
about a deployment: the fixture cannot fail the way a server can, which is the
whole reason S0-01 exists. `queue_age_probe.sh` in particular fails rather than
passing vacuously when every queue is empty, and only a real server with a
standing queue exercises the bounded lookup.
