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
`qa-s0-01-production` (destination queueing on) and `qa-s0-01-unqueued`
(queueing off). Both are `PRODUCTION` storage, so they differ in exactly one
property — whether a destination keeps a queue — which is why there are two. The
script is idempotent: the channel ids are fixed in the XML, so a re-run updates
them in place and redeploys them. It polls `/channels/statuses` rather than
trusting the deploy call, which returns 204 for a channel that then fails to
start.

### No channel here is METADATA

`qa-s0-01-unqueued` was `qa-s0-01-metadata`, a `METADATA` channel deployed so the
`unavailable_by_storage_mode` verdict had a live subject. It was retired: every
channel on this deployment is `PRODUCTION`, so a fixture claiming otherwise
described the deployment wrongly and recorded gaps that were an artefact of the
fixture rather than findings about the server. Its id is unchanged, so the deploy
script renames the existing channel instead of leaving an orphan beside a new one.

The rule it used to exercise is unchanged and still enforced. It does not need a
METADATA channel, because the storage mode is an input rather than an
observation — name one and the verdict appears:

```bash
MIRTH_STORAGE_MODES="aec99d5b-6d9e-4e0d-a1ef-565504150ae3=METADATA" \
  test/capability_matrix_test.sh
```

That is the honest way to exercise it now: assert what the code does with a mode,
without asserting that the server has a channel in it.

Deploying a `METADATA` channel whose destination has queueing enabled is refused
by Mirth itself, which is worth knowing when a channel deploys but will not start:

```
DeployException: ... one or more destination connectors have queueing
enabled, but the current storage settings do not support queueing on
destination connectors.
```

Mirth reports it only in `/opt/connect/logs/mirth.log`, never in the API
response. That contradiction is the same one slot 2 checks for, arriving from
the server instead.

### Queue age is absent where there is no queue

The exporter used to publish `mirth_destination_oldest_queued_age_seconds 0.0`
for every destination, including ones that cannot hold a queue at all, and a `0`
there says "the queue is empty" rather than "there is no queue" — a rule reading
it cannot tell a drained queue from one that cannot exist. It now publishes the
series only for a destination whose `queueEnabled` is true, which is what the
family's "absent when not observed" already promised.

So on this deployment exactly one destination carries an age, and the other three
are reported as gaps rather than as zeros:

```
3 capability gap(s) recorded — findings about the deployment, not failures:
  - qa-s0-01-unqueued/Source:   absent (storage mode PRODUCTION)
  - qa-s0-01-unqueued/To MLLP:  absent (storage mode PRODUCTION)
  - qa-s0-01-production/Source: absent (storage mode PRODUCTION)
```

A source connector never has a destination queue, so the two `Source` rows are
expected; `qa-s0-01-unqueued/To MLLP` is a destination that could queue on this
storage mode but has queueing turned off.

## Storage modes are an input, not an observation

`/metrics` does not carry a channel's Mirth storage mode, and it must not be
guessed: §13 item 2 settled that it is read from the server. Pass it:

```bash
MIRTH_STORAGE_MODES="c1=PRODUCTION,c2=METADATA" test/capability_matrix_test.sh
```

A channel not listed takes `MIRTH_DEFAULT_STORAGE_MODE`, which is `PRODUCTION` —
true of every channel on this deployment, and the reason the command above needs
no arguments in practice. Set it to `UNKNOWN` to go back to recording nothing:
**`UNKNOWN` suppresses the `unavailable_by_storage_mode` verdict**, so every
storage-mode finding degrades to plain `absent`, and `absent` reads as "chase
this down" rather than "this is impossible". `queue_age_source_test.sh` refuses
to run at all without `MIRTH_STORAGE_MODES`, because for that check UNKNOWN
everywhere is a vacuous pass.

Only `DEVELOPMENT` and `PRODUCTION` retain a destination queue, so only those can
report queue age.

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
