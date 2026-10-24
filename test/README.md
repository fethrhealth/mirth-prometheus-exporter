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
