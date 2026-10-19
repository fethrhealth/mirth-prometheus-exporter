# Prometheus exporter for Mirth Connect

Python exporter that reads Mirth Connect's REST API and exposes channel, destination connector and server metrics to Prometheus.

This fork reads the dashboard status endpoint (`GET /channels/statuses`) instead of the channel statistics endpoint. That gives one series per destination connector, the channel and connector deployed state, and live queue depth per destination. Cumulative message statistics are declared as Prometheus counters so `rate()` and `increase()` handle Mirth statistics resets correctly.

## Requirements

- Python 3.9 or later
- `prometheus_client`
- `requests`

```bash
pip install prometheus_client requests
```

## Configuration

`mirthConfig.json` next to the script, or environment variables. Environment variables win.

| JSON key | Environment variable | Default | Meaning |
|---|---|---|---|
| `instance` | `MIRTH_INSTANCE` | `localhost` | Mirth host. `localhost` becomes `host.docker.internal` inside a container. |
| `mirthPort` | `MIRTH_PORT` | `8443` | Mirth HTTPS port. |
| `username` | `MIRTH_USERNAME` | `admin` | API user. Needs permission to view the dashboard and system stats. |
| `password` | `MIRTH_PASSWORD` | `admin` | API password. |
| `prometheusPort` | `PROMETHEUS_PORT` | `9001` | Port the exporter listens on. |
| `verifyTls` | `MIRTH_VERIFY_TLS` | `false` | Verify Mirth's certificate. Mirth ships self-signed. |
| `includeUndeployed` | `MIRTH_INCLUDE_UNDEPLOYED` | `true` | Also report channels that are not deployed. |
| `timeoutSeconds` | `MIRTH_TIMEOUT_SECONDS` | `10` | HTTP timeout per API call. |

The exporter logs in once and reuses the session cookie. It logs in again on a 401.

## Running

```bash
python mirth_exporter.py
```

Or in Docker:

```bash
docker build -t mirth-exporter .
docker run -p 9001:9001 -e MIRTH_INSTANCE=hl7-engine -e MIRTH_USERNAME=admin -e MIRTH_PASSWORD=admin mirth-exporter
```

A Prometheus scrape config for the Fethr compose stack is in `prometheus/prometheus.yml`. It also scrapes the Kestra orchestrator on its management port.

## Metrics

All series carry `mirth_instance` (the Mirth host as configured). It is deliberately not called `instance`: Prometheus owns that label for the scrape target and, without `honor_labels`, renames a clashing exporter label to `exported_instance`. Anything written against the old `instance` or `exported_instance` label on these series needs to use `mirth_instance` now.

### Channel level

Labels: `channelName`, `channelId`. Names are unchanged from the upstream exporter so the bundled Grafana dashboard keeps working.

| Metric | Type | Meaning |
|---|---|---|
| `mirth_messages_received_total` | counter | Messages received by the channel since its statistics were last reset. |
| `mirth_messages_sent_total` | counter | Messages sent by the channel. |
| `mirth_messages_filtered_total` | counter | Messages filtered by the channel. |
| `mirth_messages_errored_total` | counter | Messages that errored in the channel. |
| `mirth_messages_queued` | gauge | Messages currently queued across the channel's destinations. |
| `mirth_channel_state` | gauge | One series per Mirth deployed state with label `state`; 1 for the current state. |
| `mirth_channel_started` | gauge | 1 when the channel state is `STARTED`. |

### Destination connector level

Labels: `channelName`, `channelId`, `destination` (connector name), `metaDataId` (connector number as Mirth assigns it; the source connector is `0` if Mirth reports it).

| Metric | Type | Meaning |
|---|---|---|
| `mirth_destination_messages_received_total` | counter | Messages received by the connector. |
| `mirth_destination_messages_sent_total` | counter | Messages sent by the connector. |
| `mirth_destination_messages_filtered_total` | counter | Messages filtered by the connector. |
| `mirth_destination_messages_errored_total` | counter | Messages that errored in the connector. |
| `mirth_destination_messages_queued` | gauge | Messages currently queued on this connector. |
| `mirth_destination_state` | gauge | One series per state with label `state`; 1 for the current state. |
| `mirth_destination_started` | gauge | 1 when the connector state is `STARTED`. |
| `mirth_destination_queue_enabled` | gauge | 1 when queuing is enabled on the connector. |

### Server and scrape health

| Metric | Type | Meaning |
|---|---|---|
| `mirth_up` | gauge | 1 when the Mirth API answered this scrape. |
| `mirth_server_state` | gauge | Legacy: 0 up, 1 down. Prefer `mirth_up`. |
| `mirth_scrape_success` | gauge | 1 when every API call in the scrape succeeded. |
| `mirth_scrape_duration_seconds` | gauge | Wall time of the scrape. |
| `mirth_channels_observed` | gauge | Channels returned by the status call. |
| `mirth_server_size` | gauge | Disk used on the Mirth volume, bytes. |
| `mirth_server_disk_free_bytes` | gauge | Disk free on the Mirth volume, bytes. |
| `mirth_server_heap_used_bytes` | gauge | JVM heap allocated minus free, bytes. |
| `mirth_server_cpu_usage_percent` | gauge | CPU usage as reported by Mirth. |

## Unknown is not zero

When Mirth cannot be reached the exporter emits `mirth_up 0` and nothing else. It never writes a zero for a channel or destination it did not observe, and it omits a counter when Mirth did not return that statistic. Dashboards and alert rules must treat a missing series as unknown:

```promql
# channel data is missing while Mirth itself is up
mirth_up == 1 and absent(mirth_messages_queued{channelId="..."})

# exporter or Mirth is down
up{job="mirth"} == 0 or mirth_up == 0
```

A Grafana stat panel should set its "No value" display to `UNKNOWN`, not `0`.

## Capability matrix

What this exporter can and cannot get from the Mirth REST API. Rows marked unsupported were tried against the API and either do not exist in the dashboard status payload or need a different, heavier call that this exporter deliberately does not make.

| Capability | Status | Source | Notes |
|---|---|---|---|
| Received, sent, filtered, errored per channel | supported | `/channels/statuses` `statistics` | Cumulative since last statistics reset. |
| Received, sent, filtered, errored per destination | supported | `childStatuses[].statistics` | One series per connector. |
| Queue depth per channel | supported | `DashboardStatus.queued` | Live gauge, not a cumulative counter. |
| Queue depth per destination | supported | `childStatuses[].queued` | Live gauge. |
| Channel deployed state | supported | `DashboardStatus.state` | `STARTED`, `STOPPED`, `PAUSED`, `UNDEPLOYED` and the transitional states. |
| Destination connector state | supported | `childStatuses[].state` | Same enum. |
| Queue enabled flag per destination | supported | `childStatuses[].queueEnabled` | |
| Undeployed channels | supported | `includeUndeployed=true` | Reported with state `UNDEPLOYED` and no statistics. |
| Server disk, heap, CPU | supported | `/system/stats` | Point-in-time values. |
| Statistics reset detection | supported | counter type | Prometheus counter semantics; a reset shows as a counter reset, never a negative rate. |
| Lifetime statistics | unsupported | `lifetimeStatistics` exists in the payload | Not emitted. Would double the series count for values the current counters already cover. Add if a dashboard needs since-creation totals. |
| Oldest queued message age | unsupported | not in `/channels/statuses` | Needs a message search per channel ordered by received date with a queued status filter, and stored messages may be pruned. Not available from the dashboard payload. |
| Last message received timestamp | unsupported | not in `/channels/statuses` | Needs `/channels/{id}/messages` with `limit=1`, one call per channel per scrape. The Fethr orchestrator already does this on demand; a 15-second poll across an estate is too expensive. |
| Last successful delivery timestamp | unsupported | not in `/channels/statuses` | Same message-search cost, and the latest stored message is not necessarily the last successful downstream delivery. |
| Message processing latency | unsupported | not in `/channels/statuses` | Only derivable per message from `connectorMessages` timestamps. |
| Error detail or response text | unsupported | not in `/channels/statuses` | Message content is deliberately excluded from monitoring queries. |
| Channel-level alerts configured in Mirth | unsupported | `/alerts` | Different endpoint, not a metric. |
| Multiple Mirth servers | unsupported | | One Mirth per exporter process. Run one container per server and distinguish by `instance`. |
| Tenant or environment labels | unsupported | | Mirth has no tenant concept. Add `external_labels` or relabeling in Prometheus. |

## Tests

```bash
python -m unittest test_mirth_exporter
```

The tests use Jettison-shaped fixtures and a fake API, so they run without a Mirth server.
