"""Prometheus exporter for Mirth Connect.

Reads the dashboard status endpoint (GET /channels/statuses) so that every
destination connector becomes its own series, and declares the cumulative
message statistics as counters so Prometheus can detect statistics resets.

When Mirth cannot be reached the exporter emits only ``mirth_up 0``; it never
emits a zero for a channel it could not observe. Dashboards must treat an
absent series as unknown, not healthy. The oldest queued message per destination is looked up by a bounded background task and
exposed as an age; it is absent (unknown) when it has not been observed, and 0 only when the
dashboard says the queue is empty. The same rule applies to the system stats:
a value Mirth did not return is omitted, never reported as zero.

The Mirth host is exposed in the ``mirth_instance`` label rather than ``instance``,
because Prometheus owns ``instance`` and would rename ours to ``exported_instance``
unless the scrape job sets ``honor_labels``.
"""

import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import requests
import urllib3
from urllib.parse import quote
from prometheus_client import GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, REGISTRY

log = logging.getLogger("mirth_exporter")

STATUS_KEY = "com.mirth.connect.donkey.model.message.Status"
CHANNEL_STATES = [
    "UNDEPLOYED", "DEPLOYING", "UNDEPLOYING", "STARTING", "STARTED",
    "PAUSING", "PAUSED", "STOPPING", "STOPPED", "SYNCING", "UNKNOWN",
]
COUNTER_STATS = ("RECEIVED", "FILTERED", "SENT", "ERROR")


def load_config() -> Dict[str, Any]:
    """mirthConfig.json next to this file, with environment variables taking precedence."""
    dir_path = os.path.dirname(os.path.realpath(__file__))
    cfg: Dict[str, Any] = {}
    path = os.path.join(dir_path, "mirthConfig.json")
    if os.path.exists(path):
        with open(path) as fh:
            cfg = json.load(fh)
    env = os.environ
    cfg["instance"] = env.get("MIRTH_INSTANCE", cfg.get("instance", "localhost"))
    cfg["mirthPort"] = int(env.get("MIRTH_PORT", cfg.get("mirthPort", 8443)))
    cfg["username"] = env.get("MIRTH_USERNAME", cfg.get("username", "admin"))
    cfg["password"] = env.get("MIRTH_PASSWORD", cfg.get("password", "admin"))
    cfg["prometheusPort"] = int(env.get("PROMETHEUS_PORT", cfg.get("prometheusPort", 9001)))
    cfg["verifyTls"] = str(env.get("MIRTH_VERIFY_TLS", cfg.get("verifyTls", "false"))).lower() == "true"
    cfg["includeUndeployed"] = str(env.get("MIRTH_INCLUDE_UNDEPLOYED", cfg.get("includeUndeployed", "true"))).lower() == "true"
    cfg["timeoutSeconds"] = float(env.get("MIRTH_TIMEOUT_SECONDS", cfg.get("timeoutSeconds", 10)))
    cfg["queueAgeEnabled"] = str(env.get("MIRTH_QUEUE_AGE_ENABLED", cfg.get("queueAgeEnabled", "true"))).lower() == "true"
    cfg["queueAgeIntervalSeconds"] = float(env.get("MIRTH_QUEUE_AGE_INTERVAL_SECONDS", cfg.get("queueAgeIntervalSeconds", 60)))
    cfg["queueAgeMaxLookupsPerCycle"] = int(env.get("MIRTH_QUEUE_AGE_MAX_LOOKUPS", cfg.get("queueAgeMaxLookupsPerCycle", 25)))
    if cfg["instance"] == "localhost" and os.path.exists("/.dockerenv"):
        cfg["instance"] = "host.docker.internal"
    return cfg


class MirthApi:
    """Thin session-cached client for the Mirth REST API using Jettison JSON responses."""

    def __init__(self, cfg: Dict[str, Any]):
        self.base = f"https://{cfg['instance']}:{cfg['mirthPort']}/api"
        self.username = cfg["username"]
        self.password = cfg["password"]
        self.verify = cfg["verifyTls"]
        self.timeout = cfg["timeoutSeconds"]
        self.include_undeployed = cfg["includeUndeployed"]
        self.session: Optional[requests.Session] = None
        # Scrapes arrive on separate server threads; guard the shared session so one
        # thread cannot reset it while another is using it, and so a cold start logs in once.
        self._lock = threading.RLock()

    def reset_session(self) -> None:
        with self._lock:
            self.session = None

    def _login(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"X-Requested-With": "MirthClient", "Accept": "application/json"})
        r = s.post(
            f"{self.base}/users/_login",
            data={"username": self.username, "password": self.password},
            verify=self.verify,
            timeout=self.timeout,
        )
        r.raise_for_status()
        self.session = s
        return s

    def get(self, path: str) -> Dict[str, Any]:
        with self._lock:
            s = self.session or self._login()
            r = s.get(f"{self.base}{path}", verify=self.verify, timeout=self.timeout)
            if r.status_code == 401:
                s = self._login()
                r = s.get(f"{self.base}{path}", verify=self.verify, timeout=self.timeout)
            r.raise_for_status()
            return r.json()

    def channel_statuses(self) -> List[Dict[str, Any]]:
        flag = "true" if self.include_undeployed else "false"
        body = self.get(f"/channels/statuses?includeUndeployed={flag}")
        return unwrap_list(body.get("list"), "dashboardStatus")

    def system_stats(self) -> Dict[str, Any]:
        body = self.get("/system/stats")
        return body.get("com.mirth.connect.model.SystemStats", body)

    def queued_count(self, channel_id: str, meta_data_id: str) -> int:
        """GET /channels/{id}/messages/count for one destination's QUEUED messages. Jettison answers {"long": N}."""
        body = self.get(
            f"/channels/{quote(channel_id, safe='')}/messages/count?status=QUEUED&includedMetaDataId={quote(meta_data_id, safe='')}"
        )
        count = to_int(body.get("long")) if isinstance(body, dict) else None
        if count is None:
            raise ValueError(f"unexpected queued count payload for channel {channel_id}: {body!r}")
        return count

    def oldest_queued_received(self, channel_id: str, meta_data_id: str) -> Optional[float]:
        """Epoch seconds at which the oldest QUEUED message reached this destination, or None if none is queued.

        Mirth returns search results newest first, so the oldest is the last row: count the queued messages,
        then fetch only the row at offset count-1, without content. Messages that arrive or drain between the
        two calls shift the row by a few places, so the result is approximate. Raises when Mirth cannot answer,
        which the caller must treat as unknown rather than as an empty queue.
        """
        count = self.queued_count(channel_id, meta_data_id)
        if count <= 0:
            return None
        body = self.get(
            f"/channels/{quote(channel_id, safe='')}/messages?status=QUEUED&includedMetaDataId={quote(meta_data_id, safe='')}"
            f"&includeContent=false&limit=1&offset={count - 1}"
        )
        messages = unwrap_list(body.get("list") if isinstance(body.get("list"), dict) else body, "message")
        if not messages:
            return None  # drained between the count and the fetch
        received = connector_received_seconds(messages[0], meta_data_id)
        if received is None:
            raise ValueError(f"queued message of channel {channel_id} destination {meta_data_id} has no received date")
        return received


def unwrap_list(container: Any, element: str) -> List[Dict[str, Any]]:
    """Jettison renders a list as {element: [..]} or, for a single item, {element: {..}}."""
    if not isinstance(container, dict):
        return []
    value = container.get(element)
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def parse_statistics(status: Dict[str, Any]) -> Dict[str, int]:
    """{"statistics":{"entry":[{"...Status":"RECEIVED","long":8}, ...]}} -> {"RECEIVED": 8, ...}"""
    out: Dict[str, int] = {}
    stats = status.get("statistics")
    if not isinstance(stats, dict):
        return out
    entries = stats.get("entry")
    if isinstance(entries, dict):
        entries = [entries]
    if not isinstance(entries, list):
        return out
    for e in entries:
        if isinstance(e, dict) and STATUS_KEY in e and "long" in e:
            try:
                out[str(e[STATUS_KEY])] = int(e["long"])
            except (TypeError, ValueError):
                continue
    return out


def to_int(value: Any) -> Optional[int]:
    """int(value), or None when Mirth sent something that is not a number."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def child_statuses(status: Dict[str, Any]) -> List[Dict[str, Any]]:
    return unwrap_list(status.get("childStatuses"), "dashboardStatus")


def date_seconds(value: Any) -> Optional[float]:
    """Mirth renders a Calendar as {"time": <epoch millis>, "timezone": ...}; accept a bare number too."""
    if isinstance(value, dict):
        value = value.get("time")
    millis = to_float(value)
    return None if millis is None else millis / 1000.0


def connector_received_seconds(message: Dict[str, Any], meta_data_id: str) -> Optional[float]:
    """Received date of one destination's connector message, falling back to the message's own received date.

    connectorMessages is {"entry": [{"int": 1, "connectorMessage": {...}}, ...]}, or an object for a single entry.
    """
    container = message.get("connectorMessages")
    entries = container.get("entry") if isinstance(container, dict) else None
    if isinstance(entries, dict):
        entries = [entries]
    if isinstance(entries, list):
        for entry in entries:
            node = entry.get("connectorMessage") if isinstance(entry, dict) else None
            if isinstance(node, dict) and str(node.get("metaDataId")) == str(meta_data_id):
                received = date_seconds(node.get("receivedDate"))
                if received is not None:
                    return received
    return date_seconds(message.get("receivedDate"))


class QueueAgeTracker:
    """Finds the oldest queued message of each destination that has a queue, off the scrape path.

    The status poll says which destinations hold queued messages; this tracker asks Mirth for the oldest one,
    at most ``max_lookups`` destinations per cycle (the ones observed longest ago first), and keeps the answer
    as a timestamp so the age stays right between refreshes. A failed lookup leaves the destination unknown.
    """

    def __init__(self, api: Any, interval: float, max_lookups: int, clock: Callable[[], float] = time.time):
        self.api = api
        self.interval = interval
        self.max_lookups = max(1, max_lookups)
        self.clock = clock
        self._lock = threading.Lock()
        self._targets: Set[Tuple[str, str]] = set()
        self._oldest: Dict[Tuple[str, str], Tuple[float, float]] = {}  # key -> (received seconds, observed at)
        self.errors = 0
        self.last_run: Optional[float] = None
        self._stop = threading.Event()

    def update_targets(self, targets: Iterable[Tuple[str, str]]) -> None:
        keep = set(targets)
        with self._lock:
            self._targets = keep
            for key in [k for k in self._oldest if k not in keep]:
                del self._oldest[key]

    def oldest_received(self, key: Tuple[str, str]) -> Optional[float]:
        """Epoch seconds of the oldest queued message, or None when unknown or the answer is too old to trust."""
        with self._lock:
            entry = self._oldest.get(key)
        if entry is None or self.clock() - entry[1] > 3 * self.interval:
            return None
        return entry[0]

    def run_once(self) -> None:
        with self._lock:
            order = sorted(self._targets, key=lambda k: self._oldest.get(k, (0.0, 0.0))[1])
        for key in order[: self.max_lookups]:
            try:
                received = self.api.oldest_queued_received(*key)
            except Exception as e:  # noqa: BLE001
                self.errors += 1
                log.warning("queue age lookup failed for channel %s destination %s: %s", key[0], key[1], e)
                continue
            with self._lock:
                if received is None:
                    self._oldest.pop(key, None)  # drained since the status poll
                elif key in self._targets:
                    self._oldest[key] = (received, self.clock())
        self.last_run = self.clock()

    def start(self) -> threading.Thread:
        def loop() -> None:
            while not self._stop.wait(self.interval):
                try:
                    self.run_once()
                except Exception:  # noqa: BLE001
                    log.exception("queue age cycle failed")

        thread = threading.Thread(target=loop, name="queue-age", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        self._stop.set()


class MirthStatsCollector:
    def __init__(self, api: MirthApi, instance: str, queue_age: Optional[QueueAgeTracker] = None,
                 clock: Callable[[], float] = time.time):
        self.api = api
        self.instance = instance
        self.queue_age = queue_age
        self.clock = clock

    def collect(self) -> Iterable[Any]:
        started = time.monotonic()
        up = GaugeMetricFamily("mirth_up", "1 when the Mirth REST API answered this scrape, else 0.", labels=["mirth_instance"])
        server_state = GaugeMetricFamily("mirth_server_state", "Legacy: 0 means up, 1 means down. Prefer mirth_up.", labels=["mirth_instance"])
        scrape_ok = GaugeMetricFamily("mirth_scrape_success", "1 when every API call in this scrape succeeded.", labels=["mirth_instance"])
        scrape_duration = GaugeMetricFamily("mirth_scrape_duration_seconds", "Wall time of this scrape.", labels=["mirth_instance"])
        channel_count = GaugeMetricFamily("mirth_channels_observed", "Channels returned by /channels/statuses in this scrape.", labels=["mirth_instance"])

        ch_labels = ["mirth_instance", "channelName", "channelId"]
        ch_received = CounterMetricFamily("mirth_messages_received", "Cumulative messages received by the channel since its statistics were last reset.", labels=ch_labels)
        ch_sent = CounterMetricFamily("mirth_messages_sent", "Cumulative messages sent by the channel since its statistics were last reset.", labels=ch_labels)
        ch_filtered = CounterMetricFamily("mirth_messages_filtered", "Cumulative messages filtered by the channel since its statistics were last reset.", labels=ch_labels)
        ch_errored = CounterMetricFamily("mirth_messages_errored", "Cumulative messages errored in the channel since its statistics were last reset.", labels=ch_labels)
        ch_queued = GaugeMetricFamily("mirth_messages_queued", "Messages currently queued across the channel's destinations.", labels=ch_labels)
        ch_state = GaugeMetricFamily("mirth_channel_state", "1 for the channel's current deployed state, 0 for every other state.", labels=ch_labels + ["state"])
        ch_started = GaugeMetricFamily("mirth_channel_started", "1 when the channel state is STARTED.", labels=ch_labels)

        dst_labels = ["mirth_instance", "channelName", "channelId", "destination", "metaDataId"]
        dst_received = CounterMetricFamily("mirth_destination_messages_received", "Cumulative messages received by the connector since its statistics were last reset.", labels=dst_labels)
        dst_sent = CounterMetricFamily("mirth_destination_messages_sent", "Cumulative messages sent by the connector since its statistics were last reset.", labels=dst_labels)
        dst_filtered = CounterMetricFamily("mirth_destination_messages_filtered", "Cumulative messages filtered by the connector since its statistics were last reset.", labels=dst_labels)
        dst_errored = CounterMetricFamily("mirth_destination_messages_errored", "Cumulative messages errored in the connector since its statistics were last reset.", labels=dst_labels)
        dst_queued = GaugeMetricFamily("mirth_destination_messages_queued", "Messages currently queued on the destination connector.", labels=dst_labels)
        dst_state = GaugeMetricFamily("mirth_destination_state", "1 for the connector's current state, 0 for every other state.", labels=dst_labels + ["state"])
        dst_started = GaugeMetricFamily("mirth_destination_started", "1 when the connector state is STARTED.", labels=dst_labels)
        dst_queue_enabled = GaugeMetricFamily("mirth_destination_queue_enabled", "1 when queuing is enabled on the destination connector.", labels=dst_labels)

        dst_queue_age = GaugeMetricFamily("mirth_destination_oldest_queued_age_seconds", "Age of the oldest message queued on the destination connector, 0 when its queue is empty. Absent when not observed.", labels=dst_labels)
        qa_errors = CounterMetricFamily("mirth_queue_age_lookup_errors", "Failed queue age lookups since the exporter started.", labels=["mirth_instance"])
        qa_last_run = GaugeMetricFamily("mirth_queue_age_last_run_timestamp_seconds", "Unix time the queue age task last finished a cycle.", labels=["mirth_instance"])
        disk_used = GaugeMetricFamily("mirth_server_size", "Disk used on the Mirth server volume, in bytes.", labels=["mirth_instance"])
        disk_free = GaugeMetricFamily("mirth_server_disk_free_bytes", "Disk free on the Mirth server volume, in bytes.", labels=["mirth_instance"])
        heap_used = GaugeMetricFamily("mirth_server_heap_used_bytes", "JVM heap allocated minus free, in bytes.", labels=["mirth_instance"])
        cpu = GaugeMetricFamily("mirth_server_cpu_usage_percent", "CPU usage reported by Mirth.", labels=["mirth_instance"])

        inst = [self.instance]
        ok = True
        try:
            statuses = self.api.channel_statuses()
        except Exception as e:  # noqa: BLE001
            log.warning("Mirth unreachable: %s", e)
            self.api.reset_session()
            up.add_metric(inst, 0)
            server_state.add_metric(inst, 1)
            scrape_ok.add_metric(inst, 0)
            scrape_duration.add_metric(inst, time.monotonic() - started)
            yield from (up, server_state, scrape_ok, scrape_duration)
            return

        up.add_metric(inst, 1)
        server_state.add_metric(inst, 0)
        channel_count.add_metric(inst, len(statuses))

        queued_targets: List[Tuple[str, str]] = []
        for st in statuses:
            try:
                self._collect_channel(
                    st, (ch_received, ch_sent, ch_filtered, ch_errored, ch_queued, ch_state, ch_started),
                    (dst_received, dst_sent, dst_filtered, dst_errored, dst_queued, dst_state, dst_started, dst_queue_enabled),
                    dst_queue_age if self.queue_age is not None else None,
                    queued_targets,
                )
            except Exception as e:  # noqa: BLE001
                # One malformed channel must not take the whole scrape down; its series are
                # simply absent (unknown) and the scrape is reported as not fully successful.
                log.warning("skipping malformed channel status %r: %s", st.get("channelId"), e)
                ok = False

        if self.queue_age is not None:
            self.queue_age.update_targets(queued_targets)
            qa_errors.add_metric(inst, self.queue_age.errors)
            if self.queue_age.last_run is not None:
                qa_last_run.add_metric(inst, self.queue_age.last_run)

        try:
            sys_stats = self.api.system_stats()
        except Exception as e:  # noqa: BLE001
            log.warning("system stats unavailable: %s", e)
            ok = False
        else:
            ok = self._emit_system(sys_stats, inst, disk_used, disk_free, heap_used, cpu) and ok

        scrape_ok.add_metric(inst, 1 if ok else 0)
        scrape_duration.add_metric(inst, time.monotonic() - started)

        yield from (
            up, server_state, scrape_ok, scrape_duration, channel_count,
            ch_received, ch_sent, ch_filtered, ch_errored, ch_queued, ch_state, ch_started,
            dst_received, dst_sent, dst_filtered, dst_errored, dst_queued, dst_state, dst_started, dst_queue_enabled,
            disk_used, disk_free, heap_used, cpu,
        )
        if self.queue_age is not None:
            yield from (dst_queue_age, qa_errors, qa_last_run)

    def _collect_channel(self, st: Dict[str, Any], ch: Any, dst: Any, queue_age: Any = None,
                         queued_targets: Optional[List[Tuple[str, str]]] = None) -> None:
        ch_received, ch_sent, ch_filtered, ch_errored, ch_queued, ch_state, ch_started = ch
        dst_received, dst_sent, dst_filtered, dst_errored, dst_queued, dst_state, dst_started, dst_queue_enabled = dst
        cid = str(st.get("channelId", ""))
        cname = str(st.get("name", ""))
        labels = [self.instance, cname, cid]
        self._emit_counters(parse_statistics(st), labels, ch_received, ch_sent, ch_filtered, ch_errored)
        queued = to_int(st.get("queued"))
        if queued is not None:
            ch_queued.add_metric(labels, queued)
        state = str(st.get("state", "UNKNOWN"))
        for s in CHANNEL_STATES:
            ch_state.add_metric(labels + [s], 1 if s == state else 0)
        ch_started.add_metric(labels, 1 if state == "STARTED" else 0)

        for child in child_statuses(st):
            meta = str(child.get("metaDataId", ""))
            dname = str(child.get("name", ""))
            dlabels = [self.instance, cname, cid, dname, meta]
            self._emit_counters(parse_statistics(child), dlabels, dst_received, dst_sent, dst_filtered, dst_errored)
            cqueued = to_int(child.get("queued"))
            if cqueued is not None:
                dst_queued.add_metric(dlabels, cqueued)
            if queue_age is not None and cqueued is not None:
                self._emit_queue_age(queue_age, dlabels, (cid, meta), cqueued, queued_targets)
            cstate = str(child.get("state", "UNKNOWN"))
            for s in CHANNEL_STATES:
                dst_state.add_metric(dlabels + [s], 1 if s == cstate else 0)
            dst_started.add_metric(dlabels, 1 if cstate == "STARTED" else 0)
            if child.get("queueEnabled") is not None:
                dst_queue_enabled.add_metric(dlabels, 1 if str(child["queueEnabled"]).lower() == "true" else 0)

    def _emit_queue_age(self, family: Any, dlabels: List[str], key: Tuple[str, str], queued: int,
                        targets: Optional[List[Tuple[str, str]]]) -> None:
        if queued <= 0:
            family.add_metric(dlabels, 0.0)
            return
        if targets is not None:
            targets.append(key)
        received = self.queue_age.oldest_received(key) if self.queue_age is not None else None
        if received is not None:
            family.add_metric(dlabels, max(0.0, self.clock() - received))

    @staticmethod
    def _emit_system(sys_stats: Dict[str, Any], inst: List[str], disk_used, disk_free, heap_used, cpu) -> bool:
        """Emit only what Mirth actually returned. Returns False when anything was missing or unreadable."""
        complete = True
        total = to_int(sys_stats.get("diskTotalBytes"))
        free = to_int(sys_stats.get("diskFreeBytes"))
        if free is not None:
            disk_free.add_metric(inst, free)
        else:
            complete = False
        if total is not None and free is not None:
            disk_used.add_metric(inst, total - free)
        else:
            complete = False
        alloc = to_int(sys_stats.get("allocatedMemoryBytes"))
        heap_free = to_int(sys_stats.get("freeMemoryBytes"))
        if alloc is not None and heap_free is not None:
            heap_used.add_metric(inst, alloc - heap_free)
        else:
            complete = False
        cpu_pct = to_float(sys_stats.get("cpuUsagePct"))
        if cpu_pct is not None:
            cpu.add_metric(inst, cpu_pct)
        else:
            complete = False
        return complete

    @staticmethod
    def _emit_counters(stats: Dict[str, int], labels: List[str], received, sent, filtered, errored) -> None:
        if "RECEIVED" in stats:
            received.add_metric(labels, stats["RECEIVED"])
        if "SENT" in stats:
            sent.add_metric(labels, stats["SENT"])
        if "FILTERED" in stats:
            filtered.add_metric(labels, stats["FILTERED"])
        if "ERROR" in stats:
            errored.add_metric(labels, stats["ERROR"])


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config()
    if not cfg["verifyTls"]:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    api = MirthApi(cfg)
    tracker = None
    if cfg["queueAgeEnabled"]:
        tracker = QueueAgeTracker(api, cfg["queueAgeIntervalSeconds"], cfg["queueAgeMaxLookupsPerCycle"])
        tracker.start()
    REGISTRY.register(MirthStatsCollector(api, cfg["instance"], tracker))
    for c in (GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR):
        try:
            REGISTRY.unregister(c)
        except KeyError:
            pass
    start_http_server(cfg["prometheusPort"])
    log.info("serving metrics on :%s for %s", cfg["prometheusPort"], api.base)
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
