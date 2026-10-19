"""Prometheus exporter for Mirth Connect.

Reads the dashboard status endpoint (GET /channels/statuses) so that every
destination connector becomes its own series, and declares the cumulative
message statistics as counters so Prometheus can detect statistics resets.

When Mirth cannot be reached the exporter emits only ``mirth_up 0``; it never
emits a zero for a channel it could not observe. Dashboards must treat an
absent series as unknown, not healthy. The same rule applies to the system stats:
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
from typing import Any, Dict, Iterable, List, Optional

import requests
import urllib3
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


class MirthStatsCollector:
    def __init__(self, api: MirthApi, instance: str):
        self.api = api
        self.instance = instance

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

        for st in statuses:
            try:
                self._collect_channel(
                    st, (ch_received, ch_sent, ch_filtered, ch_errored, ch_queued, ch_state, ch_started),
                    (dst_received, dst_sent, dst_filtered, dst_errored, dst_queued, dst_state, dst_started, dst_queue_enabled),
                )
            except Exception as e:  # noqa: BLE001
                # One malformed channel must not take the whole scrape down; its series are
                # simply absent (unknown) and the scrape is reported as not fully successful.
                log.warning("skipping malformed channel status %r: %s", st.get("channelId"), e)
                ok = False

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

    def _collect_channel(self, st: Dict[str, Any], ch: Any, dst: Any) -> None:
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
            cstate = str(child.get("state", "UNKNOWN"))
            for s in CHANNEL_STATES:
                dst_state.add_metric(dlabels + [s], 1 if s == cstate else 0)
            dst_started.add_metric(dlabels, 1 if cstate == "STARTED" else 0)
            if child.get("queueEnabled") is not None:
                dst_queue_enabled.add_metric(dlabels, 1 if str(child["queueEnabled"]).lower() == "true" else 0)

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
    REGISTRY.register(MirthStatsCollector(api, cfg["instance"]))
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
