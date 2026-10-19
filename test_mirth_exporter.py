"""Offline tests using Jettison-shaped fixtures. Run: python -m unittest test_mirth_exporter"""

import threading
import time
import unittest
from typing import Any, Dict, List

from prometheus_client.core import CollectorRegistry
from prometheus_client.exposition import generate_latest

import mirth_exporter as mx

STATUS = mx.STATUS_KEY


def stats(received=0, filtered=0, sent=0, error=0, queued=0) -> Dict[str, Any]:
    return {"entry": [
        {STATUS: "RECEIVED", "long": received},
        {STATUS: "FILTERED", "long": filtered},
        {STATUS: "SENT", "long": sent},
        {STATUS: "ERROR", "long": error},
        {STATUS: "QUEUED", "long": queued},
    ]}


def channel(cid, name, state, queued, children: List[Dict[str, Any]]):
    return {
        "channelId": cid, "name": name, "state": state, "queued": queued,
        "statistics": stats(received=10, sent=9, error=1),
        "childStatuses": {"dashboardStatus": children if len(children) != 1 else children[0]},
    }


def destination(meta, name, state, queued, sent, error, queue_enabled=True):
    return {
        "metaDataId": meta, "name": name, "state": state, "queued": queued, "queueEnabled": queue_enabled,
        "statistics": stats(received=10, sent=sent, error=error),
    }


class FakeApi:
    def __init__(self, statuses, sys_stats=None, fail=False):
        self._statuses = statuses
        self._sys = sys_stats if sys_stats is not None else {"diskTotalBytes": 1000, "diskFreeBytes": 400, "allocatedMemoryBytes": 500, "freeMemoryBytes": 100, "cpuUsagePct": 12.5}
        self._fail = fail
        self.session = object()

    def reset_session(self):
        self.session = None

    def channel_statuses(self):
        if self._fail:
            raise ConnectionError("boom")
        return self._statuses

    def system_stats(self):
        return self._sys


def scrape(api) -> str:
    reg = CollectorRegistry()
    reg.register(mx.MirthStatsCollector(api, "mirth-test"))
    return generate_latest(reg).decode()


class ExporterTests(unittest.TestCase):
    def test_destination_series_with_labels_and_counter_type(self):
        good = destination(1, "Lab system", "STARTED", queued=0, sent=9, error=0)
        bad = destination(2, "Billing", "STOPPED", queued=42, sent=3, error=6)
        out = scrape(FakeApi([channel("abc", "ADT Outbound", "STARTED", 42, [good, bad])]))
        self.assertIn("# TYPE mirth_destination_messages_sent_total counter", out)
        self.assertIn('mirth_destination_messages_sent_total{channelId="abc",channelName="ADT Outbound",destination="Billing",metaDataId="2",mirth_instance="mirth-test"} 3.0', out)
        self.assertIn('mirth_destination_messages_errored_total{channelId="abc",channelName="ADT Outbound",destination="Billing",metaDataId="2",mirth_instance="mirth-test"} 6.0', out)
        self.assertIn('mirth_destination_messages_queued{channelId="abc",channelName="ADT Outbound",destination="Billing",metaDataId="2",mirth_instance="mirth-test"} 42.0', out)
        self.assertIn('mirth_destination_started{channelId="abc",channelName="ADT Outbound",destination="Lab system",metaDataId="1",mirth_instance="mirth-test"} 1.0', out)
        self.assertIn('mirth_destination_started{channelId="abc",channelName="ADT Outbound",destination="Billing",metaDataId="2",mirth_instance="mirth-test"} 0.0', out)
        self.assertIn('mirth_destination_state{channelId="abc",channelName="ADT Outbound",destination="Billing",metaDataId="2",mirth_instance="mirth-test",state="STOPPED"} 1.0', out)

    def test_channel_level_names_stay_backward_compatible(self):
        out = scrape(FakeApi([channel("abc", "ADT Outbound", "STARTED", 5, [])]))
        self.assertIn("# TYPE mirth_messages_received_total counter", out)
        self.assertIn('mirth_messages_received_total{channelId="abc",channelName="ADT Outbound",mirth_instance="mirth-test"} 10.0', out)
        self.assertIn('mirth_messages_queued{channelId="abc",channelName="ADT Outbound",mirth_instance="mirth-test"} 5.0', out)
        self.assertIn('mirth_channel_started{channelId="abc",channelName="ADT Outbound",mirth_instance="mirth-test"} 1.0', out)
        self.assertIn('mirth_server_size{mirth_instance="mirth-test"} 600.0', out)
        self.assertIn('mirth_server_heap_used_bytes{mirth_instance="mirth-test"} 400.0', out)

    def test_single_child_object_is_handled(self):
        only = destination(1, "Only", "STARTED", queued=0, sent=1, error=0)
        out = scrape(FakeApi([channel("x", "One dest", "STARTED", 0, [only])]))
        self.assertIn('destination="Only"', out)

    def test_unreachable_emits_only_up_zero_and_no_channel_series(self):
        out = scrape(FakeApi([], fail=True))
        self.assertIn('mirth_up{mirth_instance="mirth-test"} 0.0', out)
        self.assertIn('mirth_server_state{mirth_instance="mirth-test"} 1.0', out)
        self.assertNotIn("mirth_messages_received_total{", out)
        self.assertNotIn("mirth_messages_queued{", out)

    def test_missing_statistics_emits_no_counter(self):
        st = {"channelId": "y", "name": "No stats", "state": "UNDEPLOYED"}
        out = scrape(FakeApi([st]))
        self.assertNotIn('mirth_messages_received_total{channelId="y"', out)
        self.assertNotIn('mirth_messages_queued{channelId="y"', out)
        self.assertIn('mirth_channel_state{channelId="y",channelName="No stats",mirth_instance="mirth-test",state="UNDEPLOYED"} 1.0', out)

    def test_missing_system_stats_are_omitted_not_zero(self):
        out = scrape(FakeApi([channel("abc", "ADT", "STARTED", 0, [])], sys_stats={}))
        self.assertNotIn("mirth_server_size{", out)
        self.assertNotIn("mirth_server_disk_free_bytes{", out)
        self.assertNotIn("mirth_server_heap_used_bytes{", out)
        self.assertNotIn("mirth_server_cpu_usage_percent{", out)
        self.assertIn('mirth_scrape_success{mirth_instance="mirth-test"} 0.0', out)
        self.assertIn('mirth_messages_queued{channelId="abc"', out)

    def test_partial_system_stats_emit_only_complete_values(self):
        out = scrape(FakeApi([], sys_stats={"diskTotalBytes": 1000, "diskFreeBytes": 400, "cpuUsagePct": "n/a"}))
        self.assertIn('mirth_server_size{mirth_instance="mirth-test"} 600.0', out)
        self.assertNotIn("mirth_server_heap_used_bytes{", out)
        self.assertNotIn("mirth_server_cpu_usage_percent{", out)
        self.assertIn('mirth_scrape_success{mirth_instance="mirth-test"} 0.0', out)

    def test_malformed_queued_value_does_not_fail_the_scrape(self):
        bad = channel("bad", "Broken", "STARTED", "oops", [destination(1, "D", "STARTED", queued="x", sent=1, error=0)])
        good = channel("good", "Fine", "STARTED", 7, [])
        out = scrape(FakeApi([bad, good]))
        self.assertIn('mirth_up{mirth_instance="mirth-test"} 1.0', out)
        self.assertNotIn('mirth_messages_queued{channelId="bad"', out)
        self.assertNotIn('mirth_destination_messages_queued{channelId="bad"', out)
        self.assertIn('mirth_messages_queued{channelId="good",channelName="Fine",mirth_instance="mirth-test"} 7.0', out)

    def test_unexpected_channel_shape_is_skipped_and_reported(self):
        class Boom(dict):
            def get(self, key, default=None):
                if key == "state":
                    raise RuntimeError("bad payload")
                return super().get(key, default)
        broken = Boom({"channelId": "z", "name": "Boom"})
        good = channel("good", "Fine", "STARTED", 7, [])
        out = scrape(FakeApi([broken, good]))
        self.assertIn('mirth_messages_queued{channelId="good"', out)
        self.assertIn('mirth_scrape_success{mirth_instance="mirth-test"} 0.0', out)

    def test_mirth_instance_label_does_not_collide_with_prometheus_instance(self):
        out = scrape(FakeApi([channel("abc", "ADT", "STARTED", 1, [])]))
        self.assertNotRegex(out, r'[{,]instance="')

    def test_concurrent_requests_log_in_once_and_never_see_a_reset_session(self):
        api = mx.MirthApi({"instance": "h", "mirthPort": 1, "username": "u", "password": "p",
                           "verifyTls": False, "timeoutSeconds": 1, "includeUndeployed": True})
        logins = []

        class Resp:
            status_code = 200
            def raise_for_status(self): pass
            def json(self): return {"ok": True}

        class Sess:
            def get(self, *a, **k):
                time.sleep(0.01)
                return Resp()

        def fake_login():
            logins.append(1)
            time.sleep(0.05)
            api.session = Sess()
            return api.session

        api._login = fake_login
        errors = []

        def worker(i):
            try:
                api.get("/x")
                if i % 3 == 0:
                    api.reset_session()
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(9)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(errors, [])
        self.assertEqual(logins[0], 1)

    def test_unwrap_list_shapes(self):
        self.assertEqual(mx.unwrap_list({"a": [{"k": 1}]}, "a"), [{"k": 1}])
        self.assertEqual(mx.unwrap_list({"a": {"k": 1}}, "a"), [{"k": 1}])
        self.assertEqual(mx.unwrap_list(None, "a"), [])
        self.assertEqual(mx.unwrap_list({}, "a"), [])


if __name__ == "__main__":
    unittest.main()
