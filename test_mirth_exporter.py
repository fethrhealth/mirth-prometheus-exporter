"""Offline tests using Jettison-shaped fixtures. Run: python -m unittest test_mirth_exporter"""

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
        self._sys = sys_stats or {"diskTotalBytes": 1000, "diskFreeBytes": 400, "allocatedMemoryBytes": 500, "freeMemoryBytes": 100, "cpuUsagePct": 12.5}
        self._fail = fail
        self.session = object()

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
        self.assertIn('mirth_destination_messages_sent_total{channelId="abc",channelName="ADT Outbound",destination="Billing",instance="mirth-test",metaDataId="2"} 3.0', out)
        self.assertIn('mirth_destination_messages_errored_total{channelId="abc",channelName="ADT Outbound",destination="Billing",instance="mirth-test",metaDataId="2"} 6.0', out)
        self.assertIn('mirth_destination_messages_queued{channelId="abc",channelName="ADT Outbound",destination="Billing",instance="mirth-test",metaDataId="2"} 42.0', out)
        self.assertIn('mirth_destination_started{channelId="abc",channelName="ADT Outbound",destination="Lab system",instance="mirth-test",metaDataId="1"} 1.0', out)
        self.assertIn('mirth_destination_started{channelId="abc",channelName="ADT Outbound",destination="Billing",instance="mirth-test",metaDataId="2"} 0.0', out)
        self.assertIn('mirth_destination_state{channelId="abc",channelName="ADT Outbound",destination="Billing",instance="mirth-test",metaDataId="2",state="STOPPED"} 1.0', out)

    def test_channel_level_names_stay_backward_compatible(self):
        out = scrape(FakeApi([channel("abc", "ADT Outbound", "STARTED", 5, [])]))
        self.assertIn("# TYPE mirth_messages_received_total counter", out)
        self.assertIn('mirth_messages_received_total{channelId="abc",channelName="ADT Outbound",instance="mirth-test"} 10.0', out)
        self.assertIn('mirth_messages_queued{channelId="abc",channelName="ADT Outbound",instance="mirth-test"} 5.0', out)
        self.assertIn('mirth_channel_started{channelId="abc",channelName="ADT Outbound",instance="mirth-test"} 1.0', out)
        self.assertIn('mirth_server_size{instance="mirth-test"} 600.0', out)
        self.assertIn('mirth_server_heap_used_bytes{instance="mirth-test"} 400.0', out)

    def test_single_child_object_is_handled(self):
        only = destination(1, "Only", "STARTED", queued=0, sent=1, error=0)
        out = scrape(FakeApi([channel("x", "One dest", "STARTED", 0, [only])]))
        self.assertIn('destination="Only"', out)

    def test_unreachable_emits_only_up_zero_and_no_channel_series(self):
        out = scrape(FakeApi([], fail=True))
        self.assertIn('mirth_up{instance="mirth-test"} 0.0', out)
        self.assertIn('mirth_server_state{instance="mirth-test"} 1.0', out)
        self.assertNotIn("mirth_messages_received_total{", out)
        self.assertNotIn("mirth_messages_queued{", out)

    def test_missing_statistics_emits_no_counter(self):
        st = {"channelId": "y", "name": "No stats", "state": "UNDEPLOYED"}
        out = scrape(FakeApi([st]))
        self.assertNotIn('mirth_messages_received_total{channelId="y"', out)
        self.assertNotIn('mirth_messages_queued{channelId="y"', out)
        self.assertIn('mirth_channel_state{channelId="y",channelName="No stats",instance="mirth-test",state="UNDEPLOYED"} 1.0', out)

    def test_unwrap_list_shapes(self):
        self.assertEqual(mx.unwrap_list({"a": [{"k": 1}]}, "a"), [{"k": 1}])
        self.assertEqual(mx.unwrap_list({"a": {"k": 1}}, "a"), [{"k": 1}])
        self.assertEqual(mx.unwrap_list(None, "a"), [])
        self.assertEqual(mx.unwrap_list({}, "a"), [])


if __name__ == "__main__":
    unittest.main()
