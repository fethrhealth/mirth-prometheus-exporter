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



class FakeClock:
    def __init__(self, now: float):
        self.now = now

    def __call__(self) -> float:
        return self.now


def message_with_queued_destination(meta, received_ms, single=False):
    entry = {"int": meta, "connectorMessage": {"metaDataId": meta, "status": "QUEUED", "receivedDate": {"time": received_ms, "timezone": "UTC"}}}
    return {"messageId": 7, "receivedDate": {"time": received_ms - 5000, "timezone": "UTC"},
            "connectorMessages": {"entry": entry if single else [{"int": 0, "connectorMessage": {"metaDataId": 0, "status": "TRANSFORMED", "receivedDate": {"time": received_ms - 5000}}}, entry]}}


def make_api(responses):
    api = mx.MirthApi({"instance": "h", "mirthPort": 1, "username": "u", "password": "p",
                       "verifyTls": False, "timeoutSeconds": 1, "includeUndeployed": True})
    api.calls = []

    def fake_get(path):
        api.calls.append(path)
        return responses.pop(0)

    api.get = fake_get
    return api


class QueueAgeApiTests(unittest.TestCase):
    def test_reads_the_last_row_of_the_queued_search_without_content(self):
        api = make_api([{"long": 3}, {"list": {"message": message_with_queued_destination(1, 1_700_000_000_000, single=True)}}])
        self.assertEqual(api.oldest_queued_received("chan-1", "1"), 1_700_000_000.0)
        self.assertEqual(api.calls[0], "/channels/chan-1/messages/count?status=QUEUED&includedMetaDataId=1")
        self.assertEqual(api.calls[1], "/channels/chan-1/messages?status=QUEUED&includedMetaDataId=1&includeContent=false&limit=1&offset=2")

    def test_empty_queue_needs_only_the_count(self):
        api = make_api([{"long": 0}])
        self.assertIsNone(api.oldest_queued_received("chan-1", "1"))
        self.assertEqual(len(api.calls), 1)

    def test_queue_drained_between_count_and_fetch_is_none(self):
        api = make_api([{"long": 2}, {"list": {}}])
        self.assertIsNone(api.oldest_queued_received("chan-1", "1"))

    def test_uses_the_destination_entry_in_a_list_of_connector_messages(self):
        api = make_api([{"long": 1}, {"list": {"message": [message_with_queued_destination(2, 1_700_000_100_000)]}}])
        self.assertEqual(api.oldest_queued_received("chan-1", "2"), 1_700_000_100.0)

    def test_falls_back_to_the_message_received_date(self):
        msg = {"messageId": 1, "receivedDate": {"time": 1_700_000_200_000}, "connectorMessages": {"entry": []}}
        api = make_api([{"long": 1}, {"list": {"message": msg}}])
        self.assertEqual(api.oldest_queued_received("chan-1", "1"), 1_700_000_200.0)

    def test_message_without_any_date_raises_instead_of_guessing(self):
        api = make_api([{"long": 1}, {"list": {"message": {"messageId": 1}}}])
        with self.assertRaises(ValueError):
            api.oldest_queued_received("chan-1", "1")

    def test_unexpected_count_payload_raises(self):
        api = make_api([{"unexpected": True}])
        with self.assertRaises(ValueError):
            api.oldest_queued_received("chan-1", "1")


class FakeQueueApi:
    def __init__(self, answers):
        self.answers = answers  # key -> float | None | Exception
        self.asked = []

    def oldest_queued_received(self, channel_id, meta_id):
        self.asked.append((channel_id, meta_id))
        answer = self.answers[(channel_id, meta_id)]
        if isinstance(answer, Exception):
            raise answer
        return answer


class QueueAgeTrackerTests(unittest.TestCase):
    def test_limits_lookups_per_cycle_and_asks_longest_unobserved_first(self):
        clock = FakeClock(1000.0)
        keys = [("c1", "1"), ("c2", "1"), ("c3", "1")]
        api = FakeQueueApi({k: 900.0 for k in keys})
        tracker = mx.QueueAgeTracker(api, interval=60, max_lookups=2, clock=clock)
        tracker.update_targets(keys)
        tracker.run_once()
        self.assertEqual(len(api.asked), 2)
        clock.now += 10
        api.asked.clear()
        tracker.run_once()
        self.assertEqual(api.asked[0], keys[2])  # the one never observed goes first

    def test_failed_lookup_stays_unknown_and_is_counted(self):
        api = FakeQueueApi({("c1", "1"): RuntimeError("boom")})
        tracker = mx.QueueAgeTracker(api, interval=60, max_lookups=5, clock=FakeClock(1000.0))
        tracker.update_targets([("c1", "1")])
        tracker.run_once()
        self.assertIsNone(tracker.oldest_received(("c1", "1")))
        self.assertEqual(tracker.errors, 1)
        self.assertEqual(tracker.last_run, 1000.0)

    def test_answers_expire_after_three_intervals(self):
        clock = FakeClock(1000.0)
        tracker = mx.QueueAgeTracker(FakeQueueApi({("c1", "1"): 900.0}), interval=60, max_lookups=5, clock=clock)
        tracker.update_targets([("c1", "1")])
        tracker.run_once()
        self.assertEqual(tracker.oldest_received(("c1", "1")), 900.0)
        clock.now += 181
        self.assertIsNone(tracker.oldest_received(("c1", "1")))

    def test_drained_queue_and_dropped_targets_are_forgotten(self):
        api = FakeQueueApi({("c1", "1"): 900.0, ("c2", "1"): 800.0})
        tracker = mx.QueueAgeTracker(api, interval=60, max_lookups=5, clock=FakeClock(1000.0))
        tracker.update_targets([("c1", "1"), ("c2", "1")])
        tracker.run_once()
        tracker.update_targets([("c1", "1")])
        self.assertIsNone(tracker.oldest_received(("c2", "1")))
        api.answers[("c1", "1")] = None
        tracker.run_once()
        self.assertIsNone(tracker.oldest_received(("c1", "1")))


class QueueAgeCollectorTests(unittest.TestCase):
    def collector_output(self, destinations, tracker=None, now=1000.0):
        statuses = [channel("abc", "ADT Outbound", "STARTED", 0, destinations)]
        reg = CollectorRegistry()
        reg.register(mx.MirthStatsCollector(FakeApi(statuses), "mirth-test", tracker, clock=FakeClock(now)))
        return generate_latest(reg).decode()

    def test_age_is_zero_when_the_dashboard_says_the_queue_is_empty(self):
        tracker = mx.QueueAgeTracker(FakeQueueApi({}), interval=60, max_lookups=5, clock=FakeClock(1000.0))
        out = self.collector_output([destination(1, "Lab", "STARTED", queued=0, sent=1, error=0)], tracker)
        self.assertIn('mirth_destination_oldest_queued_age_seconds{channelId="abc",channelName="ADT Outbound",destination="Lab",metaDataId="1",mirth_instance="mirth-test"} 0.0', out)

    def test_age_is_computed_from_the_cached_timestamp_at_scrape_time(self):
        clock = FakeClock(1000.0)
        tracker = mx.QueueAgeTracker(FakeQueueApi({("abc", "1"): 940.0}), interval=60, max_lookups=5, clock=clock)
        tracker.update_targets([("abc", "1")])
        tracker.run_once()
        clock.now = 1030.0
        reg = CollectorRegistry()
        reg.register(mx.MirthStatsCollector(FakeApi([channel("abc", "ADT Outbound", "STARTED", 5, [destination(1, "Lab", "STARTED", queued=5, sent=1, error=0)])]), "mirth-test", tracker, clock=clock))
        out = generate_latest(reg).decode()
        self.assertIn('destination="Lab",metaDataId="1",mirth_instance="mirth-test"} 90.0', out)

    def test_age_is_absent_when_a_queued_destination_has_not_been_observed(self):
        tracker = mx.QueueAgeTracker(FakeQueueApi({}), interval=60, max_lookups=5, clock=FakeClock(1000.0))
        out = self.collector_output([destination(1, "Lab", "STARTED", queued=5, sent=1, error=0)], tracker)
        self.assertNotIn("mirth_destination_oldest_queued_age_seconds{", out)
        self.assertIn("# TYPE mirth_destination_oldest_queued_age_seconds gauge", out)
        self.assertEqual(tracker._targets, {("abc", "1")})

    def test_nothing_is_emitted_when_the_tracker_is_disabled(self):
        out = self.collector_output([destination(1, "Lab", "STARTED", queued=0, sent=1, error=0)], None)
        self.assertNotIn("mirth_destination_oldest_queued_age_seconds", out)
        self.assertNotIn("mirth_queue_age_lookup_errors", out)

    def test_health_series_report_errors_and_the_last_cycle(self):
        clock = FakeClock(1000.0)
        tracker = mx.QueueAgeTracker(FakeQueueApi({("abc", "1"): RuntimeError("x")}), interval=60, max_lookups=5, clock=clock)
        tracker.update_targets([("abc", "1")])
        tracker.run_once()
        out = self.collector_output([destination(1, "Lab", "STARTED", queued=0, sent=1, error=0)], tracker)
        self.assertIn('mirth_queue_age_lookup_errors_total{mirth_instance="mirth-test"} 1.0', out)
        self.assertIn('mirth_queue_age_last_run_timestamp_seconds{mirth_instance="mirth-test"} 1000.0', out)


if __name__ == "__main__":
    unittest.main()
