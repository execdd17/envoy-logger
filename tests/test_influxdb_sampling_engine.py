import threading
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import pytest
import requests
from influxdb_client.client.flux_table import FluxTable, TableList
from tests.sample_data import (
    create_inverter_data,
    create_production_only_sample_data,
    create_sample_data,
)

from envoy_logger.influxdb_sampling_engine import InfluxdbSamplingEngine
from envoy_logger.model import SampleData, parse_inverter_data


def _inverter_payload(serial: str, report_time: datetime, watts: int = 123) -> dict:
    """Inverter payload with a specific lastReportDate for filtering tests."""
    return {
        "serialNumber": serial,
        "lastReportDate": report_time.timestamp(),
        "lastReportWatts": watts,
    }


class _LoopDone(BaseException):
    """Raised to stop the inverter loop after one cycle in tests (not caught by except Exception)."""


def _power_daily_flux_records():
    """Records as returned by Flux for power-only daily query (no inverter)."""
    from influxdb_client.client.flux_table import FluxRecord

    return [
        FluxRecord(
            {}, {"measurement-type": "consumption", "line-idx": 0, "_value": 100.0}
        ),
        FluxRecord({}, {"measurement-type": "net", "line-idx": 0, "_value": 50.0}),
        FluxRecord(
            {}, {"measurement-type": "production", "line-idx": 0, "_value": 200.0}
        ),
    ]


def _inverter_daily_flux_records():
    """Records as returned by Flux for inverter daily query."""
    from influxdb_client.client.flux_table import FluxRecord

    return [
        FluxRecord(
            {}, {"measurement-type": "inverter", "serial": "foobar", "_value": 50.0}
        ),
    ]


@mock.patch("influxdb_client.client.query_api.QueryApi")
@mock.patch("envoy_logger.influxdb_sampling_engine.InfluxDBClient")
@mock.patch("envoy_logger.envoy.Envoy")
@mock.patch("envoy_logger.config.Config")
class TestInfluxdbSamplingEngine(unittest.TestCase):
    def test_power_high_rate_points(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        test_sample_data = SampleData.create(sample_data=create_sample_data())
        mock_envoy.get_power_data.return_value = test_sample_data

        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        points = sampling_engine._power_high_rate_points(test_sample_data)

        self.assertIsInstance(points, list)
        # consumption + production + net, each with 3 lines = 9 points
        self.assertEqual(len(points), 9)
        # Assert point shape matches what we write to InfluxDB (tags and field P)
        first = points[0]
        self.assertEqual(first._name.split("-")[0], "consumption")
        self.assertEqual(first._tags["source"], "envoy")
        self.assertIn(
            first._tags["measurement-type"], ("consumption", "production", "net")
        )
        self.assertIn("P", first._fields)

    def test_inverter_high_rate_points(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        test_inverter_data = parse_inverter_data([create_inverter_data("inv1")])
        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        points = sampling_engine._inverter_high_rate_points(test_inverter_data)

        self.assertEqual(len(points), 1)
        self.assertEqual(points[0]._tags["serial"], "inv1")
        self.assertEqual(points[0]._tags["measurement-type"], "inverter")
        self.assertEqual(points[0]._fields["P"], 123)

    def test_compute_power_daily_Wh_points(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        mock_table = mock.Mock(FluxTable)
        mock_table.records = _power_daily_flux_records()
        mock_query_api.query.return_value = TableList([mock_table])

        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        sampling_engine.influxdb_query_api = mock_query_api

        ts = datetime.now(tz=timezone.utc)
        points = sampling_engine._compute_power_daily_Wh_points(ts)

        self.assertEqual(len(points), 3)
        mock_query_api.query.assert_called_once()

    def test_compute_power_daily_Wh_points_when_flux_returns_empty_returns_empty_list(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When the daily power Flux query returns no rows, we return no points (no write on rollover)."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        mock_query_api.query.return_value = TableList([])

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api

        ts = datetime.now(tz=timezone.utc)
        points = engine._compute_power_daily_Wh_points(ts)

        self.assertEqual(points, [])
        mock_query_api.query.assert_called_once()

    def test_compute_inverter_daily_Wh_points(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {"foobar": {}, "unreported_serial": {}}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_table = mock.Mock(FluxTable)
        mock_table.records = _inverter_daily_flux_records()
        mock_query_api.query.return_value = TableList([mock_table])

        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        sampling_engine.influxdb_query_api = mock_query_api

        ts = datetime.now(tz=timezone.utc)
        points = sampling_engine._compute_inverter_daily_Wh_points(ts)

        # One from Flux (foobar) + one 0 Wh for unreported_serial
        self.assertEqual(len(points), 2)
        mock_query_api.query.assert_called_once()

    def test_compute_inverter_daily_Wh_points_when_flux_returns_empty_still_returns_zero_wh_for_configured(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When daily inverter Flux returns no rows, we still emit 0 Wh for each configured inverter."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {"inv1": {}, "inv2": {}}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_query_api.query.return_value = TableList([])

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api

        ts = datetime.now(tz=timezone.utc)
        points = engine._compute_inverter_daily_Wh_points(ts)

        self.assertEqual(len(points), 2)
        serials = {p._tags["serial"] for p in points}
        self.assertEqual(serials, {"inv1", "inv2"})
        for p in points:
            self.assertEqual(p._fields["Wh"], 0.0)
        mock_query_api.query.assert_called_once()

    def test_compute_inverter_daily_Wh_points_when_flux_empty_and_no_configured_inverters_returns_empty(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When Flux returns no rows and config.inverters is empty, we return no points."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_query_api.query.return_value = TableList([])

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api

        ts = datetime.now(tz=timezone.utc)
        points = engine._compute_inverter_daily_Wh_points(ts)

        self.assertEqual(points, [])

    def test_power_day_rollover_when_same_date_does_not_query_or_write(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When power_todays_date is already today, rollover does nothing (no Flux query, no write)."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        write_api = engine.influxdb_write_api
        # power_todays_date is set in __init__ to date.today(), so same day
        ts = datetime.now(tz=timezone.utc)

        engine._power_day_rollover(ts)

        mock_query_api.query.assert_not_called()
        write_api.write.assert_not_called()

    def test_power_day_rollover_when_date_changed_queries_and_writes_to_bucket_lr(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When power_todays_date is in the past, rollover runs Flux query and writes daily points to bucket_lr."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        mock_table = mock.Mock(FluxTable)
        mock_table.records = _power_daily_flux_records()
        mock_query_api.query.return_value = TableList([mock_table])

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        engine.influxdb_write_api = (
            mock_influxdb_client.return_value.write_api.return_value
        )
        engine.power_todays_date = date.today() - timedelta(days=1)
        ts = datetime.now(tz=timezone.utc)

        engine._power_day_rollover(ts)

        mock_query_api.query.assert_called_once()
        engine.influxdb_write_api.write.assert_called_once()
        call_kw = engine.influxdb_write_api.write.call_args[1]
        self.assertEqual(call_kw["bucket"], "foobar_lr")
        self.assertEqual(len(call_kw["record"]), 3)

    def test_power_day_rollover_when_compute_returns_empty_does_not_write(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When daily Flux returns no rows, compute returns []; rollover must not call write."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        mock_query_api.query.return_value = TableList([])

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        write_api = engine.influxdb_write_api
        engine.power_todays_date = date.today() - timedelta(days=1)
        ts = datetime.now(tz=timezone.utc)

        engine._power_day_rollover(ts)

        mock_query_api.query.assert_called_once()
        write_api.write.assert_not_called()

    def test_inverter_day_rollover_when_same_date_does_not_query_or_write(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When inverter_todays_date is already today, rollover does nothing (no Flux query, no write)."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        write_api = engine.influxdb_write_api

        engine._inverter_day_rollover()

        mock_query_api.query.assert_not_called()
        write_api.write.assert_not_called()

    def test_inverter_day_rollover_when_date_changed_queries_and_writes_to_bucket_lr(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When inverter_todays_date is in the past, rollover runs Flux query and writes daily points to bucket_lr."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {"foobar": {}, "unreported": {}}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_table = mock.Mock(FluxTable)
        mock_table.records = _inverter_daily_flux_records()
        mock_query_api.query.return_value = TableList([mock_table])

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        engine.influxdb_write_api = (
            mock_influxdb_client.return_value.write_api.return_value
        )
        engine.inverter_todays_date = date.today() - timedelta(days=1)

        engine._inverter_day_rollover()

        mock_query_api.query.assert_called_once()
        engine.influxdb_write_api.write.assert_called_once()
        call_kw = engine.influxdb_write_api.write.call_args[1]
        self.assertEqual(call_kw["bucket"], "foobar_lr")
        self.assertEqual(len(call_kw["record"]), 2)

    def test_inverter_day_rollover_when_compute_returns_empty_does_not_write(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When daily inverter Flux returns no rows and no configured inverters, compute returns []; no write."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_query_api.query.return_value = TableList([])

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        write_api = engine.influxdb_write_api
        engine.inverter_todays_date = date.today() - timedelta(days=1)

        engine._inverter_day_rollover()

        mock_query_api.query.assert_called_once()
        write_api.write.assert_not_called()

    def test_production_only_power_points(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """Production-only sample data still produces power points for production lines."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        test_sample_data = SampleData.create(
            sample_data=create_production_only_sample_data()
        )
        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        points = sampling_engine._power_high_rate_points(test_sample_data)

        # Only production has lines (3)
        self.assertEqual(len(points), 3)

    def test_custom_polling_intervals(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """Custom polling intervals from config are used."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 30
        mock_config.inverter_polling_interval = 120

        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)

        self.assertEqual(sampling_engine.interval_seconds, 30)
        self.assertEqual(sampling_engine.inverter_interval_seconds, 120)

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_power_loop_when_write_raises_skips_cycle_continues(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When write() raises (e.g. InfluxDB down), the power loop catches, skips the cycle, and continues."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        test_sample_data = SampleData.create(sample_data=create_sample_data())
        mock_envoy.get_power_data.return_value = test_sample_data
        write_api = mock_influxdb_client.return_value.write_api.return_value
        write_api.write.side_effect = Exception("InfluxDB write failed")

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.wait_for_next_cycle = mock.Mock(
            side_effect=[None, None, _LoopDone("exit after two cycles")]
        )

        thread = threading.Thread(target=engine._power_loop)
        thread.start()
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive())
        self.assertGreaterEqual(write_api.write.call_count, 1)
        self.assertEqual(write_api.write.call_args[1]["bucket"], "foobar_hr")

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_power_loop_when_get_power_data_raises_skips_cycle_no_write(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When get_power_data raises (e.g. Envoy unreachable), the power loop skips the cycle and does not write."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        mock_envoy.get_power_data.side_effect = requests.exceptions.ConnectionError(
            "Envoy unreachable"
        )
        write_api = mock_influxdb_client.return_value.write_api.return_value

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.wait_for_next_cycle = mock.Mock(
            side_effect=[None, _LoopDone("exit after one cycle")]
        )

        thread = threading.Thread(target=engine._power_loop)
        thread.start()
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive(), "power loop should have exited")

        write_api.write.assert_not_called()

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_inverter_loop_when_get_inverter_data_raises_skips_cycle_no_write(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When get_inverter_data raises (e.g. Envoy unreachable), inverter loop skips cycle and does not write."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_query_api.query.return_value = TableList([])
        mock_envoy.get_inverter_data.side_effect = requests.exceptions.Timeout(
            "Envoy timeout"
        )
        write_api = mock_influxdb_client.return_value.write_api.return_value

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        engine.wait_for_next_cycle = mock.Mock(
            side_effect=[None, _LoopDone("exit after one cycle")]
        )

        thread = threading.Thread(target=engine._inverter_loop)
        thread.start()
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive(), "inverter loop should have exited")

        write_api.write.assert_not_called()

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_inverter_loop_when_write_raises_skips_cycle_continues(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When write() raises (e.g. InfluxDB down), the inverter loop catches, skips the cycle, and continues."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_query_api.query.return_value = TableList([])
        mock_envoy.get_inverter_data.return_value = parse_inverter_data(
            [create_inverter_data("inv1")]
        )
        write_api = mock_influxdb_client.return_value.write_api.return_value
        write_api.write.side_effect = Exception("InfluxDB write failed")

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        engine.wait_for_next_cycle = mock.Mock(
            side_effect=[None, None, _LoopDone("exit after two cycles")]
        )

        thread = threading.Thread(target=engine._inverter_loop)
        thread.start()
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive())
        # At least one cycle attempted a write (it raised); loop continued and exited on _LoopDone.
        self.assertGreaterEqual(write_api.write.call_count, 1)
        self.assertEqual(write_api.write.call_args[1]["bucket"], "foobar_hr")

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_inverter_loop_when_flux_query_raises_writes_nothing(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When the Flux query for last inverter timestamp raises, the cycle is skipped and no dupes are written."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_query_api.query.side_effect = Exception("Flux query failed")
        mock_envoy.get_inverter_data.return_value = parse_inverter_data(
            [create_inverter_data("inv1")]
        )

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        write_api = engine.influxdb_write_api

        # First call returns (run one cycle); second call raises to exit the loop
        engine.wait_for_next_cycle = mock.Mock(
            side_effect=[None, _LoopDone("exit after one cycle")]
        )

        thread = threading.Thread(target=engine._inverter_loop)
        thread.start()
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive(), "inverter loop should have exited")

        write_api.write.assert_not_called()

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_inverter_loop_when_flux_query_returns_cutoff_filters_and_writes_only_new(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When the Flux query returns a cutoff timestamp, only inverter data newer than cutoff is written."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=1)
        # One inverter before cutoff, one after; only the one after should be written
        raw_inverters = [
            _inverter_payload("inv_old", cutoff - timedelta(minutes=1)),
            _inverter_payload("inv_new", cutoff + timedelta(minutes=1)),
        ]
        mock_envoy.get_inverter_data.return_value = parse_inverter_data(raw_inverters)

        # Query returns the cutoff so filter keeps only inv_new; second call is day-rollover Flux
        mock_record = mock.Mock()
        mock_record.get_time.return_value = cutoff
        mock_table = mock.Mock(FluxTable)
        mock_table.records = [mock_record]
        mock_query_api.query.side_effect = [
            TableList([mock_table]),
            TableList([]),  # day-rollover query returns no rows
        ]

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        write_api = engine.influxdb_write_api
        engine.wait_for_next_cycle = mock.Mock(
            side_effect=[None, _LoopDone("exit after one cycle")]
        )

        thread = threading.Thread(target=engine._inverter_loop)
        thread.start()
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive(), "inverter loop should have exited")

        write_api.write.assert_called_once()
        call_kw = write_api.write.call_args[1]
        self.assertIn("record", call_kw)
        points = call_kw["record"]
        self.assertEqual(
            len(points), 1, "only the inverter newer than cutoff should be written"
        )
        self.assertEqual(call_kw["bucket"], "foobar_hr")
        # Assert the written point is inv_new, not inv_old (filter applied correctly)
        self.assertEqual(points[0]._tags["serial"], "inv_new")
        self.assertEqual(points[0]._name, "inverter-production-inv_new")

    @pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
    def test_inverter_loop_when_flux_query_returns_empty_writes_all_inverters(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """When the Flux query returns no rows (e.g. first run), no filter is applied; all inverter data is written."""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300
        mock_config.apply_tags_to_inverter_point = mock.Mock()

        mock_query_api.query.return_value = TableList([])
        mock_envoy.get_inverter_data.return_value = parse_inverter_data(
            [
                create_inverter_data("inv1"),
                create_inverter_data("inv2"),
            ]
        )

        engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        engine.influxdb_query_api = mock_query_api
        write_api = engine.influxdb_write_api
        engine.wait_for_next_cycle = mock.Mock(
            side_effect=[None, _LoopDone("exit after one cycle")]
        )

        thread = threading.Thread(target=engine._inverter_loop)
        thread.start()
        thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive(), "inverter loop should have exited")

        write_api.write.assert_called_once()
        call_kw = write_api.write.call_args[1]
        self.assertIn("record", call_kw)
        points = call_kw["record"]
        self.assertEqual(
            len(points), 2, "all inverters should be written when query returns nothing"
        )
        self.assertEqual(call_kw["bucket"], "foobar_hr")


if __name__ == "__main__":
    unittest.main()
