import unittest
from datetime import datetime, timezone
from unittest import mock

from influxdb_client.client.flux_table import FluxTable, TableList
from tests.sample_data import (
    create_inverter_data,
    create_production_only_sample_data,
    create_sample_data,
)

from envoy_logger.influxdb_sampling_engine import InfluxdbSamplingEngine
from envoy_logger.model import SampleData, parse_inverter_data


def _power_daily_flux_records():
    """Records as returned by Flux for power-only daily query (no inverter)."""
    from influxdb_client.client.flux_table import FluxRecord

    return [
        FluxRecord({}, {"measurement-type": "consumption", "line-idx": 0, "_value": 100.0}),
        FluxRecord({}, {"measurement-type": "net", "line-idx": 0, "_value": 50.0}),
        FluxRecord({}, {"measurement-type": "production", "line-idx": 0, "_value": 200.0}),
    ]


def _inverter_daily_flux_records():
    """Records as returned by Flux for inverter daily query."""
    from influxdb_client.client.flux_table import FluxRecord

    return [
        FluxRecord({}, {"measurement-type": "inverter", "serial": "foobar", "_value": 50.0}),
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
        self.assertGreater(len(points), 0)
        # consumption + production + net, each with 3 lines = 9 points
        self.assertEqual(len(points), 9)

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


if __name__ == "__main__":
    unittest.main()
