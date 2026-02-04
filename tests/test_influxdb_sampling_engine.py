import unittest
from datetime import date
from unittest import mock

from influxdb_client.client.flux_table import FluxTable, TableList
from tests.sample_data import (
    create_influxdb_records,
    create_inverter_data,
    create_production_only_sample_data,
    create_sample_data,
)

from envoy_logger.influxdb_sampling_engine import InfluxdbSamplingEngine
from envoy_logger.model import SampleData, parse_inverter_data


@mock.patch("influxdb_client.client.query_api.QueryApi")
@mock.patch("envoy_logger.influxdb_sampling_engine.InfluxDBClient")
@mock.patch("envoy_logger.envoy.Envoy")
@mock.patch("envoy_logger.config.Config")
class TestInfluxdbSamplingEngine(unittest.TestCase):
    def test_collect_samples(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        mock_flux_table = mock.Mock(FluxTable)
        mock_flux_table.records = create_influxdb_records()
        test_table_list = TableList()
        test_table_list.append(mock_flux_table)
        mock_query_api.query.return_value = test_table_list

        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {"foo": {}, "bar": {}}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        test_sample_data = SampleData.create(sample_data=create_sample_data())
        test_inverter_data = parse_inverter_data([create_inverter_data("foobarA")])

        mock_envoy.get_power_data.return_value = test_sample_data
        mock_envoy.get_inverter_data.return_value = test_inverter_data

        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        sampling_engine.influxdb_query_api = mock_query_api

        # Collect first (missing inverter data)
        sampling_engine._collect_samples()

        # Collect again (now has new inverter data)
        test_inverter_data = parse_inverter_data([create_inverter_data("foobarB")])
        mock_envoy.get_inverter_data.return_value = test_inverter_data

        sampling_engine._collect_samples()

        # This has to be called directly since the date calculations in _low_rate_points prohibit us
        # from hitting it indirectly
        sampling_engine._compute_daily_Wh_points(date.today())

    def test_collect_production_only_samples(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        mock_flux_table = mock.Mock(FluxTable)
        mock_flux_table.records = create_influxdb_records()
        test_table_list = TableList()
        test_table_list.append(mock_flux_table)
        mock_query_api.query.return_value = test_table_list

        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {"foo": {}, "bar": {}}
        mock_config.polling_interval = 60
        mock_config.inverter_polling_interval = 300

        test_sample_data = SampleData.create(
            sample_data=create_production_only_sample_data()
        )
        test_inverter_data = parse_inverter_data([create_inverter_data("foobarA")])

        mock_envoy.get_power_data.return_value = test_sample_data
        mock_envoy.get_inverter_data.return_value = test_inverter_data

        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)
        sampling_engine.influxdb_query_api = mock_query_api

        # Collect first (missing inverter data)
        sampling_engine._collect_samples()

        # Collect again (now has new inverter data)
        test_inverter_data = parse_inverter_data([create_inverter_data("foobarB")])
        mock_envoy.get_inverter_data.return_value = test_inverter_data

        sampling_engine._collect_samples()

        # This has to be called directly since the date calculations in _low_rate_points prohibit us
        # from hitting it indirectly
        sampling_engine._compute_daily_Wh_points(date.today())

    def test_custom_polling_intervals(
        self,
        mock_config,
        mock_envoy,
        mock_influxdb_client,
        mock_query_api,
    ):
        """Test that custom polling intervals from config are used"""
        mock_config.influxdb_bucket_hr = "foobar_hr"
        mock_config.influxdb_bucket_lr = "foobar_lr"
        mock_config.source_tag = "envoy"
        mock_config.inverters = {}
        mock_config.polling_interval = 30
        mock_config.inverter_polling_interval = 120

        sampling_engine = InfluxdbSamplingEngine(envoy=mock_envoy, config=mock_config)

        # Verify that the custom intervals are set
        self.assertEqual(sampling_engine.interval_seconds, 30)
        self.assertEqual(sampling_engine.inverter_interval_seconds, 120)


if __name__ == "__main__":
    unittest.main()
