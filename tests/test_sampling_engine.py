import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from requests import ConnectTimeout

from envoy_logger.model import InverterSample, SampleData
from envoy_logger.sampling_engine import SamplingEngine


class SamplingEngineChildClass(SamplingEngine):
    def run(self) -> None:
        raise NotImplementedError("Not implemented")


@mock.patch("envoy_logger.envoy.Envoy")
class TestSamplingEngine(unittest.TestCase):
    def test_collect_samples_with_retry(self, mock_envoy):
        mock_sample_data = mock.Mock(SampleData)
        mock_inverter_sample = mock.Mock(InverterSample)
        mock_inverter_sample.ts = datetime.now(tz=timezone.utc)

        mock_inverter_data = {"foobar": mock_inverter_sample}

        mock_envoy.get_power_data.return_value = mock_sample_data
        mock_envoy.get_inverter_data.return_value = mock_inverter_data

        sampling_engine = SamplingEngineChildClass(envoy=mock_envoy)

        # First call polls both power and inverter data (last_inverter_poll is None)
        sample_data, inverter_data = sampling_engine.collect_samples_with_retry()

        self.assertEqual(sample_data, mock_sample_data)
        self.assertEqual(inverter_data, mock_inverter_data)

    def test_collect_samples_with_retry_timeout(self, mock_envoy):
        mock_envoy.get_power_data.side_effect = mock.Mock(
            side_effect=ConnectTimeout("foobar")
        )

        sampling_engine = SamplingEngineChildClass(envoy=mock_envoy)

        with self.assertRaises(TimeoutError) as ex:
            sampling_engine.collect_samples_with_retry(retries=3, wait_seconds=0.1)

        self.assertEqual(str(ex.exception), "Sample collection timed out.")

    def test_inverter_interval_skips_when_not_elapsed(self, mock_envoy):
        mock_sample_data = mock.Mock(SampleData)
        mock_inverter_sample = mock.Mock(InverterSample)
        mock_inverter_sample.ts = datetime.now(tz=timezone.utc)
        mock_inverter_data = {"foobar": mock_inverter_sample}

        mock_envoy.get_power_data.return_value = mock_sample_data
        mock_envoy.get_inverter_data.return_value = mock_inverter_data

        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, inverter_interval_seconds=300
        )

        # First call: inverters are polled (last_inverter_poll is None)
        _, inverter_data = sampling_engine.collect_samples_with_retry()
        self.assertEqual(inverter_data, mock_inverter_data)
        self.assertIsNotNone(sampling_engine.last_inverter_poll)

        # Second call: inverter interval hasn't elapsed, should get empty dict
        _, inverter_data = sampling_engine.collect_samples_with_retry()
        self.assertEqual(inverter_data, {})
        # get_inverter_data should have been called only once total
        self.assertEqual(mock_envoy.get_inverter_data.call_count, 1)

    def test_inverter_interval_polls_when_elapsed(self, mock_envoy):
        mock_sample_data = mock.Mock(SampleData)
        mock_inverter_sample = mock.Mock(InverterSample)
        mock_inverter_sample.ts = datetime.now(tz=timezone.utc)
        mock_inverter_data = {"foobar": mock_inverter_sample}

        mock_envoy.get_power_data.return_value = mock_sample_data
        mock_envoy.get_inverter_data.return_value = mock_inverter_data

        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, inverter_interval_seconds=300
        )

        # First call: inverters are polled
        sampling_engine.collect_samples_with_retry()

        # Simulate that the inverter interval has elapsed
        sampling_engine.last_inverter_poll = datetime.now(tz=timezone.utc) - timedelta(
            seconds=301
        )

        # Second call: inverter interval has elapsed, should poll again
        _, inverter_data = sampling_engine.collect_samples_with_retry()
        self.assertEqual(inverter_data, mock_inverter_data)
        self.assertEqual(mock_envoy.get_inverter_data.call_count, 2)

    def test_default_intervals(self, mock_envoy):
        sampling_engine = SamplingEngineChildClass(envoy=mock_envoy)
        self.assertEqual(sampling_engine.interval_seconds, 60)
        self.assertEqual(sampling_engine.inverter_interval_seconds, 300)


if __name__ == "__main__":
    unittest.main()
