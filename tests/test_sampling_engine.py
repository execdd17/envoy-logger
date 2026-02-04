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

        # Update inverter sample timestamp to be newer than last_sample_timestamp
        mock_inverter_sample.ts = datetime.now(tz=timezone.utc)

        # Second call: inverter interval has elapsed, should poll again
        _, inverter_data = sampling_engine.collect_samples_with_retry()
        self.assertEqual(inverter_data, mock_inverter_data)
        self.assertEqual(mock_envoy.get_inverter_data.call_count, 2)

    def test_default_intervals(self, mock_envoy):
        sampling_engine = SamplingEngineChildClass(envoy=mock_envoy)
        self.assertEqual(sampling_engine.interval_seconds, 60)
        self.assertEqual(sampling_engine.inverter_interval_seconds, 300)

    def test_custom_intervals(self, mock_envoy):
        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, interval_seconds=30, inverter_interval_seconds=120
        )
        self.assertEqual(sampling_engine.interval_seconds, 30)
        self.assertEqual(sampling_engine.inverter_interval_seconds, 120)

    def test_wait_for_next_cycle_boundary_case(self, mock_envoy):
        """Test that wait_for_next_cycle handles boundary cases correctly"""
        from unittest.mock import MagicMock, patch

        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, interval_seconds=60
        )

        # Mock datetime.now() to return a timestamp exactly divisible by 60
        with patch("envoy_logger.sampling_engine.datetime") as mock_datetime:
            # Create a datetime with timestamp exactly divisible by 60
            # Use a fixed timestamp: 1704067200 (2024-01-01 00:00:00 UTC) is divisible by 60
            mock_now = MagicMock()
            mock_now.timestamp.return_value = 1704067200.0  # Exactly divisible by 60
            mock_datetime.now.return_value = mock_now

            # Should wait a minimal amount (0.1s) instead of full interval
            with patch("time.sleep") as mock_sleep:
                sampling_engine.wait_for_next_cycle()
                mock_sleep.assert_called_once()
                # Should sleep approximately 0.1 seconds (not 60)
                call_args = mock_sleep.call_args[0][0]
                self.assertLess(
                    call_args, 1.0, "Should wait < 1s on boundary, not full interval"
                )

    def test_wait_for_next_cycle_normal_case(self, mock_envoy):
        """Test that wait_for_next_cycle calculates correct wait time"""
        from unittest.mock import MagicMock, patch

        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, interval_seconds=60
        )

        # Mock datetime.now() to return a timestamp 30 seconds into the interval
        with patch("envoy_logger.sampling_engine.datetime") as mock_datetime:
            # Use a base timestamp divisible by 60, then add 30 seconds
            base_timestamp = 1704067200.0  # 2024-01-01 00:00:00 UTC
            mock_now = MagicMock()
            mock_now.timestamp.return_value = base_timestamp + 30.0
            mock_datetime.now.return_value = mock_now

            with patch("time.sleep") as mock_sleep:
                sampling_engine.wait_for_next_cycle()
                mock_sleep.assert_called_once()
                # Should sleep approximately 30 seconds (60 - 30)
                call_args = mock_sleep.call_args[0][0]
                self.assertGreaterEqual(call_args, 29.0)
                self.assertLessEqual(call_args, 31.0)


if __name__ == "__main__":
    unittest.main()
