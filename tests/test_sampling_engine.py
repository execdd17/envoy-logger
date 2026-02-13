import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from envoy_logger.model import InverterSample, SampleData
from envoy_logger.sampling_engine import SamplingEngine


class SamplingEngineChildClass(SamplingEngine):
    def run(self) -> None:
        raise NotImplementedError("Not implemented")


@mock.patch("envoy_logger.envoy.Envoy")
class TestSamplingEngine(unittest.TestCase):
    def test_get_power_data_returns_envoy_result(self, mock_envoy):
        mock_sample_data = mock.Mock(SampleData)
        mock_envoy.get_power_data.return_value = mock_sample_data

        sampling_engine = SamplingEngineChildClass(envoy=mock_envoy)
        result = sampling_engine.get_power_data()

        self.assertEqual(result, mock_sample_data)
        mock_envoy.get_power_data.assert_called_once()

    def test_get_inverter_data_returns_raw_envoy_result(self, mock_envoy):
        mock_inverter_sample = mock.Mock(InverterSample)
        mock_inverter_data = {"foobar": mock_inverter_sample}
        mock_envoy.get_inverter_data.return_value = mock_inverter_data

        sampling_engine = SamplingEngineChildClass(envoy=mock_envoy)
        result = sampling_engine.get_inverter_data()

        self.assertEqual(result, mock_inverter_data)
        mock_envoy.get_inverter_data.assert_called_once()

    def test_should_poll_inverters_true_when_never_polled(self, mock_envoy):
        sampling_engine = SamplingEngineChildClass(envoy=mock_envoy)
        self.assertIsNone(sampling_engine.last_inverter_poll)
        self.assertTrue(sampling_engine._should_poll_inverters())

    def test_should_poll_inverters_false_when_interval_not_elapsed(self, mock_envoy):
        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, inverter_interval_seconds=300
        )
        sampling_engine.last_inverter_poll = datetime.now(tz=timezone.utc)
        self.assertFalse(sampling_engine._should_poll_inverters())

    def test_should_poll_inverters_true_when_interval_elapsed(self, mock_envoy):
        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, inverter_interval_seconds=300
        )
        sampling_engine.last_inverter_poll = datetime.now(tz=timezone.utc) - timedelta(
            seconds=301
        )
        self.assertTrue(sampling_engine._should_poll_inverters())

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
        """wait_for_next_cycle(interval) sleeps ~0.1s when now is on boundary."""
        from unittest.mock import MagicMock, patch

        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, interval_seconds=60
        )

        with patch("envoy_logger.sampling_engine.datetime") as mock_datetime:
            mock_now = MagicMock()
            mock_now.timestamp.return_value = 1704067200.0  # divisible by 60
            mock_datetime.now.return_value = mock_now

            with patch("time.sleep") as mock_sleep:
                sampling_engine.wait_for_next_cycle(60)
                mock_sleep.assert_called_once()
                call_args = mock_sleep.call_args[0][0]
                self.assertLess(
                    call_args, 1.0, "Should wait < 1s on boundary, not full interval"
                )

    def test_wait_for_next_cycle_normal_case(self, mock_envoy):
        """wait_for_next_cycle(interval) sleeps until next boundary."""
        from unittest.mock import MagicMock, patch

        sampling_engine = SamplingEngineChildClass(
            envoy=mock_envoy, interval_seconds=60
        )

        with patch("envoy_logger.sampling_engine.datetime") as mock_datetime:
            base_timestamp = 1704067200.0
            mock_now = MagicMock()
            mock_now.timestamp.return_value = base_timestamp + 30.0
            mock_datetime.now.return_value = mock_now

            with patch("time.sleep") as mock_sleep:
                sampling_engine.wait_for_next_cycle(60)
                mock_sleep.assert_called_once()
                call_args = mock_sleep.call_args[0][0]
                self.assertGreaterEqual(call_args, 29.0)
                self.assertLessEqual(call_args, 31.0)


if __name__ == "__main__":
    unittest.main()
