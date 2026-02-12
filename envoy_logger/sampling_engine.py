import logging
import ssl
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, Optional

from requests import ConnectTimeout, ReadTimeout
from requests.exceptions import RequestException

from envoy_logger.envoy import Envoy
from envoy_logger.model import InverterSample, SampleData, filter_new_inverter_data

LOG = logging.getLogger("sampling_engine")


class SamplingEngine(ABC):
    last_sample_timestamp: Optional[datetime] = None

    def __init__(
        self,
        envoy: Envoy,
        interval_seconds: int = 60,
        inverter_interval_seconds: int = 300,
    ) -> None:
        self.envoy = envoy
        self.interval_seconds = interval_seconds
        self.inverter_interval_seconds = inverter_interval_seconds
        self.last_inverter_poll: Optional[datetime] = None

    @abstractmethod
    def run(self) -> None:
        pass

    def wait_for_next_cycle(self) -> None:
        # Determine how long until the next sample needs to be taken
        now = datetime.now(tz=timezone.utc)

        remainder = now.timestamp() % self.interval_seconds

        # If we're exactly on a boundary (remainder == 0) or very close to it,
        # wait a minimal amount to avoid waiting a full interval unnecessarily
        if remainder < 0.1:
            time_to_next = 0.1  # Small delay to avoid tight loop
        else:
            time_to_next = self.interval_seconds - remainder

        try:
            time.sleep(time_to_next)
        except KeyboardInterrupt:
            print("Exiting with Ctrl-C")
            sys.exit(0)

    def _should_poll_inverters(self) -> bool:
        if self.last_inverter_poll is None:
            return True
        elapsed = (
            datetime.now(tz=timezone.utc) - self.last_inverter_poll
        ).total_seconds()
        return elapsed >= self.inverter_interval_seconds

    def collect_samples_with_retry(
        self, retries: int = 10, wait_seconds: float = 5.0
    ) -> SampleData | Dict[str, InverterSample]:
        # Power poll (required) with retries
        power_data = None
        for attempt in range(retries):
            try:
                power_data = self.get_power_data()
                self.last_sample_timestamp = datetime.now(tz=timezone.utc)
                if attempt > 0:
                    LOG.info(
                        "Power poll succeeded after %d retries",
                        attempt + 1,
                    )
                break
            except (ReadTimeout, ConnectTimeout):
                LOG.warning(
                    "Power poll failed (timeout), retry %d/%d",
                    attempt + 1,
                    retries,
                )
                if attempt < retries - 1:
                    LOG.info(
                        "Retrying power poll (attempt %d/%d)",
                        attempt + 2,
                        retries,
                    )
                    time.sleep(wait_seconds)

        if power_data is None:
            LOG.warning("Power poll failed after %d retries", retries)
            raise TimeoutError("Power sample collection timed out.")

        LOG.debug("Sampled power data:\n%s", power_data)

        # Inverter poll (when due): single attempt, no retries (endpoint is unstable)
        if not self._should_poll_inverters():
            return power_data, {}

        try:
            inverter_data = self.get_inverter_data()
            self.last_inverter_poll = datetime.now(tz=timezone.utc)
            LOG.debug("Sampled inverter data:\n%s", inverter_data)
            return power_data, inverter_data
        except (RequestException, ssl.SSLError, OSError) as e:
            # RequestException: timeouts, connection errors, requests SSLError, etc.
            # ssl.SSLError: unwrapped SSL failures (e.g. server flakiness)
            # OSError: connection reset, broken pipe, and other socket errors
            LOG.warning(
                "Inverter poll failed (%s): %s",
                type(e).__name__,
                e,
            )
            return power_data, {}

    def get_power_data(self) -> SampleData:
        return self.envoy.get_power_data()

    def get_inverter_data(self) -> Dict[str, InverterSample]:
        inverter_data = self.envoy.get_inverter_data()
        return filter_new_inverter_data(inverter_data, self.last_sample_timestamp)
