import logging
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, Optional

from requests import ConnectTimeout, ReadTimeout

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
        for retry_loop in range(retries):
            try:
                power_data = self.get_power_data()

                if self._should_poll_inverters():
                    inverter_data = self.get_inverter_data()
                    self.last_inverter_poll = datetime.now(tz=timezone.utc)
                    LOG.debug(f"Sampled inverter data:\n{inverter_data}")
                else:
                    inverter_data = {}

                self.last_sample_timestamp = datetime.now(tz=timezone.utc)

                LOG.debug(f"Sampled power data:\n{power_data}")
            except (ReadTimeout, ConnectTimeout):
                # Envoy gets REALLY MAD if you block it's access to enphaseenergy.com using a VLAN.
                # Its software gets hung up for some reason, and some requests will stall.
                # Allow envoy requests to timeout (and skip this sample iteration)
                LOG.warning("Envoy request timed out (%d/%d)", retry_loop + 1, retries)
                time.sleep(wait_seconds)
            else:
                return power_data, inverter_data

        # If we got this far it means we've timed out, raise an exception
        raise TimeoutError("Sample collection timed out.")

    def get_power_data(self) -> SampleData:
        return self.envoy.get_power_data()

    def get_inverter_data(self) -> Dict[str, InverterSample]:
        inverter_data = self.envoy.get_inverter_data()
        return filter_new_inverter_data(inverter_data, self.last_sample_timestamp)
