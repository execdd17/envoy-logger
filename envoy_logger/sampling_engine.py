import logging
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict

from envoy_logger.envoy import Envoy
from envoy_logger.model import InverterSample, SampleData

LOG = logging.getLogger("sampling_engine")


class SamplingEngine(ABC):
    def __init__(
        self,
        envoy: Envoy,
        interval_seconds: int = 60,
        inverter_interval_seconds: int = 300,
    ) -> None:
        self.envoy = envoy
        self.interval_seconds = interval_seconds
        self.inverter_interval_seconds = inverter_interval_seconds
        self.last_inverter_poll: datetime | None = None

    @abstractmethod
    def run(self) -> None:
        pass

    def wait_for_next_cycle(self, interval_seconds: int) -> None:
        """Sleep until the next aligned boundary for the given interval (seconds)."""
        now = datetime.now(tz=timezone.utc)
        remainder = now.timestamp() % interval_seconds
        if remainder < 0.1:
            time_to_next = 0.1
        else:
            time_to_next = interval_seconds - remainder
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

    def get_power_data(self) -> SampleData:
        return self.envoy.get_power_data()

    def get_inverter_data(self) -> Dict[str, InverterSample]:
        """Return raw inverter data from Envoy (no filtering). Caller filters as needed."""
        return self.envoy.get_inverter_data()
