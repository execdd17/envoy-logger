import logging
import ssl
import threading
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from envoy_logger.config import Config
from envoy_logger.envoy import Envoy
from envoy_logger.model import InverterSample, PowerSample, SampleData, filter_new_inverter_data
from envoy_logger.sampling_engine import SamplingEngine

LOG = logging.getLogger("influxdb_sampling_engine")


class InfluxdbSamplingEngine(SamplingEngine):
    def __init__(self, envoy: Envoy, config: Config) -> None:
        super().__init__(
            envoy=envoy,
            interval_seconds=config.polling_interval,
            inverter_interval_seconds=config.inverter_polling_interval,
        )

        self.config = config

        influxdb_client = InfluxDBClient(
            url=config.influxdb_url,
            token=config.influxdb_token,
            org=config.influxdb_org,
        )

        self.influxdb_write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
        self.influxdb_query_api = influxdb_client.query_api()

        self.power_todays_date: date = date.today()
        self.inverter_todays_date: date = date.today()

    def run(self) -> None:
        LOG.info(
            "Sampling started (InfluxDB): power interval=%ds, inverter interval=%ds",
            self.interval_seconds,
            self.inverter_interval_seconds,
        )
        power_thread = threading.Thread(target=self._power_loop, daemon=False)
        inverter_thread = threading.Thread(target=self._inverter_loop, daemon=False)
        power_thread.start()
        inverter_thread.start()
        power_thread.join()
        inverter_thread.join()

    def _power_loop(self) -> None:
        while True:
            self.wait_for_next_cycle(self.interval_seconds)
            try:
                power_data = self.get_power_data()
                LOG.debug("Sampled power data:\n%s", power_data)
                points = self._power_high_rate_points(power_data)
                if points:
                    self.influxdb_write_api.write(
                        bucket=self.config.influxdb_bucket_hr, record=points
                    )
                self._power_day_rollover(power_data.ts)
            except (
                ssl.SSLError,
                requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                OSError,
            ) as e:
                LOG.warning(
                    "Power poll failed (%s): %s. Skipping this cycle.",
                    type(e).__name__,
                    e,
                )

    def _inverter_loop(self) -> None:
        while True:
            self.wait_for_next_cycle(self.inverter_interval_seconds)
            try:
                cutoff_ts = self._query_last_inverter_timestamp()
                raw_inverter_data = self.get_inverter_data()
                inverter_data = filter_new_inverter_data(raw_inverter_data, cutoff_ts)
                self.last_inverter_poll = datetime.now(tz=timezone.utc)
                LOG.debug("Sampled inverter data (filtered):\n%s", inverter_data)
                points = self._inverter_high_rate_points(inverter_data)
                if points:
                    self.influxdb_write_api.write(
                        bucket=self.config.influxdb_bucket_hr, record=points
                    )
                self._inverter_day_rollover()
            except (
                ssl.SSLError,
                requests.exceptions.SSLError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                OSError,
            ) as e:
                LOG.warning(
                    "Inverter poll failed (%s): %s. Skipping this cycle.",
                    type(e).__name__,
                    e,
                )

    def _query_last_inverter_timestamp(self) -> Optional[datetime]:
        """Query bucket_hr for the latest inverter write time; used to avoid writing duplicates."""
        query = f'''
        from(bucket: "{self.config.influxdb_bucket_hr}")
            |> range(start: -30d)
            |> filter(fn: (r) => r["source"] == "{self.config.source_tag}")
            |> filter(fn: (r) => r["measurement-type"] == "inverter")
            |> group()
            |> max(column: "_time")
            |> yield(name: "max")
        '''
        try:
            result = self.influxdb_query_api.query(query=query)
            for table in result:
                for record in table.records:
                    t = record.get_time()
                    if t is not None:
                        return t
            return None
        except Exception as e:
            LOG.warning("InfluxDB query for last inverter timestamp failed: %s", e)
            return None

    def _power_high_rate_points(self, sample_data: SampleData) -> List[Point]:
        points = []
        for line_index, line_sample in enumerate(
            sample_data.total_consumption.eim_line_samples
        ):
            p = self._idb_point_from_line("consumption", line_index, line_sample)
            points.append(p)
        for line_index, line_sample in enumerate(
            sample_data.total_production.eim_line_samples
        ):
            p = self._idb_point_from_line("production", line_index, line_sample)
            points.append(p)
        for line_index, line_sample in enumerate(
            sample_data.net_consumption.eim_line_samples
        ):
            p = self._idb_point_from_line("net", line_index, line_sample)
            points.append(p)
        return points

    def _inverter_high_rate_points(
        self, inverter_data: Dict[str, InverterSample]
    ) -> List[Point]:
        return [self._point_from_inverter(inv) for inv in inverter_data.values()]

    def _power_day_rollover(self, ts: datetime) -> None:
        new_date = date.today()
        if self.power_todays_date == new_date:
            return
        self.power_todays_date = new_date
        points = self._compute_power_daily_Wh_points(ts)
        if points:
            self.influxdb_write_api.write(
                bucket=self.config.influxdb_bucket_lr, record=points
            )

    def _inverter_day_rollover(self) -> None:
        new_date = date.today()
        if self.inverter_todays_date == new_date:
            return
        self.inverter_todays_date = new_date
        ts = datetime.now(tz=timezone.utc)
        points = self._compute_inverter_daily_Wh_points(ts)
        if points:
            self.influxdb_write_api.write(
                bucket=self.config.influxdb_bucket_lr, record=points
            )

    def _compute_power_daily_Wh_points(self, ts: datetime) -> List[Point]:
        """Flux query for power line series only (exclude inverter); build daily Wh points."""
        query = f'''
        from(bucket: "{self.config.influxdb_bucket_hr}")
            |> range(start: -24h, stop: 0h)
            |> filter(fn: (r) => r["source"] == "{self.config.source_tag}")
            |> filter(fn: (r) => r["_field"] == "P")
            |> filter(fn: (r) => r["measurement-type"] != "inverter")
            |> integral(unit: 1h)
            |> keep(columns: ["_value", "line-idx", "measurement-type"])
            |> yield(name: "total")
        '''
        result = self.influxdb_query_api.query(query=query)
        points = []
        for table in result:
            for record in table.records:
                measurement_type = record["measurement-type"]
                idx = record["line-idx"]
                p = Point(f"{measurement_type}-daily-summary-line{idx}")
                p.tag("line-idx", idx)
                p.time(ts, WritePrecision.S)
                p.tag("source", self.config.source_tag)
                p.tag("measurement-type", measurement_type)
                p.tag("interval", "24h")
                p.field("Wh", record.get_value())
                points.append(p)
        return points

    def _compute_inverter_daily_Wh_points(self, ts: datetime) -> List[Point]:
        """Flux query for inverter series only; build daily Wh points and 0 Wh for unreported."""
        query = f'''
        from(bucket: "{self.config.influxdb_bucket_hr}")
            |> range(start: -24h, stop: 0h)
            |> filter(fn: (r) => r["source"] == "{self.config.source_tag}")
            |> filter(fn: (r) => r["_field"] == "P")
            |> filter(fn: (r) => r["measurement-type"] == "inverter")
            |> integral(unit: 1h)
            |> keep(columns: ["_value", "serial", "measurement-type"])
            |> yield(name: "total")
        '''
        result = self.influxdb_query_api.query(query=query)
        unreported_inverters = set(self.config.inverters.keys())
        points = []
        for table in result:
            for record in table.records:
                serial = record["serial"]
                unreported_inverters.discard(serial)
                p = Point(f"inverter-daily-summary-{serial}")
                p.tag("serial", serial)
                self.config.apply_tags_to_inverter_point(p, serial)
                p.time(ts, WritePrecision.S)
                p.tag("source", self.config.source_tag)
                p.tag("measurement-type", "inverter")
                p.tag("interval", "24h")
                p.field("Wh", record.get_value())
                points.append(p)
        for serial in unreported_inverters:
            p = Point(f"inverter-daily-summary-{serial}")
            p.tag("serial", serial)
            self.config.apply_tags_to_inverter_point(p, serial)
            p.time(ts, WritePrecision.S)
            p.tag("source", self.config.source_tag)
            p.tag("measurement-type", "inverter")
            p.tag("interval", "24h")
            p.field("Wh", 0.0)
            points.append(p)
        return points

    def _idb_point_from_line(
        self, measurement_type: str, idx: int, data: PowerSample
    ) -> Point:
        p = Point(f"{measurement_type}-line{idx}")
        p.time(data.ts, WritePrecision.S)
        p.tag("source", self.config.source_tag)
        p.tag("measurement-type", measurement_type)
        p.tag("line-idx", idx)

        p.field("P", data.wNow)
        p.field("Q", data.reactPwr)
        p.field("S", data.apprntPwr)

        p.field("I_rms", data.rmsCurrent)
        p.field("V_rms", data.rmsVoltage)

        return p

    def _point_from_inverter(self, inverter: InverterSample) -> Point:
        p = Point(f"inverter-production-{inverter.serial}")
        p.time(inverter.ts, WritePrecision.S)
        p.tag("source", self.config.source_tag)
        p.tag("measurement-type", "inverter")
        p.tag("serial", inverter.serial)
        self.config.apply_tags_to_inverter_point(p, inverter.serial)

        p.field("P", inverter.watts)

        return p
