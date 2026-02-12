# CLAUDE.md — Agent context for envoy-logger

This file gives AI agents enough context to work in this repo without re-deriving everything.

## Project summary

**envoy-logger** is a Python service that:

1. Gets an Enphase JWT from enphaseenergy.com (using email/password and Envoy serial).
1. Authenticates to the local Enphase Envoy (HTTPS, self-signed cert) and keeps a session.
1. Polls the Envoy on two schedules:
   - **Power (EIM):** `GET /production.json?details=1` → net/total consumption, total production per phase.
   - **Inverters:** `GET /api/v1/production/inverters` → per-panel watts (slower, less frequent).
1. Sends data to a time-series backend:
   - **InfluxDB:** push to two buckets (high-rate + low-rate); low-rate gets daily Wh summaries on day rollover.
   - **Prometheus:** expose metrics on a configurable port; Prometheus scrapes the app.

Entrypoint: `envoy_logger.cli.main()` (e.g. `python3 -m envoy_logger`). CLI args: `--config` (YAML path), `--db` (`influxdb` | `prometheus`). Env: `ENVOY_LOGGER_CFG_PATH`, `ENVOY_LOGGER_DB`, `LOG_LEVEL`; secrets: `ENPHASE_EMAIL`, `ENPHASE_PASSWORD`, `INFLUXDB_TOKEN`.

## Repo layout

- **`envoy_logger/`** — main package
  - **`cli.py`** — argparse, `main()`: load config, build `EnphaseEnergy` + `Envoy`, then run `InfluxdbSamplingEngine` or `PrometheusSamplingEngine`.
  - **`config.py`** — YAML load, `Config` and `InverterConfig`; validates polling intervals; supports env overrides for credentials and InfluxDB token.
  - **`enphase_energy.py`** — `EnphaseEnergy`: login to Enlighten, fetch token from Entrez for the given Envoy serial, refresh before expiry.
  - **`envoy.py`** — `Envoy`: JWT auth to local device, session cookie; `get_power_data()`, `get_inverter_data()`, `get_inventory()`.
  - **`model.py`** — `PowerSample`, `EIMSample`, `SampleData`, `InverterSample`; parsers from Envoy JSON; `filter_new_inverter_data()` (only report inverters that updated since last sample).
  - **`sampling_engine.py`** — Abstract `SamplingEngine`: interval/aligned sleep, `_should_poll_inverters()`, `collect_samples_with_retry()` (power + optional inverter fetch), delegates to `envoy.get_power_data()` / `get_inverter_data()`.
  - **`influxdb_sampling_engine.py`** — `InfluxdbSamplingEngine`: writes power + inverter points to high-rate bucket; on date change, runs Flux integral query and writes daily Wh to low-rate bucket; applies inverter tags from config.
  - **`prometheus_sampling_engine.py`** — `PrometheusSamplingEngine`: starts HTTP server, each cycle updates Gauges/Info for power lines and inverters.
- **`tests/`** — pytest; uses `sample_data` and mocks; `pythonpath` set in `pyproject.toml`.
- **`docs/`** — `config.yml` example, Flux queries, dashboard screenshots, Prometheus panel example.
- **Scripts:** `launcher.sh` (sources `.env`, runs `poetry run python3 -m envoy_logger "$@"`), `install_python_deps.sh` (pip + poetry install), `test.sh` (lint + pytest + coverage, optional shellcheck), `format.sh` (black, isort, mdformat).

## Config (YAML)

- **enphaseenergy:** email, password.
- **envoy:** serial, url, optional tag.
- **influxdb:** url, token, org; optional bucket_hr / bucket_lr (or bucket).
- **prometheus:** listening_port.
- **polling:** interval (power, seconds), inverter_interval (seconds).
- **inverters:** optional map of serial → tags (applied to InfluxDB inverter points and daily summaries).

Config is loaded once at startup; database choice is from CLI/env. Missing required keys cause `config.py` to log and `sys.exit(1)`.

## Conventions and gotchas

- **Python:** 3.10+. Style: black, isort (profile black), flake8. Tests: pytest, coverage. No type hints in a few legacy spots; prefer adding them when touching code.
- **Envoy:** Local HTTPS with self-signed cert; `urllib3.disable_warnings(InsecureRequestWarning)` in `envoy.py`. Timeouts and retries in `collect_samples_with_retry()` to avoid hanging on Envoy/network issues.
- **InfluxDB daily summary:** Implemented in `_low_rate_points()` / `_compute_daily_Wh_points()`: Flux `integral(unit: 1h)` over last 24h; written when the process sees a new calendar day. Inverters that didn’t report get a 0 Wh point.
- **Prometheus:** Gauges/Info are created on first use and reused; metric names include measurement type, line index, and for inverters serial. Logger is scrape target; no push.
- **Prometheus engine:** In `run()`, power data is in variable `power_data` and passed as `sample_data=power_data` to `_update_power_data_info`.
- **Inverter filtering:** `filter_new_inverter_data()` returns only inverters whose `lastReportDate` is newer than `last_sample_timestamp` so we don’t re-write stale data; InfluxDB daily summary still fills 0 Wh for configured inverters that didn’t report.

## Common tasks

- **Add a new Envoy endpoint:** Add method in `envoy.py`, call from a sampling engine if needed, extend `model.py` if response shape changes.
- **Add a backend:** New engine in `envoy_logger/` extending `SamplingEngine`, add branch in `cli.main()`, extend `Config` in `config.py` for the new backend’s options.
- **Change polling or buckets:** Edit `config.py` (defaults/validation) and `docs/config.yml` (example). Document in README.
- **Run locally:** `./launcher.sh --config /path/to/config.yml --db influxdb|prometheus`. Ensure Envoy reachable and InfluxDB/Prometheus config correct.
- **CI:** `.github/workflows/` — install deps, `./test.sh`, Docker build (no push on PR). Push to main triggers build; separate workflow for build-and-push if used.

Use this file plus the code and README for consistent, context-aware edits.
