# Enphase Envoy data logging service

![docker-ci](https://github.com/jasonajack/envoy-logger/actions/workflows/docker-build-ci.yml/badge.svg)
![docker-push](https://github.com/jasonajack/envoy-logger/actions/workflows/build-and-push.yml/badge.svg)

**Fork of:** [amykyta3/envoy-logger](https://github.com/amykyta3/envoy-logger)

Log solar production from an Enphase Envoy locally and feed it into InfluxDB.

## What it does

- **Auth:** Fetches an Enphase access token from enphaseenergy.com, then uses it to authenticate with your local Envoy (JWT + session cookie).
- **Scrape:** Polls the Envoy for:
  - **Power (EIM):** Per-phase production, consumption, and net (true/reactive/apparent power, voltage, current). Source: `/production.json?details=1`.
  - **Inverters:** Per-panel production. Source: `/api/v1/production/inverters` (polled less frequently; see config).
- **Store:** Pushes high-rate points (power + inverters) to InfluxDB and, on day rollover, daily summary points (integrated Wh) to configurable buckets.

You can then visualize data in Grafana (or any client that talks to InfluxDB).

## Screenshots

**Dashboard (live):**\
![dashboard-live](docs/dashboard-live.png)

**Dashboard (daily totals):**\
![dashboard-daily-totals](docs/dashboard-daily-totals.png)

## Configuration

### 1. Database

#### InfluxDB

- [Docker image](https://hub.docker.com/_/influxdb/)
- Create two buckets (or one): e.g. `envoy_high_rate`, `envoy_low_rate`. The logger writes high-rate samples to the high-rate bucket and daily summaries to the low-rate bucket.
- Create an org and a token with write access; use the token in config (or `INFLUXDB_TOKEN` env).

Example compose:

```yaml
version: '3'

services:
  influxdb:
    image: influxdb:alpine
    container_name: influxdb
    volumes:
      - influxdb-data:/var/lib/influxdb2
      - influxdb-config:/etc/influxdb2
    ports:
      - 8086:8086
    restart: unless-stopped

volumes:
  influxdb-config:
  influxdb-data:
```

### 2. config.yml

Create a YAML config. Example: [docs/config.yml](docs/config.yml).

Required / common:

- **enphaseenergy:** `email`, `password` (for token fetch). Can override with `ENPHASE_EMAIL`, `ENPHASE_PASSWORD`.
- **envoy:** `serial` (from Enlighten IQ Gateway info), `url` (e.g. `https://envoy.local` or `https://192.168.x.x`). Optional: `tag` (source tag for points).
- **influxdb:** `url`, `token` (or `INFLUXDB_TOKEN`), `org`. Optional: `bucket_hr` / `bucket_lr`, or single `bucket`.
- **polling:** `interval` (seconds between power polls; default 60), `inverter_interval` (seconds between inverter polls; default 300). Community guidance: power no faster than once per minute; inverters are slow/unreliable, so poll less often.
- **inverters** (optional): Map of serial → `tags` (e.g. `array`, `face`, `location`, `x`, `y`) for enrichment in InfluxDB and dashboards.

### 3. Run locally

```bash
./install_python_deps.sh
./launcher.sh --config /path/to/config.yml
```

Defaults if omitted: `--config` uses `ENVOY_LOGGER_CFG_PATH` or `/etc/envoy-logger/config.yml`. Log level: `LOG_LEVEL` (e.g. `DEBUG`).

Verify: logs show successful Envoy and DB/auth, and data appears in InfluxDB (Data Explorer).

### 4. Docker

Example compose:

```yaml
version: '3'

services:
  envoy_logger:
    image: ghcr.io/jasonajack/envoy-logger:latest
    container_name: envoy_logger
    environment:
      # ENVOY_LOGGER_CFG_PATH: /etc/envoy_logger/config.yml
    volumes:
      - /path/to/config.yml:/etc/envoy_logger/config.yml
      - /etc/localtime:/etc/localtime:ro
      - /etc/timezone:/etc/timezone:ro
    restart: unless-stopped
```

If you mount the config at a different path, set `ENVOY_LOGGER_CFG_PATH` to that path.

## Grafana

- [Install Grafana](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/) and add an InfluxDB data source using your token / URL.
- Use Flux; example queries: [docs/flux_queries](docs/flux_queries).

## Development

- **Install deps:** `./install_python_deps.sh` (Poetry + project install).
- **Lint & test:** `./test.sh` (black, isort, flake8, yamllint, mdformat, pytest, coverage; optional shellcheck).
- **Format:** `./format.sh` (black, isort, mdformat).
- **Run:** `./launcher.sh --config /path/to/config.yml` (uses `poetry run python3 -m envoy_logger`).
- **Tests:** `poetry run pytest` from repo root; `pythonpath` is set in `pyproject.toml` so `envoy_logger` and `tests` are on the path.

## License and attribution

Original author: Alex Mykyta (amykyta3). This repo is a fork with CI/images under jasonajack. See repository history and upstream for license and contributions.
