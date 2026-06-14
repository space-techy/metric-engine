# metric-engine — Live Metrics Aggregator

> **Part of the [Quant-Force](https://github.com/space-techy/quant-force-infra)
> platform** — the aggregator that measures latency the honest way
> (coordinated-omission corrected, full-population HdrHistogram percentiles,
> never a mean). See `docs/ARCHITECTURE.md` in the platform repo.

Consumes `order_response` telemetry from Kafka, computes 1-second latency /
throughput / error-rate windows, and publishes them to Redis for the
leaderboard frontend. One aggregator process runs per contestant submission
(`TEST_ID` = team).

See [PROJECT_README.md](PROJECT_README.md) for how this fits into the overall
benchmarking platform.

## How it works

```
wait for run:{team}:status = "running"
    │
    ▼
consume order-response-{team}  ──►  1s windows  ──►  agg:{team}:latest / :history
    │                               (+ console report every 5s)
    ▼
bots:{team}:status or run:{team}:status = "complete" ──► drain Kafka until idle
    │
    ▼
whole-run rollup ──► agg:{team}:summary, final emit, exit
```

- **Never crashes on a missing topic.** The aggregator usually starts before
  the bots; if `order-response-{team}` doesn't exist yet it just keeps polling
  until the first event creates it.
- **Quiet Kafka ≠ done.** Idle stretches are waited out; the aggregator only
  stops when Redis says the bots are complete (then drains until Kafka has
  been idle for `DRAIN_IDLE_S`), the run is aborted, or it gets SIGINT/SIGTERM.
  All exits flush the open window and phase first.
- **Bot-side errors are excluded.** Errors like `order not found` happen when
  a bot cancels an order that was already filled — a fleet limitation, not an
  engine failure. They're excluded from `error_rate` and reported separately
  as `bot_errors` / `bot_errors_total`. Engine rejections
  (`message_code == 5`) are what `error_rate` measures.

## Redis contract

| Key | Direction | Meaning |
|---|---|---|
| `run:{team}:status` | read | `running` → start; `complete`/`done` → drain then stop; `stopped`/`aborted` → immediate graceful shutdown |
| `bots:{team}:status` | read | `complete`/`done` → drain remaining events, then stop |
| `agg:{team}:status` | write | `waiting` \| `running` \| `complete` \| `failed` |
| `agg:{team}:latest` | write | JSON of the most recent 1s window |
| `agg:{team}:history` | write | list of window JSON, newest first, trimmed to 600 |
| `agg:{team}:summary` | write | whole-run rollup, written once at shutdown |

`latest` / `history` sample:

```json
{
  "timestamp": 1760000000.0,
  "throughput": 4812, "trades_per_sec": 1633,
  "p50_ms": 0.42, "p90_ms": 1.78, "p95_ms": 2.21, "p99_ms": 2.91, "p999_ms": 7.05,
  "error_rate": 0.0021, "bot_errors": 3, "sample_count": 4812,
  "orders_processed": 188210, "trades_count": 61240, "bot_errors_total": 41
}
```

`summary`: same percentile spread plus `throughput_avg, error_rate, bot_errors,
total_orders, total_trades, duration_s` over the whole run.

## Running

```bash
pip install -r requirements.txt

TEST_ID=team1 \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
REDIS_URL=redis://localhost:6379/0 \
python -m aggregator
```

Kick it off manually for a local test:

```bash
redis-cli set run:team1:status running     # aggregator starts consuming
redis-cli set bots:team1:status complete   # aggregator drains and exits
```

## Running in Docker

```bash
docker build -t aggregator .

docker run --rm \
  -e TEST_ID=team1 \
  -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  -e REDIS_URL=redis://host.docker.internal:6379/0 \
  aggregator
```

Inside a container `localhost` is the container itself — point the env vars at
`host.docker.internal` (Kafka/Redis on the host) or at service names when
everything shares a compose network / Kubernetes namespace. If Kafka connects
but consuming never starts, check the broker's `advertised.listeners`: a broker
advertising `localhost:9092` sends the container back to itself on the first
metadata response.

`docker stop` sends SIGTERM → same graceful shutdown as Ctrl-C (final window,
run summary, status all flushed to Redis).

## Configuration (env vars)

| Variable | Default | Purpose |
|---|---|---|
| `TEST_ID` | `team1` | Team/submission id; Kafka topic suffix + Redis key prefix |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `WINDOW_S` | `1.0` | Metrics window width (seconds) |
| `HISTORY_LEN` | `600` | Windows kept in `agg:{team}:history` |
| `DRAIN_IDLE_S` | `3.0` | Idle time after bots finish before exiting |
| `CONSOLE_REPORT_S` | `5.0` | Seconds between live metrics lines on the console |
| `RETRY_BACKOFF_S` | `3.0` | Backoff when Kafka/Redis are unreachable |
| `REJECT_MESSAGE_CODE` | `5` | Engine code counted as an engine error |
| `EXCLUDED_ERROR_PATTERNS` | `not found` | Comma-separated substrings marking bot-side errors |

## Layout

```
aggregator/
├── __main__.py      # entry point, signal handling
├── service.py       # lifecycle + main loop
├── consumer.py      # Kafka consumer (retries, missing-topic tolerance)
├── coordination.py  # Redis signals in, metrics out
├── metrics.py       # pure computation: windows, percentiles, run rollup (no I/O)
└── settings.py      # env-driven configuration
tests/
└── test_metrics.py  # unit tests for the computation layer
```

## Tests

```bash
pip install -e .[dev]
pytest
```
