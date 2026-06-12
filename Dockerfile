# Live metrics aggregator.
#
# Build:  docker build -t aggregator .
# Run:    docker run --rm \
#             -e TEST_ID=team1 \
#             -e KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
#             -e REDIS_URL=redis://host.docker.internal:6379/0 \
#             aggregator

FROM python:3.12-slim

# Logs must reach `docker logs` immediately, not sit in Python's stdout buffer.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Dependencies first, on their own layer, so code edits don't re-install them.
# confluent-kafka ships manylinux wheels with librdkafka bundled — no apt
# packages or compiler needed on top of slim.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aggregator/ ./aggregator/

# Don't run as root; the aggregator needs no privileges of any kind.
RUN useradd --no-create-home appuser
USER appuser

# SIGTERM from `docker stop` / pod termination hits the Python process
# directly (exec form, no shell wrapper) → graceful shutdown: final window,
# run summary, and agg:{team}:status are all flushed to Redis before exit.
ENTRYPOINT ["python", "-m", "aggregator"]
