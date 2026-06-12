"""Environment-driven settings for the aggregator.

Everything is configurable via env vars so the same image runs locally and in
Kubernetes. TEST_ID identifies one contestant submission (team) and is the
suffix on the Kafka topic and the namespace prefix on every Redis key —
matching the conventions used by the bot fleet.
"""

import os
import re

# ── identity ─────────────────────────────────────────────────────────────────
TEST_ID = os.environ.get("TEST_ID", "team1")

# Kafka topic names allow only [a-zA-Z0-9._-]; team-name test_ids may not.
# Same slugification the bot fleet's KafkaSink uses, so we read the topic the
# bots actually write to.
_BAD_TOPIC_CHARS = re.compile(r"[^a-zA-Z0-9._-]")


def topic_safe(s: str) -> str:
    return _BAD_TOPIC_CHARS.sub("-", s)


RESPONSE_TOPIC = f"order-response-{topic_safe(TEST_ID)}"

# ── transports ───────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", f"aggregator-{topic_safe(TEST_ID)}")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# ── timing ───────────────────────────────────────────────────────────────────
# Width of one metrics window (seconds) and how many windows of history to keep
# in the Redis list for the frontend's live chart (600 = 10 min at 1s windows).
WINDOW_S = float(os.environ.get("WINDOW_S", "1.0"))
HISTORY_LEN = int(os.environ.get("HISTORY_LEN", "600"))

# Kafka consume timeout per loop iteration. Keeps the loop responsive to Redis
# signals even when no events are flowing.
POLL_TIMEOUT_S = float(os.environ.get("POLL_TIMEOUT_S", "0.1"))

# How often to re-read the run/bots status keys from Redis.
STATUS_POLL_S = float(os.environ.get("STATUS_POLL_S", "0.5"))

# How often to print a live metrics line to the console. Redis still receives
# every window; this only throttles the on-screen report.
CONSOLE_REPORT_S = float(os.environ.get("CONSOLE_REPORT_S", "5.0"))

# After the bots report done, keep draining Kafka until it has been idle this
# long — in-flight batches can still be landing.
DRAIN_IDLE_S = float(os.environ.get("DRAIN_IDLE_S", "3.0"))

# Backoff between reconnect attempts when Kafka/Redis are unreachable.
RETRY_BACKOFF_S = float(os.environ.get("RETRY_BACKOFF_S", "3.0"))

# ── error classification ─────────────────────────────────────────────────────
# Engine response code that means "order rejected" → counts toward error_rate.
REJECT_MESSAGE_CODE = int(os.environ.get("REJECT_MESSAGE_CODE", "5"))

# Errors that are the BOT's fault, not the engine's (e.g. cancelling an order
# that was already filled). These are tracked separately and excluded from
# error_rate. Comma-separated, case-insensitive substring match on the event's
# `error` field.
EXCLUDED_ERROR_PATTERNS = [
    p.strip().lower()
    for p in os.environ.get("EXCLUDED_ERROR_PATTERNS", "not found").split(",")
    if p.strip()
]
