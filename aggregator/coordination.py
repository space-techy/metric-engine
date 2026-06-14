"""Redis side of the aggregator: lifecycle signals in, metrics out.

Key contract (team == TEST_ID):

READS
  run:{team}:status    "running"            → start consuming
                       "complete"/"done"    → drain remaining Kafka events, stop
                       "stopped"/"aborted"  → shut down immediately
  bots:{team}:status   "complete"           → drain remaining Kafka events, stop

WRITES
  agg:{team}:status    "waiting" | "running" | "complete" | "failed"
  agg:{team}:latest    JSON — most recent 1-second window
  agg:{team}:history   LIST of window JSON (lpush, ltrim to HISTORY_LEN)
  agg:{team}:summary   JSON — whole-run rollup, written once at shutdown

Every operation retries through transient Redis outages instead of crashing:
the aggregator must outlive infrastructure blips mid-test.
"""

import json
import logging
import time

import redis

from aggregator import settings

log = logging.getLogger("aggregator.redis")

# Status values that carry the same meaning under different spellings, so an
# orchestrator written later has some slack in what it sets.
# "testing" is the run-status the orchestrator sets during phase 5 (the value the
# frontend/backend show); accepting it here lets the aggregator start without the
# orchestrator having to write a second, redundant status value.
START_VALUES = {"running", "started", "start", "testing"}
ABORT_VALUES = {"stop", "stopped", "abort", "aborted", "failed", "cancelled", "kill"}
DONE_VALUES = {"complete", "completed", "done", "finished"}


class RedisCoordinator:
    def __init__(self, test_id: str = settings.TEST_ID):
        self.test_id = test_id
        self.run_status_key = f"run:{test_id}:status"
        self.bots_status_key = f"bots:{test_id}:status"
        self.agg_status_key = f"agg:{test_id}:status"
        self.latest_key = f"agg:{test_id}:latest"
        self.history_key = f"agg:{test_id}:history"
        self.summary_key = f"agg:{test_id}:summary"
        self.r = redis.from_url(settings.REDIS_URL, decode_responses=True)

    # ── resilience wrapper ───────────────────────────────────────────────────

    def _safe(self, op, default=None):
        """Run one Redis operation; on connection trouble log, back off, and
        return `default` so the caller's loop keeps going."""
        try:
            return op()
        except redis.RedisError as e:
            log.warning("redis unavailable (%s) — retrying in %.1fs",
                        e, settings.RETRY_BACKOFF_S)
            time.sleep(settings.RETRY_BACKOFF_S)
            return default

    # ── lifecycle signals (reads) ────────────────────────────────────────────

    def _status(self, key: str) -> str:
        val = self._safe(lambda: self.r.get(key), default="")
        return (val or "").strip().lower()

    def run_started(self) -> bool:
        return self._status(self.run_status_key) in START_VALUES

    def run_aborted(self) -> bool:
        return self._status(self.run_status_key) in ABORT_VALUES

    def test_finished(self) -> bool:
        """True when either the bots or the run itself report completion —
        an orchestrator may only ever set one of the two keys."""
        return (self._status(self.bots_status_key) in DONE_VALUES
                or self._status(self.run_status_key) in DONE_VALUES)

    # ── metric publication (writes) ──────────────────────────────────────────

    def set_status(self, status: str):
        self._safe(lambda: self.r.set(self.agg_status_key, status))
        log.info("agg status → %s", status)

    def publish_window(self, sample: dict):
        payload = json.dumps(sample)

        def op():
            pipe = self.r.pipeline()
            pipe.set(self.latest_key, payload)
            pipe.lpush(self.history_key, payload)
            pipe.ltrim(self.history_key, 0, settings.HISTORY_LEN - 1)
            pipe.execute()

        self._safe(op)

    def publish_summary(self, summary: dict):
        self._safe(lambda: self.r.set(self.summary_key, json.dumps(summary)))
