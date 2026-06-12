"""The aggregator's lifecycle and main loop.

    wait for run:{team}:status = "running"
        │
        ▼
    consume order-response-{team} ──► 1-second windows ──► agg:{team}:latest/:history
        │                             (+ console report every CONSOLE_REPORT_S)
        ▼
    bots:{team}:status or run:{team}:status = "complete" ──► drain Kafka until idle
        │
        ▼
    whole-run rollup ──► agg:{team}:summary, final emit, exit

It also exits on an abort signal (run status set to "stopped"/"aborted") and on
SIGINT/SIGTERM, flushing the open window and the run summary either way.
"""

import logging
import threading
import time

from aggregator import settings
from aggregator.consumer import ResponseConsumer
from aggregator.coordination import RedisCoordinator
from aggregator.metrics import RunAccumulator, WindowAccumulator, classify_error

log = logging.getLogger("aggregator")


def _format_report(sample: dict) -> str:
    return (
        f"{sample['throughput']:>6}/s  "
        f"p50={sample['p50_ms']:.3f}ms  p90={sample['p90_ms']:.3f}ms  "
        f"p95={sample['p95_ms']:.3f}ms  p99={sample['p99_ms']:.3f}ms  "
        f"p999={sample['p999_ms']:.3f}ms  "
        f"err={sample['error_rate']:.4f}  "
        f"orders={sample['orders_processed']}  trades={sample['trades_count']}"
    )


class Aggregator:
    def __init__(self, coordinator: RedisCoordinator | None = None,
                 stop_event: threading.Event | None = None):
        self.coord = coordinator or RedisCoordinator()
        # Set externally (signal handler) to request a graceful shutdown.
        self.stop_event = stop_event or threading.Event()

        self.window = WindowAccumulator()
        self.window_start = time.monotonic()
        self.run_stats = RunAccumulator()
        self._last_report_at = 0.0

    # ── lifecycle ────────────────────────────────────────────────────────────

    def run(self) -> bool:
        """Full lifecycle. Returns True on a clean finish, False on abort."""
        self.coord.set_status("waiting")
        if not self._wait_for_start():
            self.coord.set_status("failed")
            return False

        consumer = ResponseConsumer()
        self.coord.set_status("running")
        self.window_start = time.monotonic()
        try:
            clean = self._consume_loop(consumer)
        except Exception:
            log.exception("aggregator crashed")
            self.coord.set_status("failed")
            raise
        finally:
            consumer.close()

        self._finish()
        self.coord.set_status("complete" if clean else "failed")
        return clean

    def _wait_for_start(self) -> bool:
        """Block until the run starts. False if aborted while waiting."""
        log.info("waiting for %s to become 'running'", self.coord.run_status_key)
        while not self.stop_event.is_set():
            if self.coord.run_started():
                log.info("run started — consuming %s", settings.RESPONSE_TOPIC)
                log.info("will stop when %s or %s is set to complete/done "
                         "(or the run is aborted)",
                         self.coord.bots_status_key, self.coord.run_status_key)
                return True
            if self.coord.run_aborted():
                log.info("run aborted before it started")
                return False
            time.sleep(settings.STATUS_POLL_S)
        return False

    def _consume_loop(self, consumer: ResponseConsumer) -> bool:
        """Poll → process → emit windows, until done/aborted.
        Returns True for a normal finish, False for an abort."""
        last_status_check = 0.0
        draining = False
        last_event_at = time.monotonic()

        while True:
            events = consumer.poll_events(settings.POLL_TIMEOUT_S)
            now = time.monotonic()
            if events:
                last_event_at = now
                for event in events:
                    self._process_event(event)

            if now - self.window_start >= settings.WINDOW_S:
                self._emit_window()
                self.window_start = now

            if self.stop_event.is_set():
                log.info("shutdown signal received — stopping")
                return True

            # Redis status checks are throttled: they steer the lifecycle, the
            # hot path is Kafka.
            if now - last_status_check >= settings.STATUS_POLL_S:
                last_status_check = now
                if self.coord.run_aborted():
                    log.info("run aborted — stopping")
                    return False
                if not draining and self.coord.test_finished():
                    log.info("test complete — draining remaining events")
                    draining = True
                    last_event_at = now  # full idle period from this point

            if draining and now - last_event_at >= settings.DRAIN_IDLE_S:
                log.info("kafka idle for %.1fs after bots finished — done",
                         settings.DRAIN_IDLE_S)
                return True

    def _finish(self):
        """Flush the open window and publish the whole-run summary."""
        self._emit_window()
        summary = self.run_stats.finalize()
        self.coord.publish_summary(summary)
        log.info(
            "run summary: %d orders, %d trades over %.1fs  (avg %.0f/s)\n"
            "    p50=%.3fms  p90=%.3fms  p95=%.3fms  p99=%.3fms  p999=%.3fms\n"
            "    error_rate=%.4f  bot-side errors excluded=%d",
            summary["total_orders"], summary["total_trades"],
            summary["duration_s"], summary["throughput_avg"],
            summary["p50_ms"], summary["p90_ms"], summary["p95_ms"],
            summary["p99_ms"], summary["p999_ms"],
            summary["error_rate"], summary["bot_errors"],
        )

    # ── per-event / per-window work ──────────────────────────────────────────

    def _process_event(self, event: dict):
        latency_ns = event.get("latency_ns")
        error_class = classify_error(event)
        trade_count = len(event.get("trades") or [])

        self.window.add(latency_ns, error_class, trade_count)
        self.run_stats.add(latency_ns, error_class, trade_count,
                           event.get("t_recv_ns"))

    def _emit_window(self):
        # No events this window → nothing to publish. The frontend keeps
        # showing the previous :latest; empty windows aren't zero-padded into
        # the history.
        if self.window.total == 0:
            return
        sample = self.window.snapshot(
            timestamp=time.time(),
            orders_processed=self.run_stats.total,
            trades_count=self.run_stats.trades,
            bot_errors_total=self.run_stats.bot_errors,
        )
        self.coord.publish_window(sample)
        self.window.reset()

        # Live console report, throttled so the terminal stays readable while
        # Redis still gets every window.
        now = time.monotonic()
        if now - self._last_report_at >= settings.CONSOLE_REPORT_S:
            self._last_report_at = now
            log.info("%s", _format_report(sample))
