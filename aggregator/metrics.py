"""Pure metric computation: error classification, percentiles, window and
whole-run accumulators.

No I/O lives here, which keeps everything in this module trivially unit-testable.
"""

from dataclasses import dataclass, field

from aggregator import settings

# ── error classification ─────────────────────────────────────────────────────
# Three buckets per event:
#   "ok"     → successful response
#   "engine" → the engine rejected/failed the order → counts toward error_rate
#   "bot"    → our own fleet's limitation (e.g. "order not found" because the
#              bot cancelled an order that had already been filled). Not the
#              engine's fault, so kept OUT of error_rate and reported
#              separately as bot_errors.

OK = "ok"
ENGINE_ERROR = "engine"
BOT_ERROR = "bot"


def classify_error(event: dict) -> str:
    error_text = (event.get("error") or "").lower()
    if error_text and any(p in error_text for p in settings.EXCLUDED_ERROR_PATTERNS):
        return BOT_ERROR
    if event.get("message_code") == settings.REJECT_MESSAGE_CODE:
        return ENGINE_ERROR
    return OK


# ── percentiles ──────────────────────────────────────────────────────────────

def percentile(sorted_values: list, q: float) -> float:
    """Nearest-rank percentile over an already-sorted list. 0.0 when empty."""
    if not sorted_values:
        return 0.0
    idx = min(len(sorted_values) - 1, int(len(sorted_values) * q))
    return sorted_values[idx]


def latency_summary_ms(latencies_ns: list) -> dict:
    """The full percentile spread, in milliseconds."""
    s = sorted(latencies_ns)
    return {
        "p50_ms": percentile(s, 0.50) / 1e6,
        "p90_ms": percentile(s, 0.90) / 1e6,
        "p95_ms": percentile(s, 0.95) / 1e6,
        "p99_ms": percentile(s, 0.99) / 1e6,
        "p999_ms": percentile(s, 0.999) / 1e6,
    }


# ── 1-second window ──────────────────────────────────────────────────────────

@dataclass
class WindowAccumulator:
    """Stats for the current window. Reset after every emit."""

    latencies_ns: list = field(default_factory=list)
    total: int = 0
    engine_errors: int = 0
    bot_errors: int = 0
    trades: int = 0

    def add(self, latency_ns, error_class: str, trade_count: int):
        self.total += 1
        if latency_ns is not None:
            self.latencies_ns.append(latency_ns)
        if error_class == ENGINE_ERROR:
            self.engine_errors += 1
        elif error_class == BOT_ERROR:
            self.bot_errors += 1
        self.trades += trade_count

    def reset(self):
        self.latencies_ns.clear()
        self.total = 0
        self.engine_errors = 0
        self.bot_errors = 0
        self.trades = 0

    def snapshot(self, *, timestamp: float,
                 orders_processed: int, trades_count: int,
                 bot_errors_total: int) -> dict:
        """One metrics sample for agg:{team}:latest / :history."""
        return {
            "timestamp": timestamp,
            "throughput": self.total,
            "trades_per_sec": self.trades,
            # bot-side errors are excluded from both numerator and denominator:
            # those requests never gave the engine real work to fail at.
            "error_rate": self.engine_errors / max(1, self.total - self.bot_errors),
            "bot_errors": self.bot_errors,
            "sample_count": len(self.latencies_ns),
            "orders_processed": orders_processed,
            "trades_count": trades_count,
            "bot_errors_total": bot_errors_total,
            **latency_summary_ms(self.latencies_ns),
        }


# ── whole-run rollup ─────────────────────────────────────────────────────────

@dataclass
class RunAccumulator:
    """Latency stats over the entire run; finalized once at shutdown into
    agg:{team}:summary. Keeps every latency sample in memory — fine for test
    runs lasting minutes, revisit if runs grow to hours."""

    latencies_ns: list = field(default_factory=list)
    total: int = 0
    engine_errors: int = 0
    bot_errors: int = 0
    trades: int = 0
    first_recv_ns: int | None = None
    last_recv_ns: int | None = None

    def add(self, latency_ns, error_class: str, trade_count: int, t_recv_ns):
        self.total += 1
        if latency_ns is not None:
            self.latencies_ns.append(latency_ns)
        if error_class == ENGINE_ERROR:
            self.engine_errors += 1
        elif error_class == BOT_ERROR:
            self.bot_errors += 1
        self.trades += trade_count
        if t_recv_ns is not None:
            if self.first_recv_ns is None:
                self.first_recv_ns = t_recv_ns
            self.last_recv_ns = t_recv_ns

    def finalize(self) -> dict:
        if self.first_recv_ns is not None and self.last_recv_ns is not None:
            duration_s = max(1e-9, (self.last_recv_ns - self.first_recv_ns) / 1e9)
        else:
            duration_s = 0.0
        return {
            "throughput_avg": self.total / duration_s if duration_s else 0.0,
            "error_rate": self.engine_errors / max(1, self.total - self.bot_errors),
            "bot_errors": self.bot_errors,
            "total_orders": self.total,
            "total_trades": self.trades,
            "duration_s": duration_s,
            **latency_summary_ms(self.latencies_ns),
        }
