"""Pure metric computation: error classification, percentiles, window and
whole-run accumulators.

No I/O lives here, which keeps everything in this module trivially unit-testable.
"""

from dataclasses import dataclass, field

from aggregator import settings

# HdrHistogram gives correct tail percentiles (p999/p9999) at bounded memory,
# which a growing Python list can't promise over a long high-rate run. It is the
# source of truth for the whole-run SUMMARY. If the wheel isn't installed we fall
# back to an exact sorted-list digest (more memory, identical semantics) so the
# aggregator still runs everywhere — both paths report the FULL POPULATION, never
# an average of per-window percentiles.
try:
    from hdrh.histogram import HdrHistogram
except ImportError:  # pragma: no cover - exercised only when hdrh is absent
    HdrHistogram = None

# What we report, and what we deliberately DON'T. This string travels with the
# summary so every consumer (frontend included) labels the number honestly.
LATENCY_METRIC_LABEL = (
    "end-to-end round-trip latency, client-measured, coordinated-omission "
    "corrected, under fixed offered load"
)

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
    """The full percentile spread, in milliseconds. Used for the 1-second LIVE
    windows (small lists, exact). The whole-run summary uses LatencyDigest."""
    s = sorted(latencies_ns)
    return {
        "p50_ms": percentile(s, 0.50) / 1e6,
        "p90_ms": percentile(s, 0.90) / 1e6,
        "p95_ms": percentile(s, 0.95) / 1e6,
        "p99_ms": percentile(s, 0.99) / 1e6,
        "p999_ms": percentile(s, 0.999) / 1e6,
    }


# ── whole-run latency digest (HdrHistogram, with an exact fallback) ───────────

# Track 1ns … 5 minutes at 3 significant figures (~0.1% quantization). Anything
# slower than the ceiling is clamped to it rather than dropped, so a pathological
# tail still shows up as "at least this bad".
_HDR_LOWEST_NS = 1
_HDR_HIGHEST_NS = 5 * 60 * 1_000_000_000  # 5 minutes
_HDR_SIG_FIGS = 3


class LatencyDigest:
    """Accumulates the whole run's latency samples and yields tail percentiles.

    Prefers HdrHistogram (bounded memory, correct deep tail); falls back to an
    exact sorted list when hdrh isn't installed. Either way it holds the ENTIRE
    population — the summary is never a mean of per-window percentiles."""

    def __init__(self):
        self.count = 0
        if HdrHistogram is not None:
            self._hist = HdrHistogram(_HDR_LOWEST_NS, _HDR_HIGHEST_NS, _HDR_SIG_FIGS)
            self._values = None
        else:
            self._hist = None
            self._values = []  # ns

    def record(self, latency_ns) -> None:
        if latency_ns is None:
            return
        v = int(latency_ns)
        if v < 1:
            v = 1  # HdrHistogram won't record < lowest; a 0 sample is ~1ns
        self.count += 1
        if self._hist is not None:
            self._hist.record_value(min(v, _HDR_HIGHEST_NS))
        else:
            self._values.append(v)

    def _at(self, q: float) -> float:
        if self.count == 0:
            return 0.0
        if self._hist is not None:
            return self._hist.get_value_at_percentile(q * 100.0)
        return percentile(sorted(self._values), q)

    def _max(self) -> float:
        if self.count == 0:
            return 0.0
        if self._hist is not None:
            return self._hist.get_max_value()
        return max(self._values)

    def summary_ms(self) -> dict:
        """p50 … p9999 + max, in milliseconds. No mean — by design."""
        return {
            "p50_ms": self._at(0.50) / 1e6,
            "p90_ms": self._at(0.90) / 1e6,
            "p95_ms": self._at(0.95) / 1e6,
            "p99_ms": self._at(0.99) / 1e6,
            "p999_ms": self._at(0.999) / 1e6,
            "p9999_ms": self._at(0.9999) / 1e6,
            "max_ms": self._max() / 1e6,
            "sample_count": self.count,
            "latency_metric": LATENCY_METRIC_LABEL,
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
    agg:{team}:summary. Latency percentiles come from an HdrHistogram over the
    full sample population (LatencyDigest) — NOT an average of the per-window
    percentiles, which would be statistically meaningless for a tail metric.

    ``throughput_avg`` is an average of THROUGHPUT (orders/sec), which is a valid
    thing to average; no latency value here is ever a mean."""

    digest: LatencyDigest = field(default_factory=LatencyDigest)
    total: int = 0
    engine_errors: int = 0
    bot_errors: int = 0
    trades: int = 0
    first_recv_ns: int | None = None
    last_recv_ns: int | None = None

    def add(self, latency_ns, error_class: str, trade_count: int, t_recv_ns):
        self.total += 1
        self.digest.record(latency_ns)
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
            **self.digest.summary_ms(),
        }
