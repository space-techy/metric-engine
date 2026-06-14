"""Unit tests for the pure computation layer (metrics.py)."""

import pytest

from aggregator.metrics import (
    BOT_ERROR,
    ENGINE_ERROR,
    OK,
    LatencyDigest,
    RunAccumulator,
    WindowAccumulator,
    classify_error,
    latency_summary_ms,
    percentile,
)


# ── error classification ─────────────────────────────────────────────────────

def test_clean_response_is_ok():
    assert classify_error({"message_code": 0, "error": ""}) == OK


def test_rejection_is_engine_error():
    assert classify_error({"message_code": 5, "error": "price out of band"}) == ENGINE_ERROR


def test_order_not_found_is_bot_error_not_engine():
    # Bot cancelled an order that was already filled — fleet limitation,
    # must not count against the contestant's engine.
    event = {"message_code": 5, "error": "Order not found"}
    assert classify_error(event) == BOT_ERROR


def test_missing_fields_default_to_ok():
    assert classify_error({}) == OK


# ── percentiles ──────────────────────────────────────────────────────────────

def test_percentile_empty():
    assert percentile([], 0.99) == 0.0


def test_percentile_single_value():
    assert percentile([7], 0.5) == 7
    assert percentile([7], 0.999) == 7


def test_percentile_never_indexes_out_of_range():
    vals = list(range(100))
    assert percentile(vals, 0.999) == 99
    assert percentile(vals, 0.50) == 50


def test_latency_summary_has_full_spread():
    latencies_ns = [i * 1_000_000 for i in range(1, 101)]  # 1..100 ms
    summary = latency_summary_ms(latencies_ns)
    assert summary["p50_ms"] == pytest.approx(51.0)
    assert summary["p90_ms"] == pytest.approx(91.0)
    assert summary["p95_ms"] == pytest.approx(96.0)
    assert summary["p99_ms"] == pytest.approx(100.0)
    assert summary["p999_ms"] == pytest.approx(100.0)


# ── window accumulator ───────────────────────────────────────────────────────

def make_window():
    w = WindowAccumulator()
    # 10 clean orders at 1..10 ms, one engine rejection, one bot error
    for i in range(1, 11):
        w.add(i * 1_000_000, OK, trade_count=2)
    w.add(50_000_000, ENGINE_ERROR, trade_count=0)
    w.add(60_000_000, BOT_ERROR, trade_count=0)
    return w


def test_window_snapshot_counts():
    snap = make_window().snapshot(
        timestamp=123.0,
        orders_processed=12, trades_count=20, bot_errors_total=1)
    assert snap["throughput"] == 12
    assert snap["trades_per_sec"] == 20
    assert snap["sample_count"] == 12
    # bot error excluded from the error-rate denominator: 1 engine error / 11
    assert snap["error_rate"] == pytest.approx(1 / 11)
    assert snap["bot_errors"] == 1
    for key in ("p50_ms", "p90_ms", "p95_ms", "p99_ms", "p999_ms"):
        assert key in snap


def test_window_reset_clears_everything():
    w = make_window()
    w.reset()
    assert w.total == 0 and w.trades == 0 and not w.latencies_ns
    assert w.engine_errors == 0 and w.bot_errors == 0


def test_empty_window_snapshot_has_zero_percentiles():
    snap = WindowAccumulator().snapshot(
        timestamp=0.0,
        orders_processed=0, trades_count=0, bot_errors_total=0)
    assert snap["p50_ms"] == 0.0 and snap["p999_ms"] == 0.0
    assert snap["error_rate"] == 0.0


# ── whole-run accumulator ────────────────────────────────────────────────────

def test_run_finalize_throughput_and_duration():
    r = RunAccumulator()
    t0 = 1_000_000_000_000
    # 100 orders spread over ~2 seconds of t_recv_ns
    for i in range(100):
        r.add(2_000_000, OK, trade_count=1, t_recv_ns=t0 + i * 20_000_000)
    summary = r.finalize()
    assert summary["total_orders"] == 100
    assert summary["total_trades"] == 100
    assert summary["duration_s"] == pytest.approx(99 * 0.02)
    assert summary["throughput_avg"] == pytest.approx(100 / (99 * 0.02))
    # HdrHistogram (3 sig figs) quantizes within ~0.1%, so allow a small rel tol.
    assert summary["p50_ms"] == pytest.approx(2.0, rel=2e-3)


def test_run_summary_reports_full_tail_and_no_mean():
    r = RunAccumulator()
    # 1..1000 ms — a wide spread so the deep tail is meaningful.
    for i in range(1, 1001):
        r.add(i * 1_000_000, OK, trade_count=0, t_recv_ns=i)
    summary = r.finalize()
    for key in ("p50_ms", "p99_ms", "p999_ms", "p9999_ms", "max_ms"):
        assert key in summary
    # Tail percentiles must be ordered and bounded by the max observed sample.
    assert summary["p50_ms"] <= summary["p99_ms"] <= summary["p999_ms"]
    assert summary["p999_ms"] <= summary["p9999_ms"] <= summary["max_ms"]
    assert summary["max_ms"] == pytest.approx(1000.0, rel=2e-3)
    # The reported metric is labelled honestly and carries no latency mean.
    assert "round-trip" in summary["latency_metric"]
    assert not any("mean" in k or "avg_latency" in k for k in summary)


def test_latency_digest_holds_full_population():
    d = LatencyDigest()
    for i in range(1, 101):
        d.record(i * 1_000_000)  # 1..100 ms
    assert d.count == 100
    s = d.summary_ms()
    assert s["p50_ms"] == pytest.approx(50.0, rel=5e-2)
    assert s["max_ms"] == pytest.approx(100.0, rel=2e-3)
    # A None sample is ignored (no latency on that response).
    d.record(None)
    assert d.count == 100


def test_run_with_single_event_does_not_divide_by_zero():
    r = RunAccumulator()
    r.add(1_000_000, OK, trade_count=0, t_recv_ns=123)
    summary = r.finalize()
    assert summary["total_orders"] == 1
    assert summary["throughput_avg"] >= 0


def test_run_error_rate_excludes_bot_errors():
    r = RunAccumulator()
    for _ in range(8):
        r.add(1_000_000, OK, trade_count=0, t_recv_ns=None)
    r.add(1_000_000, ENGINE_ERROR, trade_count=0, t_recv_ns=None)
    r.add(1_000_000, BOT_ERROR, trade_count=0, t_recv_ns=None)
    summary = r.finalize()
    assert summary["error_rate"] == pytest.approx(1 / 9)
    assert summary["bot_errors"] == 1
