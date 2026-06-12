"""Live metrics aggregator for the benchmarking platform.

Consumes order_response telemetry from Kafka, computes 1-second latency /
throughput / error-rate windows, and publishes them to Redis for the
leaderboard frontend.
"""

__version__ = "0.1.0"
