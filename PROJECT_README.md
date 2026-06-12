# IICPC Summer Hackathon 2026 — Distributed Benchmarking Platform

## What This Project Is

A platform that evaluates contestant-submitted **matching engines** (order book implementations) by stress-testing them with synthetic order flow and measuring latency, throughput, and correctness.

Think of it like LeetCode, but instead of running a function against test cases, we deploy the contestant's entire C++ server in a container, connect trading bots to it via WebSocket, blast it with thousands of orders per second, and measure how fast and correctly it responds.

## The Three Major Components

```
┌─────────────────────┐     WebSocket      ┌──────────────────────┐
│   BOT FLEET          │ ←───────────────→  │  CONTESTANT ENGINE   │
│   (Python)           │   orders/responses  │  (Their C++ code)    │
│                      │                    │                      │
│  Order Generator     │                    │  Matching Engine     │
│  Async Sender/Recv   │                    │  Order Book          │
│  Telemetry Collector │                    │  WebSocket Server    │
└──────────┬───────────┘                    └──────────────────────┘
           │
           │ telemetry events
           ▼
┌─────────────────────┐
│   PLATFORM SERVICES  │
│                      │
│  Orchestrator        │  ← controls test lifecycle
│  Aggregator          │  ← computes p50/p99 latency from telemetry
│  Validator           │  ← replays orders through reference engine, checks correctness
│  Leaderboard (Next)  │  ← displays results
│  Redis               │  ← phase coordination + live metrics
│  Kafka               │  ← telemetry transport
└──────────────────────┘
```

## How a Test Run Works (End to End)

1. Contestant uploads their C++ matching engine code
2. Platform containerizes and deploys it in an isolated Kubernetes namespace
3. Orchestrator creates bot pods and signals them to start
4. Bots connect via WebSocket to the contestant's engine
5. Test runs through phases: build_book → light_mixed → heavy_mixed → cancel_storm → matching_spike → recovery
6. Each phase has different order flow characteristics (operation mix, rate, book depth)
7. Bots record latency for every order (send time → response time)
8. Telemetry flows to Kafka → Aggregator computes live metrics → Redis → Leaderboard
9. After test: Validator replays all orders through reference engine, compares results
10. Final score: latency percentiles + throughput + correctness

## What We Measure

**Latency (primary metric):** Time from bot sending an order to receiving the full response. This includes network round-trip + JSON parsing + matching engine processing + JSON serialization. Reported as p50, p90, p99, p99.9.

**Throughput:** Maximum sustained orders/second the engine can handle before latency degrades beyond a threshold.

**Correctness:** Does the engine produce the right trades? Verified by replaying the same order sequence through our reference engine and comparing outputs.

## Why Synthetic Order Flow (Not Market Data)

We generate orders algorithmically rather than replaying real market data because:

1. **Fairness:** Same seed = identical order sequence for every contestant. No contestant gets an easier test.
2. **Control:** We can precisely target specific engine properties (deep book cancellation, multi-level matching sweeps, burst load).
3. **Independence:** No dependency on contestant's market data output. The test difficulty doesn't change based on the contestant's implementation.
4. **Scalability:** Generate any volume on the fly. No file size limits.
5. **Reproducibility:** Seed + parameters fully determine the sequence. Any judge can re-run and verify.

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Matching Engine (reference) | C++20, Crow, Glaze | Performance-critical, needs microsecond processing |
| Bot Fleet | Python, asyncio, websockets | Fast development, sufficient performance for load generation |
| Orchestrator | Python, kubernetes-client | Manages K8s resources programmatically |
| Telemetry Transport | Kafka | High-throughput event streaming |
| Live Metrics / Coordination | Redis | Low-latency key-value for phase signals + metric storage |
| Leaderboard | Next.js | Real-time frontend reading from Redis |
| Infrastructure | GKE (Kubernetes), Terraform | Container orchestration + infrastructure as code |

## Current State (June 8, 2026)

**Complete:**
- C++ matching engine with multi-symbol support, cancel, modify, STP-CN
- Threading with ThreadSafeQueue and single-writer principle
- WebSocket server with Crow
- Glaze JSON serialization
- Order generator (Python) with configurable phases
- Phase configs for different stress scenarios
- Bot runner with async sender/receiver pipeline
- Bot successfully connects to engine and records telemetry

**In Progress:**
- Bot fleet improvements (multi-bot support, stats, telemetry flushing)
- Matching engine response format improvements
- Debug logging for both systems

**Not Started:**
- Kubernetes deployment
- Orchestrator
- Kafka/Redis integration
- Aggregator
- Validator
- Leaderboard frontend

## Repository Structure

```
project/
├── matching-engine/          # C++ matching engine + WebSocket server
│   ├── include/
│   │   └── matching_engine/
│   │       ├── type.hpp
│   │       ├── order.hpp
│   │       ├── order_book.hpp
│   │       ├── matching_engine.hpp
│   │       ├── thread_safe_queue.hpp
│   │       ├── server.hpp
│   │       ├── glaze_meta.hpp
│   │       └── json_helpers.hpp
│   ├── src/
│   │   ├── order_book.cpp
│   │   ├── matching_engine.cpp
│   │   ├── server.cpp
│   │   └── main.cpp
│   └── CMakeLists.txt
│
├── exchange-bot-fleet/       # Python bot fleet for stress testing
│   ├── order_generator.py    # Core order generation logic
│   ├── configs.py            # Phase configs and test plans
│   ├── bot_runner.py         # Async WebSocket bot
│   └── telemetry.py          # Telemetry collection (TODO)
│
├── platform/                 # Orchestrator + infrastructure (TODO)
│   ├── orchestrator/
│   ├── aggregator/
│   ├── validator/
│   └── terraform/
│
└── frontend/                 # Next.js leaderboard (TODO)
```
