"""Entry point: `python -m aggregator`.

Wires up logging and OS signal handling, then runs the service. SIGINT/SIGTERM
(Ctrl-C locally, pod termination on Kubernetes) request a graceful stop: the
open window and phase are flushed to Redis before exit.
"""

import logging
import signal
import sys
import threading

from aggregator import settings
from aggregator.service import Aggregator


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    log = logging.getLogger("aggregator")
    log.info("aggregator starting  test_id=%s  topic=%s",
             settings.TEST_ID, settings.RESPONSE_TOPIC)

    stop_event = threading.Event()

    def request_stop(signum, frame):
        log.info("received signal %s — shutting down gracefully", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    clean = Aggregator(stop_event=stop_event).run()
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
