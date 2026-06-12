"""Kafka side of the aggregator: a consumer that never gives up.

Two realities this wraps around:

  1. The aggregator usually starts BEFORE the bots have produced anything, so
     the order-response-{test_id} topic may not exist yet. That surfaces as
     UNKNOWN_TOPIC_OR_PART errors from the broker — we swallow them and keep
     polling; the subscription picks the topic up automatically once the first
     bot event creates it.

  2. Event flow is bursty. An empty poll means "nothing right now", never
     "we're done" — test completion is signalled via Redis, not via Kafka
     going quiet.
"""

import json
import logging
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from aggregator import settings

log = logging.getLogger("aggregator.kafka")

BATCH_SIZE = 500

# Broker errors that just mean "topic not there yet / caught up" — expected
# during startup and idle stretches, not failures.
_TRANSIENT_CODES = {
    KafkaError.UNKNOWN_TOPIC_OR_PART,
    KafkaError._PARTITION_EOF,
    KafkaError._TRANSPORT,
    KafkaError._ALL_BROKERS_DOWN,
}


class ResponseConsumer:
    """Consumes order_response events from order-response-{test_id}."""

    def __init__(self):
        self.topic = settings.RESPONSE_TOPIC
        self._warned_missing_topic = False
        self.consumer = Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group.id": settings.KAFKA_GROUP_ID,
            # Bots may start producing before we manage to connect — read the
            # topic from the beginning so no early events are missed.
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            # Don't let a missing topic kill the subscription.
            "allow.auto.create.topics": False,
        })
        self.consumer.subscribe([self.topic])
        log.info("subscribed to %s", self.topic)

    def poll_events(self, timeout_s: float) -> list[dict]:
        """One batch of parsed order_response events. Empty list when idle.
        Transient broker errors are logged (once for the missing topic) and
        absorbed — this method never raises for them."""
        try:
            msgs = self.consumer.consume(num_messages=BATCH_SIZE, timeout=timeout_s)
        except KafkaException as e:
            log.warning("kafka consume failed (%s) — backing off %.1fs",
                        e, settings.RETRY_BACKOFF_S)
            time.sleep(settings.RETRY_BACKOFF_S)
            return []

        events = []
        for msg in msgs:
            err = msg.error()
            if err is not None:
                if err.code() in _TRANSIENT_CODES:
                    if (err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART
                            and not self._warned_missing_topic):
                        log.info("topic %s does not exist yet — waiting for the "
                                 "bots to produce the first event", self.topic)
                        self._warned_missing_topic = True
                else:
                    log.warning("kafka error: %s", err)
                continue

            try:
                event = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                log.warning("dropping malformed event on %s", self.topic)
                continue
            if event.get("type") != "order_response":
                continue
            events.append(event)
        return events

    def close(self):
        try:
            self.consumer.close()
        except KafkaException:
            pass
