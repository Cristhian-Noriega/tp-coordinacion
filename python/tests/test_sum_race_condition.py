import sys
import types
import unittest
from unittest.mock import patch

from common import message_protocol

if "pika" not in sys.modules:
    sys.modules["pika"] = types.SimpleNamespace(
        exceptions=types.SimpleNamespace(
            AMQPConnectionError=Exception,
            AMQPChannelError=Exception,
            StreamLostError=Exception,
        ),
        BlockingConnection=None,
        ConnectionParameters=None,
    )

from sum.main import Config, SumFilter


class _FakeQueue:
    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name

    def start_consuming(self, _callback):
        return None

    def stop_consuming(self):
        return None

    def close(self):
        return None


class _FakeExchange:
    instances = []

    def __init__(self, host, exchange_name, routing_keys, exchange_type="direct"):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.exchange_type = exchange_type
        self.sent_messages = []
        _FakeExchange.instances.append(self)

    def send(self, message):
        self.sent_messages.append(message)

    def start_consuming(self, _callback):
        return None

    def stop_consuming(self):
        return None

    def close(self):
        return None


class SumRaceConditionTest(unittest.TestCase):
    def setUp(self):
        _FakeExchange.instances.clear()

    def test_sum_should_not_finalize_before_late_data_is_accounted(self):
        config = Config()
        config.ID = 0
        config.MOM_HOST = "fake-host"
        config.INPUT_QUEUE = "input"
        config.AGGREGATION_AMOUNT = 1
        config.AGGREGATION_PREFIX = "agg"

        with patch("sum.main.middleware.MessageMiddlewareQueueRabbitMQ", _FakeQueue), patch(
            "sum.main.middleware.MessageMiddlewareExchangeRabbitMQ", _FakeExchange
        ):
            sum_filter = SumFilter(config)
            client_id = "client-race"

            ack_count = {"value": 0}
            nack_count = {"value": 0}

            def ack():
                ack_count["value"] += 1

            def nack():
                nack_count["value"] += 1

            # DATA arrives and is accumulated normally.
            first_data = message_protocol.internal.serialize_client_data(
                client_id, "apple", 10
            )
            sum_filter.on_message_received(first_data, ack, nack)

            # EOF is received and only a control signal is emitted (no flush here).
            eof_original = message_protocol.internal.serialize_client_eof(client_id, 0)
            sum_filter.on_message_received(eof_original, ack, nack)

            # Control signal is processed before a late data message arrives.
            control_signal = message_protocol.internal.serialize_client_eof_signal(
                client_id, 0
            )
            sum_filter.on_control_message(control_signal, ack, nack)

            # Late data arrives after flush, recreating storage for an already processed client.
            late_data = message_protocol.internal.serialize_client_data(
                client_id, "banana", 5
            )
            sum_filter.on_message_received(late_data, ack, nack)

        self.assertEqual(nack_count["value"], 0, "No message should be nacked in this flow.")
        self.assertEqual(ack_count["value"], 4, "All callbacks should ack successfully.")
        self.assertNotIn(
            client_id,
            sum_filter.storage,
            "Client state should remain closed after finalize.",
        )

        aggregation_exchange = next(
            exchange
            for exchange in _FakeExchange.instances
            if exchange.exchange_name == config.AGGREGATION_PREFIX
        )
        sent_records = [
            message_protocol.internal.deserialize(payload)
            for payload in aggregation_exchange.sent_messages
        ]

        self.assertIn([client_id, "apple", 10], sent_records)
        self.assertIn(
            [client_id, "banana", 5],
            sent_records,
            "Late data must be included before EOF/finalization.",
        )


if __name__ == "__main__":
    unittest.main()
