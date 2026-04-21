import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # necesito acumular los tops parciales por cliente
        self.partial_tops: dict[str, list] = {}
        self.agg_completed: dict[str, int] = {} # esto para saber cuantos aggs respondieron

    def process_messsage(self, message, ack, nack):

        # cuando llega un top parcial de un aggregator, lo fusiono con los anteriores
        # recien cuando llegan todos, calculo el top final y mando al gateway
        try:
            logging.info("Join: Received message")
            fields = message_protocol.internal.deserialize(message)
            logging.info(f"Join: Deserialized: {fields}, forwarding to output")

            client_id = fields[0]
            partial_top = fields[1]

            self.partial_tops.setdefault(client_id, [])
            self.partial_tops[client_id].extend(partial_top)

            self.agg_completed[client_id] = self.agg_completed.get(client_id, 0) + 1

            logging.info(f"Join: Partial top received for client {client_id}. Total partials received: {self.agg_completed[client_id]}/{AGGREGATION_AMOUNT}")

            if self.agg_completed[client_id] == AGGREGATION_AMOUNT:
                self.merge_and_send(client_id)

            ack()


        except:
            logging.error("Join: Error processing message", exc_info=True)
            nack()

    def merge_and_send(self, client_id: str):
        all_fruits = self.partial_tops.get(client_id, [])

        sorted_fruits = sorted(all_fruits, key=lambda x: x[1], reverse=True)
        top = sorted_fruits[:TOP_SIZE]

        logging.info(f"Join: Final top for client {client_id}: {top}")
        self.output_queue.send(message_protocol.internal.serialize_client_result(client_id, top))
        del self.partial_tops[client_id]
        del self.agg_completed[client_id]
    
    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
