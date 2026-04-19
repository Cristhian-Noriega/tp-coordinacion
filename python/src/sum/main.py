import os
import logging
from common import middleware, message_protocol, fruit_item

# cambio el sum para manejar datos por cliente
# ahora acumulo frutas por cliente, y cuando llega un EOF envio todo a UN solo Aggregator (por ahora)


DATA_MSG_LENGTH = 3
EOF_MSG_LENGTH = 1

class Config: 
    ID = int(os.environ.get("ID", 0))
    MOM_HOST = os.environ.get("MOM_HOST", "localhost")
    INPUT_QUEUE = os.environ.get("INPUT_QUEUE", "input")
    AGGREGATION_AMOUNT = int(os.environ.get("AGGREGATION_AMOUNT", 1))
    AGGREGATION_PREFIX = os.environ.get("AGGREGATION_PREFIX", "output")
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

class SumFilter:
    def __init__(self, config: Config):
        self.id = config.ID
        self.config = config
        # siendo client id -> fruta -> acumulado, y  FruitItem para suma y orden
        self.storage: dict[str, dict[str, fruit_item.FruitItem]] = {}
        
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            self.config.MOM_HOST, self.config.INPUT_QUEUE
        )
        
        self.data_output_exchanges = []
        self._init_output_exchanges()

    # inicializo conexiones a exchanges de salida (uno por Aggregator)
    def _init_output_exchanges(self):
        for i in range(self.config.AGGREGATION_AMOUNT):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                self.config.MOM_HOST, 
                self.config.AGGREGATION_PREFIX, 
                [f"{self.config.AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(exchange)

    def _update_accumulation(self, client_id: str, fruit: str, amount: int):
        client_data = self.storage.setdefault(client_id, {})
        current_item = client_data.get(fruit, fruit_item.FruitItem(fruit, 0))
        client_data[fruit] = current_item + fruit_item.FruitItem(fruit, int(amount))
        logging.debug(f"Sum {self.id}: Client {client_id} updated {fruit}.")

    def _flush_client_data(self, client_id: str):
        if client_id not in self.storage:
            logging.warning(f"Sum {self.id}: Received EOF for unknown client {client_id}")
            return

        for item in self.storage[client_id].values():
            msg = message_protocol.internal.serialize_client_data(
                client_id, item.fruit, item.amount
            )
            self._broadcast(msg)

        self._broadcast(message_protocol.internal.serialize_client_eof(client_id))
        del self.storage[client_id]
        logging.info(f"Sum {self.id}: Client {client_id} processed and cleared.")

    def _broadcast(self, payload: bytes):

        for exchange in self.data_output_exchanges:
            exchange.send(payload)

    def on_message_received(self, body, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(body)
            if len(fields) == DATA_MSG_LENGTH:
                self._update_accumulation(*fields)
            elif len(fields) == EOF_MSG_LENGTH:
                self._flush_client_data(fields[0])
            ack()
        except Exception as e:
            logging.error(f"Sum {self.id}: Error processing message: {e}")
            nack()
            
    def start(self):
        logging.info(f"Sum {self.id}: Starting consumer on {self.config.INPUT_QUEUE}...")
        self.input_queue.start_consuming(self.on_message_received)

def main():
    config = Config()
    logging.basicConfig(level=config.LOG_LEVEL)
    sum_filter = SumFilter(config)
    sum_filter.start()

if __name__ == "__main__":
    main()