import os
import logging
import bisect
from typing import Dict, List, Set

from common import middleware, message_protocol, fruit_item

# cambio el Aggregator para procesar datos por cliente en lugar de globalmente
# ahora mantengo un top por cliente, y cuando llega EOF, envio el top final

DATA_MSG_LENGTH = 3
EOF_MSG_LENGTH = 2

class Config:
    ID = int(os.environ.get("ID", 0))
    MOM_HOST = os.environ.get("MOM_HOST", "localhost")
    OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "output")
    AGGREGATION_PREFIX = os.environ.get("AGGREGATION_PREFIX", "agg_prefix")
    TOP_SIZE = int(os.environ.get("TOP_SIZE", 3))
    LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
    SUM_AMOUNT = int(os.environ.get("SUM_AMOUNT", 1))

class AggregationFilter:
    def __init__(self, config: Config):
        self.config = config
        self.id = config.ID
        
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            config.MOM_HOST, 
            config.AGGREGATION_PREFIX, 
            [f"{config.AGGREGATION_PREFIX}_{self.id}"]
        )
        
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            config.MOM_HOST, config.OUTPUT_QUEUE
        )
        
        self.storage: Dict[str, List[fruit_item.FruitItem]] = {}
        self.sums_completed: Dict[str, Set[int]] = {}
        self.sum_amount = int(os.environ.get("SUM_AMOUNT", 1))

    def _update_top(self, client_id: str, fruit: str, amount: int):
        fruit_list = self.storage.setdefault(client_id, [])
        new_item = fruit_item.FruitItem(fruit, int(amount))

        for i, item in enumerate(fruit_list):
            if item.fruit == fruit:
                updated_item = item + new_item
                fruit_list.pop(i)
                bisect.insort(fruit_list, updated_item)
                return

        bisect.insort(fruit_list, new_item)

    def _send_final_result(self, client_id: str):
        # al recibir EOF, tomo el top de frutas y lo envío al joiner
        if client_id not in self.storage:
            logging.warning(f"Aggregator {self.id}: No data for client {client_id}")
            return

        top_items = self.storage[client_id][-self.config.TOP_SIZE:]
        top_items.reverse()

        formatted_results = [(fruit_item.fruit, fruit_item.amount) for fruit_item in top_items]

        logging.info(f"Aggregator {self.id}: Sending TOP {self.config.TOP_SIZE} for client {client_id}")
        
        msg = message_protocol.internal.serialize_client_result(client_id, formatted_results)
        self.output_queue.send(msg)
        del self.storage[client_id]

    def on_message_received(self, body, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(body)
            
            if len(fields) == DATA_MSG_LENGTH:
                self._update_top(fields[0], fields[1], fields[2])
                
            elif len(fields) == EOF_MSG_LENGTH:
                client_id = fields[0]
                sum_id = fields[1]
                
                if client_id not in self.sums_completed:
                    self.sums_completed[client_id] = set()
                
                self.sums_completed[client_id].add(sum_id)
                
                logging.info(f"Aggregator {self.id}: Received EOF from Sum {sum_id} for client {client_id} "
                           f"({len(self.sums_completed[client_id])}/{self.config.SUM_AMOUNT})")
                
                if len(self.sums_completed[client_id]) == self.sum_amount:
                    self._send_final_result(client_id)
                    del self.sums_completed[client_id]
            
            ack()
            
        except Exception as e:
            logging.error(f"Aggregator {self.id}: Error processing message: {e}")
            nack()

    def start(self):
        logging.info(f"Aggregator {self.id}: Consuming from {self.config.AGGREGATION_PREFIX}_{self.id}")
        self.input_exchange.start_consuming(self.on_message_received)

def main():
    config = Config()
    logging.basicConfig(level=config.LOG_LEVEL)
    aggregator = AggregationFilter(config)
    aggregator.start()

if __name__ == "__main__":
    main()