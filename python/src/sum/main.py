import os
import logging
from common import middleware, message_protocol, fruit_item
import threading

# cambio el sum para manejar datos por cliente
# ahora acumulo frutas por cliente, y cuando llega un EOF envio todo a UN solo Aggregator (por ahora)


DATA_MSG_LENGTH = 3
EOF_MSG_LENGTH = 2

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
        self._init_control_exchange()
        self.processed_clients = set()  # para trackear clientes que ya procese EOF y evitar duplicados
        # planeo usar threads asi que necesito lockssss
        self.storage_lock = threading.Lock()
        self.processed_clients_lock = threading.Lock()

    # inicializo conexiones a exchanges de salida (uno por Aggregator)
    def _init_output_exchanges(self):
        for i in range(self.config.AGGREGATION_AMOUNT):
            exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                self.config.MOM_HOST, 
                self.config.AGGREGATION_PREFIX, 
                [f"{self.config.AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(exchange)

    # inicializo conexión al exchange de control para enviar signals de EOF a los otros Sums
    def _init_control_exchange(self):
        # conexión separada para enviar (hilo principal)
        self.control_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            self.config.MOM_HOST,
            "sum_control_exchange",
            [],
            exchange_type="fanout"
        )
        # conexion separada para consumir (hilo de control)
        self.control_receiver = middleware.MessageMiddlewareExchangeRabbitMQ(
            self.config.MOM_HOST,
            "sum_control_exchange",
            [],
            exchange_type="fanout"
        )

    def _update_accumulation(self, client_id: str, fruit: str, amount: int):
        with self.storage_lock:
            client_data = self.storage.setdefault(client_id, {})
            current_item = client_data.get(fruit, fruit_item.FruitItem(fruit, 0))
            client_data[fruit] = current_item + fruit_item.FruitItem(fruit, int(amount))
            logging.debug(f"Sum {self.id}: Client {client_id} updated {fruit}.")

    def _flush_client_data(self, client_id: str):
        with self.storage_lock:
            if client_id not in self.storage:
                logging.warning(f"Sum {self.id}: No data for client {client_id}, skipping flush.")
                return
            items = list(self.storage[client_id].values())
            del self.storage[client_id]

        # broadcast fuera del lock para no bloquearlo durante I/O
        for item in items:
            msg = message_protocol.internal.serialize_client_data(
                client_id, item.fruit, item.amount
            )
            self._broadcast(msg)

        self._broadcast(message_protocol.internal.serialize_client_eof(client_id, self.id))
        logging.info(f"Sum {self.id}: Client {client_id} flushed and cleared.")

    def _broadcast(self, payload: bytes):
        for exchange in self.data_output_exchanges:
            exchange.send(payload)

    def on_message_received(self, body, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(body)
            if len(fields) == DATA_MSG_LENGTH:
                self._update_accumulation(*fields)
            elif len(fields) == EOF_MSG_LENGTH:
                #self._flush_client_data(fields[0])
                # cuando me llega un EOF, debo mandar un signal a los demas Sums por el fanout exchange 
                # para que ellos también hagan flush de ese cliente, y así me aseguro que el resultado final se envíe aunque un sum no haya recibido datos de ese cliente
                client_id = fields[0]
                self.control_sender.send(message_protocol.internal.serialize_client_eof_signal(client_id, self.id))
            ack()
        except Exception as e:
            logging.error(f"Sum {self.id}: Error processing message: {e}")
            nack()

    def on_control_message(self, body, ack, nack):
        # este es el callback para el exchange de control, cuando me llega un signal del sum que le llego el EOF
        # me fijo si el cliente ya fue procesado, y si no, envío el resultado final
        # si me llego tengo que flushear  

        # update:  mando un signal de EOF con el client id y el sum id que lo envió, así los otros Sums saben que tienen que hacer flush de ese cliente, y si ya lo hicieron, lo ignoran porque ya lo procesaron (lo trackeo con processed_clients)
        # agrego proteccion de concurrencia con locks porque el callback de control y el callback de datos pueden correr concurrentemente y ambos acceden a processed_clients y al storage para hacer flush
        try:
            client_id = message_protocol.internal.deserialize_control_signal(body)
            with self.processed_clients_lock:
                if client_id in self.processed_clients:
                    logging.warning(f"Sum {self.id}: Duplicate EOF signal for client {client_id}, ignoring.")
                    ack()
                    return
                self.processed_clients.add(client_id)

            logging.info(f"Sum {self.id}: Flushing client {client_id} from control signal.")
            self._flush_client_data(client_id)
            ack()
        except Exception as e:
            logging.error(f"Sum {self.id}: Error processing control message: {e}")
            nack()

    def _consume_control(self):
        # aca tengo que crear la cola privada para este sum y bindearla al exchange de control
        self.control_receiver.start_consuming(self.on_control_message)
        logging.info(f"Sum {self.id}: Started consuming control messages on {self.control_receiver._exchange_name}")
            
    def start(self):
        control_thread = threading.Thread(target=self._consume_control, daemon=True)
        logging.info(f"Sum {self.id}: Starting control thread for {self.control_receiver._exchange_name}...")
        control_thread.start()

        logging.info(f"Sum {self.id}: Starting consumer on {self.config.INPUT_QUEUE}...")
        self.input_queue.start_consuming(self.on_message_received)

def main():
    config = Config()
    logging.basicConfig(level=config.LOG_LEVEL)
    sum_filter = SumFilter(config)
    sum_filter.start()

if __name__ == "__main__":
    main()