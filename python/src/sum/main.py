import os
import logging
from common import middleware, message_protocol, fruit_item
import threading
import zlib
import signal

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

        # creo un lock unico para ver la cantidad de mensajes in flight. Esto lo van a ver control y data threads
        self.in_flight: dict[str, int] = {}
        # este lock lo comparten para in flight y drain events
        self.in_flight_lock = threading.Lock()

        # creo un dict que mapea cada client id a su threading event. Un event es una señal que se puede setear y waitear hilos
        # con estos events, el data thread llama a set y el control thread atiende el EOF y hace wait al event
        self.drain_events: dict[str, threading.Event] = {}
        

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
                # igual tengo que mandar el EOF porque si no, el Join se queda esperando un resultado que nunca llega porque el control thread no sabe que ese cliente ya no tiene datos y que ya se procesó
                self._broadcast(message_protocol.internal.serialize_client_eof(client_id, self.id))
                return
            items = list(self.storage[client_id].values())
            del self.storage[client_id]

        # broadcast fuera del lock para no bloquearlo durante I/O
        for item in items:
            msg = message_protocol.internal.serialize_client_data(
                client_id, item.fruit, item.amount
            )
            self._route(item.fruit, msg)

        self._broadcast(message_protocol.internal.serialize_client_eof(client_id, self.id))
        logging.info(f"Sum {self.id}: Client {client_id} flushed and cleared.")

    def _broadcast(self, payload: bytes):
        for exchange in self.data_output_exchanges:
            exchange.send(payload)

    def _route(self, fruit: str, payload: bytes):
        index = zlib.crc32(fruit.encode()) % len(self.data_output_exchanges)
        self.data_output_exchanges[index].send(payload)

    def on_message_received(self, body, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(body)

            # PROBLEMAaa -> no hay ningun registro de que ese mensaje esta en vuelo. El hilo de datos procesa y listo
            # el hilo de control no tiene forma de saber cuantos mensajes estan siendo procesados en este momento
            # 
            # con el lock tomado, ahora debo incrementar el in flight count
            if len(fields) == DATA_MSG_LENGTH:
                client_id = fields[0]
                with self.processed_clients_lock:
                    if client_id in self.processed_clients:
                        logging.warning(f"Sum {self.id}: Ignoring data message for already processed client {client_id}")
                        ack()
                        return
                with self.in_flight_lock:
                    logging.info(f"Sum {self.id}: Incrementing in_flight for client {client_id} (current: {self.in_flight.get(client_id, 0)})")
                    self.in_flight[client_id] = self.in_flight.get(client_id, 0) + 1
                self._update_accumulation(*fields)
                with self.in_flight_lock:
                    # decremento antes del check
                    self.in_flight[client_id] -= 1
                    logging.info(f"Sum {self.id}: Decremented in_flight for client {client_id} (remaining: {self.in_flight[client_id]}). Event set: {client_id in self.drain_events}")
                    if self.in_flight[client_id] == 0:
                        if client_id in self.drain_events:
                            self.drain_events[client_id].set()
                        del self.in_flight[client_id]
                        self.drain_events.pop(client_id, None) # limpio el event por las dudas, si ya se seteo y se hizo wait, no hace nada, y si no se seteo, lo borro del dict para que no quede basura
                        
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

            # PROBLEMAAAA -> luego del lock, se flushea al aggregator los datos el control thread
            # SIN SABER si en el data thead llegaron todos los datos de ese cliente

            # debo agregar la espera, crear el event, guardarlo en drain_events, verificar si ya es 0, llamar a wait
            # y luego limpiar el event del dict despues del flush
            
            with self.processed_clients_lock:
                logging.info(f"Sum {self.id}: Processing EOF signal for client {client_id}. Already processed: {client_id in self.processed_clients}")
                if client_id in self.processed_clients:
                    logging.warning(f"Sum {self.id}: Duplicate EOF signal for client {client_id}, ignoring.")
                    ack()
                    return
                self.processed_clients.add(client_id)

            event = threading.Event()
            logging.info(f"Sum {self.id}: Creating drain event for client {client_id}. In-flight count: {self.in_flight.get(client_id, 0)}")
            with self.in_flight_lock:
                self.drain_events[client_id] = event # lo guardo para que el data thread lo pueda usar
                if self.in_flight.get(client_id, 0) == 0:
                    event.set() # si ya no hay nada en vuelo, lo setea el mismo
            
            event.wait() # espero a que el data thread termine de procesar
            logging.info(f"Sum {self.id}: Drain completed for client {client_id}. Proceeding to flush.")

            # limpio l event del dict despues del flush 
            with self.in_flight_lock:
                del self.drain_events[client_id]

            logging.info(f"Sum {self.id}: Event cleaned for client {client_id}. Ready to flush.")
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

    def _handle_sigterm(self, signum, frame):
        self.input_queue.stop_consuming()

    def _shutdown(self):
        for resource in [self.input_queue, self.control_sender, self.control_receiver] + self.data_output_exchanges:
            try:
                resource.close()
            except Exception as e:
                logging.error(f"Sum {self.id}: Error closing resource: {e}")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._shutdown()
                
            
    def start(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        control_thread = threading.Thread(target=self._consume_control, daemon=True)
        logging.info(f"Sum {self.id}: Starting control thread for {self.control_receiver._exchange_name}...")
        control_thread.start()

        logging.info(f"Sum {self.id}: Starting consumer on {self.config.INPUT_QUEUE}...")
        self.input_queue.start_consuming(self.on_message_received)

        self._shutdown()

def main():
    config = Config()
    logging.basicConfig(level=config.LOG_LEVEL)
    with SumFilter(config) as sum_filter:
        sum_filter.start()

if __name__ == "__main__":
    main()