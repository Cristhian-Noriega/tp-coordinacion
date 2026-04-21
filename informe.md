# Informe

## 1. Soporte Multi-Cliente (Escenario 2)

Para que el sistema procese múltiples clientes simultáneamente sin mezclar sus datos, se implementó un aislamiento estricto del estado basado en un identificador único por sesión.

Gateway (message_handler.py): Como el Gateway crea una nueva instancia del handler por cada conexión TCP, se inicializa un client_id (UUID) único al momento de la conexión. Este identificador se incluye en cada mensaje DATA y en el EOF original. Al recibir resultados, el Gateway verifica que el client_id del mensaje coincida con el de su sesión local antes de entregarlo al cliente.

Protocolo de mensajes (internal.py): Se modificó la serialización para transportar el client_id en todos los mensajes del pipeline interno (formato [client_id, fruit, amount]).

Estado de los nodos (Sum, Aggregator, Joiner): Se refactorizó la gestión de memoria en todos los filtros, reemplazando el estado global por diccionarios anidados donde la clave principal es el client_id.

---

## 2. Coordinación de Terminación (Escenario 3 - Fanout Exchange)
El Gateway emite un solo EOF por cliente, pero la capa Sum consume de una cola compartida en modalidad Round-Robin. Para que todas las instancias de Sum se enteren del cierre, se diseñó un canal de control dedicado usando un exchange de tipo fanout.

Implementación:
Extensión del middleware: Se agregó soporte para exchange_type="fanout". Cuando se usa este tipo, el middleware declara automáticamente una cola exclusiva y temporal (exclusive=True) para cada consumidor, y la bindea al exchange fanout sin necesidad de routing keys.

Doble hilo en Sum: Cada nodo Sum ejecuta un hilo secundario dedicado exclusivamente a escuchar su cola privada del fanout. El hilo principal sigue consumiendo datos de la input_queue sin bloqueos.

Propagación de la señal de cierre: Cuando un Sum (el que recibe el EOF original del Gateway) procesa ese mensaje, no hace flush inmediato. En su lugar, publica una señal de control (EOF_SIGNAL) al exchange fanout, incluyendo su propio sum_id.

Barrera en Aggregator: Cada Sum, al recibir la señal por su cola privada, envía sus datos acumulados y un EOF al Aggregator. El Aggregator mantiene un Set por cliente (sums_completed[client_id]) donde registra qué Sum ya envió su EOF. Solo cuando la cantidad de EOFs recibidos es igual a SUM_AMOUNT (cantidad total de réplicas de Sum), el Aggregator considera los datos completos y procede a calcular el top final.

---

## 3. Problema Detectado: Condición de Carrera

Si bien la implementacion actual pasan los test, se detecto un problema de consistencia de datos. 

El canal de control (fanout + hilo secundario) puede operar muy rápido, potencialmente superando al procesamiento normal de datos. La secuencia problemática es:

Un Sum recibe el EOF original desde la input_queue y publica una EOF_SIGNAL al fanout.

La señal viaja rápidamente por RabbitMQ y activa los hilos de control de los demás Sum.

Estos Sum, al recibir la señal, adquieren los locks y realizan el flush de sus datos acumulados hacia el Aggregator, enviando también su EOF.

El Aggregator recibe los EOFs de todos los Sum y, creyendo que todos los datos fueron procesados, cierra el cliente y envía el top final.

Problema: El hilo principal de algún Sum todavía tiene mensajes de ese cliente pendientes de procesar en su input_queue local, porque el Round-Robin distribuyó esos mensajes antes del EOF pero el procesamiento no terminó antes de que llegara la señal de control.

---

## 4. Solucion

Un punto a mencionar, es que la race condition anterior es intra-nodo, es decir, ocurre entre dos hilos del mismo proceso, no entre nodos distintos. Cada Sum necesitan garantizar que sus propios mensajes en vuelo fueron procesados antes de hacer flush. No requiere una coordinacion con los demas Sums. 

Teniendo esto en cuenta, se implemento un mecanismo de drain counter con tres estructuras compartidas entre el hilo de datos y el de control de cada nodo Sum: 

* in_flight: contador de mensajes por cada cliente. 
* drain_events: un `threading.Event` por cliente, creado por el hilo de control al recibir un EOF signal. Es como la "comunicacion" o "coordinacion" entre los dos hilos
* is_flight_lock: un unico lock que protege ambas estructuras de forma atomica. Es muy importante que sea uno solo, ya que si estuviera por separado, el hilo de datos podria decrementar a 0 entre los dos acquires del hilo de control, y el event quedaria sin setearse nunca.


Para esta solucion fue importante familiarizarse con los events de threading, y operaciones como: 

* `event.wait()` si flag == false bloquea el hilo hasta que alguien llame a `set()`
* `event.set()` pone flag = true y despierta.a todos los hilos qque esten en `wait()`
* `event.clear()` restea flag = false

El flujo de coordinación es el siguiente:

El hilo de datos incrementa in_flight[client_id] en cuanto on_message_received es invocado — antes de cualquier procesamiento. Al terminar la acumulación, lo decrementa. Si llega a 0 y existe un Event en drain_events para ese cliente, llama a event.set().
El hilo de control, al recibir la señal de EOF, crea un threading.Event y lo registra en drain_events[client_id] bajo in_flight_lock. Dentro del mismo lock verifica si in_flight[client_id] ya es 0; si es así, setea el event él mismo (el hilo de datos ya terminó). Luego llama a event.wait().
Solo cuando event.wait() retorna ejecuta _flush_client_data, garantizando que todos los datos del cliente fueron acumulados. La atomicidad del lock garantiza que no existe una ventana de tiempo en la que el hilo de datos decrementa a 0 sin ver el event, y el hilo de control crea el event sin ver que el contador ya es 0. Exactamente uno de los dos hilos setea el event.


Supuestos: la solución es correcta bajo las condiciones ordinarias del sistema — sin caída de nodos ni pérdida de mensajes en el broker, tal como establece el enunciado. RabbitMQ garantiza orden FIFO dentro de una cola, por lo que el control signal viaja por red con latencia de varios milisegundos mientras que el gap entre mensajes consecutivos en el mismo consumer (con prefetch_count=1) es sub-milisegundo. En condiciones ordinarias, este gap no es alcanzable por el control signal. Los escenarios causados de fallas de conexión o caída de nodos quedan fuera del scope.

Otra aclaracion es el uso de threads. que al realizar operaciones I/O bound, el GIL de python se libera durante ese tipo de operaciones y llamadas bloqueantes, permitiendo que el hilo de datos y el hilo de control se solapen sin competir por CPU.  


## Multiples replicas de Aggregators (escenario 4)

Para este escenario, se realizo un routing deterministico por fruta. En vez de tener un broadcast de datos, se tiene un routing basado en hash sobre el nombre de la fruta. Con esto, la misma fruta siempre va a al mismo aggregator, independientemente de la instancia de Sum que lo esta enviando. Entonces un solo aggregator mantiene la suma total de una particion disjunta de frutas. 

El EOF de cada Sum sigue siendo un broadcast a todos los aggregators porque cada uno necesita recibir SUM_AMOUNT EOFs para saber cuando cerrar su barrera y enviar su top parcial. 

Por el lado del joiner, se implementa una segunda barrera que consiste en acumualr el aggregation amount tops parciales por cliente. Cuando los recibe todos, fusiona las listas, ordena por cantidad descendente y toma los mejores top size. 

Como guarda, se agrego que si un aggregator no recibio datos de un cliente (porque ninguna fruta de su particion fue enviada por ese cliente) , igual se envia un resultado vacio al joiner, para no trabarlo. 




