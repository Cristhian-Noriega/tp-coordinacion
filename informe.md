# Informe

## 1. Soporte Multi-Cliente (Escenario 2)

Para que el sistema procese múltiples clientes simultáneamente sin mezclar sus datos, se implementó un aislamiento estricto del estado basado en un identificador único por sesión.

Gateway (message_handler.py): Como el Gateway crea una nueva instancia del handler por cada conexión TCP, se inicializa un client_id (UUID) único al momento de la conexión. Este identificador se incluye en cada mensaje DATA y en el EOF original. Al recibir resultados, el Gateway verifica que el client_id del mensaje coincida con el de su sesión local antes de entregarlo al cliente.

Protocolo de mensajes (internal.py): Se modificó la serialización para transportar el client_id en todos los mensajes del pipeline interno (formato [client_id, fruit, amount]).

Estado de los nodos (Sum, Aggregator, Joiner): Se refactorizó la gestión de memoria en todos los filtros, reemplazando el estado global por diccionarios anidados donde la clave principal es el client_id.

---

## 2. Coordinación de Terminación (Fanout Exchange)
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



