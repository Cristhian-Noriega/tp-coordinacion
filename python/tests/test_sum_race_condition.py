import threading
import sys
import types
import unittest
from unittest.mock import patch
import time

class SimpleLockSum: 
    def __init__(self):
        self.storage = {}
        self.lock = threading.Lock()
        self.flush_results = {}

    def on_message_received(self, client_id, fruit, amount):
        # gap desprotegido: msj recibido, lock todavia no adquirido
        time.sleep(0.05)
        with self.lock:
            self.storage.setdefault(client_id, {})
            self.storage[client_id][fruit] = self.storage[client_id].get(fruit, 0) + amount

    def flush(self, client_id):
        with self.lock:
            if client_id in self.storage:
                result = dict(self.storage[client_id])
                del self.storage[client_id]
                return result
            return {}

class DrainCounterSum:
    def __init__(self):
        self.storage = {}
        self.lock = threading.Lock()
        self.in_flight = {}
        self.drain_events = {}
        self.in_flight_lock = threading.Lock()

    def on_message_received(self, client_id, fruit, amount):
        # Simula llegada de mensaje: incrementa in_flight
        with self.in_flight_lock:
            self.in_flight[client_id] = self.in_flight.get(client_id, 0) + 1
        
        # Procesa el mensaje (con delay para simular concurrencia)
        time.sleep(0.05)
        with self.lock:
            self.storage.setdefault(client_id, {})
            self.storage[client_id][fruit] = self.storage[client_id].get(fruit, 0) + amount
        
        # Decrementa in_flight y setea event si llega a 0
        with self.in_flight_lock:
            self.in_flight[client_id] -= 1
            if self.in_flight[client_id] == 0 and client_id in self.drain_events:
                self.drain_events[client_id].set()
            # Limpia si llega a 0 y no hay event
            if self.in_flight[client_id] == 0:
                self.in_flight.pop(client_id, None)
                self.drain_events.pop(client_id, None)

    def flush(self, client_id):
        # Simula control thread: crea event, espera si in_flight > 0
        event = threading.Event()
        with self.in_flight_lock:
            self.drain_events[client_id] = event
            if self.in_flight.get(client_id, 0) == 0:
                event.set()
        
        event.wait()  # Espera a que in_flight llegue a 0
        
        # Flush: toma lock, obtiene resultados, popea de storage
        with self.lock:
            if client_id in self.storage:
                result = dict(self.storage[client_id])
                del self.storage[client_id]
                return result
            return {}

class TestRaceCondition(unittest.TestCase):
    def test_concurrent_updates(self):
        sum_filter = SimpleLockSum()
        client_id = "test_client"
        
        # Datos esperados
        expected = {"apple": 10, "banana": 20}
        
        def update_fruit(fruit, amount):
            sum_filter.on_message_received(client_id, fruit, amount)
        
        # Crear hilos para simular concurrencia
        threads = []
        for fruit, amount in expected.items():
            t = threading.Thread(target=update_fruit, args=(fruit, amount))
            threads.append(t)
        
        # Iniciar hilos
        for t in threads:
            t.start()
        
        # Esperar a que terminen
        for t in threads:
            t.join()
        
        # Verificar resultado
        result = sum_filter.flush(client_id)
        self.assertEqual(result, expected)

    def test_drain_counter(self):
        sum_filter = DrainCounterSum()
        client_id = "test_client"
        
        # Datos esperados
        expected = {"apple": 5, "banana": 10}
        
        # Crear hilos para mensajes de data (simulando llegada concurrente)
        threads = []
        for fruit, amount in expected.items():
            t = threading.Thread(target=sum_filter.on_message_received, args=(client_id, fruit, amount))
            threads.append(t)
        
        # Iniciar hilos de data
        for t in threads:
            t.start()
        
        # Simular delay para que algunos mensajes estén en vuelo
        time.sleep(0.02)
        
        # Llamar flush en hilo principal (simula control thread)
        result = sum_filter.flush(client_id)
        
        # Esperar a que terminen los hilos de data
        for t in threads:
            t.join()
        
        # Verificar que el flush esperó y obtuvo el resultado correcto
        self.assertEqual(result, expected)
        # Verificar que storage esté vacío
        self.assertNotIn(client_id, sum_filter.storage)

if __name__ == "__main__":
    unittest.main()
