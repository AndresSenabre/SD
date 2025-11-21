#!/usr/bin/env python3
"""
EV_CP_E - Engine del Charging Point
Sistema de Punto de Recarga - Modulo Principal
ARQUITECTURA: 
- Engine <-> CENTRAL: Kafka (registros, comandos, datos)
- Engine <-> Monitor: Socket (health checks locales)
OPTIMIZADO PARA KAFKA 4.1.0
"""

import socket
import threading
import time
import json
import sys
from enum import Enum
from dataclasses import dataclass
from typing import Optional
import random

try:
    from kafka import KafkaProducer, KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    print("WARNING: kafka-python no disponible")
    KAFKA_AVAILABLE = False

class CPState(Enum):
    """Estados del Charging Point"""
    DISCONNECTED = "DISCONNECTED"
    ACTIVATED = "ACTIVATED"
    STOPPED = "STOPPED"
    SUPPLYING = "SUPPLYING"
    BROKEN = "BROKEN"

@dataclass
class ChargingSession:
    """Informacion de sesion de carga"""
    driver_id: str
    start_time: float
    current_consumption: float = 0.0
    total_consumed: float = 0.0
    price_per_kwh: float = 0.35
    total_cost: float = 0.0

class ChargingPointEngine:
    def __init__(self, cp_id: str, location: str, central_host: str, central_port: int, 
                 kafka_host: str, kafka_port: int, monitor_port: int):
        self.cp_id = cp_id
        self.location = location
        self.central_host = central_host
        self.central_port = central_port
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.monitor_port = monitor_port
        
        # Estado del CP
        self.state = CPState.DISCONNECTED
        self.current_session: Optional[ChargingSession] = None
        self.price_per_kwh = 0.35
        
        # Conexiones
        self.monitor_socket = None
        self.running = True
        
        # Kafka
        self.kafka_producer = None
        self.kafka_consumer = None
        
        # Threads
        self.monitor_thread = None
        self.charging_thread = None
        self.kafka_thread = None
        self.menu_thread = None

    def start(self):
        """Iniciar el Engine del CP"""
        print(f"[ENGINE] Iniciando Charging Point Engine - ID: {self.cp_id}")
        print(f"[LOCATION] {self.location}")
        
        try:
            self.connect_to_kafka()
            self.register_to_central()
            self.start_monitor_server()
            self.start_menu()
            
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n[STOP] Deteniendo Charging Point Engine...")
        finally:
            self.cleanup()

    def connect_to_kafka(self):
        """Conectar a Kafka optimizado para Kafka 4.1.0"""
        if not KAFKA_AVAILABLE:
            print("[ERROR] Kafka no disponible")
            self.running = False
            return
            
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                print(f"[KAFKA] Intento de conexión {attempt + 1}/{max_retries}...")
                
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=f'{self.kafka_host}:{self.kafka_port}',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=1,
                    linger_ms=10,
                    batch_size=16384,
                    max_block_ms=5000,
                    request_timeout_ms=10000,
                    api_version=(3, 0, 0)
                )
                
                self.kafka_consumer = KafkaConsumer(
                    'cp_commands',
                    f'cp_{self.cp_id}',
                    bootstrap_servers=f'{self.kafka_host}:{self.kafka_port}',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    group_id=f'cp_engine_{self.cp_id}',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    fetch_min_bytes=1,
                    fetch_max_wait_ms=100,
                    max_poll_records=10,
                    max_poll_interval_ms=300000,
                    session_timeout_ms=10000,
                    heartbeat_interval_ms=3000,
                    consumer_timeout_ms=1000,
                    api_version=(3, 0, 0)
                )
                
                print(f"[OK] Conectado a Kafka ({self.kafka_host}:{self.kafka_port})")
                print(f"[SUBSCRIBE] Escuchando: cp_commands, cp_{self.cp_id}")
                
                self.kafka_thread = threading.Thread(target=self.listen_kafka, daemon=True)
                self.kafka_thread.start()
                
                return
                
            except Exception as e:
                print(f"[ERROR] Intento {attempt + 1} fallido: {e}")
                if attempt < max_retries - 1:
                    print(f"[KAFKA] Reintentando en {retry_delay} segundos...")
                    time.sleep(retry_delay)
                else:
                    print("[ERROR] No se pudo conectar a Kafka")
                    self.running = False

    def register_to_central(self):
        """Registrarse en CENTRAL via Kafka"""
        if not self.kafka_producer:
            print("[ERROR] No hay conexion Kafka para registro")
            self.state = CPState.DISCONNECTED
            return
        
        try:
            register_msg = {
                "type": "REGISTER_CP",
                "cp_id": self.cp_id,
                "location": self.location,
                "price_per_kwh": self.price_per_kwh,
                "timestamp": time.time()
            }
            
            self.kafka_producer.send('cp_registrations', register_msg)
            self.kafka_producer.flush()
            print(f"[KAFKA] Registro enviado a CENTRAL")
            print(f"[KAFKA] Esperando confirmacion en topico cp_{self.cp_id}")
            
            time.sleep(1)
            self.change_state(CPState.ACTIVATED)
            
        except Exception as e:
            print(f"[ERROR] Error en registro: {e}")
            self.state = CPState.DISCONNECTED

    def listen_kafka(self):
        """Escuchar eventos de Kafka con polling optimizado"""
        print("[KAFKA] Listener iniciado")
        while self.running and self.kafka_consumer:
            try:
                messages = self.kafka_consumer.poll(timeout_ms=100, max_records=10)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        event = record.value
                        topic = record.topic
                        print(f"[KAFKA/{topic}] Evento recibido: {event.get('type')}")
                        self.handle_kafka_event(event)
                        
            except Exception as e:
                if self.running:
                    print(f"[WARNING] Error en consumidor Kafka: {e}")
                    time.sleep(0.1)

    def handle_kafka_event(self, event):
        """Manejar eventos recibidos por Kafka"""
        event_type = event.get('type')
        
        if event_type == 'REGISTER_CONFIRMED':
            if event.get('cp_id') == self.cp_id:
                print("[OK] Registro confirmado por CENTRAL")
                if self.state == CPState.DISCONNECTED:
                    self.change_state(CPState.ACTIVATED)
        
        elif event_type == 'AUTHORIZE_CHARGING':
            driver_id = event.get('driver_id')
            target_cp = event.get('cp_id')
            
            if target_cp == self.cp_id:
                print(f"[AUTHORIZED] Autorizacion recibida para conductor: {driver_id}")
                self.prepare_charging(driver_id)
        
        elif event_type == 'STOP_CP':
            target_cp = event.get('cp_id')
            if target_cp == 'ALL' or target_cp == self.cp_id:
                print("[COMMAND] STOP recibido")
                if self.state == CPState.SUPPLYING:
                    self.stop_charging()
                self.change_state(CPState.STOPPED)
        
        elif event_type == 'RESUME_CP':
            target_cp = event.get('cp_id')
            if target_cp == 'ALL' or target_cp == self.cp_id:
                print("[COMMAND] RESUME recibido")
                self.change_state(CPState.ACTIVATED)

    def start_monitor_server(self):
        """Iniciar servidor para el Monitor LOCAL (socket)"""
        def monitor_server():
            monitor_srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            monitor_srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            monitor_srv.bind(('localhost', self.monitor_port))
            monitor_srv.listen(1)
            print(f"[MONITOR] Servidor escuchando en puerto {self.monitor_port}")
            
            while self.running:
                try:
                    conn, addr = monitor_srv.accept()
                    self.monitor_socket = conn
                    print(f"[MONITOR] Conectado desde {addr}")
                    
                    health_thread = threading.Thread(target=self.handle_monitor_health, daemon=True)
                    health_thread.start()
                    
                except Exception as e:
                    if self.running:
                        print(f"[ERROR] Error en servidor Monitor: {e}")
        
        self.monitor_thread = threading.Thread(target=monitor_server, daemon=True)
        self.monitor_thread.start()

    def handle_monitor_health(self):
        """Manejar health checks del Monitor local via socket"""
        while self.running and self.monitor_socket:
            try:
                data = self.monitor_socket.recv(1024).decode()
                if data:
                    response = "KO" if self.state == CPState.BROKEN else "OK"
                    self.monitor_socket.send(response.encode())
                else:
                    break
            except Exception as e:
                print(f"[ERROR] Error en health check: {e}")
                break

    def prepare_charging(self, driver_id: str):
        """Preparar para iniciar carga"""
        if self.state != CPState.ACTIVATED:
            print(f"[ERROR] No se puede cargar en estado: {self.state.value}")
            return
        
        print(f"[READY] CP preparado para cargar. Conductor: {driver_id}")
        print("[INFO] Presiona 'c' para simular conexion del vehiculo")
        self.waiting_driver_id = driver_id

    def start_charging(self, driver_id: str):
        """Iniciar proceso de carga"""
        if self.state != CPState.ACTIVATED:
            print(f"[ERROR] No se puede iniciar carga en estado: {self.state.value}")
            return
        
        self.current_session = ChargingSession(
            driver_id=driver_id,
            start_time=time.time(),
            price_per_kwh=self.price_per_kwh
        )
        
        self.change_state(CPState.SUPPLYING)
        print(f"[CHARGING] Iniciando suministro para conductor {driver_id}")
        
        self.charging_thread = threading.Thread(target=self.charging_simulation, daemon=True)
        self.charging_thread.start()

    def charging_simulation(self):
        """Simulacion del proceso de carga"""
        while self.running and self.state == CPState.SUPPLYING and self.current_session:
            try:
                self.current_session.current_consumption = random.uniform(7.0, 22.0)
                
                hour_fraction = 1.0 / 3600.0
                consumed_this_second = self.current_session.current_consumption * hour_fraction
                self.current_session.total_consumed += consumed_this_second
                self.current_session.total_cost = (
                    self.current_session.total_consumed * self.current_session.price_per_kwh
                )
                
                self.send_charging_data()
                
                print(f"[SUPPLY] {self.current_session.current_consumption:.2f} KW | "
                      f"Total: {self.current_session.total_consumed:.3f} KWh | "
                      f"Coste: {self.current_session.total_cost:.2f} EUR")
                
                time.sleep(1)
                
            except Exception as e:
                print(f"[ERROR] Error en simulacion de carga: {e}")
                break

    def send_charging_data(self):
        """Enviar datos de carga a CENTRAL via Kafka"""
        if not self.current_session or not self.kafka_producer:
            return
        
        data = {
            "type": "CHARGING_DATA",
            "cp_id": self.cp_id,
            "driver_id": self.current_session.driver_id,
            "current_consumption": self.current_session.current_consumption,
            "total_consumed": self.current_session.total_consumed,
            "total_cost": self.current_session.total_cost,
            "timestamp": time.time()
        }
        
        try:
            self.kafka_producer.send('charging_events', data)
        except Exception as e:
            print(f"[WARNING] Error publicando datos en Kafka: {e}")

    def stop_charging(self):
        """Detener proceso de carga"""
        if not self.current_session:
            print("[ERROR] No hay sesion de carga activa")
            return
        
        print(f"[STOP] Finalizando suministro...")
        
        final_ticket = {
            "type": "CHARGING_FINISHED",
            "cp_id": self.cp_id,
            "driver_id": self.current_session.driver_id,
            "total_consumed": self.current_session.total_consumed,
            "total_cost": self.current_session.total_cost,
            "duration": time.time() - self.current_session.start_time,
            "timestamp": time.time()
        }
        
        if self.kafka_producer:
            try:
                self.kafka_producer.send('charging_events', final_ticket)
                self.kafka_producer.flush()
            except Exception as e:
                print(f"[WARNING] Error publicando finalizacion en Kafka: {e}")
        
        print(f"[TICKET] Consumo: {self.current_session.total_consumed:.3f} KWh | "
              f"Coste: {self.current_session.total_cost:.2f} EUR")
        
        self.current_session = None

        if self.state != CPState.BROKEN:
            self.change_state(CPState.ACTIVATED)

    def change_state(self, new_state: CPState):
        """Cambiar estado del CP y publicar en Kafka"""
        old_state = self.state

        if new_state == CPState.BROKEN and self.current_session:
            print("[FAULT] Finalizando carga por fallo del CP...")
            self.stop_charging()

        self.state = new_state
        
        print(f"[STATE] {old_state.value} -> {new_state.value}")
        
        state_msg = {
            "type": "STATE_CHANGE",
            "cp_id": self.cp_id,
            "old_state": old_state.value,
            "new_state": new_state.value,
            "timestamp": time.time()
        }
        
        if self.kafka_producer:
            try:
                self.kafka_producer.send('cp_status', state_msg)
                self.kafka_producer.flush()
            except Exception as e:
                print(f"[WARNING] Error publicando estado en Kafka: {e}")

    def start_menu(self):
        """Iniciar menu interactivo"""
        def menu_loop():
            print("\n" + "="*50)
            print("MENU CHARGING POINT ENGINE")
            print("="*50)
            print("c - Conectar vehiculo (iniciar carga)")
            print("d - Desconectar vehiculo (finalizar carga)")
            print("f - Simular fallo (reportar KO al monitor)")
            print("r - Recuperar de fallo")
            print("s - Ver estado actual")
            print("q - Salir")
            print("="*50)
            
            while self.running:
                try:
                    cmd = input(">> Comando: ").strip().lower()
                    
                    if cmd == 'c':
                        if hasattr(self, 'waiting_driver_id'):
                            self.start_charging(self.waiting_driver_id)
                            delattr(self, 'waiting_driver_id')
                        else:
                            print("[TEST] Modo testing - simulando conductor TEST_DRIVER")
                            self.start_charging("TEST_DRIVER")
                    
                    elif cmd == 'd':
                        self.stop_charging()
                    
                    elif cmd == 'f':
                        self.change_state(CPState.BROKEN)
                        if self.current_session:
                            self.stop_charging()
                    
                    elif cmd == 'r':
                        if self.state == CPState.BROKEN:
                            self.change_state(CPState.ACTIVATED)
                    
                    elif cmd == 's':
                        self.show_status()
                    
                    elif cmd == 'q':
                        self.running = False
                        break
                        
                except KeyboardInterrupt:
                    self.running = False
                    break
        
        self.menu_thread = threading.Thread(target=menu_loop, daemon=True)
        self.menu_thread.start()

    def show_status(self):
        """Mostrar estado actual del CP"""
        print(f"\n{'='*50}")
        print(f"ESTADO DEL CP {self.cp_id}")
        print(f"{'='*50}")
        print(f"Ubicacion: {self.location}")
        print(f"Estado: {self.state.value}")
        print(f"Precio: {self.price_per_kwh} EUR/KWh")
        print(f"Kafka: {'OK' if self.kafka_producer else 'NO'}")
        print(f"Monitor: {'OK' if self.monitor_socket else 'NO'}")
        
        if self.current_session:
            print(f"\n[SESION ACTIVA]")
            print(f"Conductor: {self.current_session.driver_id}")
            print(f"Consumo actual: {self.current_session.current_consumption:.2f} KW")
            print(f"Total consumido: {self.current_session.total_consumed:.3f} KWh")
            print(f"Coste actual: {self.current_session.total_cost:.2f} EUR")
        print(f"{'='*50}\n")

    def cleanup(self):
        """Limpieza al cerrar"""
        self.running = False
        if self.monitor_socket:
            self.monitor_socket.close()
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.kafka_consumer:
            self.kafka_consumer.close()
        print("[CLEANUP] Limpieza completada")


def main():
    if len(sys.argv) < 7:
        print("Uso: python EV_CP_E.py <cp_id> <location> <central_host> <central_port> <kafka_host> <kafka_port> [monitor_port]")
        print("Ejemplo: python EV_CP_E.py CP001 Madrid-Centro localhost 8080 localhost 9092 8081")
        return
    
    cp_id = sys.argv[1]
    location = sys.argv[2]
    central_host = sys.argv[3]
    central_port = int(sys.argv[4])
    kafka_host = sys.argv[5]
    kafka_port = int(sys.argv[6])
    monitor_port = int(sys.argv[7]) if len(sys.argv) > 7 else 8081
    
    engine = ChargingPointEngine(cp_id, location, central_host, central_port, 
                                kafka_host, kafka_port, monitor_port)
    engine.start()

if __name__ == "__main__":
    main()