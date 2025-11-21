#!/usr/bin/env python3
"""
Mock CENTRAL - Simulador con Kafka + Socket
Para testing del sistema sin necesidad de CENTRAL real
- Kafka: Para Engine y Driver
- Socket: Para Monitor
OPTIMIZADO PARA KAFKA 4.1.0 + ACCESO DE RED AUTOMÁTICO
"""

import json
import time
import threading
import socket
from datetime import datetime

try:
    from kafka import KafkaProducer, KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    print("[ERROR] kafka-python no disponible")
    KAFKA_AVAILABLE = False

class MockCentral:
    def __init__(self, socket_port=8080, kafka_host='localhost', kafka_port=9092):
        self.socket_port = socket_port
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.running = True
        
        # Registros
        self.registered_cps = {}
        self.registered_drivers = {}
        self.registered_monitors = {}
        self.active_sessions = {}
        
        # Kafka
        self.producer = None
        self.consumer = None
        
        # Socket
        self.socket_server = None
        self.monitor_clients = {}
        
    def get_local_ip(self):
        """Obtener IP local automáticamente"""
        try:
            # Crear socket temporal para descubrir IP local
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))  # No envía datos, solo descubre ruta
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return "127.0.0.1"
    
    def start(self):
        print("[MOCK CENTRAL] Iniciando...")
        
        if not KAFKA_AVAILABLE:
            print("[ERROR] Kafka no disponible")
            return
        
        try:
            # 1. Iniciar Kafka
            self.start_kafka()
            
            # 2. Iniciar Socket Server para Monitors
            self.start_socket_server()
            
            print("\n[MOCK CENTRAL] Listo - procesando eventos...")
            print("="*60)
            
            # Mantener vivo
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n[STOP] Cerrando Mock CENTRAL...")
        finally:
            self.cleanup()
    
    def start_kafka(self):
        """Iniciar conexion Kafka con configuración optimizada para Kafka 4.1.0"""
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                print(f"[KAFKA] Intento de conexión {attempt + 1}/{max_retries}...")
                
                # Producer optimizado para baja latencia
                self.producer = KafkaProducer(
                    bootstrap_servers=f'{self.kafka_host}:{self.kafka_port}',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=1,
                    linger_ms=10,
                    batch_size=16384,
                    buffer_memory=33554432,
                    max_block_ms=5000,
                    request_timeout_ms=10000,
                    api_version=(3, 0, 0)
                )
                
                # Consumer optimizado para baja latencia
                self.consumer = KafkaConsumer(
                    'cp_registrations',
                    'driver_requests',
                    'charging_events',
                    'cp_status',
                    bootstrap_servers=f'{self.kafka_host}:{self.kafka_port}',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    group_id='mock_central',
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
                
                print(f"[OK] Kafka conectado ({self.kafka_host}:{self.kafka_port})")
                print("[KAFKA] Listening: cp_registrations, driver_requests, charging_events, cp_status")
                
                # Thread para procesar eventos Kafka
                kafka_thread = threading.Thread(target=self.process_kafka_events, daemon=True)
                kafka_thread.start()
                
                return
                
            except Exception as e:
                print(f"[ERROR] Intento {attempt + 1} fallido: {e}")
                if attempt < max_retries - 1:
                    print(f"[KAFKA] Reintentando en {retry_delay} segundos...")
                    time.sleep(retry_delay)
                else:
                    print("[ERROR] No se pudo conectar a Kafka")
                    raise
    
    def start_socket_server(self):
        """Iniciar servidor Socket para Monitors - ACCESIBLE DESDE RED"""
        def socket_server():
            self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Escuchar en todas las interfaces
            self.socket_server.bind(('0.0.0.0', self.socket_port))
            self.socket_server.listen(5)
            
            # Obtener IP local automáticamente
            local_ip = self.get_local_ip()
            
            print(f"[OK] Socket server escuchando en puerto {self.socket_port}")
            print(f"[NETWORK] Conectarse desde otros PCs usando:")
            print(f"  python EV_CP_M.py CP001 <engine_host> <engine_port> {local_ip} {self.socket_port}")
            print(f"[LOCAL] Conectarse desde este PC usando:")
            print(f"  python EV_CP_M.py CP001 localhost 8081 localhost {self.socket_port}")
            print("[SOCKET] Esperando conexiones de Monitors...")
            
            while self.running:
                try:
                    client_socket, addr = self.socket_server.accept()
                    print(f"[SOCKET] Nueva conexion desde {addr}")
                    
                    # Thread para manejar cliente
                    client_thread = threading.Thread(
                        target=self.handle_monitor_client,
                        args=(client_socket, addr),
                        daemon=True
                    )
                    client_thread.start()
                    
                except Exception as e:
                    if self.running:
                        print(f"[ERROR] Socket server: {e}")
        
        socket_thread = threading.Thread(target=socket_server, daemon=True)
        socket_thread.start()
    
    def handle_monitor_client(self, client_socket, addr):
        """Manejar conexion de un Monitor via socket"""
        monitor_id = None
        cp_id = None
        
        try:
            while self.running:
                data = client_socket.recv(1024).decode()
                if not data:
                    break
                
                try:
                    message = json.loads(data)
                    msg_type = message.get('type')
                    
                    if msg_type == 'REGISTER_MONITOR':
                        cp_id = message.get('cp_id')
                        monitor_id = message.get('monitor_id')
                        
                        self.registered_monitors[cp_id] = {
                            'monitor_id': monitor_id,
                            'socket': client_socket,
                            'addr': addr
                        }
                        
                        self.monitor_clients[cp_id] = client_socket
                        
                        print(f"[SOCKET] Monitor registrado: {monitor_id} para CP {cp_id} (desde {addr[0]}:{addr[1]})")
                        
                        # Confirmar registro
                        response = json.dumps({
                            'type': 'REGISTER_CONFIRMED',
                            'monitor_id': monitor_id,
                            'message': 'Monitor registrado'
                        })
                        client_socket.send(response.encode())
                    
                    elif msg_type == 'CP_HEALTH_CHANGE':
                        cp_id = message.get('cp_id')
                        new_status = message.get('new_status')
                        old_status = message.get('old_status')
                        
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        print(f"[{timestamp}] [SOCKET/HEALTH] CP {cp_id}: {old_status} -> {new_status}")
                        
                        # Actualizar estado interno
                        if cp_id in self.registered_cps:
                            self.registered_cps[cp_id]['health_status'] = new_status
                        
                        # NUEVO: Si el CP falló y hay sesión activa, notificar al driver
                        if new_status == 'BROKEN' and cp_id in self.active_sessions:
                            driver_id = self.active_sessions[cp_id]
                            
                            print(f"  [!] CP {cp_id} falló durante suministro a {driver_id}")
                            
                            failure_notification = {
                                'type': 'CHARGING_INTERRUPTED',
                                'cp_id': cp_id,
                                'driver_id': driver_id,
                                'reason': 'CP reportó fallo durante el suministro',
                                'timestamp': time.time()
                            }
                            
                            self.producer.send(f'driver_{driver_id}', failure_notification)
                            self.producer.flush()
                            
                            print(f"  <- Notificación de interrupción enviada a driver_{driver_id}")
                            
                            # Limpiar sesión activa
                            del self.active_sessions[cp_id]
                            print(f"  <- Sesión {cp_id} limpiada")
                        
                        # Si el CP se recupera, actualizar estado
                        elif new_status == 'OK' and old_status == 'BROKEN':
                            print(f"  [OK] CP {cp_id} recuperado y disponible")
                    
                except json.JSONDecodeError:
                    print(f"[WARNING] JSON invalido de {addr}: {data}")
                    
        except Exception as e:
            print(f"[ERROR] Cliente {addr}: {e}")
        finally:
            # Limpiar al desconectar
            if cp_id and cp_id in self.monitor_clients:
                del self.monitor_clients[cp_id]
            if cp_id and cp_id in self.registered_monitors:
                del self.registered_monitors[cp_id]
            
            client_socket.close()
            print(f"[SOCKET] Monitor desconectado: {addr}")
    
    def process_kafka_events(self):
        """Procesar eventos de Kafka con polling optimizado"""
        print("[KAFKA] Procesador de eventos iniciado")
        while self.running:
            try:
                messages = self.consumer.poll(timeout_ms=100, max_records=10)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        event = record.value
                        topic = record.topic
                        event_type = event.get('type')
                        
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        print(f"[{timestamp}] [KAFKA/{topic}] {event_type}")
                        
                        if topic == 'cp_registrations':
                            self.handle_cp_registration(event)
                        elif topic == 'driver_requests':
                            self.handle_driver_request(event)
                        elif topic == 'charging_events':
                            self.handle_charging_event(event)
                        elif topic == 'cp_status':
                            self.handle_cp_status(event)
                        
            except Exception as e:
                if self.running:
                    print(f"[ERROR] Kafka consumer: {e}")
                    time.sleep(0.1)
    
    def handle_cp_registration(self, event):
        """Manejar registro de CP"""
        cp_id = event.get('cp_id')
        location = event.get('location')
        price = event.get('price_per_kwh')
        
        self.registered_cps[cp_id] = {
            'location': location,
            'price_per_kwh': price,
            'status': 'ACTIVATED',
            'health_status': 'UNKNOWN',
            'registered_at': time.time()
        }
        
        print(f"  -> CP registrado: {cp_id} ({location}) - {price} EUR/KWh")
        
        confirmation = {
            'type': 'REGISTER_CONFIRMED',
            'cp_id': cp_id,
            'message': 'Registro exitoso',
            'timestamp': time.time()
        }
        
        self.producer.send(f'cp_{cp_id}', confirmation)
        self.producer.flush()
        print(f"  <- Confirmacion enviada a cp_{cp_id}")
    
    def handle_driver_request(self, event):
        """Manejar peticion de conductor"""
        request_type = event.get('type')
        
        if request_type == 'REGISTER_DRIVER':
            driver_id = event.get('driver_id')
            self.registered_drivers[driver_id] = {
                'registered_at': time.time()
            }
            print(f"  -> Driver registrado: {driver_id}")
            
            available_cps = [
                {
                    'cp_id': cp_id,
                    'location': data['location'],
                    'price_per_kwh': data['price_per_kwh'],
                    'status': data['status']
                }
                for cp_id, data in self.registered_cps.items()
            ]
            
            response = {
                'type': 'AVAILABLE_CPS',
                'charging_points': available_cps,
                'timestamp': time.time()
            }
            
            self.producer.send(f'driver_{driver_id}', response)
            self.producer.flush()
            print(f"  <- Lista de {len(available_cps)} CPs enviada a driver_{driver_id}")
        
        elif request_type == 'REQUEST_CHARGING':
            driver_id = event.get('driver_id')
            cp_id = event.get('cp_id')
            
            print(f"  -> Solicitud de carga: {driver_id} en {cp_id}")
            
            if cp_id in self.registered_cps:
                cp_status = self.registered_cps[cp_id]['status']
                cp_health = self.registered_cps[cp_id].get('health_status', 'UNKNOWN')
                
                # Verificar que el CP esté disponible Y saludable
                if cp_status == 'ACTIVATED' and cp_health != 'BROKEN':
                    # Verificar que no esté ocupado
                    if cp_id in self.active_sessions:
                        print(f"  <- DENEGADO - CP {cp_id} ocupado por {self.active_sessions[cp_id]}")
                        denial = {
                            'type': 'CHARGING_DENIED',
                            'cp_id': cp_id,
                            'driver_id': driver_id,
                            'reason': f'CP ocupado',
                            'timestamp': time.time()
                        }
                        self.producer.send(f'driver_{driver_id}', denial)
                        self.producer.flush()
                    else:
                        print(f"  <- AUTORIZANDO carga en {cp_id}")
                        
                        auth_to_cp = {
                            'type': 'AUTHORIZE_CHARGING',
                            'cp_id': cp_id,
                            'driver_id': driver_id,
                            'timestamp': time.time()
                        }
                        self.producer.send(f'cp_{cp_id}', auth_to_cp)
                        
                        auth_to_driver = {
                            'type': 'CHARGING_AUTHORIZED',
                            'cp_id': cp_id,
                            'driver_id': driver_id,
                            'message': 'Carga autorizada',
                            'timestamp': time.time()
                        }
                        self.producer.send(f'driver_{driver_id}', auth_to_driver)
                        self.producer.flush()
                        
                        self.active_sessions[cp_id] = driver_id
                    
                else:
                    reason = f'CP no disponible (estado: {cp_status}, salud: {cp_health})'
                    print(f"  <- DENEGADO - {reason}")
                    denial = {
                        'type': 'CHARGING_DENIED',
                        'cp_id': cp_id,
                        'driver_id': driver_id,
                        'reason': reason,
                        'timestamp': time.time()
                    }
                    self.producer.send(f'driver_{driver_id}', denial)
                    self.producer.flush()
            else:
                print(f"  <- DENEGADO - CP {cp_id} no registrado")
                denial = {
                    'type': 'CHARGING_DENIED',
                    'cp_id': cp_id,
                    'driver_id': driver_id,
                    'reason': 'CP no registrado',
                    'timestamp': time.time()
                }
                self.producer.send(f'driver_{driver_id}', denial)
                self.producer.flush()
    
    def handle_charging_event(self, event):
        """Manejar eventos de carga"""
        event_type = event.get('type')
        cp_id = event.get('cp_id')
        driver_id = event.get('driver_id')
        
        if event_type == 'CHARGING_DATA':
            consumption = event.get('current_consumption', 0)
            cost = event.get('total_cost', 0)
            print(f"  -> {cp_id}: {consumption:.2f}KW - {cost:.2f}EUR (Driver: {driver_id})")
            
            # Reenviar al topic privado del driver
            self.producer.send(f'driver_{driver_id}', event)
        
        elif event_type == 'CHARGING_FINISHED':
            total_consumed = event.get('total_consumed', 0)
            total_cost = event.get('total_cost', 0)
            duration = event.get('duration', 0)
            
            print(f"  -> FINALIZADO {cp_id}: {total_consumed:.3f}KWh - {total_cost:.2f}EUR - {duration:.0f}s")
            
            # Reenviar al topic privado del driver
            self.producer.send(f'driver_{driver_id}', event)
            self.producer.flush()
            
            # Limpiar sesión activa
            if cp_id in self.active_sessions:
                del self.active_sessions[cp_id]
                print(f"  <- Sesión {cp_id} finalizada")
    
    def handle_cp_status(self, event):
        """Manejar cambios de estado de CP"""
        cp_id = event.get('cp_id')
        new_state = event.get('new_state')
        old_state = event.get('old_state')
        
        if cp_id in self.registered_cps:
            self.registered_cps[cp_id]['status'] = new_state
            print(f"  -> Estado {cp_id}: {old_state} -> {new_state}")
            
            # Si el CP pasa a BROKEN mientras está suministrando
            if new_state == 'BROKEN' and cp_id in self.active_sessions:
                driver_id = self.active_sessions[cp_id]
                print(f"  [!] CP {cp_id} cambió a BROKEN durante suministro a {driver_id}")
                
                # Nota: El Engine ya debería haber enviado CHARGING_FINISHED
                # pero por si acaso, aseguramos que el driver sea notificado
    
    def cleanup(self):
        """Limpieza"""
        self.running = False
        
        for cp_id, sock in list(self.monitor_clients.items()):
            try:
                sock.close()
            except:
                pass
        
        if self.socket_server:
            self.socket_server.close()
        
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()
        
        print("[CLEANUP] Mock CENTRAL cerrado")

def main():
    mock = MockCentral()
    mock.start()

if __name__ == "__main__":
    main()