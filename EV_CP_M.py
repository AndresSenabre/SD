#!/usr/bin/env python3
"""
EV_CP_M - Monitor del Charging Point
Sistema de monitorización de salud del punto de recarga
"""

import socket
import threading
import time
import json
import sys
from enum import Enum
from dataclasses import dataclass

class HealthStatus(Enum):
    """Estados de salud del CP"""
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"
    DISCONNECTED = "DISCONNECTED"

@dataclass
class HealthCheck:
    """Resultado de health check"""
    timestamp: float
    status: HealthStatus
    response_time: float
    error_message: str = ""

class ChargingPointMonitor:
    def __init__(self, cp_id: str, engine_host: str, engine_port: int, 
                 central_host: str, central_port: int):
        self.cp_id = cp_id
        self.engine_host = engine_host
        self.engine_port = engine_port
        self.central_host = central_host
        self.central_port = central_port
        
        # Estado del monitor
        self.health_status = HealthStatus.DISCONNECTED
        self.last_health_check = None
        self.consecutive_failures = 0
        self.running = True
        
        # Conexiones
        self.engine_socket = None
        self.central_socket = None
        
        # Threads
        self.health_thread = None
        self.central_thread = None
        self.menu_thread = None
        
        # Configuración
        self.health_check_interval = 1  # segundos
        self.failure_threshold = 3      # fallos consecutivos para reportar avería

    def start(self):
        """Iniciar el Monitor del CP"""
        print(f"🔍 Iniciando Charging Point Monitor - CP: {self.cp_id}")
        
        try:
            # Conectar a CENTRAL
            self.connect_to_central()
            
            # Conectar al Engine
            self.connect_to_engine()
            
            # Iniciar health checks
            self.start_health_monitoring()
            
            # Iniciar menú interactivo
            self.start_menu()
            
            # Mantener el programa ejecutándose
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n🛑 Deteniendo Monitor...")
        finally:
            self.cleanup()

    def connect_to_central(self):
        """Conectar a CENTRAL para reportar estado"""
        print(f"🔄 Intentando conectar a CENTRAL {self.central_host}:{self.central_port}...")
        try:
            self.central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.central_socket.connect((self.central_host, self.central_port))
            
            # Registrarse como monitor
            register_msg = {
                "type": "REGISTER_MONITOR",
                "cp_id": self.cp_id,
                "monitor_id": f"MON_{self.cp_id}"
            }
            
            self.send_to_central(register_msg)
            print(f"✅ Conectado a CENTRAL ({self.central_host}:{self.central_port})")
            
        except Exception as e:
            print(f"❌ Error conectando a CENTRAL: {e}")
            print("⚠️  Continuando sin CENTRAL...")

    def connect_to_engine(self):
        """Conectar al Engine del CP"""
        print(f"🔄 Intentando conectar al Engine {self.engine_host}:{self.engine_port}...")
        try:
            self.engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.engine_socket.connect((self.engine_host, self.engine_port))
            print(f"✅ Conectado al Engine ({self.engine_host}:{self.engine_port})")
            
            self.health_status = HealthStatus.HEALTHY
            self.consecutive_failures = 0
            
        except Exception as e:
            print(f"❌ Error conectando al Engine: {e}")
            print(f"⚠️  Asegúrate de que Engine esté ejecutándose en puerto {self.engine_port}")
            self.health_status = HealthStatus.DISCONNECTED

    def start_health_monitoring(self):
        """Iniciar monitorización de salud"""
        def health_monitor():
            print(f"💓 Iniciando health checks cada {self.health_check_interval}s")
            
            while self.running:
                try:
                    # Realizar health check
                    health_check = self.perform_health_check()
                    
                    # Procesar resultado
                    self.process_health_result(health_check)
                    
                    # Mostrar estado
                    self.display_health_status(health_check)
                    
                    time.sleep(self.health_check_interval)
                    
                except Exception as e:
                    print(f"❌ Error en health monitoring: {e}")
                    time.sleep(self.health_check_interval)
        
        self.health_thread = threading.Thread(target=health_monitor, daemon=True)
        self.health_thread.start()

    def perform_health_check(self) -> HealthCheck:
        """Realizar health check al Engine"""
        start_time = time.time()
        
        try:
            if not self.engine_socket:
                self.connect_to_engine()
            
            # Enviar ping
            self.engine_socket.send(b"HEALTH_CHECK")
            
            # Esperar respuesta (timeout 2 segundos)
            self.engine_socket.settimeout(2.0)
            response = self.engine_socket.recv(1024).decode()
            
            response_time = time.time() - start_time
            
            if response == "OK":
                return HealthCheck(
                    timestamp=time.time(),
                    status=HealthStatus.HEALTHY,
                    response_time=response_time
                )
            elif response == "KO":
                return HealthCheck(
                    timestamp=time.time(),
                    status=HealthStatus.UNHEALTHY,
                    response_time=response_time,
                    error_message="Engine reportó KO"
                )
            else:
                return HealthCheck(
                    timestamp=time.time(),
                    status=HealthStatus.UNHEALTHY,
                    response_time=response_time,
                    error_message=f"Respuesta inesperada: {response}"
                )
                
        except socket.timeout:
            return HealthCheck(
                timestamp=time.time(),
                status=HealthStatus.UNHEALTHY,
                response_time=time.time() - start_time,
                error_message="Timeout en health check"
            )
        except Exception as e:
            return HealthCheck(
                timestamp=time.time(),
                status=HealthStatus.DISCONNECTED,
                response_time=time.time() - start_time,
                error_message=f"Error de conexión: {str(e)}"
            )

    def process_health_result(self, health_check: HealthCheck):
        """Procesar resultado del health check"""
        self.last_health_check = health_check
        previous_status = self.health_status
        
        # Actualizar estado
        if health_check.status == HealthStatus.HEALTHY:
            self.consecutive_failures = 0
            self.health_status = HealthStatus.HEALTHY
        else:
            self.consecutive_failures += 1
            
            if health_check.status == HealthStatus.DISCONNECTED:
                self.health_status = HealthStatus.DISCONNECTED
            elif self.consecutive_failures >= self.failure_threshold:
                self.health_status = HealthStatus.UNHEALTHY

        # Si cambió el estado, notificar a CENTRAL
        if previous_status != self.health_status:
            self.report_status_change(previous_status, self.health_status)

    def report_status_change(self, old_status: HealthStatus, new_status: HealthStatus):
        """Reportar cambio de estado a CENTRAL"""
        status_msg = {
            "type": "CP_HEALTH_CHANGE",
            "cp_id": self.cp_id,
            "monitor_id": f"MON_{self.cp_id}",
            "old_status": old_status.value,
            "new_status": new_status.value,
            "consecutive_failures": self.consecutive_failures,
            "timestamp": time.time()
        }
        
        if self.last_health_check and self.last_health_check.error_message:
            status_msg["error_message"] = self.last_health_check.error_message
        
        self.send_to_central(status_msg)
        
        print(f"📢 Reportando a CENTRAL: {old_status.value} → {new_status.value}")

    def display_health_status(self, health_check: HealthCheck):
        """Mostrar estado de salud en pantalla"""
        timestamp_str = time.strftime("%H:%M:%S", time.localtime(health_check.timestamp))
        
        if health_check.status == HealthStatus.HEALTHY:
            status_icon = "✅"
            status_color = "VERDE"
        elif health_check.status == HealthStatus.UNHEALTHY:
            status_icon = "❌" 
            status_color = "ROJO"
        else:
            status_icon = "🔌"
            status_color = "GRIS"
        
        print(f"[{timestamp_str}] {status_icon} CP {self.cp_id} - {status_color} "
              f"({health_check.response_time:.3f}s) "
              f"Fallos: {self.consecutive_failures}")
        
        if health_check.error_message:
            print(f"         ⚠️  {health_check.error_message}")

    def send_to_central(self, message):
        """Enviar mensaje a CENTRAL"""
        if self.central_socket:
            try:
                data = json.dumps(message)
                self.central_socket.send(data.encode())
                time.sleep(0.1)  # Evitar mensajes concatenados
            except Exception as e:
                print(f"❌ Error enviando a CENTRAL: {e}")

    def start_menu(self):
        """Iniciar menú interactivo"""
        def menu_loop():
            print("\n" + "="*50)
            print("🔍 MENÚ CHARGING POINT MONITOR")
            print("="*50)
            print("s - Ver estado actual del monitor")
            print("h - Ver historial de health checks")
            print("r - Reconectar al Engine")
            print("t - Cambiar intervalo de health check")
            print("q - Salir")
            print("="*50)
            
            while self.running:
                try:
                    cmd = input("👉 Comando: ").strip().lower()
                    
                    if cmd == 's':
                        self.show_monitor_status()
                    
                    elif cmd == 'h':
                        self.show_health_history()
                    
                    elif cmd == 'r':
                        self.reconnect_engine()
                    
                    elif cmd == 't':
                        self.change_health_interval()
                    
                    elif cmd == 'q':
                        self.running = False
                        break
                        
                except KeyboardInterrupt:
                    self.running = False
                    break
        
        self.menu_thread = threading.Thread(target=menu_loop, daemon=True)
        self.menu_thread.start()

    def show_monitor_status(self):
        """Mostrar estado actual del monitor"""
        print(f"\n📊 ESTADO DEL MONITOR - CP {self.cp_id}")
        print(f"🔍 Estado de salud: {self.health_status.value}")
        print(f"🔄 Intervalo checks: {self.health_check_interval}s")
        print(f"❌ Fallos consecutivos: {self.consecutive_failures}")
        print(f"⚠️  Umbral de fallos: {self.failure_threshold}")
        
        if self.last_health_check:
            timestamp_str = time.strftime("%H:%M:%S", 
                                        time.localtime(self.last_health_check.timestamp))
            print(f"📅 Último check: {timestamp_str}")
            print(f"⏱️  Tiempo respuesta: {self.last_health_check.response_time:.3f}s")
            
            if self.last_health_check.error_message:
                print(f"💥 Último error: {self.last_health_check.error_message}")

    def show_health_history(self):
        """Mostrar historial de health checks (simplified)"""
        print(f"\n📋 HISTORIAL DE HEALTH CHECKS - CP {self.cp_id}")
        print("(Esta funcionalidad se puede expandir para guardar historial)")
        
        if self.last_health_check:
            self.display_health_status(self.last_health_check)

    def reconnect_engine(self):
        """Reconectar al Engine"""
        print("🔄 Reconectando al Engine...")
        if self.engine_socket:
            self.engine_socket.close()
        
        self.connect_to_engine()

    def change_health_interval(self):
        """Cambiar intervalo de health checks"""
        try:
            new_interval = float(input("Nuevo intervalo en segundos (actual: {}): ".format(self.health_check_interval)))
            if 0.1 <= new_interval <= 60:
                self.health_check_interval = new_interval
                print(f"✅ Intervalo cambiado a {new_interval}s")
            else:
                print("❌ Intervalo debe estar entre 0.1 y 60 segundos")
        except ValueError:
            print("❌ Valor inválido")

    def cleanup(self):
        """Limpieza al cerrar"""
        self.running = False
        if self.engine_socket:
            self.engine_socket.close()
        if self.central_socket:
            self.central_socket.close()
        print("🧹 Monitor - Limpieza completada")

def main():
    if len(sys.argv) < 6:
        print("❌ Uso: python EV_CP_M.py <cp_id> <engine_host> <engine_port> <central_host> <central_port>")
        print("📝 Ejemplo: python EV_CP_M.py CP001 localhost 8081 localhost 8080")
        return
    
    cp_id = sys.argv[1]
    engine_host = sys.argv[2]
    engine_port = int(sys.argv[3])
    central_host = sys.argv[4]
    central_port = int(sys.argv[5])
    
    monitor = ChargingPointMonitor(cp_id, engine_host, engine_port, central_host, central_port)
    monitor.start()

if __name__ == "__main__":
    main()