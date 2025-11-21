import socket
import tkinter as tk
from tkinter import ttk
import threading
from db_manager import DBManager
from kafka import KafkaProducer, KafkaConsumer
import json
import time

HOST = "0.0.0.0"
PORT = 5001

db = DBManager("evcharging.db")

# Diccionario de sesiones activas: (driver_id, cp_id) -> session_id
active_sessions = {}
active_monitor_connections = {}

cps_activated_by_monitor = set()


# ===== Kafka Producer =====
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def publish_event(event_type, data):
    """Publicar eventos en Kafka"""
    event = {
        "type": event_type,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "data": data
    }
    producer.send("ev_events", event)
    producer.flush()

def handle_cp_registration(event):
        """Manejar registro de CP via Kafka"""
        cp_id = event.get('cp_id')
        location = event.get('location')
        price_per_kwh = event.get('price_per_kwh', 0.35)
        recovering = event.get('recovering', False)
        
        cp_current = db.get_cp(cp_id)
        if cp_current and cp_current['estado'] == 'AVERIA':
            print(f"[REGISTER] ⚠️  CP {cp_id} se registra pero está en estado AVERÍA")

        # Guardar en BD
        db.add_cp(cp_id, location)
        print(f"[KAFKA] CP {cp_id} registrado en {location}")

        interrupted_session = None
        if recovering:
            print(f"[RECOVERY] CP {cp_id} recuperándose - verificando sesiones...")
            
            # Buscar sesión activa en BD para este CP
            for (driver_id, session_cp_id), session_id in list(active_sessions.items()):
                if session_cp_id == cp_id:
                    print(f"[RECOVERY] ⚠️  Sesión activa encontrada: Driver {driver_id}")
                    
                    # Obtener datos completos de la sesión desde BD
                    session_info = db.get_session(session_id)
                    print(f"[RECOVERY] 📊 Datos de BD: {session_info}")

                    if session_info:
                        interrupted_session = {
                            'session_id': session_id,
                            'driver_id': driver_id,
                            'total_consumed': session_info.get('total_consumed', 0.0),
                            'total_cost': session_info.get('total_cost', 0.0),
                            'duration': time.time() - session_info.get('start_time', time.time())
                        }
                        
                        print(f"[RECOVERY] Datos recuperados de BD:")
                        print(f"           - Consumo: {interrupted_session['total_consumed']:.3f} kWh")
                        print(f"           - Coste: {interrupted_session['total_cost']:.2f} EUR")
                    break
            if not interrupted_session:
                print(f"[RECOVERY] ℹ️  No se encontró sesión interrumpida en memoria")

        current_cp = db.get_cp(cp_id)
        final_state = "ACTIVADO"
        if current_cp and current_cp['estado'] == 'AVERIA':
            final_state = "AVERIA"
            print(f"[REGISTER] 🔄 Manteniendo estado AVERÍA para CP {cp_id}")
        
        # Confirmar al CP
        confirmation = {
            "type": "REGISTER_CONFIRMED",
            "cp_id": cp_id,
            "interrupted_session": interrupted_session,
            "current_state": final_state,
            "timestamp": time.time()
        }
        producer.send(f'cp_{cp_id}', confirmation)
        producer.flush()
        
        # Publicar evento general
        publish_event("REGISTER_CP", {
            "cp_id": cp_id, 
            "ubicacion": location,
            "precio": price_per_kwh,
            "estado": final_state 
        })
        
        if interrupted_session:
            print(f"[KAFKA] Confirmación enviada con datos de sesión interrumpida")
        else:
            print(f"[KAFKA] Confirmación enviada a cp_{cp_id}")

def cleanup_inconsistent_states():
    """Limpiar estados inconsistentes al arrancar la central"""
    

    print("\n[STARTUP] Verificando conexiones activas...")
    all_cps = db.get_all_cps()
    
    print("[STARTUP] Esperando 15 segundos para reconexión de Monitores...")
    time.sleep(15)

    print("[STARTUP] Verificando conexiones después del periodo de espera...")
    
    for cp in all_cps:
        cp_id = cp['cp_id']
        current_state = cp['status']
        
        # Solo verificar CPs que estaban en estado activo
        if current_state in ['ACTIVADO', 'SUMINISTRANDO', 'PARADO']:
            if cp_id not in active_monitor_connections:
                print(f"[STARTUP] ❌ CP {cp_id} estaba {current_state} - SIN Monitor después de espera")
                db.set_cp_state(cp_id, "DESCONECTADO")
                publish_event("CP_DISCONNECTED", {
                    "cp_id": cp_id, 
                    "reason": "Monitor didn't reconnect after central restart",
                    "previous_state": current_state
                })
            else:
                print(f"[STARTUP] ✅ CP {cp_id} se reconectó - Monitor activo")

    print("[STARTUP] Verificando estados inconsistentes...")
    
    # 1. RECUPERAR SESIONES ACTIVAS DESDE BD
    open_sessions = db.get_open_sessions()  # Sesiones sin fecha de fin
    
    pending_sessions = {}
    # Reconstruir el diccionario active_sessions
    for session in open_sessions:
        driver_id = session['driver_id']
        cp_id = session['cp_id']
        session_id = session['session_id']
        pending_sessions[cp_id] = {
            'driver_id': driver_id,
            'session_id': session_id,
            'session_info': session
        }
        print(f"[STARTUP] Sesión pendiente de confirmar: CP {cp_id}, Driver {driver_id}")
    
    time.sleep(10)

    
    # 2. VERIFICAR CONSISTENCIA DE ESTADOS
    for cp_id, session_data in pending_sessions.items():
        session_id = session_data['session_id']
        driver_id = session_data['driver_id']
        
        # Verificar si el CP reportó sesión activa al reconectarse
        # (esto se haría si el Engine incluyó la info en REGISTER_CP)
        
        # Por ahora, cerrar sesiones de CPs que NO están en SUMINISTRANDO
        cp = db.get_cp(cp_id)
        estado = cp['estado'] if cp else None
        
        if estado in ['ACTIVADO', 'PARADO']:
            # El CP no está suministrando -> la sesión terminó
            session_info = session_data['session_info']
            total_consumed = session_info.get('total_consumed', 0.0)
            total_cost = session_info.get('total_cost', 0.0)

            db.end_session(session_id, total_consumed, total_cost)
            print(f"[STARTUP] ✓ Sesión cerrada (BD normal): CP {cp_id}, Driver {driver_id}")
            print(f"            Consumo: {total_consumed:.3f} kWh, Coste: {total_cost:.2f} EUR")           
        else:
            # El CP SÍ está suministrando -> sesión activa
            print(f"[STARTUP] ⏳ Sesión en estado {estado}: Esperando confirmación del Monitor para CP {cp_id}")
    


def handle_charging_finished(event):
    """Manejar finalización de carga desde Engine"""
    cp_id = event.get('cp_id')
    driver_id = event.get('driver_id')
    session_id = event.get('session_id')
    total_consumed = event.get('total_consumed', 0.0)
    total_cost = event.get('total_cost', 0.0)
    reason = event.get('reason', '')
    interrupted = event.get('interrupted', False)
    duration = event.get('duration', 0)

    was_failure = 'averia' in reason.lower() or 'failure' in reason.lower() or 'error' in reason.lower()

    if interrupted:
        print(f"\n{'='*70}")
        if was_failure:
            print(f"💥 CARGA INTERRUMPIDA POR AVERÍA")
        else:
            print(f"⚠️  SESIÓN INTERRUMPIDA RECUPERADA Y FINALIZADA")
        print(f"{'='*70}")
        print(f"CP: {cp_id}")
        print(f"Driver: {driver_id}")
        print(f"Session ID: {session_id}")
        print(f"Razón: {reason}")
        print(f"Consumo final: {total_consumed:.3f} kWh")
        print(f"Coste final: {total_cost:.2f} EUR")
        print(f"{'='*70}\n")
    else:
        print(f"[KAFKA] Carga finalizada: CP {cp_id}, Driver {driver_id}")
    
    
    # Buscar sesión activa
    key = (driver_id, cp_id)
    if key in active_sessions:
        session_id = active_sessions.pop(key)
    elif not session_id:
        print(f"[WARNING] No se encontró sesión activa para Driver {driver_id}, CP {cp_id}")
        if not interrupted:
            return
    
    # Actualizar BD
    db.end_session(session_id, total_consumed, total_cost)

    # ✅ NUEVO: Solo activar si el CP NO está DESCONECTADO
    cp = db.get_cp(cp_id)
    current_state = cp.get('estado') if cp else None

    if current_state == 'DESCONECTADO':
        print(f"[BD] CP {cp_id} permanece DESCONECTADO")
    elif current_state == 'AVERIA':
        print(f"[BD] CP {cp_id} permanece en AVERIA")
    elif was_failure:
        # ✅ NUEVO: Si fue por avería, marcar como AVERÍA
        db.set_cp_state(cp_id, "AVERIA")
        print(f"[BD] CP {cp_id} marcado como AVERÍA (finalización por fallo)")
    else:
        db.set_cp_state(cp_id, "ACTIVADO")
        print(f"[BD] CP {cp_id} marcado como ACTIVADO")
    
    print(f"[BD] Sesión {session_id} finalizada: {total_consumed:.3f} kWh, {total_cost:.2f} EUR")
    
    # Publicar evento general
    publish_event("END_CHARGE", {
        "session": session_id,
        "energia": total_consumed,
        "coste": total_cost,
        "interrupted": interrupted,
        "failure": was_failure,
    })
    
    # Notificar al driver
    finish_msg = {
        "type": "CHARGING_FINISHED",
        "driver_id": driver_id,
        "cp_id": cp_id,
        "total_consumed": total_consumed,
        "total_cost": total_cost,
        "duration": duration,
        "interrupted": interrupted,
        "was_failure": was_failure,
        "reason": reason if interrupted else "Carga completada normalmente",
        "timestamp": time.time()
    }
    try:
        producer.send(f'driver_{driver_id}', finish_msg)
        producer.flush()
        
        if interrupted:
            print(f"[KAFKA] ✅ Ticket de RECUPERACIÓN enviado a driver_{driver_id}")
        else:
            print(f"[KAFKA] ✅ Ticket final enviado a driver_{driver_id}")
            
    except Exception as e:
        print(f"[ERROR] Error enviando ticket al driver: {e}")
    
    

def handle_state_change(event):
    """Manejar cambios de estado desde Engine"""
    cp_id = event.get('cp_id')
    new_state = event.get('new_state')
    old_state = event.get('old_state')
    
    
    print(f"[KAFKA] Cambio de estado CP {cp_id}: {old_state} -> {new_state}")

    cp = db.get_cp(cp_id)
    current_bd_state = cp.get('estado') if cp else None

    MONITOR_PRIORITY_STATES = ['DESCONECTADO', 'AVERIA']

    if current_bd_state in MONITOR_PRIORITY_STATES:
        print(f"[STATE CHANGE] ❌ IGNORADO: CP {cp_id} está en {current_bd_state} (reportado por Monitor)")
        print(f"[STATE CHANGE] Estado del Engine DESCARTADO: {old_state} -> {new_state}")
        return  # ❌ NO ACTUALIZAR BD
    
    if new_state == "ACTIVATED" and current_bd_state == "SUMINISTRANDO":
        print(f"[STATE CHANGE] ⚠️  CP {cp_id} terminó suministro - verificando con Monitor...")
        
        # ✅ Pequeña pausa para dar tiempo al Monitor a reportar avería si existe
        time.sleep(3)
        
        # ✅ Verificar nuevamente el estado después de la pausa
        cp_updated = db.get_cp(cp_id)
        updated_state = cp_updated.get('estado') if cp_updated else None
        
        if updated_state in MONITOR_PRIORITY_STATES:
            print(f"[STATE CHANGE] ❌ CONFIRMADO: Monitor reportó {updated_state} - IGNORANDO Engine")
            return
        
        print(f"[STATE CHANGE] ✅ No hay reporte del Monitor - procediendo con estado ACTIVADO")
    
    # Mapear estados del Engine a estados de BD
    state_mapping = {
        "DISCONNECTED": "DESCONECTADO",
        "ACTIVATED": "ACTIVADO",
        "STOPPED": "PARADO",
        "SUPPLYING": "SUMINISTRANDO",
        "BROKEN": "AVERIA"
    }
    
    bd_state = state_mapping.get(new_state, new_state)

    cp = db.get_cp(cp_id)
    current_state = cp.get('estado') if cp else None
    
    if current_state == 'DESCONECTADO':
        print(f"[BD] ⚠️  CP {cp_id} está DESCONECTADO - Ignorando cambio de estado del Engine")
        print(f"[BD] Estado ignorado: {old_state} -> {new_state}")
        return  # ✅ NO actualizar la BD, mantener DESCONECTADO
    
    # Actualizar BD
    db.set_cp_state(cp_id, bd_state)
    print(f"[BD] Estado actualizado: CP {cp_id} -> {bd_state}")
    
    # Publicar evento general
    publish_event("SET_STATE", {
        "cp_id": cp_id,
        "estado": bd_state
    })
    
    # Notificar cambio en cp_status (para drivers que estén monitoreando)
    status_msg = {
        "type": "CP_STATUS_CHANGE",
        "cp_id": cp_id,
        "new_state": bd_state,
        "timestamp": time.time()
    }
    producer.send('cp_status', status_msg)
    producer.flush()

# ===== Kafka Consumer para Drivers =====
def start_kafka_consumer():
    """Escuchar solicitudes de drivers via Kafka"""
    consumer = KafkaConsumer(
        'driver_requests',
        'cp_registrations',
        'charging_events',      # ✅ AÑADIR: Eventos de carga (finalizaciones)
        'cp_status',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='central_group',
        enable_auto_commit=True
    )
    
    print("[KAFKA] Consumer iniciado - escuchando topics múltiples")
    
    for message in consumer:
        try:
            event = message.value
            event_type = event.get('type')
            
            print(f"[KAFKA] Evento recibido: {event_type}")
            
            if event_type == 'REGISTER_DRIVER':
                handle_driver_registration(event)
            
            elif event_type == 'REQUEST_CHARGING':
                handle_charging_request(event)

            elif event_type == 'REGISTER_CP':  
                handle_cp_registration(event)

                
            elif event_type == 'CHARGING_FINISHED':
                handle_charging_finished(event)
            
            
            elif event_type == 'STATE_CHANGE':
                handle_state_change(event)

            elif event_type == 'CLEANUP_SESSION':
                handle_cleanup_session(event)
                
        except Exception as e:
            print(f"[KAFKA ERROR] {e}")

    

def handle_driver_registration(event):
    """Manejar registro de driver via Kafka"""
    try:        
        driver_id = event.get('driver_id')
                
        # Registrar en BD
        db.add_driver(driver_id)
        
        # Enviar lista de CPs disponibles
        cps = db.get_all_cps()
        
        response = {
            "type": "AVAILABLE_CPS",
            "charging_points": cps,
            "timestamp": time.time()
        }
        
        # Publicar en topic privado del driver
        producer.send(f'driver_{driver_id}', response)
        
        producer.flush()
        
        print(f"[KAFKA] Lista de CPs enviada a driver_{driver_id}")
        
    except Exception as e:
        print(f"[ERROR DETALLADO] {e}")
        import traceback
        traceback.print_exc()


def handle_cleanup_session(event):
    """Limpiar sesiones fantasma cuando un Engine se recupera"""
    cp_id = event.get('cp_id')
    reason = event.get('reason', 'Unknown')
    
    print(f"\n{'='*70}")
    print(f"🧹 LIMPIEZA DE SESIÓN SOLICITADA")
    print(f"{'='*70}")
    print(f"CP ID: {cp_id}")
    print(f"Razón: {reason}")
    
    # Buscar y eliminar cualquier sesión activa de este CP
    sessions_to_clean = []
    for (driver_id, session_cp_id), session_id in list(active_sessions.items()):
        if session_cp_id == cp_id:
            sessions_to_clean.append((driver_id, session_cp_id, session_id))
    
    if sessions_to_clean:
        for driver_id, session_cp_id, session_id in sessions_to_clean:
            print(f"🗑️  Limpiando sesión fantasma:")
            print(f"   - Session ID: {session_id}")
            print(f"   - Driver: {driver_id}")
            print(f"   - CP: {session_cp_id}")
            
            print(f"   ℹ️  Sesión mantenida en memoria para recuperación")
            
            # Notificar al driver
            cleanup_notification = {
                "type": "SESSION_CLEANED",
                "driver_id": driver_id,
                "cp_id": cp_id,
                "session_id": session_id,
                "reason": "CP reiniciado - sesión interrumpida",
                "timestamp": time.time()
            }
            producer.send(f'driver_{driver_id}', cleanup_notification)
        
        producer.flush()
        print(f"✅ Limpiadas {len(sessions_to_clean)} sesiones fantasma")
    else:
        print(f"ℹ️  No hay sesiones activas para limpiar en CP {cp_id}")
    
    # Asegurar que el CP está en estado correcto
    cp = db.get_cp(cp_id)
    if cp:
        current_state = cp.get('estado')
        if current_state not in ['DESCONECTADO', 'AVERIA']:
            db.set_cp_state(cp_id, "ACTIVADO")
            print(f"✅ CP {cp_id} marcado como ACTIVADO")
    
    print(f"{'='*70}\n")
    
    # Publicar evento general
    publish_event("SESSION_CLEANUP", {
        "cp_id": cp_id,
        "sessions_cleaned": len(sessions_to_clean)
    })

def handle_charging_request(event):
    """Manejar solicitud de carga via Kafka"""
    driver_id = event.get('driver_id')
    cp_id = event.get('cp_id')
    
    print(f"[KAFKA] Solicitud de carga: Driver {driver_id} -> CP {cp_id}")
    
    # Verificar CP
    cp = db.get_cp(cp_id)
    
    if not cp:
        # CP no encontrado
        response = {
            "type": "CHARGING_DENIED",
            "cp_id": cp_id,
            "reason": "CP no encontrado",
            "timestamp": time.time()
        }
        producer.send(f'driver_{driver_id}', response)
        producer.flush()
        return
    
    if cp["estado"] != "ACTIVADO":
        # CP no disponible
        response = {
            "type": "CHARGING_DENIED",
            "cp_id": cp_id,
            "reason": f"CP en estado {cp['estado']}",
            "timestamp": time.time()
        }
        producer.send(f'driver_{driver_id}', response)
        producer.flush()
        print(f"[KAFKA] Carga DENEGADA: CP {cp_id} en estado {cp['estado']}")
        return
    
    # Autorizar carga
    session_id = db.start_session(driver_id, cp_id)
    active_sessions[(driver_id, cp_id)] = session_id
    db.set_cp_state(cp_id, "SUMINISTRANDO")
    
    # Notificar autorización al driver
    response = {
        "type": "CHARGING_AUTHORIZED",
        "cp_id": cp_id,
        "driver_id": driver_id,
        "session_id": session_id,
        "timestamp": time.time()
    }
    producer.send(f'driver_{driver_id}', response)
    producer.flush()

    cp_authorization = {
    "type": "AUTHORIZE_CHARGING",
    "cp_id": cp_id,
    "driver_id": driver_id,
    "session_id": session_id,
    "timestamp": time.time()
    }
    producer.send(f'cp_{cp_id}', cp_authorization)
    producer.flush()

    print(f"[KAFKA] Autorización enviada a cp_{cp_id}")
    
    # Publicar evento general
    publish_event("REQUEST_CHARGE", {
        "driver_id": driver_id,
        "cp_id": cp_id,
        "session": session_id
    })
    
    print(f"[KAFKA] Carga autorizada: Driver {driver_id}, CP {cp_id}, Sesión {session_id}")

# ===== Procesamiento de mensajes por SOCKET (para CPs) =====
def handle_monitor_message(data):
    """NUEVO: Manejar mensajes JSON del Monitor"""
    msg_type = data.get('type')
    
    if msg_type == 'REGISTER_MONITOR':
        cp_id = data.get('cp_id')
        monitor_id = data.get('monitor_id')
        location = data.get('location', 'Unknown')
        
        active_monitor_connections[cp_id] = {
            'monitor_id': monitor_id,
            'connection_time': time.time(),
            'last_activity': time.time()
        }
        print(f"[MONITOR] ✅ Conexión registrada para CP {cp_id}")

        print(f"\n{'='*70}")
        print(f"[MONITOR] Registro de Monitor: {monitor_id}")
        print(f"{'='*70}")
        print(f"CP ID: {cp_id}")
        print(f"Ubicación: {location}")
        print(f"{'='*70}\n")
        
        cp = db.get_cp(cp_id)
        
        if cp:
            print(f"[AUTH] ✅ Monitor {monitor_id} autenticado para CP {cp_id}")
            print(f"[INFO] Esperando confirmación de health check...\n")

            return json.dumps({
                "status": "REGISTERED",
                "cp_id": cp_id,
                "cp_info": {
                    "location": cp.get('location', cp.get('ubicacion', 'Unknown')),
                    "status": cp.get('estado', cp.get('status', 'Unknown'))
                }
            })
        else:
            print(f"[ERROR] ❌ CP {cp_id} NO EXISTE en el sistema")
            print(f"[ERROR] El CP debe ser registrado primero por su Engine")
            print(f"[ERROR] RECHAZANDO registro de Monitor {monitor_id}")
            print(f"{'='*70}\n")
            
            return json.dumps({
                "status": "REJECTED",
                "reason": f"CP {cp_id} no existe en el sistema. El Engine debe registrarlo primero.",
                "cp_id": cp_id
            })
    
    elif msg_type == 'CP_HEALTH_CHANGE':
        cp_id = data.get('cp_id')
        new_status = data.get('new_status')
        old_status = data.get('old_status')
        error_msg = data.get('error_message', '')
        message_detail = data.get('message', '')
        
        print(f"\n{'='*70}")

        # ✅ PRIMER OK - Activar automáticamente el CP
        if new_status == 'OK' and message_detail == 'Engine ready - First OK':
            print("🟢 PRIMER OK DETECTADO - ACTIVANDO CP")
            print("="*70)
            print(f"CP ID: {cp_id}")
            print(f"Monitor confirmó: Engine operativo")
            print("="*70 + "\n")
            
            # Marcar como activado por Monitor
            cps_activated_by_monitor.add(cp_id)
            
            # Actualizar BD a ACTIVADO
            db.set_cp_state(cp_id, "ACTIVADO")
            
            # ✅ ENVIAR ACTIVATE_CP al Engine para que cambie su estado operacional
            activate_msg = {
                "type": "ACTIVATE_CP",
                "cp_id": cp_id,
                "timestamp": time.time()
            }
            producer.send(f'cp_{cp_id}', activate_msg)
            producer.send('cp_commands', activate_msg)
            producer.flush()
            
            print(f"[KAFKA] ✅ Comando ACTIVATE_CP enviado a Engine {cp_id}")
            print(f"[BD] CP {cp_id} marcado como ACTIVADO\n")
            
            # Publicar evento general
            publish_event("CP_ACTIVATED", {"cp_id": cp_id})
            
            return f"CP {cp_id} ACTIVADO por Monitor"
        
        # ⚠️ AVERÍA DETECTADA
        elif new_status == 'BROKEN':
            print("💥 AVERÍA REPORTADA POR MONITOR")
            print("="*70)
            print(f"CP ID: {cp_id}")
            print(f"Estado previo: {old_status}")
            print(f"Estado nuevo: AVERIA")
            if error_msg:
                print(f"Detalle: {error_msg}")
            print("="*70 + "\n")
            
            # Obtener estado actual antes de cambiar
            cp = db.get_cp(cp_id)
            was_supplying = cp and cp['estado'] == 'SUMINISTRANDO'
            
            # Cambiar a AVERIA
            db.set_cp_state(cp_id, "AVERIA")
            
            # Publicar evento
            publish_event("CP_AVERIA", {"cp_id": cp_id, "error": error_msg})
            
            # Notificar por Kafka
            status_msg = {
                "type": "CP_STATUS_CHANGE",
                "cp_id": cp_id,
                "new_state": "AVERIA",
                "reason": "fault_detected",
                "timestamp": time.time()
            }
            producer.send('cp_status', status_msg)
            producer.send(f'cp_{cp_id}', status_msg)
            producer.flush()
            
            # Mensaje adicional si estaba suministrando
            if was_supplying:
                print(f"⚠️  [CRITICAL] CP {cp_id} estaba SUMINISTRANDO - Carga interrumpida\n")
            
            print(f"[BD] CP {cp_id} marcado como AVERIA")
            
            return f"CP {cp_id} marcado como AVERIA"
        
        # ✅ RECUPERACIÓN DE AVERÍA
        elif new_status == 'OK' and old_status == 'BROKEN':
            print("✅ RECUPERACIÓN REPORTADA POR MONITOR")
            print("="*70)
            print(f"CP ID: {cp_id}")
            print(f"Estado previo: AVERIA")
            print(f"Estado nuevo: ACTIVADO")
            print("="*70 + "\n")
            
            # Recuperar de AVERIA
            cp = db.get_cp(cp_id)
            if cp and cp['estado'] == 'AVERIA':
                db.set_cp_state(cp_id, "ACTIVADO")
                
                # ✅ Reactivar el Engine
                activate_msg = {
                    "type": "ACTIVATE_CP",
                    "cp_id": cp_id,
                    "timestamp": time.time()
                }
                producer.send(f'cp_{cp_id}', activate_msg)
                producer.send('cp_commands', activate_msg)
                producer.flush()
                
                publish_event("CP_RECUPERADO", {"cp_id": cp_id})
                
                status_msg = {
                    "type": "CP_STATUS_CHANGE",
                    "cp_id": cp_id,
                    "new_state": "ACTIVADO",
                    "reason": "fault_recovered",
                    "timestamp": time.time()
                }
                producer.send('cp_status', status_msg)
                producer.flush()
                
                print(f"[KAFKA] ✅ Comando ACTIVATE_CP enviado a Engine {cp_id}")
                print(f"[BD] ✅ CP {cp_id} RECUPERADO y ACTIVADO\n")
                return f"CP {cp_id} recuperado y activado"
            else:
                print(f"[INFO] CP {cp_id} ya está operativo (estado: {cp['estado'] if cp else 'Unknown'})\n")
                return f"CP {cp_id} ya está operativo"
        
        return f"Estado de salud procesado: {cp_id}" 
       
    # ✅ MONITOR DESCONECTADO
    elif msg_type == 'MONITOR_SHUTDOWN':
        cp_id = data.get('cp_id')
        monitor_id = data.get('monitor_id')
        
        print(f"\n{'='*70}")
        print(f"⚠️  MONITOR DESCONECTADO")
        print(f"{'='*70}")
        print(f"CP ID: {cp_id}")
        print(f"Monitor ID: {monitor_id}")
        print(f"{'='*70}\n")

        cp = db.get_cp(cp_id)
        was_supplying = cp and cp['estado'] == 'SUMINISTRANDO'

        if was_supplying:
            print(f"⚠️  CRITICAL: CP {cp_id} estaba SUMINISTRANDO")
            print(f"{'='*70}")
            
            # Buscar sesión activa para obtener driver_id
            active_driver = None
            for (driver_id, session_cp_id), session_id in active_sessions.items():
                if session_cp_id == cp_id:
                    active_driver = driver_id
                    break
            
            if active_driver:
                print(f"🛑 Finalizando suministro del conductor {active_driver}")
                
                # Enviar comando de interrupción al Engine
                interrupt_msg = {
                    "type": "INTERRUPT_CHARGING",
                    "cp_id": cp_id,
                    "driver_id": active_driver,
                    "reason": "Monitor desconectado",
                    "timestamp": time.time()
                }
                producer.send(f'cp_{cp_id}', interrupt_msg)
                producer.send('cp_commands', interrupt_msg)
                producer.flush()
                
                # Notificar al driver
                driver_interrupt_msg = {
                    "type": "CHARGING_INTERRUPTED",
                    "cp_id": cp_id,
                    "driver_id": active_driver,
                    "reason": "Monitor desconectado - Suministro finalizado",
                    "timestamp": time.time()
                }
                producer.send(f'driver_{active_driver}', driver_interrupt_msg)
                producer.flush()
                
                print(f"📤 Comando INTERRUPT_CHARGING enviado a Engine {cp_id}")
                print(f"📤 Notificación enviada a Driver {active_driver}")
            else:
                print(f"⚠️  WARNING: No se encontró driver activo para CP {cp_id}")
        
        print(f"{'='*70}\n")
        
        # Marcar CP como DESCONECTADO
        db.set_cp_state(cp_id, "DESCONECTADO")
        
        # Publicar evento
        publish_event("MONITOR_SHUTDOWN", {"cp_id": cp_id})
        
        # Notificar por Kafka
        status_msg = {
            "type": "CP_STATUS_CHANGE",
            "cp_id": cp_id,
            "new_state": "DESCONECTADO",
            "reason": "monitor_shutdown",
            "timestamp": time.time()
        }
        producer.send('cp_status', status_msg)
        producer.flush()
        
        print(f"[BD] CP {cp_id} marcado como DESCONECTADO")
        
        return f"CP {cp_id} desconectado - Monitor cerrado"
    
    else:
        return f"Tipo de mensaje desconocido: {msg_type}"
    

def handle_message(message):
    """Procesar mensaje recibido del CP o Monitor"""
    try:
        data = json.loads(message)
        
        # Determinar tipo de cliente y delegar
        msg_type = data.get('type')
        
        if msg_type in ['REGISTER_MONITOR', 'CP_HEALTH_CHANGE', 'MONITOR_SHUTDOWN']:
            # Mensaje del Monitor
            return handle_monitor_message(data)
        
        else:
            # Mensaje desconocido
            print(f"[WARN] Tipo de mensaje no reconocido: {msg_type}")
            return json.dumps({"status": "ERROR", "message": "Tipo de mensaje desconocido"})
    
    except json.JSONDecodeError as e:
        print(f"[ERROR] Mensaje no es JSON válido: {e}")
        return json.dumps({"status": "ERROR", "message": "Formato JSON inválido"})
    
    except Exception as e:
        print(f"[ERROR] Error procesando mensaje: {e}")
        import traceback
        traceback.print_exc()
        return json.dumps({"status": "ERROR", "message": str(e)})
    
# ===== Servidor TCP para CPs =====
def client_thread(conn, addr):
    """Manejar conexión persistente con buffer"""
    print(f"[DEBUG CENTRAL] ====== Nueva conexión desde {addr} ======")
    print(f"[SOCKET] Conexión desde {addr}")
    
    buffer = ""
    DELIMITER = "\n"  # Delimitador de mensajes
    mensaje_num = 0
    client_cp_id = None

    try:
        while True:
            # Recibir chunk
            chunk = conn.recv(4096).decode("utf-8")
            if not chunk:
                print(f"[DEBUG CENTRAL] Cliente desconectado")
                break
            print(f"[DEBUG CENTRAL] Chunk recibido: {len(chunk)} bytes")
            buffer += chunk
            
            # Procesar mensajes completos
            while DELIMITER in buffer:
                message, buffer = buffer.split(DELIMITER, 1)
                
                if message.strip():
                    mensaje_num += 1
                    print(f"[DEBUG CENTRAL] Mensaje #{mensaje_num}: {message[:100]}")

                    try:
                        data = json.loads(message)
                        if data.get('type') == 'REGISTER_MONITOR':
                            client_cp_id = data.get('cp_id')
                            print(f"[SOCKET] Registrada conexión para CP {client_cp_id}")
                    except:
                        pass
                
                    response = handle_message(message)
                    
                    if response:
                        conn.sendall((response + DELIMITER).encode("utf-8"))
                        print(f"[DEBUG CENTRAL] Respuesta enviada")
                        
    except Exception as e:
        print(f"[SOCKET] Error: {e}")
        import traceback  # ✅ AÑADE
        traceback.print_exc()
    finally:
        if client_cp_id and client_cp_id in active_monitor_connections:
            del active_monitor_connections[client_cp_id]
            print(f"[SOCKET] Conexión eliminada para CP {client_cp_id} por desconexión")

        conn.close()

def start_tcp_server():
    """Servidor TCP para CPs y Monitor"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((HOST, PORT))
        server_socket.listen()
        print(f"[SOCKET] Servidor TCP escuchando en {HOST}:{PORT}...")

        while True:
            conn, addr = server_socket.accept()
            threading.Thread(target=client_thread, args=(conn, addr), daemon=True).start()

def start_server():
    """Iniciar ambos servidores: TCP y Kafka"""
    cleanup_inconsistent_states() 

    # Thread 1: Servidor TCP (para CPs)
    tcp_thread = threading.Thread(target=start_tcp_server, daemon=True)
    tcp_thread.start()
    
    # Thread 2: Consumer Kafka (para Drivers)
    kafka_thread = threading.Thread(target=start_kafka_consumer, daemon=True)
    kafka_thread.start()
    
    print("\n" + "="*60)
    print("EV_CENTRAL INICIADO")
    print("="*60)
    print("✓ Servidor TCP: puerto 5001 (CPs, Monitor)")
    print("✓ Kafka Consumer: topic 'driver_requests'")
    print("✓ Kafka Producer: topics 'ev_events', 'driver_{id}', 'cp_status'")
    print("="*60 + "\n")
    
    # Mantener vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[STOP] Cerrando servidor...")

# ===== AÑADIR AL FINAL DEL ARCHIVO =====

class CPPanel(tk.Frame):
    """Panel individual para cada Charging Point"""
    
    def __init__(self, parent, cp_data):
        super().__init__(parent, relief=tk.RAISED, borderwidth=2)
        
        self.cp_id = cp_data['cp_id']
        self.estado = cp_data.get('estado', 'DESCONECTADO')
        
        # Estado colores
        self.color_map = {
            'ACTIVADO': '#28a745',      # Verde
            'SUMINISTRANDO': '#28a745', # Verde
            'PARADO': '#ff9800',         # Naranja
            'AVERIA': '#dc3545',         # Rojo
            'DESCONECTADO': '#6c757d'    # Gris
        }
        
        color = self.color_map.get(self.estado, '#6c757d')
        self.configure(bg=color, width=200, height=150)
        
        # Header: ID y Ubicación
        header = tk.Label(self, text=f"{cp_data['cp_id']}", 
                         font=('Arial', 14, 'bold'),
                         bg=color, fg='white')
        header.pack(pady=5)
        
        location = tk.Label(self, text=cp_data['location'],
                           font=('Arial', 10),
                           bg=color, fg='white')
        location.pack()
        
        # Precio
        price = cp_data.get('price_per_kwh', 0.35)  # Valor por defecto si no existe
        price_label = tk.Label(self, text=f"{price:.2f} €/kWh", 
                font=('Arial', 9),
                bg=color, fg='white')
        price_label.pack(pady=2)
        
        # Frame para datos dinámicos
        self.data_frame = tk.Frame(self, bg=color)
        self.data_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Labels dinámicos
        self.status_label = tk.Label(self.data_frame, 
                                     text=self.get_status_text(),
                                     font=('Arial', 10, 'bold'),
                                     bg=color, fg='white')
        self.status_label.pack()
        
        # Datos de suministro
        self.supply_frame = tk.Frame(self.data_frame, bg=color)
        self.supply_frame.pack()
        
        self.consumption_label = None
        self.cost_label = None
        self.driver_label = None
    
    def get_status_text(self):
        """Obtener texto del estado"""
        status_text = {
            'ACTIVADO': 'Disponible',
            'SUMINISTRANDO': 'Suministrando',
            'PARADO': 'Out of Order',
            'AVERIA': 'Averiado',
            'DESCONECTADO': 'Desconectado'
        }
        return status_text.get(self.estado, self.estado)
    
    def update_state(self, new_state):
        """Actualizar estado del CP"""
        self.estado = new_state
        color = self.color_map.get(new_state, '#6c757d')
        
        # Actualizar colores de todos los widgets
        self.configure(bg=color)
        self.data_frame.configure(bg=color)
        self.supply_frame.configure(bg=color)
        
        for widget in self.winfo_children():
            try:
                widget.configure(bg=color)
            except:
                pass
        
        for widget in self.data_frame.winfo_children():
            try:
                widget.configure(bg=color)
            except:
                pass
        
        # Actualizar texto
        self.status_label.configure(text=self.get_status_text(), bg=color)
        
        # Limpiar datos de suministro si no está suministrando
        if new_state != 'SUMINISTRANDO':
            self.clear_supply_data()
    
    def update_supply_data(self, consumption, cost, driver_id):
        """Actualizar datos de suministro en tiempo real"""
        color = self.color_map['SUMINISTRANDO']
        
        # Crear labels si no existen
        if not self.consumption_label:
            self.consumption_label = tk.Label(self.supply_frame, 
                                             font=('Arial', 9),
                                             bg=color, fg='white')
            self.consumption_label.pack()
        
        if not self.cost_label:
            self.cost_label = tk.Label(self.supply_frame,
                                      font=('Arial', 9),
                                      bg=color, fg='white')
            self.cost_label.pack()
        
        if not self.driver_label:
            self.driver_label = tk.Label(self.supply_frame,
                                        font=('Arial', 9, 'italic'),
                                        bg=color, fg='white')
            self.driver_label.pack()
        
        # Actualizar texto
        self.consumption_label.configure(text=f"{consumption:.2f} kW")
        self.cost_label.configure(text=f"{cost:.2f} €")
        self.driver_label.configure(text=f"Driver: {driver_id}")
    
    def clear_supply_data(self):
        """Limpiar datos de suministro"""
        if self.consumption_label:
            self.consumption_label.destroy()
            self.consumption_label = None
        if self.cost_label:
            self.cost_label.destroy()
            self.cost_label = None
        if self.driver_label:
            self.driver_label.destroy()
            self.driver_label = None


class CentralMonitorGUI:
    """Interfaz Gráfica de Monitorización"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.root = tk.Tk()
        self.root.title("EV CHARGING SOLUTION - MONITORIZATION PANEL")
        self.root.geometry("1400x900")
        self.root.configure(bg='#2c3e50')
        
        # Diccionario de paneles CP
        self.cp_panels = {}
        
        # Diccionario de solicitudes activas
        self.active_requests = {}
        
        self.setup_ui()
        self.load_charging_points()

        self.verify_and_clean_gui_data()
        
        # Actualizar cada segundo desde BD
        self.update_from_db()
    
    def setup_ui(self):
        """Configurar interfaz de usuario"""
        # Header
        header = tk.Frame(self.root, bg='#1a252f', height=70)
        header.pack(fill=tk.X, side=tk.TOP)
        header.pack_propagate(False)
        
        title = tk.Label(header, 
                        text="*** SD EV CHARGING SOLUTION. MONITORIZATION PANEL ***",
                        font=('Arial', 18, 'bold'),
                        bg='#1a252f', fg='#00ff00')
        title.pack(pady=20)
        
        # Frame principal dividido
        content = tk.Frame(self.root, bg='#2c3e50')
        content.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # ===== IZQUIERDA: Grid de CPs con scroll =====
        left_frame = tk.Frame(content, bg='#34495e')
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        cp_label = tk.Label(left_frame, text="CHARGING POINTS STATUS",
                           font=('Arial', 12, 'bold'),
                           bg='#34495e', fg='white')
        cp_label.pack(pady=5)
        
        # Canvas con scrollbar para CPs
        canvas = tk.Canvas(left_frame, bg='#34495e', highlightthickness=0)
        scrollbar = ttk.Scrollbar(left_frame, orient="vertical", command=canvas.yview)
        self.cp_container = tk.Frame(canvas, bg='#34495e')
        
        self.cp_container.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=self.cp_container, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # ===== DERECHA: Solicitudes y mensajes =====
        right_frame = tk.Frame(content, bg='#34495e', width=400)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=5)
        right_frame.pack_propagate(False)
        
        
        # Panel de mensajes del sistema
        msg_label = tk.Label(right_frame, text="*** APPLICATION MESSAGES ***",
                            font=('Arial', 11, 'bold'),
                            bg='#34495e', fg='white')
        msg_label.pack(pady=(15, 5))
        
        msg_scroll = ttk.Scrollbar(right_frame)
        msg_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.message_text = tk.Text(right_frame, height=15, bg='#ecf0f1',
                                   font=('Courier', 9),
                                   yscrollcommand=msg_scroll.set)
        self.message_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        msg_scroll.config(command=self.message_text.yview)

        control_frame = tk.Frame(self.root, bg='#34495e', height=60)
        control_frame.pack(fill=tk.X, side=tk.BOTTOM, padx=10, pady=5)
        control_frame.pack_propagate(False)
        
        tk.Label(control_frame, text="Control Manual CPs:",
                font=('Arial', 11, 'bold'),
                bg='#34495e', fg='white').pack(side=tk.LEFT, padx=10)
        
        # Entrada para CP ID
        tk.Label(control_frame, text="CP ID:",
                bg='#34495e', fg='white').pack(side=tk.LEFT, padx=5)
        
        self.cp_id_entry = tk.Entry(control_frame, width=12, font=('Arial', 10))
        self.cp_id_entry.pack(side=tk.LEFT, padx=5)
        
        # Botón PARAR CP específico
        stop_btn = tk.Button(control_frame, text="⏸ PARAR CP",
                            bg='#ff9800', fg='white',
                            command=self.stop_cp,
                            font=('Arial', 10, 'bold'),
                            padx=10, pady=5)
        stop_btn.pack(side=tk.LEFT, padx=5)
        
        # Botón REANUDAR CP específico
        resume_btn = tk.Button(control_frame, text="▶ REANUDAR CP",
                            bg='#28a745', fg='white',
                            command=self.resume_cp,
                            font=('Arial', 10, 'bold'),
                            padx=10, pady=5)
        resume_btn.pack(side=tk.LEFT, padx=5)
        
        # Separador visual
        tk.Label(control_frame, text="|",
                bg='#34495e', fg='#7f8c8d',
                font=('Arial', 16)).pack(side=tk.LEFT, padx=10)
        
        # Botón PARAR TODOS
        stop_all_btn = tk.Button(control_frame, text="⏸ PARAR TODOS",
                                bg='#dc3545', fg='white',
                                command=self.stop_all_cps,
                                font=('Arial', 10, 'bold'),
                                padx=10, pady=5)
        stop_all_btn.pack(side=tk.LEFT, padx=5)
        
        # Botón REANUDAR TODOS
        resume_all_btn = tk.Button(control_frame, text="▶ REANUDAR TODOS",
                                bg='#17a2b8', fg='white',
                                command=self.resume_all_cps,
                                font=('Arial', 10, 'bold'),
                                padx=10, pady=5)
        resume_all_btn.pack(side=tk.LEFT, padx=5)
        
        # Estado del sistema al fondo
        footer = tk.Frame(self.root, bg='#1a252f', height=40)
        footer.pack(fill=tk.X, side=tk.BOTTOM)
        footer.pack_propagate(False)
        
        status = tk.Label(footer, text="CENTRAL system status: OK",
                         font=('Arial', 10),
                         bg='#1a252f', fg='#00ff00')
        status.pack(pady=10)

    
    def load_charging_points(self):
        """Cargar CPs desde la BD"""
        cps = self.db.get_all_cps()
        
        # Crear grid de paneles (4 columnas)
        row, col = 0, 0
        for cp in cps:
            panel = CPPanel(self.cp_container, cp)
            panel.grid(row=row, column=col, padx=8, pady=8, sticky="nsew")
            
            self.cp_panels[cp['cp_id']] = panel
            
            col += 1
            if col >= 3:  # 3 columnas
                col = 0
                row += 1
        
        self.add_message(f"Loaded {len(cps)} charging points")

    def reload_all_panels(self, cps):
        """Recargar todos los paneles (cuando se añaden nuevos CPs)"""
        # Limpiar paneles existentes
        for widget in self.cp_container.winfo_children():
            widget.destroy()
        
        self.cp_panels.clear()
        
        # Crear grid de paneles (3 columnas)
        row, col = 0, 0
        for cp in cps:
            panel = CPPanel(self.cp_container, cp)
            panel.grid(row=row, column=col, padx=8, pady=8, sticky="nsew")
            
            self.cp_panels[cp['cp_id']] = panel
            
            col += 1
            if col >= 3:  # 3 columnas
                col = 0
                row += 1
        
        self.add_message(f"Panel actualizado: {len(cps)} CPs activos")
    
    def update_from_db(self):
        """Actualizar estados desde BD periódicamente"""
        cps = self.db.get_all_cps()

        # Detectar nuevos CPs y añadirlos
        current_cp_ids = set(self.cp_panels.keys())
        new_cp_ids = set(cp['cp_id'] for cp in cps)

        # Añadir CPs nuevos
        added_cps = new_cp_ids - current_cp_ids
        if added_cps:
            print(f"[GUI] Detectados {len(added_cps)} nuevos CPs: {added_cps}")
            # Recargar todos los paneles
            self.reload_all_panels(cps)

        
        
        for cp in cps:
            cp_id = cp['cp_id']
            estado = cp['status']
            
            if cp_id in self.cp_panels:
                current_state = self.cp_panels[cp_id].estado
                if current_state != estado:
                    self.cp_panels[cp_id].update_state(estado)
                    if current_state == 'SUMINISTRANDO' and estado != 'SUMINISTRANDO':
                        self.clear_cp_supply_data(cp_id)
                        print(f"[GUI] Limpiados datos de suministro de CP {cp_id}")
        
        # Actualizar solicitudes activas
        try:
            sessions = self.db.get_active_sessions()
            self.update_request_table(sessions)
        except Exception as e:
            print(f"[GUI ERROR] Error actualizando sesiones: {e}")
        
        # Programar siguiente actualización
        self.root.after(1000, self.update_from_db)
    
    def update_request_table(self, sessions):
        """Actualizar tabla de solicitudes activas"""
        # Limpiar tabla
        for item in self.request_tree.get_children():
            self.request_tree.delete(item)
        
        # Añadir sesiones activas
        for session in sessions:
            self.request_tree.insert('', 'end', values=(
                time.strftime('%H:%M:%S', time.localtime(session.get('start_time', time.time()))),
                session.get('driver_id', 'N/A'),
                session.get('cp_id', 'N/A')
            ))
    
    def update_cp_supply_data(self, cp_id, consumption, cost, driver_id):
        """Actualizar datos de suministro en tiempo real"""
        if cp_id in self.cp_panels:
            self.cp_panels[cp_id].update_supply_data(consumption, cost, driver_id)
    
    def add_message(self, msg):
        """Añadir mensaje al panel"""
        timestamp = time.strftime("%d/%m/%y %H:%M:%S")
        self.message_text.insert(tk.END, f"[{timestamp}] {msg}\n")
        self.message_text.see(tk.END)

    def clear_cp_supply_data(self, cp_id):
        """Limpiar datos de suministro cuando un CP deja de estar activo"""
        if cp_id in self.cp_panels:
            self.cp_panels[cp_id].clear_supply_data()

    def verify_and_clean_gui_data(self):
        """Verificar y limpiar datos inconsistentes al iniciar"""
        cps = self.db.get_all_cps()
        for cp in cps:
            cp_id = cp['cp_id']
            estado = cp['status']
            
            # Si el CP no está SUMINISTRANDO, asegurar que los datos estén limpios
            if estado != 'SUMINISTRANDO' and cp_id in self.cp_panels:
                self.clear_cp_supply_data(cp_id)
                print(f"[STARTUP] Limpiados datos obsoletos de CP {cp_id}")

    def stop_cp(self):
        """Enviar comando PARAR a un CP específico"""
        cp_id = self.cp_id_entry.get().strip()
        if not cp_id:
            self.add_message("❌ ERROR: Introduce un CP ID")
            return
        
        # Verificar que el CP existe
        cp = db.get_cp(cp_id)
        if not cp:
            self.add_message(f"❌ ERROR: CP {cp_id} no encontrado")
            return
        
        estado_actual = cp['estado']
    
        if estado_actual == 'DESCONECTADO':
            self.add_message(f"⚠️ WARNING: CP {cp_id} está DESCONECTADO, no se puede parar")
            return
    
        if estado_actual == 'PARADO':
            self.add_message(f"ℹ️ INFO: CP {cp_id} ya está PARADO")
            return
    
        if estado_actual == 'AVERIA':
            self.add_message(f"⚠️ WARNING: CP {cp_id} está en AVERIA, no se puede parar")
            return
        
        # Actualizar en BD
        db.set_cp_state(cp_id, "PARADO")
        
        # Publicar evento a Kafka
        status_msg = {
            "type": "STOP_COMMAND",
            "cp_id": cp_id,
            "new_state": "PARADO",
            "timestamp": time.time()
        }
        producer.send(f'cp_{cp_id}', status_msg)
        producer.flush()

        producer.send('cp_commands', status_msg)
        producer.flush()
        
        producer.send('cp_status', status_msg)
        producer.send(f'cp_{cp_id}', status_msg)
        producer.flush()
        
        self.add_message(f"⏸ Comando PARAR enviado a CP {cp_id} (estaba {estado_actual})")
        print(f"[CENTRAL] CP {cp_id} PARADO manualmente (estado previo: {estado_actual})")

    def resume_cp(self):
        """Enviar comando REANUDAR a un CP específico"""
        cp_id = self.cp_id_entry.get().strip()
        if not cp_id:
            self.add_message("❌ ERROR: Introduce un CP ID")
            return
        
        # Verificar que el CP existe
        cp = db.get_cp(cp_id)
        if not cp:
            self.add_message(f"❌ ERROR: CP {cp_id} no encontrado")
            return
        
        # ✅ VALIDAR: Solo se puede reanudar si está PARADO
        estado_actual = cp['estado']
        
        if estado_actual == 'DESCONECTADO':
            self.add_message(f"⚠️ WARNING: CP {cp_id} está DESCONECTADO, no se puede reanudar")
            return
        
        if estado_actual == 'AVERIA':
            self.add_message(f"⚠️ WARNING: CP {cp_id} está en AVERIA, no se puede reanudar")
            return
        
        if estado_actual == 'ACTIVADO':
            self.add_message(f"ℹ️ INFO: CP {cp_id} ya está ACTIVADO")
            return
        
        if estado_actual == 'SUMINISTRANDO':
            self.add_message(f"⚠️ WARNING: CP {cp_id} está SUMINISTRANDO, no se puede reanudar")
            return
        
        if estado_actual != 'PARADO':
            self.add_message(f"⚠️ WARNING: CP {cp_id} en estado {estado_actual}, solo se puede reanudar desde PARADO")
            return
        
        # Actualizar en BD
        db.set_cp_state(cp_id, "ACTIVADO")
        
        # Publicar evento a Kafka
        status_msg = {
            "type": "RESUME_CP",
            "cp_id": cp_id,
            "new_state": "ACTIVADO",
            "timestamp": time.time()
        }
        producer.send('cp_status', status_msg)
        producer.send(f'cp_{cp_id}', status_msg)
        producer.flush()
        
        self.add_message(f"▶ Comando REANUDAR enviado a CP {cp_id}")
        print(f"[CENTRAL] CP {cp_id} REANUDADO manualmente")

    def stop_all_cps(self):
        """Enviar comando PARAR a TODOS los CPs"""
        cps = db.get_all_cps()
        
        if not cps:
            self.add_message("❌ No hay CPs registrados")
            return
        
        count_stopped = 0
        count_skipped = 0
        skipped_details = []
        for cp in cps:
            cp_id = cp['cp_id']
            estado = cp['status']

            if estado in ['DESCONECTADO', 'AVERIA', 'PARADO']:
                count_skipped += 1
                skipped_details.append(f"{cp_id}({estado})")
                continue

            db.set_cp_state(cp_id, "PARADO")
            
            status_msg = {
                "type": "STOP_COMMAND",
                "cp_id": cp_id,
                "new_state": "PARADO",
                "timestamp": time.time()
            }
            producer.send('cp_status', status_msg)
            producer.send(f'cp_{cp_id}', status_msg)
            count_stopped += 1
        
        producer.flush()

         # Mensaje detallado
        msg = f"⏸ Comando PARAR: {count_stopped} CPs parados"
        if count_skipped > 0:
            msg += f", {count_skipped} omitidos: {', '.join(skipped_details[:5])}"
            if len(skipped_details) > 5:
                msg += "..."
        
        self.add_message(msg)
        print(f"[CENTRAL] PARAR TODOS: {count_stopped} CPs parados, {count_skipped} omitidos")

    def resume_all_cps(self):
        """Enviar comando REANUDAR a TODOS los CPs"""
        cps = db.get_all_cps()
        
        if not cps:
            self.add_message("❌ No hay CPs registrados")
            return
        
        count_resumed = 0
        count_skipped = 0
        skipped_details = []

        for cp in cps:
            cp_id = cp['cp_id']
            estado = cp['status']

            if estado != 'PARADO':
                count_skipped += 1
                skipped_details.append(f"{cp_id}({estado})")
                continue

            db.set_cp_state(cp_id, "ACTIVADO")

            status_msg = {
            "type": "RESUME_CP",
            "cp_id": cp_id,
            "new_state": "ACTIVADO",
            "timestamp": time.time()
            }
            producer.send('cp_status', status_msg)
            producer.send(f'cp_{cp_id}', status_msg)
            count_resumed += 1
        
        producer.flush()

        msg = f"▶ Comando REANUDAR: {count_resumed} CPs reanudados"
        if count_skipped > 0:
            msg += f", {count_skipped} omitidos: {', '.join(skipped_details[:5])}"
            if len(skipped_details) > 5:
                msg += "..."
        
        self.add_message(msg)
        print(f"[CENTRAL] REANUDAR TODOS: {count_resumed} CPs reanudados, {count_skipped} omitidos")

    def run(self):
        """Ejecutar GUI en thread principal"""
        self.root.mainloop()


# ===== MODIFICAR LA FUNCIÓN start_server() =====

def start_server_with_gui():
    """Iniciar servidores con interfaz gráfica"""
    # Thread 1: Servidor TCP (para CPs)
    tcp_thread = threading.Thread(target=start_tcp_server, daemon=True)
    tcp_thread.start()
    
    # Thread 2: Consumer Kafka (para Drivers y eventos)
    kafka_thread = threading.Thread(target=start_kafka_consumer_backend, daemon=True)
    kafka_thread.start()
    
    print("\n" + "="*60)
    print("EV_CENTRAL INICIADO CON INTERFAZ GRÁFICA")
    print("="*60)
    print("✓ Servidor TCP: puerto 5001 (CPs, Monitor)")
    print("✓ Kafka Consumer: topics múltiples")
    print("✓ Interfaz Gráfica: Iniciando...")
    print("="*60 + "\n")
    
    # Pequeña pausa para que arranquen los servicios
    time.sleep(2)

    cleanup_inconsistent_states()
    
    # Iniciar GUI en thread principal
    gui = CentralMonitorGUI(db)
    
    # Integrar GUI con eventos Kafka
    setup_gui_kafka_integration(gui)
    
    gui.run()


def start_kafka_consumer_backend():
    """Consumer Kafka para lógica de negocio (sin GUI)"""
    # Este es el mismo consumer que tenías, solo renombrado
    start_kafka_consumer()


# Variable global para GUI
central_gui = None

def setup_gui_kafka_integration(gui):
    """Integrar eventos de Kafka con la GUI"""
    global central_gui
    central_gui = gui
    
    # Thread para actualizar GUI con eventos Kafka en tiempo real
    def kafka_to_gui():
        consumer = KafkaConsumer(
            'charging_events',
            'cp_status',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='central_gui_updates'
        )
        
        for message in consumer:
            event = message.value
            event_type = event.get('type')
            
            if event_type == 'CHARGING_DATA':
                cp_id = event.get('cp_id')
                driver_id = event.get('driver_id')
                consumption = event.get('current_consumption', 0)
                cost = event.get('total_cost', 0)
                
                # Actualizar GUI de forma segura
                gui.root.after(0, gui.update_cp_supply_data, 
                             cp_id, consumption, cost, driver_id)
            
            elif event_type == 'CHARGING_FINISHED':
                cp_id = event.get('cp_id')
                driver_id = event.get('driver_id')
                
                # ✅ Limpiar datos de suministro
                gui.root.after(0, gui.clear_cp_supply_data, cp_id)

                msg = f"Carga finalizada: CP {event.get('cp_id')}, Driver {event.get('driver_id')}"
                gui.root.after(0, gui.add_message, msg)
            
            elif event_type == 'STATE_CHANGE':
                msg = f"CP {event.get('cp_id')}: {event.get('old_state')} → {event.get('new_state')}"
                gui.root.after(0, gui.add_message, msg)
    
    kafka_gui_thread = threading.Thread(target=kafka_to_gui, daemon=True)
    kafka_gui_thread.start()


# ===== MODIFICAR EL BLOQUE if __name__ == "__main__": =====

if __name__ == "__main__":
    import sys
    
    start_server_with_gui()
