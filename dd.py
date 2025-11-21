def handle_monitor_message(data):
    """NUEVO: Manejar mensajes JSON del Monitor"""
    msg_type = data.get('type')
    
    if msg_type == 'REGISTER_MONITOR':
        cp_id = data.get('cp_id')
        monitor_id = data.get('monitor_id')
        location = data.get('location', 'Unknown')
        
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
    