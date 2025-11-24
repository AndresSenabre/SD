#!/usr/bin/env python3
"""
EV_Driver_GUI - Interfaz Gráfica para el Conductor
Muestra toda la información requerida según el enunciado
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import time
import json
import sys
import os
from enum import Enum
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

try:
    from kafka import KafkaProducer, KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

class ServiceStatus(Enum):
    """Estados de solicitud de servicio"""
    PENDING = "PENDING"
    AUTHORIZED = "AUTHORIZED"
    CHARGING = "CHARGING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

@dataclass
class ChargingService:
    """Solicitud de servicio de recarga"""
    cp_id: str
    request_time: float
    status: ServiceStatus = ServiceStatus.PENDING
    authorized_time: Optional[float] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    current_consumption: float = 0.0
    total_consumed: float = 0.0
    total_cost: float = 0.0
    error_message: str = ""

class DriverGUI:
    def __init__(self, driver_id: str, kafka_host: str, kafka_port: int):
        self.driver_id = driver_id
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.last_data_timestamp = None      # Timestamp del último dato recibido
        self.timeout_check_thread = None     # Thread de verificación
        self.CHARGING_TIMEOUT = 5  
        
        # Estado del driver
        self.current_service: Optional[ChargingService] = None
        self.service_queue: List[str] = []
        self.service_history: List[ChargingService] = []
        self.available_cps: List[dict] = []
        self.running = True
        self.central_is_down = False
        self._ticket_modal_open = False
        
        # Lock para thread safety
        self.service_lock = threading.Lock()
        
        # Kafka
        self.kafka_producer = None
        self.kafka_consumer = None
        self.kafka_thread = None
        
        # GUI
        self.root = None
        self.setup_gui()

    def setup_gui(self):
        """Configurar interfaz gráfica"""
        self.root = tk.Tk()
        self.root.title(f"EV Driver - {self.driver_id}")
        self.root.geometry("1400x1000")  # ⬅️ Más altura
        self.root.configure(bg='#2c3e50')
        
        # Frame principal
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # HEADER con info del conductor
        self.create_header(main_frame)
        
        # SERVICIO ACTUAL (destacado)
        self.create_current_service_panel(main_frame)
        
        # DOS COLUMNAS - Dar más espacio
        columns_frame = ttk.Frame(main_frame)
        columns_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Columna izquierda: CPs disponibles (MÁS ESPACIO)
        left_frame = ttk.LabelFrame(columns_frame, text="Zona de CPs", padding=10)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        self.create_available_cps_panel(left_frame)
        
        # Columna derecha: Controles y mensajes
        right_frame = ttk.Frame(columns_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))
        self.create_controls_panel(right_frame)
        self.create_messages_panel(right_frame)
        
        # HISTORIAL (abajo) - menos espacio
        history_frame = ttk.LabelFrame(main_frame, text="Historial", padding=5)
        history_frame.pack(fill=tk.BOTH, expand=False, pady=(5, 0))
        self.create_history_panel(history_frame)
        
        # Protocolo de cierre
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def create_header(self, parent):
        """Header con información del conductor"""
        header_frame = tk.Frame(parent, bg='#34495e', relief=tk.RAISED, borderwidth=2)
        header_frame.pack(fill=tk.X, pady=(0, 10))
        
        title = tk.Label(header_frame, 
                        text=f"🚗 CONDUCTOR: {self.driver_id}",
                        bg='#34495e',
                        fg='white',
                        font=('Arial', 18, 'bold'),
                        pady=10)
        title.pack()
        
        # Estado de conexión
        self.connection_label = tk.Label(header_frame,
                                        text="🔴 Desconectado",
                                        bg='#34495e',
                                        fg='#e74c3c',
                                        font=('Arial', 10))
        self.connection_label.pack()

        self.central_status_label = tk.Label(header_frame,
                                    text="🟢 CENTRAL: Operativa",
                                    bg='#34495e',
                                    fg='#2ecc71',
                                    font=('Arial', 10, 'bold'))
        self.central_status_label.pack(pady=(5, 0))

    def create_current_service_panel(self, parent):
        """Panel del servicio actual (destacado)"""
        frame = tk.LabelFrame(parent,
                             text="⚡ SERVICIO ACTUAL",
                             bg='#ecf0f1',
                             fg='#2c3e50',
                             font=('Arial', 14, 'bold'),
                             relief=tk.RIDGE,
                             borderwidth=3)
        frame.pack(fill=tk.X, pady=10)
        
        # Frame interno
        inner_frame = tk.Frame(frame, bg='#ecf0f1')
        inner_frame.pack(fill=tk.BOTH, padx=20, pady=15)
        
        # Estado grande
        self.service_status_label = tk.Label(inner_frame,
                                             text="SIN SERVICIO ACTIVO",
                                             bg='#95a5a6',
                                             fg='white',
                                             font=('Arial', 16, 'bold'),
                                             relief=tk.RAISED,
                                             borderwidth=2,
                                             padx=20,
                                             pady=10)
        self.service_status_label.pack(pady=(0, 15))
        
        # Grid con información
        info_frame = tk.Frame(inner_frame, bg='#ecf0f1')
        info_frame.pack(fill=tk.X)
        
        # Columna 1: Info básica
        col1 = tk.Frame(info_frame, bg='#ecf0f1')
        col1.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10)
        
        tk.Label(col1, text="Punto de Recarga:", bg='#ecf0f1', 
                font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        self.service_cp_label = tk.Label(col1, text="---", bg='#ecf0f1',
                                        font=('Arial', 12))
        self.service_cp_label.pack(anchor=tk.W, pady=(0, 10))
        
        tk.Label(col1, text="Tiempo transcurrido:", bg='#ecf0f1',
                font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        self.service_time_label = tk.Label(col1, text="---", bg='#ecf0f1',
                                          font=('Arial', 12))
        self.service_time_label.pack(anchor=tk.W)
        
        # Columna 2: Consumo actual
        col2 = tk.Frame(info_frame, bg='#ecf0f1')
        col2.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10)
        
        tk.Label(col2, text="⚡ Consumo Actual:", bg='#ecf0f1',
                font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        self.consumption_label = tk.Label(col2, text="0.00 KW",
                                         bg='#ecf0f1',
                                         fg='#e67e22',
                                         font=('Arial', 20, 'bold'))
        self.consumption_label.pack(anchor=tk.W, pady=(0, 10))
        
        # Columna 3: Totales
        col3 = tk.Frame(info_frame, bg='#ecf0f1')
        col3.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10)
        
        tk.Label(col3, text="📊 Total Consumido:", bg='#ecf0f1',
                font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        self.total_kwh_label = tk.Label(col3, text="0.000 KWh",
                                       bg='#ecf0f1',
                                       fg='#3498db',
                                       font=('Arial', 18, 'bold'))
        self.total_kwh_label.pack(anchor=tk.W, pady=(0, 5))
        
        tk.Label(col3, text="💰 Total a Pagar:", bg='#ecf0f1',
                font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        self.total_cost_label = tk.Label(col3, text="0.00 EUR",
                                        bg='#ecf0f1',
                                        fg='#27ae60',
                                        font=('Arial', 18, 'bold'))
        self.total_cost_label.pack(anchor=tk.W)

    def create_available_cps_panel(self, parent):
        """Panel de CPs disponibles CON BOTÓN SEPARADO"""
        # Frame principal
        main_frame = tk.Frame(parent, bg='#ecf0f1')
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # SUB-FRAME para la tabla solamente
        table_frame = tk.LabelFrame(main_frame,
                                text="📍 PUNTOS DE RECARGA DISPONIBLES",
                                bg='#ecf0f1',
                                fg='#2c3e50',
                                font=('Arial', 12, 'bold'),
                                relief=tk.RIDGE,
                                borderwidth=2)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(5, 0))
        
        # Treeview dentro del sub-frame de tabla
        scrollbar = ttk.Scrollbar(table_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.cps_tree = ttk.Treeview(
            table_frame,
            columns=('ID', 'Ubicación', 'Estado', 'Precio'),
            show='headings',
            yscrollcommand=scrollbar.set,
            height=8
        )
        
        self.cps_tree.heading('ID', text='ID')
        self.cps_tree.heading('Ubicación', text='Ubicación')
        self.cps_tree.heading('Estado', text='Estado')
        self.cps_tree.heading('Precio', text='EUR/KWh')
        
        self.cps_tree.column('ID', width=80)
        self.cps_tree.column('Ubicación', width=150)
        self.cps_tree.column('Estado', width=100)
        self.cps_tree.column('Precio', width=80)
        
        self.cps_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.cps_tree.yview)
        
        # Tags
        self.cps_tree.tag_configure('AVAILABLE', background='#2ecc71', foreground='white')      # Verde - Disponible
        self.cps_tree.tag_configure('CHARGING', background='#3498db', foreground='white')       # Azul - Cargando
        self.cps_tree.tag_configure('STOPPED', background='#f39c12', foreground='white')        # Naranja - Parado
        self.cps_tree.tag_configure('BROKEN', background='#e74c3c', foreground='white')         # Rojo - Averiado
        self.cps_tree.tag_configure('DISCONNECTED', background='#95a5a6', foreground='white')   # Gris - Desconectado
        self.cps_tree.tag_configure('UNKNOWN', background='#9b59b6', foreground='white') 
        
        # BOTÓN EN UN FRAME SEPARADO Y DISTINTO
        button_frame = tk.Frame(main_frame, bg='#34495e', relief=tk.RAISED, borderwidth=2)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        
        btn_solicitar = tk.Button(
            button_frame,
            text="🚗 SOLICITAR SERVICIO EN CP SELECCIONADO 🚗",
            command=self.request_selected_cp,
            bg='#e74c3c',
            fg='white',
            font=('Arial', 16, 'bold'),
            relief=tk.RAISED,
            borderwidth=4,
            padx=30,
            pady=20
        )
        btn_solicitar.pack(fill=tk.X, padx=10, pady=10)
        
        print("🎯 BOTÓN SEPARADO CREADO - DEBE SER VISIBLE")

    def load_auto_services_file(self):
        """Carga automática del fichero services/<driver_id>_services.txt"""
        try:
            filename = f"services/{self.driver_id}_services.txt"

            if not os.path.exists(filename):
                self.log_message(f"✗ Archivo automático no encontrado: {filename}", 'error')
                return False

            with open(filename, 'r') as f:
                services = [line.strip() for line in f if line.strip()]

            with self.service_lock:
                self.service_queue = services

            self.log_message(f"✓ Cargados {len(services)} servicios automáticos", 'success')

            valid_cps, invalid_cps = self.validate_cps_in_queue()

            for i, cp_id in enumerate(services, 1):
                status = "✓" if cp_id in valid_cps else "✗"
                self.log_message(f"  {i}. {cp_id} {status}", 
                                'info' if cp_id in valid_cps else 'warning')

            if invalid_cps:
                self.log_message(f"⚠️ Advertencia: {len(invalid_cps)} CPs no están disponibles", 'warning')
                self.log_message(f"   CPs inválidos: {', '.join(invalid_cps)}", 'warning')

            self.update_queue_display()
            return True

        except Exception as e:
            self.log_message(f"✗ Error al cargar archivo automático: {e}", 'error')
            return False

    def create_controls_panel(self, parent):
        """Panel de controles"""
        frame = tk.LabelFrame(parent,
                             text="🎮 CONTROLES",
                             bg='#ecf0f1',
                             fg='#2c3e50',
                             font=('Arial', 12, 'bold'),
                             relief=tk.RIDGE,
                             borderwidth=2)
        frame.pack(fill=tk.X, pady=(0, 10))
        
        inner = tk.Frame(frame, bg='#ecf0f1')
        inner.pack(fill=tk.BOTH, padx=10, pady=10)
        
        tk.Button(inner,
                 text="📁 Cargar Servicios desde Archivo",
                 command=self.load_file,
                 bg='#3498db',
                 fg='white',
                 font=('Arial', 10, 'bold'),
                 padx=10,
                 pady=5).pack(fill=tk.X, pady=2)
        
        tk.Button(inner,
                 text="▶️ Iniciar Servicios Automáticos",
                 command=self.start_automated,
                 bg='#9b59b6',
                 fg='white',
                 font=('Arial', 10, 'bold'),
                 padx=10,
                 pady=5).pack(fill=tk.X, pady=2)
        
        tk.Button(inner,
                 text="🔄 Actualizar Lista de CPs",
                 command=self.refresh_cps,
                 bg='#f39c12',
                 fg='white',
                 font=('Arial', 10, 'bold'),
                 padx=10,
                 pady=5).pack(fill=tk.X, pady=2)
        
        # Cola
        queue_frame = tk.Frame(inner, bg='#ecf0f1')
        queue_frame.pack(fill=tk.X, pady=(10, 0))
        
        tk.Label(queue_frame,
                text="📋 Servicios en cola:",
                bg='#ecf0f1',
                font=('Arial', 10, 'bold')).pack(side=tk.LEFT)
        
        self.queue_label = tk.Label(queue_frame,
                                   text="0",
                                   bg='#ecf0f1',
                                   fg='#e74c3c',
                                   font=('Arial', 10, 'bold'))
        self.queue_label.pack(side=tk.LEFT, padx=5)

    def create_messages_panel(self, parent):
        """Panel de mensajes"""
        frame = tk.LabelFrame(parent,
                             text="📨 MENSAJES Y NOTIFICACIONES",
                             bg='#ecf0f1',
                             fg='#2c3e50',
                             font=('Arial', 12, 'bold'),
                             relief=tk.RIDGE,
                             borderwidth=2)
        frame.pack(fill=tk.BOTH, expand=True)
        
        self.messages_text = scrolledtext.ScrolledText(frame,
                                                       bg='#2c3e50',
                                                       fg='#ecf0f1',
                                                       font=('Courier', 9),
                                                       wrap=tk.WORD,
                                                       height=15)
        self.messages_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Tags para colores
        self.messages_text.tag_configure('info', foreground='#3498db')
        self.messages_text.tag_configure('success', foreground='#2ecc71')
        self.messages_text.tag_configure('warning', foreground='#f39c12')
        self.messages_text.tag_configure('error', foreground='#e74c3c')
        self.messages_text.tag_configure('highlight', foreground='#e67e22', font=('Courier', 9, 'bold'))

    def create_history_panel(self, parent):
        """Panel de historial"""
        frame = tk.LabelFrame(parent,
                             text="📜 HISTORIAL DE SERVICIOS",
                             bg='#ecf0f1',
                             fg='#2c3e50',
                             font=('Arial', 12, 'bold'),
                             relief=tk.RIDGE,
                             borderwidth=2)
        frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        tree_frame = tk.Frame(frame, bg='#ecf0f1')
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        scrollbar = ttk.Scrollbar(tree_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.history_tree = ttk.Treeview(tree_frame,
                                        columns=('Hora', 'CP', 'Estado', 'KWh', 'EUR', 'Duración'),
                                        show='headings',
                                        yscrollcommand=scrollbar.set,
                                        height=6)
        
        self.history_tree.heading('Hora', text='Hora')
        self.history_tree.heading('CP', text='CP')
        self.history_tree.heading('Estado', text='Estado')
        self.history_tree.heading('KWh', text='KWh')
        self.history_tree.heading('EUR', text='EUR')
        self.history_tree.heading('Duración', text='Duración')
        
        self.history_tree.column('Hora', width=80)
        self.history_tree.column('CP', width=80)
        self.history_tree.column('Estado', width=100)
        self.history_tree.column('KWh', width=80)
        self.history_tree.column('EUR', width=80)
        self.history_tree.column('Duración', width=100)
        
        self.history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.history_tree.yview)
        
        self.history_tree.tag_configure('completed', background='#2ecc71', foreground='white')
        self.history_tree.tag_configure('failed', background='#e74c3c', foreground='white')

    def log_message(self, message, tag='info'):
        """Agregar mensaje al panel"""
        def _log():
            timestamp = datetime.now().strftime('%H:%M:%S')
            self.messages_text.insert(tk.END, f"[{timestamp}] ", 'highlight')
            self.messages_text.insert(tk.END, f"{message}\n", tag)
            self.messages_text.see(tk.END)
        
        if self.root:
            self.root.after(0, _log)

    def update_connection_status(self, connected):
        """Actualizar estado de conexión"""
        def _update():
            if connected:
                self.connection_label.config(text="🟢 Conectado a Kafka", fg='#2ecc71')
            else:
                self.connection_label.config(text="🔴 Desconectado", fg='#e74c3c')
        
        if self.root:
            self.root.after(0, _update)

    def update_current_service_display(self):
        """Actualizar display del servicio actual"""
        def _update():
            with self.service_lock:
                if not self.current_service:
                    self.service_status_label.config(text="SIN SERVICIO ACTIVO", bg='#95a5a6')
                    self.service_cp_label.config(text="---")
                    self.service_time_label.config(text="---")
                    self.consumption_label.config(text="0.00 KW")
                    self.total_kwh_label.config(text="0.000 KWh")
                    self.total_cost_label.config(text="0.00 EUR")
                    return
                
                s = self.current_service
                
                # Estado
                status_colors = {
                    ServiceStatus.PENDING: ('#f39c12', 'ESPERANDO AUTORIZACIÓN'),
                    ServiceStatus.AUTHORIZED: ('#3498db', 'AUTORIZADO - PREPARANDO'),
                    ServiceStatus.CHARGING: ('#2ecc71', 'SUMINISTRANDO ENERGÍA'),
                    ServiceStatus.COMPLETED: ('#95a5a6', 'COMPLETADO'),
                    ServiceStatus.FAILED: ('#e74c3c', 'FALLIDO')
                }
                
                color, text = status_colors.get(s.status, ('#95a5a6', s.status.value))
                self.service_status_label.config(text=text, bg=color)
                
                # CP
                self.service_cp_label.config(text=s.cp_id)
                
                # Tiempo
                if s.start_time:
                    elapsed = time.time() - s.start_time
                    mins, secs = divmod(int(elapsed), 60)
                    self.service_time_label.config(text=f"{mins:02d}:{secs:02d}")
                else:
                    self.service_time_label.config(text="Esperando...")
                
                # Datos en tiempo real
                self.consumption_label.config(text=f"{s.current_consumption:.2f} KW")
                self.total_kwh_label.config(text=f"{s.total_consumed:.3f} KWh")
                self.total_cost_label.config(text=f"{s.total_cost:.2f} EUR")
        
        if self.root:
            self.root.after(0, _update)

    def update_available_cps_display(self):
        """Actualización inteligente que preserva la selección"""
        def _update():
            # ✅ 1. GUARDAR estado actual
            selected_items = self.cps_tree.selection()
            selected_ids = [self.cps_tree.item(item, 'values')[0] 
                        for item in selected_items 
                        if self.cps_tree.item(item, 'values')]
            
            # ✅ 2. ACTUALIZAR SIN RECREAR
            # Crear mapa de items actuales
            current_items = {}
            for item in self.cps_tree.get_children():
                values = self.cps_tree.item(item, 'values')
                if values:
                    current_items[values[0]] = item
            
            # Crear mapa de nuevos CPs
            new_cps_map = {}
            with self.service_lock:
                for cp in self.available_cps:
                    cp_id = cp.get('cp_id', 'Unknown')
                    new_cps_map[cp_id] = cp
            
            # ✅ 3. SINCRONIZAR: actualizar existentes, añadir nuevos, eliminar desaparecidos
            items_to_keep = set()
            
            # Actualizar existentes y marcar para mantener
            for cp_id, item in current_items.items():
                if cp_id in new_cps_map:
                    cp = new_cps_map[cp_id]
                    location = cp.get('location', 'Unknown')
                    status = cp.get('status', 'UNKNOWN')
                    price = cp.get('price_per_kwh', 0)
                    status_tag = self.get_status_tag(status)
                    
                    self.cps_tree.item(item, 
                                    values=(cp_id, location, status, f"{price:.2f}"),
                                    tags=(status_tag,))
                    items_to_keep.add(cp_id)
                else:
                    # CP que ya no existe - eliminarlo
                    self.cps_tree.delete(item)
            
            # Añadir nuevos CPs
            for cp_id, cp in new_cps_map.items():
                if cp_id not in current_items:
                    location = cp.get('location', 'Unknown')
                    status = cp.get('status', 'UNKNOWN')
                    price = cp.get('price_per_kwh', 0)
                    status_tag = self.get_status_tag(status)
                    
                    item = self.cps_tree.insert('', tk.END,
                                            values=(cp_id, location, status, f"{price:.2f}"),
                                            tags=(status_tag,))
                    items_to_keep.add(cp_id)
            
            # ✅ 4. RESTAURAR SELECCIÓN
            for item in self.cps_tree.get_children():
                values = self.cps_tree.item(item, 'values')
                if values and values[0] in selected_ids:
                    self.cps_tree.selection_add(item)
                    # Solo enfocar el primero seleccionado
                    if values[0] == selected_ids[0] if selected_ids else False:
                        self.cps_tree.focus(item)
        
        if self.root:
            self.root.after(0, _update)

    def get_status_tag(self, status):
        """Obtener tag de color según estado del CP"""
        status_map = {
            'ACTIVADO': 'AVAILABLE',
            'AVAILABLE': 'AVAILABLE',
            'SUMINISTRANDO': 'CHARGING', 
            'CHARGING': 'CHARGING',
            'PARADO': 'STOPPED',
            'OUT_OF_ORDER': 'STOPPED',
            'AVERIA': 'BROKEN',
            'BROKEN': 'BROKEN',
            'DESCONECTADO': 'DISCONNECTED',
            'DISCONNECTED': 'DISCONNECTED'
        }
        return status_map.get(status, 'UNKNOWN')

    def update_queue_display(self):
        """Actualizar contador de cola"""
        def _update():
            with self.service_lock:
                count = len(self.service_queue)
            self.queue_label.config(text=str(count))
        
        if self.root:
            self.root.after(0, _update)

    def update_history_display(self):
        """Actualizar historial"""
        def _update():
            for item in self.history_tree.get_children():
                self.history_tree.delete(item)
            
            with self.service_lock:
                for s in self.service_history:
                    hora = time.strftime('%H:%M:%S', time.localtime(s.request_time))
                    
                    if s.status == ServiceStatus.COMPLETED:
                        tag = 'completed'
                        estado = '✓ OK'
                        kwh = f"{s.total_consumed:.3f}"
                        eur = f"{s.total_cost:.2f}"
                        
                        if s.start_time and s.end_time:
                            duration = int(s.end_time - s.start_time)
                            mins, secs = divmod(duration, 60)
                            duracion = f"{mins:02d}:{secs:02d}"
                        else:
                            duracion = "---"
                    else:
                        tag = 'failed'
                        estado = '✗ FAIL'
                        kwh = "---"
                        eur = "---"
                        duracion = s.error_message[:20] if s.error_message else "Error"
                    
                    self.history_tree.insert('', tk.END,
                                           values=(hora, s.cp_id, estado, kwh, eur, duracion),
                                           tags=(tag,))
        
        if self.root:
            self.root.after(0, _update)

    def start(self):
        """Iniciar aplicación"""
        self.log_message("Iniciando Driver App...", 'info')
        
        # Conectar Kafka en thread
        kafka_init_thread = threading.Thread(target=self.connect_to_kafka, daemon=True)
        kafka_init_thread.start()

        self.root.after(3000, self.start_periodic_cp_updates)

        self.timeout_check_thread = threading.Thread(target=self.check_charging_timeout, daemon=True)
        self.timeout_check_thread.start()
        self.log_message("✅ Sistema de detección de timeout activado", 'info')
        
        # Actualizar GUI periódicamente
        self.periodic_update()
        
        # Iniciar GUI (blocking)
        self.root.mainloop()

    def connect_to_kafka(self):
        """Conectar a Kafka"""
        if not KAFKA_AVAILABLE:
            self.log_message("ERROR: kafka-python no disponible", 'error')
            self.running = False
            return
        
        max_retries = 10
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self.log_message(f"Intentando conexión Kafka {attempt + 1}/{max_retries}...", 'info')
                
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=f'{self.kafka_host}:{self.kafka_port}',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks=1,
                    max_block_ms=5000,
                    request_timeout_ms=10000
                )
                
                self.kafka_consumer = KafkaConsumer(
                    f'driver_{self.driver_id}',  # Mensajes específicos del driver
                    'charging_data',             # Datos de consumo en tiempo real
                    'charging_events',           # Eventos de carga
                    'cp_status', 
                    bootstrap_servers=f'{self.kafka_host}:{self.kafka_port}',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    group_id=f'driver_{self.driver_id}_{int(time.time() * 1000)}',
                    enable_auto_commit=True,

                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    max_poll_interval_ms=300000,
                    max_poll_records=100
                )
                
                self.log_message(f"✓ Conectado a Kafka ({self.kafka_host}:{self.kafka_port})", 'success')
                self.update_connection_status(True)
                
                # Thread para escuchar
                self.kafka_thread = threading.Thread(target=self.listen_kafka, daemon=True)
                self.kafka_thread.start()
                
                # Registrarse
                self.register_to_central()
                
                return
                
            except Exception as e:
                self.log_message(f"Error conexión: {e}", 'error')
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    self.log_message("No se pudo conectar a Kafka", 'error')
                    self.running = False

    def register_to_central(self):
        """Registrarse en CENTRAL"""
        if not self.kafka_producer:
            return
        
        register_msg = {
            "type": "REGISTER_DRIVER",
            "driver_id": self.driver_id,
            "timestamp": time.time()
        }
        
        self.kafka_producer.send('driver_requests', register_msg)
        self.kafka_producer.flush()
        self.log_message("Registro enviado a CENTRAL", 'info')

    def listen_kafka(self):
        """Escuchar eventos Kafka"""
        while self.running and self.kafka_consumer:
            try:
                messages = self.kafka_consumer.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    for record in records:
                        try:
                            event = record.value
                            topic = record.topic

                            print(f"\n[DEBUG DRIVER] ========================================")
                            print(f"[DEBUG DRIVER] MENSAJE RECIBIDO")
                            print(f"[DEBUG DRIVER] Topic: {topic}")
                            print(f"[DEBUG DRIVER] Tipo: {event.get('type', 'NO_TYPE')}")
                            print(f"[DEBUG DRIVER] Contenido: {json.dumps(event, indent=2)}")
                            print(f"[DEBUG DRIVER] ========================================\n")

                            
                            if topic == f'driver_{self.driver_id}':
                                self.handle_driver_event(event)
                            elif topic == 'cp_status':
                                event_type = event.get('type')
                                if event_type == 'CENTRAL_DOWN':
                                    self.handle_central_down(event)
                                elif event_type == 'CENTRAL_RECONNECTED':
                                    self.handle_central_reconnected(event)
                                else:
                                    self.handle_cp_status(event)
                            elif topic == 'charging_data':
                                self.handle_charging_data(event)
                            elif topic == 'charging_events':
                                print(f"[DEBUG DRIVER] 📩 Mensaje de charging_events:")
                                print(f"[DEBUG DRIVER] Tipo: {event.get('type')}")
                                print(f"[DEBUG DRIVER] Driver ID en evento: {event.get('driver_id')}")
                                print(f"[DEBUG DRIVER] MI driver_id: {self.driver_id}")
                                print(f"[DEBUG DRIVER] ¿Coinciden?: {event.get('driver_id') == self.driver_id}")
                                self.handle_driver_event(event)                               
                        except Exception as e:
                            self.log_message(f"Error procesando mensaje: {e}", 'error')
                            
            except Exception as e:
                if self.running:
                    self.log_message(f"Error en Kafka: {e}", 'error')
                    time.sleep(1)

    def handle_central_down(self, event):
        # Actualizar variable de estado
        self.central_is_down = True
        
        # Actualizar indicador visual
        self.update_central_status_display()
        
        # Mostrar mensaje en log
        self.log_message("="*60, 'error')
        self.log_message("🔴 ALERTA: CENTRAL NO DISPONIBLE", 'error')
        self.log_message("="*60, 'error')

    def handle_central_reconnected(self, event):
        """⚡ Manejar evento de recuperación de la CENTRAL"""
        # Actualizar variable de estado
        self.central_is_down = False
        
        # Actualizar indicador visual
        self.update_central_status_display()
        
        # Mostrar mensaje en log
        self.log_message("="*60, 'success')
        self.log_message("🟢 CENTRAL RECUPERADA Y OPERATIVA", 'success')
        self.log_message("="*60, 'success')

    def handle_driver_event(self, event):
        """Manejar eventos del driver"""
        event_type = event.get('type')
        
        if event_type == 'AVAILABLE_CPS':
            new_cps = event.get('charging_points', [])
        
            # ✅ DETECTAR CAMBIOS (solo si no es update silencioso)
            is_silent = event.get('silent_update', False)
            
            # Detectar cambios visualmente
            changed_cps = self.detect_cp_changes(new_cps)
            if changed_cps and not is_silent:
                for cp_id, old_state, new_state in changed_cps:
                    self.log_message(f"🔄 CP {cp_id}: {old_state} → {new_state}", 'info')
            
            self.available_cps = new_cps
            self.update_available_cps_display()
        
        elif event_type == 'CHARGING_AUTHORIZED':
            cp_id = event.get('cp_id')
            driver_id = event.get('driver_id')
            
            # Verificar que es para este driver
            if driver_id != self.driver_id:
                return
                
            with self.service_lock:
                if self.current_service and self.current_service.cp_id == cp_id:
                    self.current_service.status = ServiceStatus.AUTHORIZED
                    self.current_service.authorized_time = time.time()
            
            self.log_message(f"✓ Carga AUTORIZADA en {cp_id}", 'success')
            self.log_message("Esperando inicio del suministro...", 'info')
            self.update_current_service_display()
        
        elif event_type == 'CHARGING_STARTED':  # ⬅️ NUEVO EVENTO AÑADIDO
            cp_id = event.get('cp_id')
            driver_id = event.get('driver_id')
            
            if driver_id == self.driver_id:
                with self.service_lock:
                    if self.current_service and self.current_service.cp_id == cp_id:
                        self.current_service.status = ServiceStatus.CHARGING
                        self.current_service.start_time = time.time()
                
                self.log_message(f"⚡ INICIANDO suministro en {cp_id}", 'success')
                self.update_current_service_display()
        
        elif event_type == 'CHARGING_DENIED':
            cp_id = event.get('cp_id')
            driver_id = event.get('driver_id')
            reason = event.get('reason', 'No especificado')
            
            if driver_id != self.driver_id:
                return
                
            with self.service_lock:
                if self.current_service and self.current_service.cp_id == cp_id:
                    self.current_service.status = ServiceStatus.FAILED
                    self.current_service.error_message = reason
                    self.current_service.end_time = time.time()
                    
                    self.service_history.append(self.current_service)
                    self.current_service = None
            
            self.log_message(f"✗ Carga DENEGADA en {cp_id}: {reason}", 'error')
            self.update_current_service_display()
            self.update_history_display()
            self.schedule_next_service()
        
        elif event_type == 'CHARGING_DATA':
            driver_id = event.get('driver_id')
            if driver_id == self.driver_id:
                self.handle_charging_data(event)
        
        elif event_type == 'CHARGING_FINISHED':
            print(f"[DEBUG DRIVER] 🎫 CHARGING_FINISHED DETECTADO!")
            print(f"[DEBUG DRIVER] driver_id en evento: {event.get('driver_id')}")
            print(f"[DEBUG DRIVER] MI driver_id: {self.driver_id}")
            print(f"[DEBUG DRIVER] interrupted: {event.get('interrupted', False)}")
            driver_id = event.get('driver_id')
            if driver_id == self.driver_id:
                print(f"[DEBUG DRIVER] ✅ Es para MÍ! Llamando a handle_charging_finished()")
                self.handle_charging_finished(event)
        
        elif event_type == 'CHARGING_INTERRUPTED':
            cp_id = event.get('cp_id')
            driver_id = event.get('driver_id')
            reason = event.get('reason', 'Fallo del CP')
            
            if driver_id != self.driver_id:
                return
                
            with self.service_lock:
                if self.current_service and self.current_service.cp_id == cp_id:
                    self.current_service.status = ServiceStatus.FAILED
                    self.current_service.error_message = reason
                    self.current_service.end_time = time.time()
                    
                    consumed = self.current_service.total_consumed
                    cost = self.current_service.total_cost
                    
                    self.service_history.append(self.current_service)
                    self.current_service = None
            
            self.log_message(f"⚠ SUMINISTRO INTERRUMPIDO en {cp_id}", 'warning')
            self.log_message(f"Razón: {reason}", 'warning')
            if consumed > 0:
                self.log_message(f"Consumo parcial: {consumed:.3f} KWh - {cost:.2f} EUR", 'warning')
            
            self.update_current_service_display()
            self.update_history_display()
            self.schedule_next_service()

    def handle_charging_data(self, event):
        """Manejar datos de carga en tiempo real"""
        cp_id = event.get('cp_id')
        driver_id = event.get('driver_id')

        # Verificar que el mensaje es para este driver
        if driver_id != self.driver_id:
            return
        
        with self.service_lock:

            if not self.current_service:
                self.current_service = ChargingService(
                    cp_id=cp_id,
                    request_time=time.time(),
                    status=ServiceStatus.CHARGING,
                    start_time=time.time()
                )
                self.log_message(f"🔄 Reconectado - Continuando suministro en {cp_id}", 'success')

            if self.current_service and self.current_service.cp_id == cp_id:
                # Si aún no está en estado CHARGING, actualizar el estado
                if self.current_service.status != ServiceStatus.CHARGING:
                    self.current_service.status = ServiceStatus.CHARGING
                    self.current_service.start_time = time.time()
                    self.log_message(f"⚡ INICIANDO suministro en {cp_id}", 'success')
                
                # Actualizar datos en tiempo real
                current_kw = event.get('current_consumption', 0)
                total_kwh = event.get('total_consumed', 0)
                total_eur = event.get('total_cost', 0)

                self.current_service.current_consumption = current_kw
                self.current_service.total_consumed = total_kwh
                self.current_service.total_cost = total_eur

                self.last_data_timestamp = time.time()
        
        self.update_current_service_display()
        
    def check_charging_timeout(self):
        """Verificar timeout de datos de carga"""
        while self.running:
            try:
                time.sleep(1)  # Verificar cada segundo
                
                with self.service_lock:
                    # Solo verificar si hay suministro activo
                    if (self.current_service and 
                    self.current_service.status == ServiceStatus.CHARGING):
                        
                        if not self.last_data_timestamp:
                            self.last_data_timestamp = time.time()
                            continue
                        
                        elapsed = time.time() - self.last_data_timestamp
                        
                        # Si pasaron más de 5 segundos sin datos
                        if elapsed > self.CHARGING_TIMEOUT:
                            cp_id = self.current_service.cp_id
                            consumed = self.current_service.total_consumed
                            cost = self.current_service.total_cost
                            
                            self.log_message("", 'error')
                            self.log_message("="*60, 'error')
                            self.log_message("⚠️ TIMEOUT - ENGINE NO RESPONDE", 'error')
                            self.log_message("="*60, 'error')
                            self.log_message(f"CP: {cp_id}", 'error')
                            self.log_message(f"Sin datos durante {int(elapsed)} segundos", 'error')
                            self.log_message(f"Consumo hasta ahora: {consumed:.3f} KWh", 'warning')
                            self.log_message(f"Coste acumulado: {cost:.2f} EUR", 'warning')
                            self.log_message("="*60, 'error')
                            self.log_message("", 'error')
                            
                            # Marcar servicio como fallido
                            self.current_service.status = ServiceStatus.FAILED
                            self.current_service.error_message = f"Engine timeout ({int(elapsed)}s sin datos)"
                            self.current_service.end_time = time.time()
                            
                            # Guardar en historial
                            self.service_history.append(self.current_service)
                            self.current_service = None
                            
                            # Resetear timestamp
                            self.last_data_timestamp = None
                            
                            should_schedule = True
                        else:
                            should_schedule = False
                    else:
                        should_schedule = False
                
                # Fuera del lock
                if should_schedule:
                    self.update_current_service_display()
                    self.update_history_display()
                    self.schedule_next_service()
            except Exception as e:
                print(f"[TIMEOUT THREAD ERROR] {e}")  # Log silencioso
                time.sleep(2)  # Esperar un poco antes de reintentar

    def handle_charging_finished(self, event):
        """Manejar finalización"""
        cp_id = event.get('cp_id')
        driver_id = event.get('driver_id')
        interrupted = event.get('interrupted', False)

        print(f"[DEBUG DRIVER] cp_id: {cp_id}")
        print(f"[DEBUG DRIVER] driver_id: {driver_id}")
        print(f"[DEBUG DRIVER] interrupted: {interrupted}")
        print(f"[DEBUG DRIVER] MI driver_id: {self.driver_id}")

        if driver_id != self.driver_id:
            return

        # ✅ SI ES RECUPERACIÓN, IGNORAR FLAG ANTI-DUPLICADO
        if interrupted:
            print(f"[DEBUG DRIVER] ✅ Es recuperación, procesando sin validaciones")
            
            with self.service_lock:
                # Si no hay sesión actual, crear una temporal con los datos recuperados
                if not self.current_service:
                    print(f"[DEBUG DRIVER] ✅ Creando servicio temporal para recuperación")
                    self.current_service = ChargingService(
                        cp_id=cp_id,
                        request_time=time.time() - event.get('duration', 0),
                        status=ServiceStatus.COMPLETED,
                        start_time=time.time() - event.get('duration', 0)
                    )
                    self.log_message(f"📦 Recibiendo ticket de sesión recuperada de {cp_id}", 'success')
                
                # Actualizar con datos finales
                self.current_service.total_consumed = event.get('total_consumed', 0)
                self.current_service.total_cost = event.get('total_cost', 0)
                self.current_service.end_time = time.time()
                self.current_service.status = ServiceStatus.COMPLETED
                
                # Guardar en historial
                self.service_history.append(self.current_service)
                self.current_service = None
            
            # Mostrar ticket SIN validar flag
            self.root.after(500, lambda: self.show_ticket_modal(event))
            
            self.log_message("🎫" + "="*50 + "🎫", 'highlight')
            self.log_message("     TICKET DE SESIÓN RECUPERADA GENERADO", 'success')
            self.log_message("🎫" + "="*50 + "🎫", 'highlight')
            self.log_message(f"📊 Total consumido: {event.get('total_consumed', 0):.3f} KWh", 'success')
            self.log_message(f"💰 Total a pagar: {event.get('total_cost', 0):.2f} EUR", 'success')

            self.root.after(1000, self.reset_service_state)
            
            self.update_current_service_display()
            self.update_history_display()
            return

        # ✅ FLUJO NORMAL (no interrumpido)
        has_flag = hasattr(self, '_ticket_modal_open')
        flag_value = getattr(self, '_ticket_modal_open', False) if has_flag else False

        print(f"[DEBUG DRIVER] _ticket_modal_open existe: {has_flag}")
        print(f"[DEBUG DRIVER] _ticket_modal_open valor: {flag_value}")

        if has_flag and flag_value:
            print(f"[DEBUG DRIVER] ❌ BLOQUEADO por flag anti-duplicado (carga normal)")
            self.log_message("⚠️ Ticket ya mostrado, ignorando duplicado", 'warning')
            return
        
        with self.service_lock:
            print(f"[DEBUG DRIVER] Lock adquirido")
            print(f"[DEBUG DRIVER] current_service existe: {self.current_service is not None}")

            if not self.current_service:
                self.log_message(f"⚠️ No hay servicio activo para finalizar", 'warning')
                return

            if self.current_service and self.current_service.cp_id == cp_id:
                final_consumed = event.get('total_consumed', self.current_service.total_consumed)
                final_cost = event.get('total_cost', self.current_service.total_cost)

                self.current_service.status = ServiceStatus.COMPLETED
                self.current_service.end_time = time.time()
                self.current_service.total_consumed = final_consumed
                self.current_service.total_cost = final_cost
                self.current_service.current_consumption = 0.0 
                
                kwh = self.current_service.total_consumed
                cost = self.current_service.total_cost

                if self.current_service.start_time and self.current_service.end_time:
                    duration = self.current_service.end_time - self.current_service.start_time
                else:
                    duration = event.get('duration', 0)
                
                event['duration'] = duration
                
                self.service_history.append(self.current_service)
                self.current_service = None

        print(f"[DEBUG DRIVER] Activando flag _ticket_modal_open = True")
        self._ticket_modal_open = True
        
        self.root.after(500, lambda: self.show_ticket_modal(event))

        self.log_message("🎫" + "="*50 + "🎫", 'highlight')
        self.log_message("           TICKET DE CARGA GENERADO", 'success')
        self.log_message("🎫" + "="*50 + "🎫", 'highlight')
        self.log_message(f"📊 Total consumido: {kwh:.3f} KWh", 'success')
        self.log_message(f"💰 Total a pagar: {cost:.2f} EUR", 'success')
        self.log_message(f"⏱ Duración: {int(duration)}s", 'success')

        self.root.after(1000, self.reset_service_state)
        
        self.update_current_service_display()
        self.update_history_display()

    def handle_cp_status(self, event):
        """Actualizar estado de CPs"""
        cp_id = event.get('cp_id')
        new_state = event.get('new_state')
        
        # Actualizar lista
        for cp in self.available_cps:
            if cp.get('cp_id') == cp_id:
                old_state = cp.get('status', 'UNKNOWN')
                cp['status'] = new_state
                
                if old_state != new_state:
                    self.log_message(f"🔄 CP {cp_id}: {old_state} → {new_state}", 'info')
                break
        
        self.update_available_cps_display()
        
        # Si falló durante servicio
        if new_state == 'BROKEN':
            with self.service_lock:
                if self.current_service and self.current_service.cp_id == cp_id:
                    if self.current_service.status in [ServiceStatus.PENDING, 
                                                       ServiceStatus.AUTHORIZED, 
                                                       ServiceStatus.CHARGING]:
                        self.current_service.status = ServiceStatus.FAILED
                        self.current_service.error_message = "CP falló (detectado por estado)"
                        self.current_service.end_time = time.time()
                        
                        consumed = self.current_service.total_consumed
                        cost = self.current_service.total_cost
                        
                        self.log_message(f"⚠ CP {cp_id} FALLÓ durante el servicio", 'error')
                        if consumed > 0:
                            self.log_message(f"Consumo parcial: {consumed:.3f} KWh - {cost:.2f} EUR", 'warning')
                        
                        self.service_history.append(self.current_service)
                        self.current_service = None
                        should_schedule = True
                    else:
                        should_schedule = False
                else:
                    should_schedule = False
            
            if should_schedule:
                self.update_current_service_display()
                self.update_history_display()
                self.schedule_next_service()

    def schedule_next_service(self):
        """Programar siguiente servicio con mejor manejo"""
        self.log_message("🔍 PROGRAMANDO siguiente servicio...", 'info')
        
        try:
            with self.service_lock:
                has_queue = len(self.service_queue) > 0
                has_current = self.current_service is not None
            
            self.log_message(f"🔍 Estado para programar - Cola: {has_queue}, Actual: {has_current}", 'info')
            
            if has_queue and not has_current:
                self.log_message("⏳ Programando siguiente servicio en 4 segundos...", 'info')
                # Usar after de Tkinter en lugar de threading.Timer para mejor integración
                self.root.after(4000, self.process_next_service)
            elif has_current:
                self.log_message("⚠ Servicio actual en curso, no programando siguiente", 'warning')
            else:
                self.log_message("🏁 No hay más servicios en cola", 'info')
                
        except Exception as e:
            self.log_message(f"✗ ERROR en schedule_next_service: {e}", 'error')

    def update_central_status_display(self):
        """⚡ Actualizar el indicador visual del estado de la central"""
        if self.central_is_down:
            # Central caída - Rojo
            self.central_status_label.config(
                text="🔴 CENTRAL: NO DISPONIBLE",
                fg='#e74c3c'  # Rojo
            )
        else:
            # Central operativa - Verde
            self.central_status_label.config(
                text="🟢 CENTRAL: Operativa",
                fg='#2ecc71'  # Verde
            )
    def clear_queue(self):
        """Limpiar la cola de servicios"""
        with self.service_lock:
            count = len(self.service_queue)
            self.service_queue.clear()
        
        self.log_message(f"🗑️ Cola limpiada - {count} servicios eliminados", 'warning')
        self.update_queue_display()

    def process_next_service(self):
        """Procesar siguiente servicio con mejor manejo de errores"""
        self.log_message("🔍 PROCESANDO siguiente servicio...", 'info')
        
        try:
            # Verificar condiciones antes de continuar
            with self.service_lock:
                if not self.service_queue:
                    self.log_message("ℹ️ Cola vacía, no hay más servicios", 'info')
                    return
                
                if self.current_service:
                    self.log_message("⚠ Servicio en curso, reintentando en 5 segundos...", 'warning')
                    # Reintentar después de 5 segundos
                    self.root.after(5000, self.process_next_service)
                    return
                
                # Obtener siguiente CP de la cola
                next_cp_id = self.service_queue.pop(0)
                remaining = len(self.service_queue)
            
            self.log_message(f"▶ Procesando: {next_cp_id}", 'highlight')
            self.log_message(f"📋 Servicios restantes: {remaining}", 'info')
            
            # Actualizar display de cola
            self.update_queue_display()
            
            # Intentar solicitar servicio
            success = self.request_charging_service(next_cp_id)
            
            if success:
                self.log_message(f"✅ Solicitud enviada para {next_cp_id}", 'success')
            else:
                self.log_message(f"❌ Falló solicitud para {next_cp_id}", 'error')
                # Reintentar con siguiente servicio después de delay
                if remaining > 0:
                    self.log_message("⏳ Reintentando siguiente servicio en 3 segundos...", 'info')
                    self.root.after(3000, self.process_next_service)
                else:
                    self.log_message("🏁 No hay más servicios en cola", 'info')
                    
        except Exception as e:
            self.log_message(f"✗ ERROR CRÍTICO en process_next_service: {e}", 'error')
            import traceback
            self.log_message(f"Traceback: {traceback.format_exc()}", 'error')
            
            # Intentar continuar con siguiente servicio si hay error
            with self.service_lock:
                remaining_after_error = len(self.service_queue)
            
            if remaining_after_error > 0:
                self.log_message("🔄 Reintentando después de error en 5 segundos...", 'warning')
                self.root.after(5000, self.process_next_service)

    def validate_cps_in_queue(self):
        """Validar que los CPs en la cola existen en la lista de disponibles"""
        with self.service_lock:
            valid_cps = []
            invalid_cps = []
            
            available_cp_ids = [cp.get('cp_id') for cp in self.available_cps]
            
            for cp_id in self.service_queue:
                if cp_id in available_cp_ids:
                    valid_cps.append(cp_id)
                else:
                    invalid_cps.append(cp_id)
            
            return valid_cps, invalid_cps

    def request_charging_service(self, cp_id: str):
        """Solicitar servicio"""
        with self.service_lock:
            if self.current_service:
                self.log_message("✗ Ya hay un servicio en curso", 'error')
                return False

            cp_info = None
            for cp in self.available_cps:
                if cp.get('cp_id') == cp_id:
                    cp_info = cp
                    break
            
            if not cp_info:
                self.log_message(f"✗ CP {cp_id} no encontrado en la lista", 'error')
                return False

            cp_status = cp_info.get('status', 'UNKNOWN')
            
            # Estados que NO permiten solicitar servicio
            if cp_status == 'BROKEN':
                self.log_message(f"✗ CP {cp_id} está AVERIADO - No se puede usar", 'error')
                self.log_message("   Por favor, selecciona otro punto de recarga", 'warning')
                return False
            
            elif cp_status == 'OUT_OF_ORDER':
                self.log_message(f"✗ CP {cp_id} está FUERA DE SERVICIO - No se puede usar", 'error')
                self.log_message("   Por favor, selecciona otro punto de recarga", 'warning')
                return False
            
            elif cp_status in ['DISCONNECTED', 'DESCONECTADO']:
                self.log_message(f"✗ CP {cp_id} está DESCONECTADO - No se puede usar", 'error')
                self.log_message("   Por favor, selecciona otro punto de recarga", 'warning')
                return False
            
            elif cp_status == 'CHARGING':
                self.log_message(f"⚠️ CP {cp_id} está ocupado (CARGANDO otro vehículo)", 'warning')
                self.log_message("   Por favor, espera o selecciona otro punto de recarga", 'warning')
                return False
            
            elif cp_status not in ['AVAILABLE', 'ACTIVADO']:
                self.log_message(f"✗ CP {cp_id} tiene estado desconocido: {cp_status}", 'error')
                self.log_message("   Por favor, selecciona otro punto de recarga", 'warning')
                return False
            elif self.central_is_down:
                self.log_message(f"✗ Central caida", 'error')
                return False
            
            # ✅ El CP está AVAILABLE o ACTIVADO - OK para solicitar
            self.log_message(f"✓ CP {cp_id} disponible - Procediendo con solicitud", 'info')
            
            self.current_service = ChargingService(
                cp_id=cp_id,
                request_time=time.time()
            )

            self.last_data_timestamp = None
        
        request_msg = {
            "type": "REQUEST_CHARGING",
            "driver_id": self.driver_id,
            "cp_id": cp_id,
            "timestamp": time.time()
        }
        
        self.kafka_producer.send('driver_requests', request_msg)
        self.kafka_producer.flush()
        
        self.log_message(f"📤 Solicitando servicio en {cp_id}...", 'info')
        self.update_current_service_display()
        return True
    
    def reset_service_state(self):
        """Resetear completamente el estado del servicio"""
        with self.service_lock:
            # Limpiar servicio actual
            self.current_service = None
            # Resetear timestamp de datos
            self.last_data_timestamp = None
            # Resetear flag del modal
            self._ticket_modal_open = False
        
        self.log_message("🔄 Estado del servicio reseteado", 'info')

    def request_selected_cp(self):
        """Solicitar servicio en CP seleccionado"""
        selection = self.cps_tree.selection()
        if not selection:
            messagebox.showwarning("Sin selección", 
                                  "Por favor, selecciona un Punto de Recarga")
            return
        
        if self.central_is_down:
            messagebox.showerror("Error", f"Central Desconectada")
        else:
            item = self.cps_tree.item(selection[0])
            cp_id = item['values'][0]
        
            self.request_charging_service(cp_id)

    def check_kafka_connection(self):
        """Verificar si Kafka está conectado"""
        if not self.kafka_producer:
            self.log_message("✗ Kafka producer no inicializado", 'error')
            return False
        
        try:
            # Verificar que podemos enviar mensajes
            future = self.kafka_producer.send('driver_requests', {
                "type": "TEST_CONNECTION", 
                "driver_id": self.driver_id,
                "timestamp": time.time()
            })
            future.get(timeout=5)  # Esperar confirmación
            self.log_message("✅ Conexión Kafka verificada", 'success')
            return True
        except Exception as e:
            self.log_message(f"✗ Error verificando Kafka: {e}", 'error')
            return False    

    def load_file(self):
        """Cargar archivo"""
        filename = filedialog.askopenfilename(
            title="Seleccionar archivo de servicios",
            filetypes=[("Archivos de texto", "*.txt"), ("Todos los archivos", "*.*")]
        )
        
        if not filename:
            return
        
        try:
            with open(filename, 'r') as f:
                services = [line.strip() for line in f if line.strip()]
            
            with self.service_lock:
                self.service_queue = services
            
            self.log_message(f"✓ Cargados {len(services)} servicios", 'success')

            valid_cps, invalid_cps = self.validate_cps_in_queue()
            
            for i, cp_id in enumerate(services, 1):
                status = "✓" if cp_id in valid_cps else "✗"
                self.log_message(f"  {i}. {cp_id} {status}", 'info' if cp_id in valid_cps else 'warning')

            if invalid_cps:
                self.log_message(f"⚠️ Advertencia: {len(invalid_cps)} CPs no están disponibles", 'warning')
                self.log_message(f"   CPs inválidos: {', '.join(invalid_cps)}", 'warning')
                
            self.update_queue_display()

            message_text = f"Se cargaron {len(services)} servicios."
            if invalid_cps:
                message_text += f"\n\nAdvertencia: {len(invalid_cps)} CPs no están disponibles."
                message_text += f"\nCPs inválidos: {', '.join(invalid_cps)}"
            
            messagebox.showinfo("Archivo cargado", message_text)
            
        except Exception as e:
            self.log_message(f"✗ Error al cargar archivo: {e}", 'error')
            messagebox.showerror("Error", f"No se pudo cargar el archivo:\n{e}")

    def start_automated(self):
        """Iniciar servicios automáticos cargando archivo del driver automáticamente"""
        self.log_message("🔍 INICIANDO servicios automáticos...", 'highlight')

        # ----------------------------------------------------------
        # 1) CARGA AUTOMÁTICA DEL ARCHIVO services/<driver>_services.txt
        # ----------------------------------------------------------
        if not self.load_auto_services_file():
            messagebox.showerror(
                "Error",
                f"No se encontró el archivo automático:\n"
                f"services/{self.driver_id}_services.txt"
            )
            return

        # ----------------------------------------------------------
        # 2) VALIDACIONES PREVIAS
        # ----------------------------------------------------------
        try:
            with self.service_lock:
                queue_empty = len(self.service_queue) == 0
                has_current = self.current_service is not None

            self.log_message(
                f"🔍 Estado - Cola vacía: {queue_empty}, Servicio actual: {has_current}",
                'info'
            )

            if queue_empty:
                messagebox.showwarning(
                    "Cola vacía",
                    "El archivo automático no contiene servicios válidos."
                )
                return

            if has_current:
                messagebox.showwarning(
                    "Servicio en curso",
                    "Ya hay un servicio activo.\n"
                    "Los automáticos continuarán después."
                )
                return

            # Verificar conexión Kafka
            if not self.check_kafka_connection():
                messagebox.showerror(
                    "Error de Conexión",
                    "No hay conexión con Kafka. Verifica que el servidor esté funcionando."
                )
                return

            # Validar CPs
            valid_cps, invalid_cps = self.validate_cps_in_queue()
            if invalid_cps:
                self.log_message(
                    f"⚠️ Advertencia: {len(invalid_cps)} CPs no disponibles:",
                    'warning'
                )
                for cp_id in invalid_cps:
                    self.log_message(f"   ✗ {cp_id} no está disponible", 'warning')

                if not messagebox.askyesno(
                    "CPs no disponibles",
                    f"{len(invalid_cps)} CPs no están disponibles.\n"
                    f"CPs inválidos: {', '.join(invalid_cps)}\n\n"
                    "¿Deseas continuar con los válidos?"
                ):
                    return

            self.log_message(f"✅ {len(valid_cps)} CPs válidos encontrados", 'success')
            self.log_message("▶ Iniciando servicios automáticos...", 'highlight')

            # ----------------------------------------------------------
            # 3) INICIO DEL PROCESO AUTOMÁTICO
            # ----------------------------------------------------------
            self.root.after(100, self.process_next_service)

        except Exception as e:
            self.log_message(f"✗ ERROR en start_automated: {e}", 'error')
            import traceback
            self.log_message(f"Traceback: {traceback.format_exc()}", 'error')


    def refresh_cps(self):
        """Solicitar actualización de CPs a la central"""
        if not self.kafka_producer:
            self.log_message("⚠️ Kafka no conectado, no se pueden actualizar CPs", 'warning')
            return False
        
        try:
            self.log_message("🔄 Solicitando actualización de CPs...", 'info')
            
            register_msg = {
                "type": "REGISTER_DRIVER",
                "driver_id": self.driver_id,
                "timestamp": time.time()
            }
            
            self.kafka_producer.send('driver_requests', register_msg)
            self.kafka_producer.flush(timeout=5)
            
            self.log_message("✓ Solicitud de CPs enviada a CENTRAL", 'success')
            return True
            
        except Exception as e:
            self.log_message(f"✗ Error solicitando CPs: {e}", 'error')
            return False

    def periodic_update(self):
        """Actualización periódica"""
        if not self.running:
            return
        
        # Actualizar display si está cargando
        with self.service_lock:
            if self.current_service and self.current_service.status == ServiceStatus.CHARGING:
                self.update_current_service_display()
        
        # Siguiente actualización
        self.root.after(500, self.periodic_update)

    def on_closing(self):
        """Manejo del cierre"""
        if messagebox.askokcancel("Salir", "¿Deseas cerrar la aplicación?"):
            self.cleanup()
            self.root.destroy()

    def cleanup(self):
        """Limpieza"""
        self.running = False

        if self.timeout_check_thread:
            self.timeout_check_thread.join(timeout=2)

        if self.kafka_producer:
            self.kafka_producer.close()
        if self.kafka_consumer:
            self.kafka_consumer.close()
        self.log_message("👋 Driver App cerrado", 'info')

    def show_ticket_modal(self, event):
        """Mostrar ticket final en modal destacado"""
        interrupted = event.get('interrupted', False)
        reason = event.get('reason', '')

        ticket_window = tk.Toplevel(self.root)

        print(f"[DEBUG DRIVER] interrupted: {interrupted}")
        print(f"[DEBUG DRIVER] reason: {reason}")
        print(f"[DEBUG DRIVER] cp_id: {event.get('cp_id')}")
        print(f"[DEBUG DRIVER] total_consumed: {event.get('total_consumed')}")
        print(f"[DEBUG DRIVER] total_cost: {event.get('total_cost')}")

        if interrupted:
            ticket_window.title("🎫 TICKET DE CARGA - SESIÓN RECUPERADA")
            header_text = "🎫 RECIBO DE SESIÓN RECUPERADA"
            header_bg = '#e67e22'  # Naranja para recuperación
        else:
            ticket_window.title("🎫 TICKET DE CARGA - RECIBO FINAL")
            header_text = "🎫 RECIBO DE CARGA COMPLETADA"
            header_bg = '#34495e'  # Gris normal

        ticket_window.geometry("500x650")
        ticket_window.configure(bg='#2c3e50')
        ticket_window.transient(self.root)
        ticket_window.grab_set()
        
        # Centrar ventana
        ticket_window.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() - 500) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - 600) // 2
        ticket_window.geometry(f"+{x}+{y}")
        
        # Header del ticket
        header = tk.Frame(ticket_window, bg='#34495e', height=80)
        header.pack(fill=tk.X, pady=(0, 20))
        
        tk.Label(header, 
                text=header_text,
                bg=header_bg, fg='white',
                font=('Arial', 16, 'bold')).pack(pady=20)
        
        # Contenido del ticket
        content = tk.Frame(ticket_window, bg='#ecf0f1', padx=20, pady=20)
        content.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        if interrupted:
            warning_frame = tk.Frame(content, bg='#ffe6cc', relief=tk.RAISED, borderwidth=2)
            warning_frame.pack(fill=tk.X, pady=(0, 15))
            
            tk.Label(warning_frame, 
                    text="⚠️ SESIÓN INTERRUMPIDA",
                    bg='#ffe6cc', fg='#e67e22',
                    font=('Arial', 12, 'bold')).pack(pady=5)
            
            tk.Label(warning_frame,
                    text=reason,
                    bg='#ffe6cc', fg='#666',
                    font=('Arial', 9),
                    wraplength=400).pack(pady=(0, 5))
        
        # Información de la carga
        info_labels = [
            ("Conductor:", self.driver_id),
            ("Punto de Recarga:", event.get('cp_id', 'N/A')),
            ("Fecha:", datetime.now().strftime('%d/%m/%Y %H:%M:%S')),
            ("", ""),  # Separador
            ("⚡ ENERGÍA CONSUMIDA:", f"{event.get('total_consumed', 0):.3f} KWh"),
            ("💰 IMPORTE TOTAL:", f"{event.get('total_cost', 0):.2f} €"),
            ("⏱ DURACIÓN:", f"{event.get('duration', 0):.0f} segundos")
        ]
        
        for label, value in info_labels:
            if label == "":  # Separador
                ttk.Separator(content, orient='horizontal').pack(fill=tk.X, pady=10)
                continue
                
            row = tk.Frame(content, bg='#ecf0f1')
            row.pack(fill=tk.X, pady=5)
            
            tk.Label(row, text=label, bg='#ecf0f1', 
                    font=('Arial', 12, 'bold'), width=20, anchor='w').pack(side=tk.LEFT)
            tk.Label(row, text=value, bg='#ecf0f1',
                    font=('Arial', 12), fg='#2c3e50').pack(side=tk.LEFT)
        
        # Separador final
        ttk.Separator(content, orient='horizontal').pack(fill=tk.X, pady=20)
        
        # Mensaje de agradecimiento
        tk.Label(content, text="¡Gracias por usar nuestro servicio!",
                bg='#ecf0f1', fg='#27ae60',
                font=('Arial', 14, 'bold')).pack(pady=10)

        def close_modal():
            self._ticket_modal_open = False
            ticket_window.destroy()
            self.reset_service_state()
            self.schedule_next_service()
        
        # Botón de cierre
        btn_frame = tk.Frame(content, bg='#ecf0f1')
        btn_frame.pack(fill=tk.X, pady=20)
        
        tk.Button(btn_frame, text="✅ CERRAR TICKET",
                command=close_modal,
                bg='#27ae60', fg='white',
                font=('Arial', 12, 'bold'),
                padx=20, pady=10).pack()
        
        ticket_window.protocol("WM_DELETE_WINDOW", close_modal)
        
        # Sonido de notificación
        self.root.bell()


    def start_periodic_cp_updates(self):
        """Iniciar actualizaciones automáticas cada 3 segundos"""
        def update_loop():
            # Esperar un poco antes de empezar
            time.sleep(2)
            
            while self.running:
                try:
                    # Solicitar actualización silenciosa
                    if self.kafka_producer:
                        register_msg = {
                            "type": "REGISTER_DRIVER",
                            "driver_id": self.driver_id,
                            "timestamp": time.time(),
                            "silent_update": True  # Para no spammear logs
                        }
                        self.kafka_producer.send('driver_requests', register_msg)
                        self.kafka_producer.flush(timeout=2)
                        
                    # Esperar 3 segundos entre actualizaciones
                    time.sleep(3)
                        
                except Exception as e:
                    # Error silencioso para no spammear
                    if self.running:
                        self.log_message(f"⚠️ Error en actualización automática: {e}", 'warning')
                    time.sleep(5)  # Esperar más en caso de error
        
        # Iniciar thread en segundo plano
        update_thread = threading.Thread(target=update_loop, daemon=True)
        update_thread.start()
        self.log_message("🔄 Iniciando actualizaciones automáticas de CPs (cada 3s)", 'info')

    def detect_cp_changes(self, new_cps):
        """Detectar cambios en los estados de los CPs"""
        changed_cps = []
        
        # Si no hay CPs anteriores, no hay cambios que detectar
        if not self.available_cps:
            return changed_cps
        
        # Crear diccionario de CPs actuales para comparación rápida
        current_cp_states = {}
        for cp in self.available_cps:
            cp_id = cp.get('cp_id')
            if cp_id:
                current_cp_states[cp_id] = cp.get('status', 'UNKNOWN')
        
        # Comparar con nuevos CPs
        for new_cp in new_cps:
            cp_id = new_cp.get('cp_id')
            new_status = new_cp.get('status', 'UNKNOWN')
            
            if cp_id in current_cp_states:
                old_status = current_cp_states[cp_id]
                if old_status != new_status:
                    changed_cps.append((cp_id, old_status, new_status))
            else:
                # CP nuevo
                changed_cps.append((cp_id, 'NUEVO', new_status))
        
        return changed_cps

def main():
    if len(sys.argv) < 4:
        print("Uso: python EV_Driver_GUI.py <driver_id> <kafka_host> <kafka_port>")
        print("Ejemplo: python EV_Driver_GUI.py DRV001 localhost 9092")
        return
    
    driver_id = sys.argv[1]
    kafka_host = sys.argv[2]
    kafka_port = int(sys.argv[3])
    
    driver = DriverGUI(driver_id, kafka_host, kafka_port)
    driver.start()

if __name__ == "__main__":
    main()