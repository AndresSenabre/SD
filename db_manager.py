# db_manager.py
import sqlite3
from datetime import datetime

class DBManager:
    def __init__(self, db_name):
        self.db_name = db_name
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Tabla de Charging Points
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS charging_points (
                cp_id TEXT PRIMARY KEY,
                ubicacion TEXT,
                estado TEXT DEFAULT 'DESCONECTADO',
                precio REAL DEFAULT 0.45
            )
        ''')
        
        # Tabla de Conductores
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS drivers (
                driver_id TEXT PRIMARY KEY
            )
        ''')
        
        # Tabla de Sesiones
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id TEXT,
                cp_id TEXT,
                start_time TEXT,
                end_time TEXT,
                energia REAL,
                coste REAL,
                FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
                FOREIGN KEY (cp_id) REFERENCES charging_points(cp_id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def add_cp(self, cp_id, ubicacion):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Asegurar que cp_id es string
        if isinstance(cp_id, int):
            cp_id = f"CP{cp_id:03d}"
        
        cursor.execute('''
            INSERT OR REPLACE INTO charging_points (cp_id, ubicacion, estado)
            VALUES (?, ?, 'DESCONECTADO')
        ''', (cp_id, ubicacion))
        conn.commit()
        conn.close()
    
    def get_cp(self, cp_id):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Asegurar que cp_id es string
        if isinstance(cp_id, int):
            cp_id = f"CP{cp_id:03d}"
        
        cursor.execute('SELECT * FROM charging_points WHERE cp_id = ?', (cp_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'cp_id': row[0],
                'ubicacion': row[1],
                'estado': row[2],
                'precio': row[3]
            }
        return None
    
    def set_cp_state(self, cp_id, estado):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Asegurar que cp_id es string
        if isinstance(cp_id, int):
            cp_id = f"CP{cp_id:03d}"
        
        cursor.execute('''
            UPDATE charging_points SET estado = ? WHERE cp_id = ?
        ''', (estado, cp_id))
        conn.commit()
        conn.close()
    
    def add_driver(self, driver_id):
        """Añadir driver"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Asegurar que es string simple
        driver_id = str(driver_id)
        
        # Verificar que el tuple tiene solo 1 elemento
        params = (driver_id,)
        
        cursor.execute(
            'INSERT OR IGNORE INTO drivers (driver_id) VALUES (?)',
            params
        )
        
        conn.commit()
        conn.close()
    
    def start_session(self, driver_id, cp_id):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        
        # Asegurar formato string
        if isinstance(driver_id, int):
            driver_id = f"DRIV{driver_id:03d}"
        if isinstance(cp_id, int):
            cp_id = f"CP{cp_id:03d}"
        
        cursor.execute('''
            INSERT INTO sessions (driver_id, cp_id, start_time)
            VALUES (?, ?, ?)
        ''', (driver_id, cp_id, datetime.now().isoformat()))
        session_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return session_id
    
    def end_session(self, session_id, energia, coste):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE sessions 
            SET end_time = ?, energia = ?, coste = ?
            WHERE session_id = ?
        ''', (datetime.now().isoformat(), energia, coste, session_id))
        conn.commit()
        conn.close()

    def get_all_cps(self):
        """Obtener todos los CPs registrados"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM charging_points')
        rows = cursor.fetchall()
        conn.close()
        
        cps = []
        for row in rows:
            cps.append({
                'cp_id': row[0],  # Ya es string
                'location': row[1],
                'status': row[2],
                'price_per_kwh': row[3]
            })
        return cps
    
    def get_active_sessions(self):
        """Obtener sesiones activas"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT driver_id, cp_id, start_time
            FROM sessions
            WHERE end_time IS NULL
        """)
        
        sessions = []
        for row in cursor.fetchall():
            sessions.append({
                'driver_id': row[0],
                'cp_id': row[1],
                'start_time': row[2]
            })
        
        return sessions
    
    def get_session(self, session_id):
        """Obtener información completa de una sesión"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT session_id, driver_id, cp_id, start_time, end_time, 
                energia, coste
            FROM sessions
            WHERE session_id = ?
        """, (session_id,))
        
        row = cursor.fetchone()
        if row:
            return {
                'id': row[0],
                'driver_id': row[1],
                'cp_id': row[2],
                'start_time': row[3],
                'end_time': row[4],
                'total_consumed': row[5] or 0.0,
                'total_cost': row[6] or 0.0
            }
        return None
    
    def get_open_sessions(self):
        """Obtener sesiones que no tienen fecha de finalización"""
        conn = sqlite3.connect(self.db_name)
        cursor = conn.execute("""
            SELECT session_id, driver_id, cp_id, start_time, energia, coste
            FROM sessions 
            WHERE end_time IS NULL
        """)
        
        sessions = []
        for row in cursor.fetchall():
            sessions.append({
                'session_id': row[0],
                'driver_id': row[1],
                'cp_id': row[2],
                'start_time': row[3],
                'energia': row[4],
                'coste': row[5]
            })
        return sessions