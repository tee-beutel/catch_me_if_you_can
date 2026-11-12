import os
import sys
import subprocess
import json
import random
import uuid
import time
import math
import re
import shutil
import csv
import socket
import atexit
import zipfile
import urllib.request
import sqlite3
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, request, jsonify, redirect, send_file
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import psutil
import platform

# Lädt die Variablen aus der .env-Datei (falls lokal vorhanden)
load_dotenv()

try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz
    def ZoneInfo(tz_str):
        return pytz.timezone(tz_str)

app = Flask(__name__)

# --- FLASK TRAFFIC TRACKER ---
flask_traffic_rx = 0  # Empfangene Bytes
flask_traffic_tx = 0  # Gesendete Bytes
traffic_lock = threading.Lock()

@app.before_request
def count_incoming_traffic():
    global flask_traffic_rx
    req_size = (request.content_length or 0) + 300
    with traffic_lock:
        flask_traffic_rx += req_size

    if LOG_SETTINGS.get("requests", False):
        write_debug_log("requests", {
            "method": request.method,
            "path": request.path,
            "remote_addr": request.remote_addr
        })

@app.after_request
def count_outgoing_traffic(response):
    global flask_traffic_tx
    try:
        resp_size = len(response.get_data()) + 200
        with traffic_lock:
            flask_traffic_tx += resp_size
    except Exception:
        pass
    return response

# --- PFADE & KONSTANTEN ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STOPS_DIR = os.path.join(BASE_DIR, 'stops_data')
DATA_FILE = os.path.join(STOPS_DIR, 'Catch_Haltestellen.json')
DB_FILE = os.path.join(BASE_DIR, 'game.db')

# Migration beim Start
if not os.path.exists(STOPS_DIR):
    os.makedirs(STOPS_DIR, exist_ok=True)
    old_data_file = os.path.join(BASE_DIR, 'Catch_Haltestellen.json')
    if os.path.exists(old_data_file):
        shutil.copy2(old_data_file, os.path.join(STOPS_DIR, 'Catch_Haltestellen.json'))
else:
    dst_data_file = os.path.join(STOPS_DIR, 'Catch_Haltestellen.json')
    old_data_file = os.path.join(BASE_DIR, 'Catch_Haltestellen.json')
    if not os.path.exists(dst_data_file) and os.path.exists(old_data_file):
        shutil.copy2(old_data_file, dst_data_file)

# Alle Traccar-Dateien liegen strikt isoliert im Projektordner unter ./traccar_runtime
TRACCAR_RUNTIME_DIR = os.path.join(BASE_DIR, 'traccar_runtime')
TRACCAR_DATA_DIR = os.path.join(TRACCAR_RUNTIME_DIR, 'data')
TRACCAR_LOGS_DIR = os.path.join(TRACCAR_RUNTIME_DIR, 'logs')
TRACCAR_JAR_PATH = os.path.join(TRACCAR_RUNTIME_DIR, 'tracker-server.jar')
TRACCAR_XML_PATH = os.path.join(BASE_DIR, 'traccar.xml')
TRACCAR_VERSION = "5.12"

# --- ADMIN PASSWORT / PIN --- #
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "1234-5").strip()
# Damit die Adminseiten exakt über /adminconsole-{adminpin} erreichbar sind:
ADMIN_SLUG = ADMIN_PASSWORD

# --- STANDARD-KONFIGURATION ---
DEFAULT_CONFIG = {
    "num_teams": 4,
    "team_names": [],
    "first_broadcast_time": "",
    "broadcast_interval_minutes": 15,
    "points_caught_team": 1,
    "start_time": "",
    "end_time": "",
    "timezone": "Europe/Berlin",
    "enable_geofences": False,
    "telegram_enabled": bool(os.getenv("TELEGRAM_ENABLED", "false").lower() in ("true", "1", "yes")),
    "telegram_bot_token": os.getenv("TELEGRAM_TOKEN", ""),
    "telegram_chat_ids": os.getenv("TELEGRAM_CHAT_ID", "")
}

stops_data = {}
current_active_dataset = None
GEOFENCE_OUTER = []
GEOFENCE_EXCLUSION = []


def load_active_stops_data(filename=None):
    global stops_data, current_active_dataset, GEOFENCE_OUTER, GEOFENCE_EXCLUSION
    if not filename:
        try:
            with sqlite3.connect(DB_FILE, timeout=10) as conn:
                c = conn.cursor()
                c.execute("SELECT value FROM settings WHERE key = 'active_stops_dataset'")
                row = c.fetchone()
                if row and row[0]:
                    filename = row[0]
        except Exception:
            pass
    if not filename:
        filename = "Catch_Haltestellen.json"

    safe_filename = os.path.basename(filename)
    filepath = os.path.join(STOPS_DIR, safe_filename)
    if not os.path.exists(filepath):
        base_fallback = os.path.join(BASE_DIR, safe_filename)
        if os.path.exists(base_fallback):
            filepath = base_fallback
        elif os.path.exists(STOPS_DIR):
            available = [f for f in sorted(os.listdir(STOPS_DIR)) if f.endswith('.json')]
            if available:
                filepath = os.path.join(STOPS_DIR, available[0])
                safe_filename = available[0]

    loaded = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
        except Exception as e:
            app.logger.error(f"Fehler beim Laden von {filepath}: {e}")
            loaded = {}

    stops_data.clear()
    # Geofence-Daten auslesen (Keys mit _ am Anfang)
    GEOFENCE_OUTER = loaded.get("_geofence_outer", []) or []
    GEOFENCE_EXCLUSION = loaded.get("_geofence_exclusion", []) or []

    current_active_dataset = safe_filename
    return stops_data

# --- TRACCAR KONFIGURATION & SYNC ---
TRACCAR_BASE_URL = os.getenv("TRACCAR_BASE_URL", "http://127.0.0.1:8082/api")
TRACCAR_OSMAND_URL = os.getenv("TRACCAR_OSMAND_URL", "http://127.0.0.1:5055")
TRACCAR_API_URL = f"{TRACCAR_BASE_URL}/devices"

TRACCAR_USER = os.getenv("TRACCAR_API_USER", "admin").strip()
TRACCAR_PASS = os.getenv("TRACCAR_API_PASSWORD", "admin").strip()

# --- DEBUG LOGGING KONFIGURATION ---
LOG_SETTINGS = {
    "traccar_api": False,
    "gps_background_worker": False,
    "requests": False,
    "game_actions": False,
    "server_status": False
}
TRACCAR_LOG_FILE = os.path.join(BASE_DIR, 'log.json')
traccar_log_lock = threading.Lock()
_debug_logs_buffer = {}
_debug_logs_dirty = False
_debug_logs_lock = threading.Lock()


def _init_debug_logs():
    global _debug_logs_buffer
    with _debug_logs_lock:
        _debug_logs_buffer = {}
        if os.path.exists(TRACCAR_LOG_FILE):
            try:
                with open(TRACCAR_LOG_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    if isinstance(loaded, dict):
                        for cat, entries in loaded.items():
                            if isinstance(entries, list):
                                _debug_logs_buffer[cat] = deque(entries[-1000:], maxlen=1000)
            except Exception as e:
                app.logger.error(f"Fehler beim Laden von {TRACCAR_LOG_FILE}: {e}")


def flush_debug_logs():
    global _debug_logs_dirty
    with _debug_logs_lock:
        if not _debug_logs_dirty:
            return
        try:
            dump_dict = {cat: list(entries) for cat, entries in _debug_logs_buffer.items()}
            temp_file = TRACCAR_LOG_FILE + ".tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(dump_dict, f, indent=2, ensure_ascii=False)
            os.replace(temp_file, TRACCAR_LOG_FILE)
            _debug_logs_dirty = False
        except Exception as e:
            app.logger.error(f"Fehler beim Schreiben von {TRACCAR_LOG_FILE}: {e}")


def write_debug_log(category, payload):
    """Schreibt Debug-Logs nach Kategorien getrennt thread-sicher in den In-Memory-Puffer."""
    global _debug_logs_dirty
    if not LOG_SETTINGS.get(category, False):
        return

    with _debug_logs_lock:
        if category not in _debug_logs_buffer:
            _debug_logs_buffer[category] = deque(maxlen=1000)
        _debug_logs_buffer[category].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": payload
        })
        _debug_logs_dirty = True


def _debug_log_flush_worker():
    while True:
        time.sleep(5)
        flush_debug_logs()


_init_debug_logs()
threading.Thread(target=_debug_log_flush_worker, daemon=True).start()
atexit.register(flush_debug_logs)

def write_traccar_api_log(data):
    write_debug_log("traccar_api", data)

traccar_lock = threading.RLock()
install_lock = threading.Lock()
gps_rules_lock = threading.Lock()
traccar_session = requests.Session()
traccar_process = None
traccar_ready = False
traccar_last_error = "Initialisiere..."

# In-Memory Log für das Debug-Dashboard (/traccar_debug-<adminpin>)
recent_traccar_webhooks = deque(maxlen=25)
debug_test_events = {}

# Merkt sich pro Team die zuletzt im Admin-Log gemeldete GPS-Ausfall-Stufe (5, 10, 15, ...)
gps_inactivity_notified_stage = {}




# --- DATENBANK SETUP & MIGRATION ---
def ensure_column(cursor, table_name, column_name, column_type):
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_cols = [row[1] for row in cursor.fetchall()]
    if column_name not in existing_cols:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")



# --- IN-MEMORY RATE LIMITER FÜR LOGINS ---
class LoginRateLimiter:
    def __init__(self):
        self._lock = threading.Lock()
        self._failures = {}
        self._lockouts = {}

    def is_blocked(self, ip, target):
        now = time.time()
        with self._lock:
            lockout = self._lockouts.get((ip, target), 0)
            if now < lockout:
                return True
            if (ip, target) in self._lockouts:
                del self._lockouts[(ip, target)]
            fails = [t for t in self._failures.get((ip, target), []) if now - t <= 300]
            if fails:
                self._failures[(ip, target)] = fails
            else:
                self._failures.pop((ip, target), None)
            return False

    def record_failure(self, ip, target):
        now = time.time()
        with self._lock:
            fails = [t for t in self._failures.get((ip, target), []) if now - t <= 300]
            fails.append(now)
            self._failures[(ip, target)] = fails
            if len(fails) >= 5:
                self._lockouts[(ip, target)] = now + 300
                return True
            return False

    def record_success(self, ip, target):
        with self._lock:
            self._failures.pop((ip, target), None)
            self._lockouts.pop((ip, target), None)

    def reset(self, ip=None, target=None):
        with self._lock:
            if ip is not None and target is not None:
                self._failures.pop((ip, target), None)
                self._lockouts.pop((ip, target), None)
            else:
                self._failures.clear()
                self._lockouts.clear()

login_rate_limiter = LoginRateLimiter()

def init_db():
    with sqlite3.connect(DB_FILE, timeout=10) as conn:
        c = conn.cursor()

        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")

        c.execute('''CREATE TABLE IF NOT EXISTS settings (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS teams (
                        id TEXT PRIMARY KEY,
                        display_name TEXT,
                        pin TEXT,
                        role TEXT,
                        next_role TEXT,
                        slug TEXT,
                        deactivated INTEGER,
                        manual_points INTEGER
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS team_catches (
                        hunter_id TEXT,
                        prey_id TEXT,
                        submitted_at TEXT,
                        actual_timestamp TEXT,
                        action_id TEXT,
                        FOREIGN KEY(hunter_id) REFERENCES teams(id),
                        FOREIGN KEY(prey_id) REFERENCES teams(id)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS mister_pending_inputs (
                        team_id TEXT PRIMARY KEY,
                        target_interval_iso TEXT,
                        line TEXT,
                        direction TEXT,
                        station TEXT,
                        manual_lat REAL,
                        manual_lon REAL,
                        updated_at TEXT
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS mister_broadcasts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        team_id TEXT,
                        interval_iso TEXT,
                        published_at TEXT,
                        lat REAL,
                        lon REAL,
                        line TEXT,
                        direction TEXT,
                        station TEXT,
                        is_manual_coords INTEGER,
                        is_auto INTEGER DEFAULT 0
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS pending_requests (
                        id TEXT PRIMARY KEY,
                        type TEXT,
                        team TEXT,
                        team_name TEXT,
                        stop TEXT,
                        reporter TEXT,
                        reporter_name TEXT,
                        hunter TEXT,
                        hunter_name TEXT,
                        prey TEXT,
                        prey_name TEXT,
                        timestamp TEXT,
                        is_late INTEGER,
                        gps_check_until TEXT,
                        notified_admin INTEGER,
                        detected_actual_timestamp TEXT,
                        detected_distance REAL
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS action_logs (
                        id TEXT PRIMARY KEY,
                        type TEXT,
                        team TEXT,
                        team_name TEXT,
                        stop TEXT,
                        hunter TEXT,
                        hunter_name TEXT,
                        prey TEXT,
                        prey_name TEXT,
                        timestamp TEXT,
                        actual_timestamp TEXT,
                        desc TEXT,
                        is_late INTEGER,
                        admin_only INTEGER
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS team_locations (
                        team_id TEXT PRIMARY KEY,
                        lat REAL,
                        lon REAL,
                        updated_at TEXT,
                        batteryLevel REAL,
                        charge INTEGER,
                        motion INTEGER,
                        altitude REAL,
                        speed REAL,
                        course REAL,
                        accuracy REAL,
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS location_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        team_id TEXT,
                        lat REAL,
                        lon REAL,
                        timestamp TEXT,
                        batteryLevel REAL,
                        charge INTEGER,
                        motion INTEGER,
                        altitude REAL,
                        speed REAL,
                        course REAL,
                        accuracy REAL,
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')

        c.execute('''CREATE INDEX IF NOT EXISTS idx_location_history_team_timestamp
                     ON location_history (team_id, timestamp)''')

        c.execute('''CREATE TABLE IF NOT EXISTS gps_outages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        team_id TEXT,
                        offline_from TEXT,
                        offline_until TEXT,
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS geofence_violations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        team_id TEXT,
                        violation_from TEXT,
                        violation_until TEXT,
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')

        # Drop obsolete legacy tables if they exist
        for old_table in ["team_stops", "active_stops", "bonus_stops", "stop_history", "replay_stop_lifecycle", "replay_role_lifecycle"]:
            c.execute(f"DROP TABLE IF EXISTS {old_table}")

        # Migrate team roles to mister_x / fänger
        c.execute("UPDATE teams SET role = 'mister_x' WHERE role = 'läufer'")
        c.execute("UPDATE teams SET role = 'mister_x' WHERE (role IS NULL OR role = 'keine') AND id LIKE 'mister%'")
        c.execute("UPDATE teams SET role = 'fänger' WHERE role IS NULL OR role = 'keine'")

        # Sicherstellen, dass bestehende Datenbanken nahtlos um die neuen Zeitspalten erweitert werden
        ensure_column(c, "team_catches", "submitted_at", "TEXT")
        ensure_column(c, "team_catches", "actual_timestamp", "TEXT")
        ensure_column(c, "team_catches", "action_id", "TEXT")
        ensure_column(c, "pending_requests", "gps_check_until", "TEXT")
        ensure_column(c, "pending_requests", "notified_admin", "INTEGER")
        ensure_column(c, "pending_requests", "detected_actual_timestamp", "TEXT")
        ensure_column(c, "pending_requests", "detected_distance", "REAL")
        ensure_column(c, "action_logs", "actual_timestamp", "TEXT")
        ensure_column(c, "action_logs", "admin_only", "INTEGER")
        ensure_column(c, "mister_broadcasts", "is_auto", "INTEGER DEFAULT 0")

        # Neue Telemetrie-Spalten sicherstellen
        for col_name, col_type in [
            ("batteryLevel", "REAL"),
            ("charge", "INTEGER"),
            ("motion", "INTEGER"),
            ("altitude", "REAL"),
            ("speed", "REAL"),
            ("course", "REAL"),
            ("accuracy", "REAL"),
        ]:
            ensure_column(c, "team_locations", col_name, col_type)
            ensure_column(c, "location_history", col_name, col_type)

        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('admin_slug', ?)", (ADMIN_SLUG,))
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('active_stops_dataset', 'Catch_Haltestellen.json')")

    conn.commit()

init_db()
load_active_stops_data()


def is_valid_admin_slug(slug):
    return str(slug).strip() == ADMIN_SLUG


# --- GEODÄTISCHE & ZEITLICHE HILFSFUNKTIONEN (TRACCAR SPIELLOGIK) ---
def haversine_meters(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * R * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))



@app.template_filter('iso_time')
def format_iso_time(iso_str):
    if not iso_str:
        return ""
    try:
        dt = parse_iso_utc(iso_str)
        if not dt:
            return iso_str
        tz = ZoneInfo("Europe/Berlin")
        local_dt = dt.astimezone(tz)
        return local_dt.strftime("%H:%M:%S Uhr")
    except Exception:
        return str(iso_str)

def parse_iso_utc(iso_str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_str).replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def find_closest_teams_approach(conn, hunter_id, prey_id, ref_dt=None, window_minutes=15, max_distance_meters=250.0):
    """
    Bestimmt den Zeitpunkt, an dem sich Fänger (hunter_id) und Läufer (prey_id)
    im Zeitraum vor ref_dt am nächsten waren (nur bei plausibler Annäherung < max_distance_meters).
    Falls keine passende GPS-Annäherung vorliegt, wird None zurückgegeben.
    """
    if ref_dt is None:
        ref_dt = datetime.now(timezone.utc)
    start_dt = ref_dt - timedelta(minutes=window_minutes)
    end_dt = ref_dt + timedelta(minutes=2)

    c = conn.cursor()
    c.execute("""
        SELECT lat, lon, timestamp
        FROM location_history
        WHERE team_id = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
    """, (hunter_id, start_dt.isoformat(), end_dt.isoformat()))
    hunter_rows = list(c.fetchall())
    c.execute("""
        SELECT lat, lon, updated_at
        FROM team_locations
        WHERE team_id = ? AND updated_at >= ? AND updated_at <= ?
    """, (hunter_id, start_dt.isoformat(), end_dt.isoformat()))
    hunter_live = c.fetchone()
    if hunter_live:
        hunter_rows.append(hunter_live)

    hunter_pts = [(r[0], r[1], parse_iso_utc(r[2]), r[2]) for r in hunter_rows]
    hunter_pts = [p for p in hunter_pts if p[2] is not None]

    c.execute("""
        SELECT lat, lon, timestamp
        FROM location_history
        WHERE team_id = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
    """, (prey_id, start_dt.isoformat(), end_dt.isoformat()))
    prey_rows = list(c.fetchall())
    c.execute("""
        SELECT lat, lon, updated_at
        FROM team_locations
        WHERE team_id = ? AND updated_at >= ? AND updated_at <= ?
    """, (prey_id, start_dt.isoformat(), end_dt.isoformat()))
    prey_live = c.fetchone()
    if prey_live:
        prey_rows.append(prey_live)

    prey_pts = [(r[0], r[1], parse_iso_utc(r[2]), r[2]) for r in prey_rows]
    prey_pts = [p for p in prey_pts if p[2] is not None]

    if not hunter_pts or not prey_pts:
        return None

    min_dist = None
    best_iso = None

    # Für jeden Messpunkt eines Teams den zeitlich nächstgelegenen Punkt des anderen Teams prüfen (max 120s Versatz & < 250m Distanz)
    for h_lat, h_lon, h_dt, h_iso in hunter_pts:
        closest_p = min(prey_pts, key=lambda p: abs((p[2] - h_dt).total_seconds()))
        p_lat, p_lon, p_dt, p_iso = closest_p
        if abs((p_dt - h_dt).total_seconds()) <= 120:
            dist = haversine_meters(h_lat, h_lon, p_lat, p_lon)
            if dist < max_distance_meters and (min_dist is None or dist < min_dist):
                min_dist = dist
                mid_ts = (h_dt.timestamp() + p_dt.timestamp()) / 2.0
                best_iso = datetime.fromtimestamp(mid_ts, tz=timezone.utc).isoformat()

    return best_iso


# --- LOKALER JAVA-TRACCAR MANAGER ---
def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1.0)
        return s.connect_ex(('127.0.0.1', port)) == 0


def get_java_executable():
    sys_java = shutil.which("java")
    if sys_java:
        return sys_java

    if shutil.which("apt"):
        print("☕ Java wurde nicht gefunden. Installiere 'default-jre-headless'...")
        try:
            subprocess.run(["sudo", "apt", "update"], check=True)
            subprocess.run(["sudo", "apt", "install", "-y", "default-jre-headless"], check=True)
            return shutil.which("java")
        except Exception as e:
            print(f"⚠️ Konnte Java nicht automatisch via apt installieren: {e}")

    return None


def ensure_traccar_xml():
    if os.path.exists(TRACCAR_XML_PATH):
        with open(TRACCAR_XML_PATH, "r", encoding="utf-8") as f:
            content = f.read()
        updated = content
        if "host.docker.internal" in updated:
            updated = updated.replace("host.docker.internal", "127.0.0.1")
        if "<entry key='database.user'>admin</entry>" in updated:
            updated = updated.replace("<entry key='database.user'>admin</entry>", "<entry key='database.user'>sa</entry>")
            updated = re.sub(r"<entry key='database\.password'>.*?</entry>", "<entry key='database.password'></entry>", updated)
        if updated != content:
            print("🔧 Aktualisiere traccar.xml für lokalen Java-Betrieb...")
            with open(TRACCAR_XML_PATH, "w", encoding="utf-8") as f:
                f.write(updated)
        return

    xml_content = """<?xml version='1.0' encoding='UTF-8'?>
<!DOCTYPE properties SYSTEM 'http://java.sun.com/dtd/properties.dtd'>
<properties>
    <entry key='config.default'>./conf/default.xml</entry>
    <entry key='database.driver'>org.h2.Driver</entry>
    <entry key='database.url'>jdbc:h2:./data/database</entry>
    <entry key='database.user'>sa</entry>
    <entry key='database.password'></entry>
    <entry key='web.port'>8082</entry>
    <entry key='osmand.port'>5055</entry>
    <entry key='forward.enable'>true</entry>
    <entry key='forward.type'>json</entry>
    <entry key='forward.url'>http://127.0.0.1:5000/api/traccar</entry>
    <entry key='forward.retry.enable'>true</entry>
</properties>
"""
    with open(TRACCAR_XML_PATH, "w", encoding="utf-8") as f:
        f.write(xml_content)


def ensure_traccar_installed():
    with install_lock:
        ensure_traccar_xml()
        os.makedirs(TRACCAR_DATA_DIR, exist_ok=True)
        os.makedirs(TRACCAR_LOGS_DIR, exist_ok=True)

        if os.path.isfile(TRACCAR_JAR_PATH):
            return True

        print(f"📥 Lade portables Traccar v{TRACCAR_VERSION} (traccar-other) in ./traccar_runtime herunter...")
        os.makedirs(TRACCAR_RUNTIME_DIR, exist_ok=True)

        url = f"https://github.com/traccar/traccar/releases/download/v{TRACCAR_VERSION}/traccar-other-{TRACCAR_VERSION}.zip"
        zip_path = os.path.join(TRACCAR_RUNTIME_DIR, f"traccar-other-{TRACCAR_VERSION}.zip")

        try:
            if os.path.exists(zip_path):
                os.remove(zip_path)

            urllib.request.urlretrieve(url, zip_path)
            print("📦 Entpacke Traccar direkt in ./traccar_runtime ...")
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(TRACCAR_RUNTIME_DIR)

            if os.path.isfile(zip_path):
                os.remove(zip_path)

            if os.path.isfile(TRACCAR_JAR_PATH):
                print("✅ Traccar erfolgreich lokal in ./traccar_runtime bereitgestellt!")
                return True
            else:
                print("❌ Fehler: tracker-server.jar wurde nach dem Entpacken nicht gefunden.")
                return False
        except Exception as e:
            print(f"❌ Download/Entpacken von Traccar fehlgeschlagen: {e}")
            if os.path.exists(zip_path):
                try:
                    os.remove(zip_path)
                except Exception:
                    pass
            return False


def stop_traccar_process():
    global traccar_process
    if traccar_process and traccar_process.poll() is None:
        print("🛑 Beende lokalen Java-Traccar-Server...")
        try:
            traccar_process.terminate()
            traccar_process.wait(timeout=5)
        except Exception:
            try:
                traccar_process.kill()
            except Exception:
                pass
    traccar_process = None

atexit.register(stop_traccar_process)


def start_traccar_server():
    global traccar_process, traccar_last_error

    if not ensure_traccar_installed():
        traccar_last_error = "Traccar-Download in ./traccar_runtime fehlgeschlagen"
        return False

    if traccar_process and traccar_process.poll() is None:
        return True

    if is_port_in_use(8082):
        if ensure_traccar_credentials():
            print("ℹ️ Bestehender lokaler Traccar-Prozess auf Port 8082 gefunden und übernommen.")
            return True
        else:
            print("⚠️ Port 8082 ist bereits belegt (alter Container oder Prozess). Bereinige Port 8082 & 5055...")
            subprocess.run(["fuser", "-k", "8082/tcp"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            subprocess.run(["fuser", "-k", "5055/tcp"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(1)

    java_bin = get_java_executable()
    if not java_bin:
        traccar_last_error = "Kein Java (JRE) gefunden! Bitte 'sudo apt install default-jre-headless' ausführen."
        print(f"❌ {traccar_last_error}")
        return False

    log_file_path = os.path.join(TRACCAR_LOGS_DIR, "traccar-stdout.log")
    log_file = open(log_file_path, "a", encoding="utf-8")

    print(f"☕ Starte lokalen Java-Traccar-Server ({java_bin} -jar tracker-server.jar)...")
    try:
        traccar_process = subprocess.Popen(
            [java_bin, "-Dfile.encoding=UTF-8", "-jar", TRACCAR_JAR_PATH, TRACCAR_XML_PATH],
            cwd=TRACCAR_RUNTIME_DIR,
            stdout=log_file,
            stderr=subprocess.STDOUT
        )
        print(f"✅ Lokaler Traccar-Prozess gestartet (PID: {traccar_process.pid}).")
        return True
    except Exception as e:
        traccar_last_error = f"Startfehler: {e}"
        print(f"❌ Fehler beim Starten von Java-Traccar: {e}")
        return False


def reset_traccar_process_and_db():
    global traccar_ready, traccar_last_error, traccar_session
    with traccar_lock:
        print("🧨 Setze lokalen Java-Traccar-Server und H2-Datenbank zurück...")
        traccar_ready = False
        traccar_last_error = "Datenbank wird zurückgesetzt..."
        traccar_session = requests.Session()

        stop_traccar_process()
        subprocess.run(["fuser", "-k", "8082/tcp"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(["fuser", "-k", "5055/tcp"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(1)

        try:
            if os.path.exists(TRACCAR_DATA_DIR):
                shutil.rmtree(TRACCAR_DATA_DIR, ignore_errors=True)
            os.makedirs(TRACCAR_DATA_DIR, exist_ok=True)
            return start_traccar_server()
        except Exception as e:
            traccar_last_error = f"Reset-Fehler: {e}"
            return False


def ensure_traccar_credentials():
    global traccar_ready, traccar_last_error
    with traccar_lock:
        try:
            resp = traccar_session.post(
                f"{TRACCAR_BASE_URL}/session",
                data={"email": TRACCAR_USER, "password": TRACCAR_PASS},
                timeout=3
            )
            if resp.status_code == 200:
                traccar_session.auth = HTTPBasicAuth(TRACCAR_USER, TRACCAR_PASS)
                traccar_ready = True
                traccar_last_error = None
                return True

            reg_payload = {
                "name": TRACCAR_USER,
                "email": TRACCAR_USER,
                "password": TRACCAR_PASS
            }
            reg_resp = requests.post(f"{TRACCAR_BASE_URL}/users", json=reg_payload, timeout=4)
            if reg_resp.status_code in (200, 201):
                print(f"🔐 Frische Traccar-Datenbank erkannt: Admin-User '{TRACCAR_USER}' erfolgreich erstregistriert!")
                login_resp = traccar_session.post(
                    f"{TRACCAR_BASE_URL}/session",
                    data={"email": TRACCAR_USER, "password": TRACCAR_PASS},
                    timeout=3
                )
                if login_resp.status_code == 200:
                    traccar_session.auth = HTTPBasicAuth(TRACCAR_USER, TRACCAR_PASS)
                    traccar_ready = True
                    traccar_last_error = None
                    return True

            resp_default = traccar_session.post(
                f"{TRACCAR_BASE_URL}/session",
                data={"email": "admin", "password": "admin"},
                timeout=3
            )
            if resp_default.status_code == 200:
                user_data = resp_default.json()
                user_id = user_data.get("id")
                user_data["email"] = TRACCAR_USER
                user_data["name"] = TRACCAR_USER
                user_data["password"] = TRACCAR_PASS

                update_resp = traccar_session.put(
                    f"{TRACCAR_BASE_URL}/users/{user_id}",
                    json=user_data,
                    auth=HTTPBasicAuth("admin", "admin"),
                    timeout=5
                )
                if update_resp.status_code == 200:
                    print(f"🔐 Traccar Standard-Login erfolgreich auf '{TRACCAR_USER}' (.env) umgestellt!")
                    traccar_session.post(
                        f"{TRACCAR_BASE_URL}/session",
                        data={"email": TRACCAR_USER, "password": TRACCAR_PASS},
                        timeout=3
                    )
                    traccar_session.auth = HTTPBasicAuth(TRACCAR_USER, TRACCAR_PASS)
                    traccar_ready = True
                    traccar_last_error = None
                    return True
                else:
                    traccar_last_error = f"Admin-Update fehlgeschlagen (HTTP {update_resp.status_code})"
            else:
                traccar_last_error = f"Login abgelehnt (HTTP {resp.status_code}, Reg-HTTP {reg_resp.status_code})"

        except requests.exceptions.ConnectionError:
            traccar_last_error = "Port 8082 bootet noch (H2-Migration läuft)..."
        except requests.exceptions.Timeout:
            traccar_last_error = "Timeout auf Port 8082"
        except Exception as e:
            traccar_last_error = f"Fehler: {e}"

        traccar_ready = False
        return False


def sync_traccar_devices(teams_data=None):
    with traccar_lock:
        if not traccar_ready:
            if not ensure_traccar_credentials():
                return False

        if teams_data is None:
            try:
                with sqlite3.connect(DB_FILE, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    state = get_state(conn)
                    teams_data = state.get("teams_data", {})
            except Exception as e:
                print(f"⚠️ Konnte Teams für Traccar-Sync nicht aus DB lesen: {e}")
                return False

        try:
            resp = traccar_session.get(TRACCAR_API_URL, timeout=5)
            if resp.status_code == 401:
                if not ensure_traccar_credentials():
                    return False
                resp = traccar_session.get(TRACCAR_API_URL, timeout=5)

            if resp.status_code != 200:
                print(f"⚠️ Traccar Sync: Fehler beim Abrufen der Geräte (HTTP {resp.status_code})")
                return False

            existing_devices = resp.json()
            existing_by_slug = {dev["uniqueId"]: dev for dev in existing_devices if "uniqueId" in dev}

            desired_devices = {}
            for t_id, t_data in teams_data.items():
                if not t_data.get("deactivated", False):
                    slug = t_data.get("slug")
                    name = t_data.get("display_name", t_id)
                    if slug:
                        desired_devices[slug] = name

            for slug, name in desired_devices.items():
                if slug not in existing_by_slug:
                    payload = {"name": name, "uniqueId": slug}
                    create_res = traccar_session.post(TRACCAR_API_URL, json=payload, timeout=5)
                    if create_res.status_code in (200, 201):
                        print(f"✅ Traccar Sync: Gerät '{name}' (ID: {slug}) angelegt.")
                else:
                    dev = existing_by_slug[slug]
                    if dev.get("name") != name:
                        dev["name"] = name
                        upd_res = traccar_session.put(f"{TRACCAR_API_URL}/{dev['id']}", json=dev, timeout=5)
                        if upd_res.status_code == 200:
                            print(f"🔄 Traccar Sync: Gerät '{slug}' in '{name}' umbenannt.")

            for slug, dev in existing_by_slug.items():
                if slug.startswith("debug-koblenz-"):
                    continue
                if slug not in desired_devices:
                    del_res = traccar_session.delete(f"{TRACCAR_API_URL}/{dev['id']}", timeout=5)
                    if del_res.status_code in (200, 204):
                        print(f"🗑️ Traccar Sync: Veraltetes Gerät '{dev.get('name')}' (ID: {slug}) entfernt.")

            return True

        except Exception as e:
            print(f"⚠️ Traccar Sync fehlgeschlagen: {e}")
            return False


def trigger_traccar_sync_async(teams_data=None):
    """Startet die Traccar-Gerätesynchronisation asynchron im Hintergrund."""
    threading.Thread(target=sync_traccar_devices, args=(teams_data,), daemon=True).start()


def point_in_polygon(lat, lon, polygon):
    """
    Ray casting algorithm. Prüft, ob ein Punkt (lat, lon) innerhalb eines Polygons liegt.
    polygon ist eine Liste von [lat, lon] Koordinaten oder Dicts {"lat": ..., "lon": ...}.
    """
    if not polygon or not isinstance(polygon, (list, tuple)):
        return False

    pts = []
    for pt in polygon:
        if isinstance(pt, (list, tuple)) and len(pt) >= 2:
            try:
                pts.append((float(pt[0]), float(pt[1])))
            except (ValueError, TypeError):
                continue
        elif isinstance(pt, dict):
            try:
                p_lat = float(pt.get("lat"))
                p_lon = float(pt.get("lon") if pt.get("lon") is not None else pt.get("lng"))
                pts.append((p_lat, p_lon))
            except (ValueError, TypeError):
                continue

    if len(pts) < 3:
        return False

    # Duplizierten Schlusspunkt entfernen falls vorhanden
    if pts[0] == pts[-1] and len(pts) > 3:
        pts = pts[:-1]

    n = len(pts)
    inside = False
    p1_lat, p1_lon = pts[0]
    for i in range(1, n + 1):
        p2_lat, p2_lon = pts[i % n]
        if min(p1_lat, p2_lat) < lat <= max(p1_lat, p2_lat):
            if p1_lat != p2_lat:
                x_inters = (lat - p1_lat) * (p2_lon - p1_lon) / (p2_lat - p1_lat) + p1_lon
                if lon <= x_inters:
                    inside = not inside
        p1_lat, p1_lon = p2_lat, p2_lon

    return inside


# --- MISTER X STANDORT-VERÖFFENTLICHUNG (INTERVALL-LOGIK) ---
def get_all_broadcast_intervals(config):
    tz = ZoneInfo(config.get("timezone", "Europe/Berlin"))
    start_dt_utc = parse_iso_utc(config.get("start_time"))
    end_dt_utc = parse_iso_utc(config.get("end_time"))
    if not start_dt_utc or not end_dt_utc:
        return []

    raw_fbt = str(config.get("first_broadcast_time", "")).strip()
    start_local = start_dt_utc.astimezone(tz)

    t0_utc = None
    if "T" in raw_fbt:
        try:
            dt = datetime.fromisoformat(raw_fbt.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tz)
            t0_utc = dt.astimezone(timezone.utc)
        except Exception:
            t0_utc = parse_iso_utc(raw_fbt)
    elif ":" in raw_fbt:
        parts = raw_fbt.split(":")
        try:
            hh = int(parts[0])
            mm = int(parts[1])
            fbt_local = start_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
            t0_utc = fbt_local.astimezone(timezone.utc)
        except Exception:
            pass

    try:
        interval_mins = max(1, int(config.get("broadcast_interval_minutes", 15)))
    except (ValueError, TypeError):
        interval_mins = 15

    if not t0_utc:
        t0_utc = start_dt_utc + timedelta(minutes=interval_mins)

    delta = timedelta(minutes=interval_mins)
    intervals = []
    curr = t0_utc
    max_count = 100
    while curr <= end_dt_utc and len(intervals) < max_count:
        intervals.append(curr)
        curr = curr + delta

    return intervals


def format_broadcast_display_time(interval_iso, published_at, is_auto, tz):
    """
    Liefert den formatierten Zeit-String (HH:MM) für einen Mister X Broadcast:
    - Bei fristgerechter manueller Einstellung innerhalb des 3-Minuten-Zeitfensters
      [Tk - 2 Min., Tk + 1 Min.] wird die tatsächliche Zeit (published_at) verwendet.
    - Andernfalls (z. B. automatischer GPS-Fallback oder verspätet) die geplante Intervallzeit (interval_iso).
    """
    int_dt = parse_iso_utc(interval_iso)
    pub_dt = parse_iso_utc(published_at)
    auto_flag = bool(is_auto) if is_auto is not None else False
    if not auto_flag and int_dt and pub_dt:
        window_start = int_dt - timedelta(minutes=2, seconds=5)
        window_end = int_dt + timedelta(minutes=1, seconds=5)
        if window_start <= pub_dt <= window_end:
            return pub_dt.astimezone(tz).strftime("%H:%M")
    if int_dt:
        return int_dt.astimezone(tz).strftime("%H:%M")
    if pub_dt:
        return pub_dt.astimezone(tz).strftime("%H:%M")
    return ""


def get_mister_x_interval_info(config, team_id, conn, now_utc=None):
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    tz = ZoneInfo(config.get("timezone", "Europe/Berlin"))
    intervals = get_all_broadcast_intervals(config)
    end_dt_utc = parse_iso_utc(config.get("end_time"))

    if not intervals:
        return {
            "status": "inactive",
            "window_type": None,
            "can_submit": False,
            "message": "Keine Intervalle konfiguriert."
        }

    window_type = None
    target_interval = None
    next_interval = None
    seconds_left = 0
    can_submit = False

    c = conn.cursor()

    t0 = intervals[0]
    if now_utc < (t0 - timedelta(minutes=2)):
        window_type = "before_first_window"
        target_interval = t0
        next_interval = t0
        seconds_left = max(0, (t0 - timedelta(minutes=2) - now_utc).total_seconds())
        can_submit = False
    else:
        for idx, Tk in enumerate(intervals):
            pre_start = Tk - timedelta(minutes=2)
            grace_end = Tk + timedelta(minutes=1)
            next_Tk = intervals[idx + 1] if idx + 1 < len(intervals) else end_dt_utc
            late_end = (next_Tk - timedelta(minutes=2)) if next_Tk else grace_end

            if pre_start <= now_utc <= grace_end:
                target_interval = Tk
                next_interval = intervals[idx + 1] if idx + 1 < len(intervals) else None
                
                # Prüfen, ob für dieses Intervall bereits manuell gesendet wurde
                c.execute("""
                    SELECT id, is_auto 
                    FROM mister_broadcasts 
                    WHERE team_id = ? AND interval_iso = ?
                    ORDER BY id DESC LIMIT 1
                """, (team_id, Tk.isoformat()))
                b_row = c.fetchone()
                already_sent = (b_row is not None and (b_row["is_auto"] == 0 or b_row["is_auto"] is None))

                next_window_open = (next_Tk - timedelta(minutes=2)) if next_Tk else None

                if already_sent:
                    window_type = "already_published"
                    # Wenn schon gesendet: kein erneutes Absenden möglich, Countdown bis zum nächsten Meldefenster
                    seconds_left = max(0, (next_window_open - now_utc).total_seconds()) if next_window_open else 0
                    can_submit = False
                else:
                    window_type = "active_window"
                    seconds_left = max(0, (grace_end - now_utc).total_seconds())
                    can_submit = True
                break
            elif grace_end < now_utc < late_end:
                target_interval = Tk
                next_interval = intervals[idx + 1] if idx + 1 < len(intervals) else None
                
                c.execute("""
                    SELECT id, is_auto 
                    FROM mister_broadcasts 
                    WHERE team_id = ? AND interval_iso = ?
                    ORDER BY id DESC LIMIT 1
                """, (team_id, Tk.isoformat()))
                b_row = c.fetchone()
                
                already_sent = (b_row is not None and (b_row["is_auto"] == 0 or b_row["is_auto"] is None))
                next_window_open = (next_Tk - timedelta(minutes=2)) if next_Tk else None
                seconds_to_next = max(0, (next_window_open - now_utc).total_seconds()) if next_window_open else 0

                if already_sent:
                    # Standort wurde schon gesendet: Mister X kann nicht noch einmal abschicken!
                    window_type = "already_published"
                    seconds_left = seconds_to_next
                    can_submit = False
                else:
                    # Standort wurde noch nicht gesendet (nur automatischer Fallback): Nachtragen erlaubt!
                    window_type = "late_submission"
                    seconds_left = seconds_to_next
                    can_submit = True
                break

        if window_type is None:
            window_type = "all_intervals_done"
            can_submit = False

    target_iso = target_interval.isoformat() if target_interval else None
    target_time_str = target_interval.astimezone(tz).strftime("%H:%M") if target_interval else ""
    next_iso = next_interval.isoformat() if next_interval else None
    next_time_str = next_interval.astimezone(tz).strftime("%H:%M") if next_interval else ""

    broadcast = None
    if target_iso:
        c.execute("""
            SELECT id, interval_iso, published_at, lat, lon, line, direction, station, is_manual_coords, is_auto
            FROM mister_broadcasts
            WHERE team_id = ? AND interval_iso = ?
            ORDER BY id DESC LIMIT 1
        """, (team_id, target_iso))
        b_row = c.fetchone()
        if b_row:
            broadcast = dict(b_row)
            broadcast["interval_time_str"] = format_broadcast_display_time(
                broadcast.get("interval_iso"), broadcast.get("published_at"), broadcast.get("is_auto"), tz
            )

    c.execute("""
        SELECT id, interval_iso, published_at, lat, lon, line, direction, station, is_manual_coords, is_auto
        FROM mister_broadcasts
        WHERE team_id = ?
        ORDER BY id DESC LIMIT 1
    """, (team_id,))
    last_b_row = c.fetchone()
    last_broadcast = dict(last_b_row) if last_b_row else None
    if last_broadcast:
        last_broadcast["interval_time_str"] = format_broadcast_display_time(
            last_broadcast.get("interval_iso"), last_broadcast.get("published_at"), last_broadcast.get("is_auto"), tz
        )

    c.execute("SELECT lat, lon, updated_at, accuracy FROM team_locations WHERE team_id = ?", (team_id,))
    gps_row = c.fetchone()
    current_gps = dict(gps_row) if gps_row else None

    return {
        "status": window_type,
        "window_type": window_type,
        "can_submit": can_submit,
        "seconds_left": int(seconds_left),
        "target_interval_iso": target_iso,
        "target_interval_str": target_time_str,
        "target_interval_time_str": target_time_str,
        "next_interval_iso": next_iso,
        "next_interval_time_str": next_time_str,
        "broadcast": broadcast,
        "last_broadcast": last_broadcast,
        "current_gps": current_gps
    }


def publish_mister_x_broadcast(conn, state, team_id, interval_iso, line="", direction="", station="", manual_lat=None, manual_lon=None, is_auto=0):
    now_utc = datetime.now(timezone.utc)
    now_iso = now_utc.isoformat()
    tz = ZoneInfo(state["config"].get("timezone", "Europe/Berlin"))

    interval_dt = parse_iso_utc(interval_iso)
    interval_str = interval_dt.astimezone(tz).strftime("%H:%M") if interval_dt else now_utc.astimezone(tz).strftime("%H:%M")

    c = conn.cursor()
    lat = None
    lon = None
    is_manual = 0
    if manual_lat is not None and manual_lon is not None:
        try:
            lat = float(manual_lat)
            lon = float(manual_lon)
            is_manual = 1
        except (ValueError, TypeError):
            lat, lon = None, None

    if lat is None or lon is None:
        c.execute("SELECT lat, lon FROM team_locations WHERE team_id = ?", (team_id,))
        loc_row = c.fetchone()
        if loc_row and loc_row["lat"] is not None and loc_row["lon"] is not None:
            lat = float(loc_row["lat"])
            lon = float(loc_row["lon"])
        else:
            lat = 50.1065
            lon = 8.6865
        is_manual = 0

    c.execute("""
        INSERT INTO mister_broadcasts (team_id, interval_iso, published_at, lat, lon, line, direction, station, is_manual_coords, is_auto)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (team_id, interval_iso, now_iso, lat, lon, line or "", direction or "", station or "", is_manual, is_auto))

    c.execute("DELETE FROM mister_pending_inputs WHERE team_id = ?", (team_id,))

    details_parts = []
    if line:
        details_parts.append(f"Linie {line}")
    if direction:
        details_parts.append(f"Richtung {direction}")
    if station:
        details_parts.append(f"Station {station}")

    if details_parts:
        details_str = " - " + ", ".join(details_parts)
    else:
        details_str = " - Keine Haltestellen-Infos nachgereicht"

    action_text = f"📢 Neuer Standort von Mister X veröffentlicht ({interval_str} Uhr{details_str})"
    log_entry = {
        "id": str(uuid.uuid4()),
        "type": "broadcast",
        "team": team_id,
        "timestamp": now_iso,
        "actual_timestamp": now_iso,
        "desc": action_text,
        "admin_only": True
    }
    state["action_log"].insert(0, log_entry)
    send_telegram_msg(action_text)
    return True


def update_mister_x_broadcast(conn, state, broadcast_id, line="", direction="", station="", manual_lat=None, manual_lon=None):
    now_utc = datetime.now(timezone.utc)
    tz = ZoneInfo(state["config"].get("timezone", "Europe/Berlin"))
    c = conn.cursor()
    c.execute("SELECT * FROM mister_broadcasts WHERE id = ?", (broadcast_id,))
    row = c.fetchone()
    if not row:
        return False

    interval_dt = parse_iso_utc(row["interval_iso"])
    interval_str = interval_dt.astimezone(tz).strftime("%H:%M") if interval_dt else "unbekannt"

    updates = ["line = ?", "direction = ?", "station = ?", "is_auto = 0"]
    params = [line or "", direction or "", station or ""]

    if manual_lat is not None and manual_lon is not None:
        try:
            m_lat = float(manual_lat)
            m_lon = float(manual_lon)
            updates.append("lat = ?")
            params.append(m_lat)
            updates.append("lon = ?")
            params.append(m_lon)
            updates.append("is_manual_coords = 1")
        except (ValueError, TypeError):
            pass

    params.append(broadcast_id)
    c.execute(f"UPDATE mister_broadcasts SET {', '.join(updates)} WHERE id = ?", params)

    details_parts = []
    if line:
        details_parts.append(f"Linie {line}")
    if direction:
        details_parts.append(f"Richtung {direction}")
    if station:
        details_parts.append(f"Station {station}")
    details_str = (", ".join(details_parts)) if details_parts else "aktualisiert"

    action_text = f"📢 Haltestellen-Infos für Standort von {interval_str} Uhr nachgereicht ({details_str})"
    log_entry = {
        "id": str(uuid.uuid4()),
        "type": "broadcast_update",
        "team": row["team_id"],
        "timestamp": now_utc.isoformat(),
        "actual_timestamp": now_utc.isoformat(),
        "desc": action_text,
        "admin_only": True
    }
    state["action_log"].insert(0, log_entry)
    send_telegram_msg(action_text)
    return True


# --- HINTERGRUND-PRÜFUNG FÜR GPS-INAKTIVITÄT & MISTER X INTERVALL-CHECKS ---
def check_game_gps_rules():
    """
    Wird regelmäßig im Hintergrund ausgeführt:
    1. Prüft offene Haltestellen-Anfragen im 1-Minuten-Nachprüfungsfenster.
    2. Prüft während des aktiven Spiels, ob Teams seit 5, 10, 15... Minuten keinen Standort geschickt haben,
       und trägt dies in die Aktions-Historie des Admins ein.
    """
    if not gps_rules_lock.acquire(blocking=False):
        return

    telegram_messages = []
    now_utc = datetime.now(timezone.utc)
    summary_actions = {
        "auto_approvals": [],
        "new_gps_warnings": [],
        "resolved_gps_warnings": [],
        "geofence_violations": []
    }

    try:
        with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
            conn.row_factory = sqlite3.Row
            state = get_state(conn)
            phase, start_iso, end_iso, _, _ = update_game_time_state(conn, state)
            config = state["config"]
            state_changed = False

            # 5 Minuten Standort-Ausfall-Historie & Geofence-Überwachung
            if phase in ("game", "finished"):
                c = conn.cursor()
                c.execute("SELECT team_id, lat, lon, updated_at FROM team_locations")
                loc_rows = c.fetchall()
                loc_map = {r["team_id"]: parse_iso_utc(r["updated_at"]) for r in loc_rows}
                loc_data_map = {r["team_id"]: r for r in loc_rows}
                start_dt_utc = parse_iso_utc(start_iso)
                ref_start_utc = start_dt_utc
                end_dt_utc = parse_iso_utc(end_iso)
                enable_geofences = bool(config.get("enable_geofences", False))

                for t_id, t_data in state["teams_data"].items():
                    is_active_player = not t_data.get("deactivated", False) and t_data.get("role") in ("mister_x", "fänger")
                    if not is_active_player:
                        # Offene Verstöße für inaktive/deaktivierte Teams schließen
                        c.execute("SELECT id FROM geofence_violations WHERE team_id = ? AND violation_until IS NULL", (t_id,))
                        open_gv = c.fetchone()
                        if open_gv:
                            c.execute("UPDATE geofence_violations SET violation_until = ? WHERE id = ?", (now_utc.isoformat(), open_gv["id"]))
                        continue

                    last_dt = loc_map.get(t_id)
                    ref_dt = ref_start_utc if (last_dt and ref_start_utc and last_dt < ref_start_utc) else (last_dt or ref_start_utc)

                    c.execute("SELECT id FROM gps_outages WHERE team_id = ? AND offline_until IS NULL", (t_id,))
                    open_outage = c.fetchone()

                    c.execute("SELECT id FROM geofence_violations WHERE team_id = ? AND violation_until IS NULL", (t_id,))
                    open_gv = c.fetchone()

                    if phase == "finished":
                        if open_outage:
                            close_time = end_dt_utc.isoformat() if end_dt_utc else now_utc.isoformat()
                            c.execute("UPDATE gps_outages SET offline_until = ? WHERE id = ?", (close_time, open_outage["id"]))
                        if open_gv:
                            close_time = end_dt_utc.isoformat() if end_dt_utc else now_utc.isoformat()
                            c.execute("UPDATE geofence_violations SET violation_until = ? WHERE id = ?", (close_time, open_gv["id"]))
                        continue

                    # --- GPS-Ausfall-Prüfung ---
                    if ref_dt:
                        missing_sec = (now_utc - ref_dt).total_seconds()
                        if missing_sec >= 300:
                            if not open_outage:
                                c.execute("INSERT INTO gps_outages (team_id, offline_from) VALUES (?, ?)", (t_id, ref_dt.isoformat()))
                                t_name = t_data.get("display_name", t_id)
                                summary_actions["new_gps_warnings"].append({"team_id": t_id, "team_name": t_name})
                                state["action_log"].insert(0, {
                                    "id": str(uuid.uuid4()), "type": "gps_warning", "team": t_id, "team_name": t_name,
                                    "timestamp": ref_dt.isoformat(), "actual_timestamp": None, "desc": "Offline-Phase",
                                    "is_late": False, "admin_only": True
                                })
                                state_changed = True
                        else:
                            if open_outage:
                                c.execute("UPDATE gps_outages SET offline_until = ? WHERE id = ?", (now_utc.isoformat(), open_outage["id"]))
                                for log in state["action_log"]:
                                    if log.get("type") == "gps_warning" and log.get("team") == t_id and log.get("actual_timestamp") is None:
                                        log["actual_timestamp"] = now_utc.isoformat()
                                        break
                                summary_actions["resolved_gps_warnings"].append(t_id)
                                state_changed = True

                    # --- Geofence-Prüfung direkt in Python ---
                    if enable_geofences and phase == "game":
                        loc_row = loc_data_map.get(t_id)
                        if loc_row and loc_row["lat"] is not None and loc_row["lon"] is not None:
                            try:
                                t_lat = float(loc_row["lat"])
                                t_lon = float(loc_row["lon"])
                                t_updated = loc_row["updated_at"] or now_utc.isoformat()
                                loc_dt = parse_iso_utc(loc_row["updated_at"])
                                if ref_start_utc and loc_dt and loc_dt < ref_start_utc:
                                    t_violation_ts = ref_start_utc.isoformat()
                                else:
                                    t_violation_ts = t_updated

                                is_violation = False
                                # 1. Spielfeldbegrenzung: Team muss innerhalb sein
                                if GEOFENCE_OUTER and len(GEOFENCE_OUTER) >= 3:
                                    if not point_in_polygon(t_lat, t_lon, GEOFENCE_OUTER):
                                        is_violation = True

                                # 2. Sperrzonen: Team darf in keiner Sperrzone sein
                                if not is_violation and GEOFENCE_EXCLUSION:
                                    for zone in GEOFENCE_EXCLUSION:
                                        if zone and len(zone) >= 3:
                                            if point_in_polygon(t_lat, t_lon, zone):
                                                is_violation = True
                                                break

                                if is_violation:
                                    if not open_gv:
                                        c.execute("INSERT INTO geofence_violations (team_id, violation_from) VALUES (?, ?)", (t_id, t_violation_ts))
                                        summary_actions["geofence_violations"].append(t_id)
                                else:
                                    if open_gv:
                                        c.execute("UPDATE geofence_violations SET violation_until = ? WHERE id = ?", (t_updated, open_gv["id"]))
                            except Exception as gf_err:
                                print(f"⚠️ Fehler beim Geofence-Check für Team {t_id}: {gf_err}")
                    else:
                        if open_gv:
                            c.execute("UPDATE geofence_violations SET violation_until = ? WHERE id = ?", (now_utc.isoformat(), open_gv["id"]))

                # --- Mister X Intervall-Veröffentlichungen (Automatischer Fallback nach T + 1 Min.) ---
                if phase == "game":
                    intervals = get_all_broadcast_intervals(config)
                    for t_id, t_data in state["teams_data"].items():
                        if t_data.get("role") == "mister_x" and not t_data.get("deactivated", False):
                            for T_k in intervals:
                                diff_sec = (now_utc - T_k).total_seconds()
                                # Erst ab T + 1 Minute (60s) prüfen, ob automatisch gesendet werden muss
                                if diff_sec < 60:
                                    continue

                                T_k_iso = T_k.isoformat()
                                c.execute("SELECT id FROM mister_broadcasts WHERE team_id = ? AND interval_iso = ?", (t_id, T_k_iso))
                                if c.fetchone() is not None:
                                    continue

                                # Automatischer Versand, falls Zeitraum überschritten wurde
                                publish_mister_x_broadcast(
                                    conn, state, t_id, T_k_iso,
                                    line="", direction="", station="",
                                    is_auto=1
                                )
                                state_changed = True
            elif phase == "inactive":
                c = conn.cursor()
                c.execute("UPDATE geofence_violations SET violation_until = ? WHERE violation_until IS NULL", (now_utc.isoformat(),))

            if state_changed:
                save_state(conn, state)

    except Exception as e:
        print(f"⚠️ Fehler in check_game_gps_rules: {e}")
    finally:
        gps_rules_lock.release()

    for msg in telegram_messages:
        send_telegram_msg(msg)

    if LOG_SETTINGS.get("gps_background_worker", False):
        total_actions = (len(summary_actions["auto_approvals"]) +
                         len(summary_actions["new_gps_warnings"]) +
                         len(summary_actions["resolved_gps_warnings"]) +
                         len(summary_actions["geofence_violations"]))
        write_debug_log("gps_background_worker", {
            "summary": f"{len(summary_actions['auto_approvals'])} Auto-Approvals, {len(summary_actions['new_gps_warnings'])} Warnungen neu, {len(summary_actions['resolved_gps_warnings'])} Warnungen behoben, {len(summary_actions['geofence_violations'])} Geofence-Verstöße",
            "total_actions": total_actions,
            "actions": summary_actions
        })


def game_rules_background_worker():
    while True:
        time.sleep(5)
        check_game_gps_rules()


def server_status_logger_worker():
    """Hintergrund-Thread: Sammelt alle 2 Sekunden Systemmetriken, falls server_status aktiv ist."""
    while True:
        time.sleep(2)
        try:
            if LOG_SETTINGS.get("server_status", False):
                cpu_percent = psutil.cpu_percent(interval=None)
                ram = psutil.virtual_memory()
                metrics = {
                    "cpu_percent": cpu_percent,
                    "ram_percent": ram.percent,
                    "ram_used_mb": round((ram.total - ram.available) / (1024 ** 2), 1),
                    "ram_total_gb": round(ram.total / (1024 ** 3), 2)
                }
                write_debug_log("server_status", metrics)
        except Exception as e:
            app.logger.error(f"Fehler in server_status_logger_worker: {e}")


def traccar_background_worker():
    if not start_traccar_server():
        return

    print("⏳ Warte auf lokalen Traccar-Server (Port 8082)...")
    auth_401_failures = 0
    loop_count = 0
    auto_reset_done = False

    while True:
        loop_count += 1
        try:
            if traccar_process and traccar_process.poll() is not None:
                print(f"⚠️ Lokaler Traccar-Prozess hat sich beendet (Exit-Code {traccar_process.returncode}). Letzte Log-Zeilen:")
                log_file_path = os.path.join(TRACCAR_LOGS_DIR, "traccar-stdout.log")
                if os.path.exists(log_file_path):
                    with open(log_file_path, "r", encoding="utf-8", errors="ignore") as lf:
                        for line in lf.readlines()[-12:]:
                            print("   [Traccar]", line.strip())
                time.sleep(3)
                start_traccar_server()

            if ensure_traccar_credentials():
                sync_traccar_devices()
                break
            else:
                if loop_count % 3 == 0:
                    print(f"   ... warte auf Traccar ({traccar_last_error})")

                if traccar_last_error and ("HTTP 401" in traccar_last_error or "HTTP 400" in traccar_last_error):
                    auth_401_failures += 1
                    if auth_401_failures >= 3 and not auto_reset_done:
                        print("⚠️ Alte/inkompatible H2-Datenbank erkannt (Login fehlgeschlagen). Setze lokale Traccar-DB automatisch zurück...")
                        auto_reset_done = True
                        reset_traccar_process_and_db()
                        auth_401_failures = 0
                        time.sleep(4)
                        continue
        except Exception as e:
            print(f"Fehler im Traccar-Worker: {e}")
        time.sleep(2)

    print("🚀 Lokaler Java-Traccar-Server ist online, authentifiziert und synchronisiert!")
    while True:
        time.sleep(20)
        try:
            if traccar_process and traccar_process.poll() is not None:
                print("⚠️ Traccar-Prozess ausgefallen! Starte automatisch neu...")
                start_traccar_server()
                time.sleep(5)
            sync_traccar_devices()
        except Exception:
            pass


# --- HILFSFUNKTIONEN FÜR STATUS UND AKTIONEN ---
def parse_telegram_chats(raw_val):
    """
    Konvertiert sowohl das alte kommagetrennte String-Format als auch
    JSON-Listen in eine einheitliche Liste aus Dicts: [{'uid': ..., 'name': ..., 'chat_id': ...}]
    """
    if not raw_val:
        return []
    if isinstance(raw_val, list):
        result = []
        for idx, item in enumerate(raw_val):
            if isinstance(item, dict) and item.get("chat_id"):
                cid = str(item["chat_id"]).strip()
                if cid:
                    result.append({
                        "uid": str(item.get("uid") or uuid.uuid4().hex[:8]),
                        "name": str(item.get("name") or f"Chat {idx + 1}").strip(),
                        "chat_id": cid
                    })
        return result

    raw_str = str(raw_val).strip()
    if not raw_str:
        return []

    # Prüfen, ob es bereits ein JSON-String ist
    if raw_str.startswith("["):
        try:
            parsed = json.loads(raw_str)
            if isinstance(parsed, list):
                return parse_telegram_chats(parsed)
        except Exception:
            pass

    # Fallback: Klassischer kommagetrennter String (z.B. aus .env oder alter DB)
    cids = [c.strip() for c in re.split(r'[,;\s]+', raw_str) if c.strip()]
    return [
        {"uid": uuid.uuid4().hex[:8], "name": f"Empfänger {i + 1}", "chat_id": cid}
        for i, cid in enumerate(cids)
    ]


def mask_secret_string(val, visible_start=3, visible_end=3):
    """Maskiert sensible Strings wie Tokens oder Chat-IDs für die Anzeige."""
    s = str(val or "").strip()
    if not s:
        return ""
    if len(s) <= (visible_start + visible_end):
        return "•" * max(4, len(s))
    return f"{s[:visible_start]}••••{s[-visible_end:]}"


def _send_telegram_msg_async(text):
    try:
        with sqlite3.connect(DB_FILE, timeout=5) as conn:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("SELECT key, value FROM settings WHERE key IN ('config', 'telegram_enabled', 'telegram_bot_token', 'telegram_chat_ids')")
            rows = {r['key']: r['value'] for r in c.fetchall()}

        enabled = False
        token = ""
        chat_entries = []

        if 'config' in rows and rows['config']:
            try:
                cfg = json.loads(rows['config'])
                enabled = bool(cfg.get("telegram_enabled", False))
                token = str(cfg.get("telegram_bot_token", "")).strip()
                chat_entries = parse_telegram_chats(cfg.get("telegram_chat_ids", []))
            except Exception:
                pass

        if 'telegram_enabled' in rows and rows['telegram_enabled'] is not None:
            val = str(rows['telegram_enabled']).strip().lower()
            enabled = val in ('1', 'true', 'yes')
        if 'telegram_bot_token' in rows and rows['telegram_bot_token'] is not None:
            token = str(rows['telegram_bot_token']).strip()
        if 'telegram_chat_ids' in rows and rows['telegram_chat_ids'] is not None:
            chat_entries = parse_telegram_chats(rows['telegram_chat_ids'])

        # Fallbacks auf Umgebungsvariablen nur, falls noch gar nichts konfiguriert wurde
        if not token and 'telegram_bot_token' not in rows:
            token = os.getenv("TELEGRAM_TOKEN", "").strip()
        if not chat_entries and 'telegram_chat_ids' not in rows:
            chat_entries = parse_telegram_chats(os.getenv("TELEGRAM_CHAT_ID", "").strip())

        if not enabled or not token or not chat_entries:
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        for entry in chat_entries:
            chat_id = entry.get("chat_id")
            if not chat_id:
                continue
            try:
                requests.post(url, data={'chat_id': chat_id, 'text': text}, timeout=5)
            except Exception as e:
                print(f"Telegram error for chat_id {entry.get('name', chat_id)}: {e}")
    except Exception as e:
        print(f"Telegram send error: {e}")



def send_telegram_msg(text):
    threading.Thread(target=_send_telegram_msg_async, args=(text,), daemon=True).start()

def generate_slug(display_name, pin):
    clean_name = re.sub(r'[^a-z0-9]+', '-', display_name.lower()).strip('-')
    return f"{clean_name}_{pin}"


def calculate_score(team_data, config):
    if team_data.get("role") == "mister_x":
        return 0
    raw_score = (len(team_data.get("caught", [])) * config.get("points_caught_team", 1)) + team_data.get("manual_points", 0)
    return int(raw_score) if raw_score == int(raw_score) else float(raw_score)


def save_state(conn, state):
    c = conn.cursor()

    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('config', ?)", (json.dumps(state['config']),))
    if 'active_stops_dataset' in state:
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_stops_dataset', ?)", (state['active_stops_dataset'],))

    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('telegram_enabled', ?)", ('1' if state['config'].get('telegram_enabled') else '0',))
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('telegram_bot_token', ?)", (str(state['config'].get('telegram_bot_token', '')),))
    chat_ids_val = state['config'].get('telegram_chat_ids', [])
    if isinstance(chat_ids_val, list):
        chat_ids_serialized = json.dumps(chat_ids_val, ensure_ascii=False)
    else:
        chat_ids_serialized = json.dumps(parse_telegram_chats(chat_ids_val), ensure_ascii=False)
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('telegram_chat_ids', ?)", (chat_ids_serialized,))

    # teams: INSERT OR REPLACE INTO teams und DELETE nur für gelöschte Teams
    team_ids = list(state.get('teams_data', {}).keys())
    for t_id, data in state.get('teams_data', {}).items():
        c.execute("""
            INSERT OR REPLACE INTO teams (id, display_name, pin, role, next_role, slug, deactivated, manual_points)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            t_id, data['display_name'], data['pin'], data.get('role', 'fänger'),
            data.get('next_role', ''), data['slug'],
            1 if data.get('deactivated') else 0, data.get('manual_points', 0)
        ))
    if team_ids:
        placeholders = ','.join('?' for _ in team_ids)
        c.execute(f"DELETE FROM teams WHERE id NOT IN ({placeholders})", tuple(team_ids))
    else:
        c.execute("DELETE FROM teams")

    desired_catches = []
    for t_id, data in state.get('teams_data', {}).items():
        for entry in data.get('catch_timestamps', []):
            p_id = entry[0]
            sub = entry[1]
            act = entry[2] if len(entry) > 2 else sub
            act_id = entry[3] if len(entry) > 3 else None
            desired_catches.append((t_id, p_id, sub, act, act_id))

    c.execute("SELECT hunter_id, prey_id, submitted_at, actual_timestamp, action_id FROM team_catches")
    existing_catches = c.fetchall()
    existing_catch_action_ids = {r[4] for r in existing_catches if r[4]}
    desired_catch_action_ids = {item[4] for item in desired_catches if item[4]}

    if desired_catch_action_ids:
        placeholders = ','.join('?' for _ in desired_catch_action_ids)
        c.execute(f"DELETE FROM team_catches WHERE action_id IS NOT NULL AND action_id NOT IN ({placeholders})", tuple(desired_catch_action_ids))
    elif existing_catch_action_ids:
        c.execute("DELETE FROM team_catches WHERE action_id IS NOT NULL")

    for h_id, p_id, sub, act, act_id in desired_catches:
        if act_id:
            if act_id not in existing_catch_action_ids:
                c.execute("""
                    INSERT INTO team_catches (hunter_id, prey_id, submitted_at, actual_timestamp, action_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (h_id, p_id, sub, act, act_id))
        else:
            exists = any(r[0] == h_id and r[1] == p_id and r[2] == sub for r in existing_catches)
            if not exists:
                c.execute("""
                    INSERT INTO team_catches (hunter_id, prey_id, submitted_at, actual_timestamp, action_id)
                    VALUES (?, ?, ?, ?, NULL)
                """, (h_id, p_id, sub, act))

    # pending_requests: Gezielt INSERT OR REPLACE und DELETE
    req_ids = [r['id'] for r in state.get('pending_requests', [])]
    for req in state.get('pending_requests', []):
        c.execute("""
            INSERT OR REPLACE INTO pending_requests (
                id, type, team, team_name, stop, reporter, reporter_name,
                hunter, hunter_name, prey, prey_name, timestamp,
                is_late, gps_check_until, notified_admin,
                detected_actual_timestamp, detected_distance
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            req['id'], req['type'], req.get('team'), req.get('team_name'), req.get('stop'),
            req.get('reporter'), req.get('reporter_name'), req.get('hunter'), req.get('hunter_name'),
            req.get('prey'), req.get('prey_name'), req.get('timestamp'),
            1 if req.get('is_late') else 0, req.get('gps_check_until'),
            1 if req.get('notified_admin') else 0,
            req.get('detected_actual_timestamp'), req.get('detected_distance')
        ))
    if req_ids:
        placeholders = ','.join('?' for _ in req_ids)
        c.execute(f"DELETE FROM pending_requests WHERE id NOT IN ({placeholders})", tuple(req_ids))
    else:
        c.execute("DELETE FROM pending_requests")

    # action_logs: Gezielt INSERT OR REPLACE und DELETE
    log_ids = [l['id'] for l in state.get('action_log', [])]
    for log in state.get('action_log', []):
        c.execute("""
            INSERT OR REPLACE INTO action_logs (
                id, type, team, team_name, stop, hunter, hunter_name,
                prey, prey_name, timestamp, actual_timestamp, desc, is_late, admin_only
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            log['id'], log['type'], log.get('team'), log.get('team_name'), log.get('stop'),
            log.get('hunter'), log.get('hunter_name'), log.get('prey'), log.get('prey_name'),
            log.get('timestamp'), log.get('actual_timestamp'), log.get('desc'),
            1 if log.get('is_late') else 0, 1 if log.get('admin_only') else 0
        ))
    if log_ids:
        placeholders = ','.join('?' for _ in log_ids)
        c.execute(f"DELETE FROM action_logs WHERE id NOT IN ({placeholders})", tuple(log_ids))
    else:
        c.execute("DELETE FROM action_logs")

    conn.commit()



def get_state(conn):
    c = conn.cursor()
    c.execute("SELECT key, value FROM settings WHERE key != 'admin_slug'")
    rows = c.fetchall()
    settings_dict = {row['key']: row['value'] for row in rows}

    if not rows or 'config' not in settings_dict:
        load_active_stops_data("Catch_Haltestellen.json")
        config = DEFAULT_CONFIG.copy()

        teams_data = {}
        # Initial: 1 Mister X und 3 Fänger-Teams
        mister_id = "mister-1"
        mister_pin = f"{random.randint(0, 9999):04d}"
        teams_data[mister_id] = {
            "display_name": "Mister 1",
            "stops": [], "stop_timestamps": [], "caught": [], "catch_timestamps": [], "caught_by": [],
            "pin": mister_pin, "role": "mister_x", "next_role": "",
            "slug": generate_slug(mister_id, mister_pin), "deactivated": False, "manual_points": 0
        }
        for i in range(1, 4):
            t_id = f"Team {i}"
            pin = f"{random.randint(0, 9999):04d}"
            teams_data[t_id] = {
                "display_name": t_id,
                "stops": [], "stop_timestamps": [], "caught": [], "catch_timestamps": [], "caught_by": [],
                "pin": pin, "role": "fänger", "next_role": "",
                "slug": generate_slug(t_id, pin), "deactivated": False, "manual_points": 0
            }

        state = {
            "config": config,
            "active_stops_dataset": "Catch_Haltestellen.json",
            "teams_data": teams_data,
            "pending_requests": [],
            "action_log": []
        }
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_stops_dataset', ?)", ("Catch_Haltestellen.json",))
        save_state(conn, state)
        return state

    state = {}
    settings_dict = {row['key']: row['value'] for row in rows}
    raw_cfg = json.loads(settings_dict.get('config', '{}'))
    obsolete_cfg_keys = {
        "target_stops", "max_history", "num_bonus_stops", "points_stop_reached",
        "points_was_caught", "auto_approve_stops", "stop_radius_meters",
        "auto_calculate_stops", "shuffle_stops_on_swap", "enable_mid_game_swap"
    }
    state['config'] = {k: v for k, v in raw_cfg.items() if k not in obsolete_cfg_keys}
    state['active_stops_dataset'] = settings_dict.get('active_stops_dataset', 'Catch_Haltestellen.json')

    if current_active_dataset != state['active_stops_dataset'] or not stops_data:
        load_active_stops_data(state['active_stops_dataset'])

    state['teams_data'] = {}
    c.execute("SELECT * FROM teams")
    for t in c.fetchall():
        t_id = t['id']
        raw_role = t['role']
        if raw_role == "läufer" or t_id.startswith("mister"):
            role = "mister_x"
        elif raw_role == "fänger":
            role = "fänger"
        else:
            role = "mister_x" if t_id.startswith("mister") else "fänger"

        state['teams_data'][t_id] = {
            'display_name': t['display_name'],
            'pin': t['pin'],
            'role': role,
            'next_role': '',
            'slug': t['slug'],
            'deactivated': bool(t['deactivated']),
            'manual_points': t['manual_points'],
            'stops': [],
            'stop_timestamps': [],
            'caught': [],
            'catch_timestamps': [],
            'caught_by': []
        }

    state['config'].setdefault('first_broadcast_time', '')
    state['config'].setdefault('broadcast_interval_minutes', 15)
    state['config'].setdefault('points_caught_team', 1)

    if not any(d.get('role') == 'mister_x' for d in state['teams_data'].values()):
        mister_id = "mister-1"
        mister_pin = f"{random.randint(0, 9999):04d}"
        state['teams_data'][mister_id] = {
            "display_name": "Mister 1",
            "stops": [], "stop_timestamps": [], "caught": [], "catch_timestamps": [], "caught_by": [],
            "pin": mister_pin, "role": "mister_x", "next_role": "",
            "slug": generate_slug(mister_id, mister_pin), "deactivated": False, "manual_points": 0
        }

    c.execute("SELECT hunter_id, prey_id, submitted_at, actual_timestamp, action_id FROM team_catches")
    for r in c.fetchall():
        h_id, p_id = r['hunter_id'], r['prey_id']
        if h_id in state['teams_data']:
            state['teams_data'][h_id]['caught'].append(p_id)
            if r['submitted_at'] is not None or r['actual_timestamp'] is not None or r['action_id'] is not None:
                state['teams_data'][h_id]['catch_timestamps'].append(
                    (p_id, r['submitted_at'], r['actual_timestamp'], r['action_id'])
                )
        if p_id in state['teams_data']:
            state['teams_data'][p_id]['caught_by'].append(h_id)

    state['pending_requests'] = []
    c.execute("SELECT * FROM pending_requests")
    for r in c.fetchall():
        req = dict(r)
        req['is_late'] = bool(req.get('is_late'))
        req['notified_admin'] = bool(req.get('notified_admin'))
        state['pending_requests'].append({k: v for k, v in req.items() if v is not None})

    state['action_log'] = []
    c.execute("SELECT * FROM action_logs ORDER BY COALESCE(actual_timestamp, timestamp) DESC")
    for r in c.fetchall():
        log = dict(r)
        log['is_late'] = bool(log.get('is_late'))
        log['admin_only'] = bool(log.get('admin_only'))
        d = {k: v for k, v in log.items() if v is not None}
        d['actual_timestamp'] = log.get('actual_timestamp')
        state['action_log'].append(d)

    for k, v in DEFAULT_CONFIG.items():
        if k not in state["config"]:
            state["config"][k] = v

    if "telegram_enabled" in settings_dict and "telegram_enabled" not in state["config"]:
        v = str(settings_dict["telegram_enabled"]).strip().lower()
        state["config"]["telegram_enabled"] = v in ('1', 'true', 'yes')
    if "telegram_bot_token" in settings_dict and "telegram_bot_token" not in state["config"]:
        state["config"]["telegram_bot_token"] = settings_dict["telegram_bot_token"]
    if "telegram_chat_ids" in settings_dict and "telegram_chat_ids" not in state["config"]:
        state["config"]["telegram_chat_ids"] = settings_dict["telegram_chat_ids"]

    return state

# --- ZEIT- UND PHASENLOGIK ---
def update_game_time_state(conn, state):
    config = state["config"]
    tz_str = config.get("timezone", "Europe/Berlin")
    tz = ZoneInfo(tz_str)
    now = datetime.now(tz)

    start_str = config.get("start_time", "")
    end_str = config.get("end_time", "")

    if not start_str or not end_str:
        return "inactive", None, None, None, now.isoformat()

    try:
        start_dt = datetime.fromisoformat(start_str)
        end_dt = datetime.fromisoformat(end_str)

        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=tz)
        else:
            start_dt = start_dt.astimezone(tz)

        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=tz)
        else:
            end_dt = end_dt.astimezone(tz)

    except ValueError:
        return "inactive", None, None, None, now.isoformat()

    if now < start_dt:
        phase = "inactive"
    elif start_dt <= now <= end_dt:
        phase = "game"
    else:
        phase = "finished"

    return phase, start_dt.isoformat(), end_dt.isoformat(), None, now.isoformat()



def execute_team_catch(state, hunter_id, prey_id, is_late=False, submitted_ts=None, actual_ts=None, action_id=None, conn=None):
    now_iso = datetime.now(timezone.utc).isoformat()
    sub_iso = submitted_ts or now_iso
    eff_act_iso = actual_ts or sub_iso
    if not action_id:
        action_id = str(uuid.uuid4())

    if hunter_id in state['teams_data'] and prey_id in state['teams_data']:
        state['teams_data'][hunter_id]['caught'].append(prey_id)
        state['teams_data'][hunter_id].setdefault('catch_timestamps', []).append((prey_id, sub_iso, eff_act_iso, action_id))
        state['teams_data'][prey_id]['caught_by'].append(hunter_id)

        hunter_name = state['teams_data'][hunter_id]['display_name']
        prey_name = state['teams_data'][prey_id]['display_name']

        # Haltestelle aus dem letzten Broadcast von prey_id ermitteln
        last_station = None
        if conn:
            try:
                c = conn.cursor()
                c.execute("SELECT station FROM mister_broadcasts WHERE team_id = ? ORDER BY id DESC LIMIT 1", (prey_id,))
                b_row = c.fetchone()
                if b_row and b_row["station"]:
                    last_station = b_row["station"]
            except Exception:
                pass

        if last_station:
            desc = f"⚔️ {hunter_name} hat Mister X bei der Haltestelle {last_station} gefangen"
        else:
            desc = f"⚔️ {hunter_name} hat Mister X gefangen"

        if is_late:
            desc += " (Nachtrag nach Spielende)"

        state["action_log"].insert(0, {
            "id": action_id, "type": "catch", "hunter": hunter_id, "prey": prey_id,
            "hunter_name": hunter_name, "prey_name": prey_name,
            "timestamp": sub_iso, "actual_timestamp": eff_act_iso, "desc": desc, "is_late": is_late,
            "admin_only": False
        })

        if conn:
            try:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO team_catches (hunter_id, prey_id, submitted_at, actual_timestamp, action_id)
                    VALUES (?, ?, ?, ?, ?)
                """, (hunter_id, prey_id, sub_iso, eff_act_iso, action_id))
            except Exception:
                pass

        send_telegram_msg(desc)

        write_debug_log("game_actions", {
            "action": "team_catch",
            "hunter": hunter_id,
            "prey": prey_id,
            "hunter_name": hunter_name,
            "prey_name": prey_name,
            "is_late": is_late,
            "submitted_ts": sub_iso,
            "actual_ts": eff_act_iso
        })
        return True
    return False


# --- ROUTEN ---
@app.route('/')
def index():
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

    config = state["config"]
    leaderboard = []
    teams_list = []

    for t_id, d in state["teams_data"].items():
        if d.get("deactivated", False):
            continue

        teams_list.append({"id": t_id, "name": d["display_name"], "role": d.get("role", "fänger")})
        if d.get("role") == "fänger":
            score = calculate_score(d, config)
            leaderboard.append({
                "id": t_id, "display_name": d["display_name"], "score": score,
                "role": "fänger"
            })

    leaderboard.sort(key=lambda x: (-x["score"], x["display_name"].lower()))
    teams_list.sort(key=lambda x: x["name"].lower())

    public_log = [
        e for e in state.get("action_log", [])
        if not e.get("admin_only") and e.get("type") not in ("broadcast", "broadcast_update", "mister_broadcast")
    ]

    return render_template('index.html', teams=teams_list, leaderboard=leaderboard,
                           phase=phase, start_time=start_iso, end_time=end_iso,
                           mid_time=mid_iso, now_time=now_iso,
                           action_log=public_log)


@app.route('/rules')
@app.route('/rules.html')
def rules():
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
    return render_template('rules.html', config=state["config"])


@app.route('/team/<slug>')
def team_dashboard(slug):
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

    config = state["config"]
    team_id = None
    team_data = None
    for t_id, d in state["teams_data"].items():
        if d.get("slug") == slug:
            team_id = t_id
            team_data = d
            break

    if not team_id:
        return "Team nicht gefunden", 404

    role = team_data.get("role", "keine")
    next_role = team_data.get("next_role", "")
    is_deactivated = team_data.get("deactivated", False)

    leaderboard = []
    mister_x_teams = []
    faenger_teams = []

    for t_id_iter, d in state["teams_data"].items():
        if d.get("deactivated", False):
            continue

        t_role = d.get("role", "fänger")
        if t_role in ("mister_x", "läufer") and t_id_iter != team_id:
            mister_x_teams.append({"id": t_id_iter, "name": d["display_name"]})
        elif t_role == "fänger" and t_id_iter != team_id:
            faenger_teams.append({"id": t_id_iter, "name": d["display_name"]})

        if t_role == "fänger":
            score = calculate_score(d, config)
            leaderboard.append({
                "id": t_id_iter, "display_name": d["display_name"], "score": score,
                "role": "fänger"
            })

    leaderboard.sort(key=lambda x: (-x["score"], x["display_name"].lower()))

    last_caught_mx = None
    mister_x_interval = None
    c = conn.cursor()
    if role == "fänger":
        c.execute("SELECT prey_id FROM team_catches WHERE hunter_id = ? ORDER BY rowid DESC LIMIT 1", (team_id,))
        lc_row = c.fetchone()
        if lc_row:
            last_caught_mx = lc_row["prey_id"]
    elif role == "mister_x":
        now_utc = datetime.now(timezone.utc)
        mister_x_interval = get_mister_x_interval_info(config, team_id, conn, now_utc)

    team_log = [
        entry for entry in state["action_log"]
        if not entry.get("admin_only") and entry.get("type") not in ("broadcast", "broadcast_update", "mister_broadcast") and (
            entry.get("team") == team_id or entry.get("hunter") == team_id or entry.get("prey") == team_id or entry.get("type") == "system"
        )
    ]

    host_ip = request.host.split(':')[0]

    my_pending_requests = [
        r for r in state.get("pending_requests", [])
        if (r.get("type") == "stop" and r.get("team") == team_id) or (r.get("type") == "catch" and r.get("reporter") == team_id)
    ]

    open_opponent_catches = [
        r for r in state.get("pending_requests", [])
        if r.get("type") == "catch" and r.get("prey") == team_id and r.get("reporter") != team_id
    ]

    return render_template('team.html', team_id=team_id, team_name=team_data["display_name"],
                           role=role, slug=slug,
                           laeufer_teams=mister_x_teams, mister_x_teams=mister_x_teams, faenger_teams=faenger_teams,
                           last_caught_mx=last_caught_mx, mister_x_interval=mister_x_interval,
                           my_last_broadcast=mister_x_interval.get("last_broadcast") if mister_x_interval else None,
                           geofence_outer=GEOFENCE_OUTER,
                           leaderboard=leaderboard, log=team_log, phase=phase,
                           start_time=start_iso, end_time=end_iso, mid_time=mid_iso,
                           now_time=now_iso, is_deactivated=is_deactivated, host_ip=host_ip,
                           my_pending_requests=my_pending_requests, open_opponent_catches=open_opponent_catches)


@app.route('/api/team_locations')
def api_team_locations():
    """
    Liefert die für das anfragende Team sichtbaren Live-Standorte sowie den GPS-Warnstatus:
    - Eigenes Team: Immer sichtbar (solange Karte aktiv).
    - Fänger: Sehen zusätzlich ab 5 Minuten vor Spielstart (pre_game, game, post_game) die Standorte aller Läufer.
      (Bei Rollentausch wird die aktuelle Rolle aus der DB geprüft, sodass ehemalige Fänger sofort keine Läufer mehr sehen).
    - GPS-Warnung: Wenn das Team (Läufer oder Fänger) seit > 2 Minuten keinen Standort mehr gesendet hat.
    """
    team_slug = request.args.get('slug')
    if not team_slug:
        return jsonify({'success': False, 'error': 'Slug fehlt'}), 400

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, _, _, now_iso = update_game_time_state(conn, state)

        my_team_id = None
        my_team_data = None
        for t_id, d in state["teams_data"].items():
            if d.get("slug") == team_slug:
                my_team_id = t_id
                my_team_data = d
                break

        if not my_team_id:
            return jsonify({'success': False, 'error': 'Team nicht gefunden'}), 404

        my_role = my_team_data.get("role", "keine")

        now_utc = datetime.now(timezone.utc)
        c = conn.cursor()
        c.execute("SELECT team_id, lat, lon, updated_at, accuracy FROM team_locations")
        all_locs = {r["team_id"]: r for r in c.fetchall()}

        visible_locations = []
        my_loc = all_locs.get(my_team_id)
        if my_loc and my_loc["lat"] is not None and my_loc["lon"] is not None:
            dt = parse_iso_utc(my_loc["updated_at"])
            ts_epoch = dt.timestamp() if dt else time.time()
            visible_locations.append({
                "id": my_team_id,
                "name": my_team_data["display_name"],
                "role": my_role,
                "lat": my_loc["lat"],
                "lon": my_loc["lon"],
                "updated_at": ts_epoch,
                "accuracy": my_loc["accuracy"],
                "is_self": True
            })

        if my_role == "fänger":
            tz = ZoneInfo(state["config"].get("timezone", "Europe/Berlin"))
            intervals = get_all_broadcast_intervals(state["config"])
            c.execute("SELECT prey_id FROM team_catches WHERE hunter_id = ? ORDER BY rowid DESC LIMIT 1", (my_team_id,))
            lc_row = c.fetchone()
            last_caught_mx = lc_row["prey_id"] if lc_row else (my_team_data.get("caught")[-1] if my_team_data.get("caught") else None)

            for t_id, t_data in state["teams_data"].items():
                if t_data.get("role") == "mister_x" and not t_data.get("deactivated", False):
                    c.execute("""
                        SELECT id, interval_iso, published_at, lat, lon, line, direction, station, is_manual_coords, is_auto
                        FROM mister_broadcasts
                        WHERE team_id = ?
                        ORDER BY id DESC LIMIT 1
                    """, (t_id,))
                    b_row = c.fetchone()
                    if b_row and b_row["lat"] is not None and b_row["lon"] is not None:
                        pub_dt = parse_iso_utc(b_row["published_at"])
                        pub_ts = pub_dt.timestamp() if pub_dt else time.time()
                        int_dt = parse_iso_utc(b_row["interval_iso"])
                        int_str = format_broadcast_display_time(
                            b_row["interval_iso"], b_row["published_at"], b_row["is_auto"], tz
                        )

                        next_int_dt = None
                        if int_dt:
                            for Tk in intervals:
                                if Tk > int_dt:
                                    next_int_dt = Tk
                                    break

                        next_int_ts = next_int_dt.timestamp() if next_int_dt else None
                        next_int_str = next_int_dt.astimezone(tz).strftime("%H:%M") if next_int_dt else ""

                        visible_locations.append({
                            "id": t_id,
                            "name": "Mister X",
                            "role": "mister_x",
                            "lat": b_row["lat"],
                            "lon": b_row["lon"],
                            "updated_at": pub_ts,
                            "interval_time_str": int_str,
                            "next_interval_ts": next_int_ts,
                            "next_interval_time_str": next_int_str,
                            "line": b_row["line"] or "",
                            "direction": b_row["direction"] or "",
                            "station": b_row["station"] or "",
                            "is_manual_coords": bool(b_row["is_manual_coords"]),
                            "is_blocked": (t_id == last_caught_mx),
                            "is_self": False
                        })

        c.execute("SELECT id FROM geofence_violations WHERE team_id = ? AND violation_until IS NULL", (my_team_id,))
        geofence_violation_active = (c.fetchone() is not None)

        mister_x_interval = None
        my_last_broadcast = None
        if my_role == "mister_x":
            mister_x_interval = get_mister_x_interval_info(state["config"], my_team_id, conn, now_utc)
            my_last_broadcast = mister_x_interval.get("last_broadcast")

    enable_gf = bool(state["config"].get("enable_geofences", False))
    return jsonify({
        "success": True,
        "role": my_role,
        "phase": phase,
        "locations": visible_locations,
        "mister_x_interval": mister_x_interval,
        "my_last_broadcast": my_last_broadcast,
        "geofence_violation_active": geofence_violation_active,
        "geofence_outer": GEOFENCE_OUTER,
        "geofence_exclusion": GEOFENCE_EXCLUSION if enable_gf else []
    })


@app.route('/api/mister_x_submit', methods=['POST'])
@app.route('/api/mister_x_submit_interval', methods=['POST'])
def api_mister_x_submit_interval():
    data = request.get_json(silent=True) or {}
    slug = data.get('slug')
    line = str(data.get('line', '')).replace('<', '').replace('>', '').strip()
    direction = str(data.get('direction', '')).replace('<', '').replace('>', '').strip()
    station = str(data.get('station', '')).replace('<', '').replace('>', '').strip()
    manual_lat = data.get('manual_lat')
    manual_lon = data.get('manual_lon')

    if not slug:
        return jsonify({'success': False, 'error': 'Slug fehlt'}), 400

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, _, _, _, _ = update_game_time_state(conn, state)

        team_id = None
        team_data = None
        for t_id, d in state["teams_data"].items():
            if d.get("slug") == slug:
                team_id = t_id
                team_data = d
                break

        if not team_id:
            return jsonify({'success': False, 'error': 'Team nicht gefunden'}), 404

        if team_data.get("role") != "mister_x":
            return jsonify({'success': False, 'error': 'Nur für Mister X'}), 403

        if team_data.get("deactivated", False):
            return jsonify({'success': False, 'error': 'Team ist deaktiviert'}), 403

        if phase != "game":
            return jsonify({'success': False, 'error': 'Eingabe nur während des laufenden Spiels möglich'}), 400

        now_utc = datetime.now(timezone.utc)
        info = get_mister_x_interval_info(state["config"], team_id, conn, now_utc)

        target_iso = info.get("target_interval_iso")
        if not target_iso:
            return jsonify({'success': False, 'error': 'Kein aktives Intervall gefunden'}), 400

        if not info.get("can_submit"):
            return jsonify({
                'success': False,
                'error': 'Für diesen Zeitpunkt wurde der Standort bereits übermittelt. Bitte auf das nächste Meldefenster warten.'
            }), 400

        window_type = info.get("window_type")
        c = conn.cursor()

        if window_type == "active_window":
            publish_mister_x_broadcast(
                conn, state, team_id, target_iso,
                line=line, direction=direction, station=station,
                manual_lat=manual_lat, manual_lon=manual_lon,
                is_auto=0
            )
            save_state(conn, state)
            return jsonify({
                'success': True,
                'status': 'broadcast_published',
                'message': f'Standort für {info.get("target_interval_time_str")} Uhr wurde SOFORT veröffentlicht!',
                'interval_info': get_mister_x_interval_info(state["config"], team_id, conn, now_utc)
            })
        elif window_type == "late_submission":
            c.execute("SELECT id FROM mister_broadcasts WHERE team_id = ? AND interval_iso = ?", (team_id, target_iso))
            b_row = c.fetchone()
            if b_row:
                update_mister_x_broadcast(
                    conn, state, b_row["id"],
                    line=line, direction=direction, station=station,
                    manual_lat=manual_lat, manual_lon=manual_lon
                )
            else:
                publish_mister_x_broadcast(
                    conn, state, team_id, target_iso,
                    line=line, direction=direction, station=station,
                    manual_lat=manual_lat, manual_lon=manual_lon,
                    is_auto=0
                )
            save_state(conn, state)
            return jsonify({
                'success': True,
                'status': 'broadcast_updated',
                'message': f'Infos für {info.get("target_interval_time_str")} Uhr erfolgreich nachgereicht!',
                'interval_info': get_mister_x_interval_info(state["config"], team_id, conn, now_utc)
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Das Meldefenster ist aktuell nicht geöffnet.'
            }), 400


@app.route('/api/mister_x_interval_status')
def api_mister_x_interval_status():
    slug = request.args.get('slug')
    if not slug:
        return jsonify({'success': False, 'error': 'Slug fehlt'}), 400

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        team_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == slug), None)
        if not team_id:
            return jsonify({'success': False, 'error': 'Team nicht gefunden'}), 404

        now_utc = datetime.now(timezone.utc)
        info = get_mister_x_interval_info(state["config"], team_id, conn, now_utc)
        return jsonify({'success': True, 'info': info})


@app.route('/api/login_team', methods=['POST'])
def login_team():
    data = request.json or {}
    team_id = data.get('team_id')
    pin = data.get('pin')
    ip = request.remote_addr or '127.0.0.1'
    target = str(team_id) if team_id else 'unknown'

    if login_rate_limiter.is_blocked(ip, target):
        return jsonify({'success': False, 'error': 'Zu viele Fehlversuche. Bitte warte 5 Minuten.'}), 429

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)

    team_data = state["teams_data"].get(team_id)

    if team_data and str(team_data.get("pin")) == str(pin).strip():
        login_rate_limiter.record_success(ip, target)
        return jsonify({'success': True, 'slug': team_data["slug"]})

    login_rate_limiter.record_failure(ip, target)
    return jsonify({'success': False, 'error': 'Falscher PIN oder Team nicht gefunden!'})


@app.route('/api/login_admin', methods=['POST'])
def login_admin():
    data = request.json or {}
    pw = str(data.get('password', '')).strip()
    ip = request.remote_addr or '127.0.0.1'
    target = 'admin'

    if login_rate_limiter.is_blocked(ip, target):
        return jsonify({'success': False, 'error': 'Zu viele Fehlversuche. Bitte warte 5 Minuten.'}), 429

    if pw == ADMIN_PASSWORD:
        login_rate_limiter.record_success(ip, target)
        return jsonify({'success': True, 'redirect_url': f"/adminconsole-{ADMIN_SLUG}"})

    login_rate_limiter.record_failure(ip, target)
    return jsonify({'success': False, 'error': 'Falsches Admin-Passwort!'})


def compute_admin_leaderboard(conn, state, start_iso, end_iso):
    config = state["config"]
    tz = ZoneInfo(config.get("timezone", "Europe/Berlin"))
    now_utc = datetime.now(timezone.utc)

    start_dt_utc = parse_iso_utc(start_iso)
    game_end_utc = parse_iso_utc(end_iso)
    pre_start_utc = start_dt_utc - timedelta(minutes=5) if start_dt_utc else None

    c = conn.cursor()
    c.execute("SELECT team_id, offline_from, offline_until FROM gps_outages")
    all_outages = c.fetchall()

    c.execute("SELECT team_id, violation_from, violation_until FROM geofence_violations")
    all_violations = c.fetchall()

    c.execute("""
        SELECT team_id, lat, lon, updated_at, batteryLevel, charge, motion, altitude, speed, course, accuracy
        FROM team_locations
    """)
    locations_by_team = {row["team_id"]: dict(row) for row in c.fetchall()}

    leaderboard = []
    for t_id, data in state["teams_data"].items():
        score = calculate_score(data, config)

        total_offline_seconds = 0
        formatted_offline_warnings = []
        team_outages = [o for o in all_outages if o["team_id"] == t_id]

        for outage in team_outages:
            o_start = parse_iso_utc(outage["offline_from"])
            o_end = parse_iso_utc(outage["offline_until"]) or (game_end_utc if (game_end_utc and now_utc > game_end_utc) else now_utc)

            if not o_start or not o_end:
                continue

            if pre_start_utc and o_start < pre_start_utc:
                o_start = pre_start_utc
            if game_end_utc and o_end > game_end_utc:
                o_end = game_end_utc

            if o_start < o_end:
                total_offline_seconds += (o_end - o_start).total_seconds()
                formatted_offline_warnings.append({
                    "start_time_str": o_start.astimezone(tz).strftime("%H:%M"),
                    "end_time_str": o_end.astimezone(tz).strftime("%H:%M") if outage["offline_until"] else None
                })

        total_violation_seconds = 0
        formatted_violations = []
        team_violations = [v for v in all_violations if v["team_id"] == t_id]

        for v in team_violations:
            v_start = parse_iso_utc(v["violation_from"])
            v_end = parse_iso_utc(v["violation_until"]) or (game_end_utc if (game_end_utc and now_utc > game_end_utc) else now_utc)

            if not v_start or not v_end:
                continue

            if pre_start_utc and v_start < pre_start_utc:
                v_start = pre_start_utc
            if game_end_utc and v_end > game_end_utc:
                v_end = game_end_utc

            if v_start < v_end:
                total_violation_seconds += (v_end - v_start).total_seconds()
                formatted_violations.append({
                    "start_time_str": v_start.astimezone(tz).strftime("%H:%M"),
                    "end_time_str": v_end.astimezone(tz).strftime("%H:%M") if v["violation_until"] else None
                })

        loc_row = locations_by_team.get(t_id)
        telemetry = None
        if loc_row:
            has_telemetry = any(loc_row.get(k) is not None for k in [
                'batteryLevel', 'charge', 'motion', 'altitude', 'speed', 'course', 'accuracy'
            ])
            if has_telemetry:
                telemetry = {
                    "batteryLevel": loc_row.get('batteryLevel'),
                    "charge": loc_row.get('charge'),
                    "motion": loc_row.get('motion'),
                    "altitude": loc_row.get('altitude'),
                    "speed": loc_row.get('speed'),
                    "course": loc_row.get('course'),
                    "accuracy": loc_row.get('accuracy'),
                    "updated_at": loc_row.get('updated_at')
                }

        leaderboard.append({
            "id": t_id, "display_name": data["display_name"], "score": score, "role": data.get("role", "keine"),
            "next_role": data.get("next_role", ""),
            "stops": data.get("stops", []), "caught": data.get("caught", []), "caught_by": data.get("caught_by", []),
            "deactivated": data.get("deactivated", False),
            "manual_points": data.get("manual_points", 0),
            "total_offline_seconds": total_offline_seconds,
            "offline_warnings": formatted_offline_warnings,
            "total_violation_seconds": total_violation_seconds,
            "geofence_violations": formatted_violations,
            "telemetry": telemetry
        })

    leaderboard.sort(key=lambda x: x["score"], reverse=True)
    return leaderboard


@app.route('/adminconsole-<admin_slug>')
def admin(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)
        leaderboard = compute_admin_leaderboard(conn, state, start_iso, end_iso)

    config = state["config"]
    team_urls = {}
    teams_list = []
    mister_x_teams = []
    faenger_teams = []
    base_url = request.host_url.rstrip('/')
    team_names_map = {t_id: d["display_name"] for t_id, d in state["teams_data"].items()}

    for t_id, data in state["teams_data"].items():
        team_urls[data["display_name"]] = f"{base_url}/team/{data['slug']}"
        t_entry = {"id": t_id, "name": data["display_name"], "role": data.get("role", "fänger")}
        teams_list.append(t_entry)
        if data.get("role") == "mister_x":
            mister_x_teams.append(t_entry)
        else:
            faenger_teams.append(t_entry)

    teams_list.sort(key=lambda x: x["name"].lower())
    mister_x_teams.sort(key=lambda x: x["name"].lower())
    faenger_teams.sort(key=lambda x: x["name"].lower())

    return render_template('admin.html', teams=teams_list,
                           mister_x_teams=mister_x_teams, faenger_teams=faenger_teams,
                           leaderboard=leaderboard, admin_url=ADMIN_SLUG, team_urls=team_urls,
                           pending_requests=state["pending_requests"], action_log=state["action_log"],
                           phase=phase, start_time=start_iso, end_time=end_iso, mid_time=mid_iso,
                           now_time=now_iso, team_names_map=team_names_map,
                           geofence_outer=GEOFENCE_OUTER)


@app.route('/api/admin_data/<admin_slug>')
def api_admin_data(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)
        leaderboard = compute_admin_leaderboard(conn, state, start_iso, end_iso)

        c = conn.cursor()
        c.execute("""
            SELECT team_id, lat, lon, updated_at, batteryLevel, charge, motion, altitude, speed, course, accuracy
            FROM team_locations
        """)
        locations = c.fetchall()

        team_locs = []
        for loc in locations:
            t_id = loc['team_id']
            team_data = state["teams_data"].get(t_id)
            if team_data and not team_data.get("deactivated", False):
                dt = parse_iso_utc(loc['updated_at'])
                ts_epoch = dt.timestamp() if dt else time.time()
                team_locs.append({
                    "id": t_id,
                    "name": team_data["display_name"],
                    "role": team_data.get("role", "fänger"),
                    "lat": loc['lat'],
                    "lon": loc['lon'],
                    "updated_at": ts_epoch,
                    "batteryLevel": loc['batteryLevel'],
                    "charge": bool(loc['charge']) if loc['charge'] is not None else None,
                    "motion": bool(loc['motion']) if loc['motion'] is not None else None,
                    "altitude": loc['altitude'],
                    "speed": loc['speed'],
                    "course": loc['course'],
                    "accuracy": loc['accuracy']
                })

        location_hist = []

        config = state["config"]
        tz = ZoneInfo(config.get("timezone", "Europe/Berlin"))

        mister_broadcasts = []
        c.execute("""
            SELECT id, team_id, interval_iso, published_at, lat, lon, line, direction, station, is_manual_coords, is_auto
            FROM mister_broadcasts
            ORDER BY id ASC
        """)
        for b in c.fetchall():
            b_dict = dict(b)
            b_dict["interval_time_str"] = format_broadcast_display_time(
                b["interval_iso"], b["published_at"], b["is_auto"] if "is_auto" in b.keys() else 0, tz
            )
            mister_broadcasts.append(b_dict)

    team_names_map = {t_id: d["display_name"] for t_id, d in state["teams_data"].items()}
    enable_gf = bool(config.get("enable_geofences", False))

    return jsonify({
        'success': True,
        'pending_requests': state.get("pending_requests", []),
        'action_log': state.get("action_log", []),
        'phase': phase,
        'now_time': now_iso,
        'team_locations': team_locs,
        'location_history': location_hist,
        'mister_broadcasts': mister_broadcasts,
        'geofence_outer': GEOFENCE_OUTER,
        'geofence_exclusion': GEOFENCE_EXCLUSION if enable_gf else [],
        'leaderboard': leaderboard,
        'team_names_map': team_names_map
    })


@app.route('/adminconsole-<admin_slug>/settings', methods=['GET', 'POST'])
def admin_settings(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        config = state["config"]

        if request.method == 'GET':
            all_sorted = sorted(state["teams_data"].items(), key=lambda x: int(re.sub(r'[^0-9]', '', x[0]) or 999))
            mister_x_teams = [t for t in all_sorted if t[1].get('role') == 'mister_x' or t[0].startswith('mister')]
            hunter_teams = [t for t in all_sorted if t not in mister_x_teams]
            available_datasets = [f for f in sorted(os.listdir(STOPS_DIR)) if f.endswith('.json') and os.path.isfile(os.path.join(STOPS_DIR, f))]
            active_dataset = state.get("active_stops_dataset", "Catch_Haltestellen.json")
            if not available_datasets:
                available_datasets = [active_dataset]

            # Telegram-Daten für das Template maskieren (keine Klartext-Ausgabe!)
            raw_token = str(config.get("telegram_bot_token", "")).strip()
            has_telegram_token = bool(raw_token)
            masked_telegram_token = mask_secret_string(raw_token, 4, 4) if has_telegram_token else ""

            parsed_chats = parse_telegram_chats(config.get("telegram_chat_ids", []))
            # Falls neue UIDs beim Parsen alter Daten vergeben wurden, kurz im State festhalten
            if parsed_chats != config.get("telegram_chat_ids"):
                config["telegram_chat_ids"] = parsed_chats
                save_state(conn, state)

            masked_telegram_chats = [
                {
                    "uid": item["uid"],
                    "name": item["name"],
                    "masked_id": mask_secret_string(item["chat_id"], 2, 3)
                }
                for item in parsed_chats
            ]

            return render_template(
                'settings.html',
                config=config,
                has_telegram_token=has_telegram_token,
                masked_telegram_token=masked_telegram_token,
                masked_telegram_chats=masked_telegram_chats,
                mister_x_teams=mister_x_teams,
                hunter_teams=hunter_teams,
                sorted_teams=all_sorted,
                admin_url=ADMIN_SLUG,
                available_datasets=available_datasets,
                active_stops_dataset=active_dataset
            )

        data = request.json
        config["start_time"] = data.get("start_time", config.get("start_time"))
        config["end_time"] = data.get("end_time", config.get("end_time"))
        config["timezone"] = data.get("timezone", "Europe/Berlin")
        config["first_broadcast_time"] = data.get("first_broadcast_time", config.get("first_broadcast_time", ""))
        try:
            config["broadcast_interval_minutes"] = int(data.get("broadcast_interval_minutes", config.get("broadcast_interval_minutes", 15)))
        except (ValueError, TypeError):
            config["broadcast_interval_minutes"] = 15
        try:
            config["points_caught_team"] = int(data.get("points_caught_team", config.get("points_caught_team", 1)))
        except (ValueError, TypeError):
            config["points_caught_team"] = 1

        config["enable_geofences"] = bool(data.get("enable_geofences", config.get("enable_geofences", False)))
        config["telegram_enabled"] = bool(data.get("telegram_enabled", False))

        # 1. Telegram Bot-Token: Löschen, neu setzen oder bestehenden behalten
        if data.get("clear_telegram_token"):
            config["telegram_bot_token"] = ""
        else:
            new_token = str(data.get("new_telegram_bot_token", "")).strip()
            if new_token:
                config["telegram_bot_token"] = new_token

        # 2. Telegram Chat-IDs: Bestehende über UID filtern/umbenennen + neue hinzufügen
        existing_chats = parse_telegram_chats(config.get("telegram_chat_ids", []))
        existing_by_uid = {c["uid"]: c for c in existing_chats}

        kept_chats_payload = data.get("kept_telegram_chats")
        updated_chats = []
        if isinstance(kept_chats_payload, list):
            for item in kept_chats_payload:
                if isinstance(item, dict):
                    uid = str(item.get("uid", "")).strip()
                    if uid in existing_by_uid:
                        clean_name = str(item.get("name", "")).replace('<', '').replace('>', '').strip()
                        updated_chats.append({
                            "uid": uid,
                            "name": clean_name or existing_by_uid[uid]["name"],
                            "chat_id": existing_by_uid[uid]["chat_id"]
                        })
        else:
            updated_chats = existing_chats

        new_chats_payload = data.get("new_telegram_chats", [])
        if isinstance(new_chats_payload, list):
            for item in new_chats_payload:
                if isinstance(item, dict):
                    cid = str(item.get("chat_id", "")).replace('<', '').replace('>', '').strip()
                    cname = str(item.get("name", "")).replace('<', '').replace('>', '').strip()
                    if cid:
                        updated_chats.append({
                            "uid": uuid.uuid4().hex[:8],
                            "name": cname or f"Empfänger {len(updated_chats) + 1}",
                            "chat_id": cid
                        })

        config["telegram_chat_ids"] = updated_chats

        team_list = data.get("teams_list", [])
        old_teams_data = state.get("teams_data", {})
        new_teams_data = {}

        now_iso = datetime.now(timezone.utc).isoformat()
        for t in team_list:
            team_id = str(t["team_id"]).strip()
            display_name = str(t.get("new_name", "")).replace('<', '').replace('>', '').strip() or team_id
            role = str(t.get("role", "fänger")).strip().lower()
            if role not in ("mister_x", "fänger"):
                role = "mister_x" if "mister" in team_id.lower() else "fänger"
            pin = str(t.get("pin", "")).replace('<', '').replace('>', '').strip()
            active = bool(t.get("active", True))

            if team_id and team_id in old_teams_data:
                team_data = old_teams_data[team_id]
                old_role = team_data.get("role", "fänger")
                if role != old_role:
                    state["action_log"].insert(0, {
                        "id": str(uuid.uuid4()),
                        "type": "role_change",
                        "team": team_id,
                        "timestamp": now_iso,
                        "actual_timestamp": now_iso,
                        "desc": role,
                        "admin_only": True
                    })
                team_data["display_name"] = display_name
                team_data["role"] = role
                team_data["pin"] = pin
                team_data["slug"] = generate_slug(team_id, pin)
                team_data["deactivated"] = not active
                new_teams_data[team_id] = team_data
            else:
                state["action_log"].insert(0, {
                    "id": str(uuid.uuid4()),
                    "type": "role_change",
                    "team": team_id,
                    "timestamp": now_iso,
                    "actual_timestamp": now_iso,
                    "desc": role,
                    "admin_only": True
                })
                new_teams_data[team_id] = {
                    "display_name": display_name,
                    "stops": [], "stop_timestamps": [], "caught": [], "catch_timestamps": [], "caught_by": [], "pin": pin,
                    "role": role, "slug": generate_slug(team_id, pin),
                    "deactivated": not active,
                    "manual_points": 0
                }

        state["teams_data"] = new_teams_data
        config["num_teams"] = len(new_teams_data)

        # Datensatz-Änderung prüfen (für Geofencing)
        new_dataset = data.get("active_stops_dataset")
        old_dataset = state.get("active_stops_dataset", "Catch_Haltestellen.json")
        if new_dataset and new_dataset != old_dataset:
            state["active_stops_dataset"] = new_dataset
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_stops_dataset', ?)", (new_dataset,))
            load_active_stops_data(new_dataset)
            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()),
                "type": "system",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "desc": f"📁 Geofence-Datensatz geändert auf '{new_dataset}'.",
                "admin_only": True
            })

        save_state(conn, state)
        trigger_traccar_sync_async(state["teams_data"])
        threading.Thread(target=check_game_gps_rules, daemon=True).start()

    return jsonify({'success': True, 'new_admin_url': ADMIN_SLUG})


@app.route('/adminconsole-<admin_slug>/reset', methods=['POST'])
def reset_game(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    # 1. In-Memory-Caches & Statusvariablen zurücksetzen
    gps_inactivity_notified_stage.clear()
    recent_traccar_webhooks.clear()
    debug_test_events.clear()

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)

        # 2. Konfigurierte Spiel- und Intervallzeiten zurücksetzen
        # (Alle anderen Einstellungen wie Punkte, Intervall-Dauer, Geofences & Telegram bleiben erhalten)
        state["config"]["start_time"] = ""
        state["config"]["end_time"] = ""
        state["config"]["first_broadcast_time"] = ""

        # 3. Offene Anfragen und Aktions-Historie leeren
        state["pending_requests"] = []
        state["action_log"] = []

        # 4. Team-Spielstände zurücksetzen (Teams, Namen, Rollen & PINs bleiben erhalten)
        for t_id in state["teams_data"]:
            state["teams_data"][t_id]["stops"] = []
            state["teams_data"][t_id]["stop_timestamps"] = []
            state["teams_data"][t_id]["caught"] = []
            state["teams_data"][t_id]["catch_timestamps"] = []
            state["teams_data"][t_id]["caught_by"] = []
            state["teams_data"][t_id]["manual_points"] = 0

        # 5. Alle Datenbank-Tabellen mit Koordinaten, Historie und Spielereignissen restlos leeren
        c = conn.cursor()
        c.execute("DELETE FROM team_locations")
        c.execute("DELETE FROM location_history")
        c.execute("DELETE FROM geofence_violations")
        c.execute("DELETE FROM gps_outages")
        c.execute("DELETE FROM team_catches")
        c.execute("DELETE FROM mister_pending_inputs")
        c.execute("DELETE FROM mister_broadcasts")
        c.execute("DELETE FROM pending_requests")
        c.execute("DELETE FROM action_logs")

        # Zeitpunkt des Resets merken, damit verspätete Alt-Pakete von Traccar verworfen werden können
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('last_reset_ts', ?)", (str(time.time()),))

        save_state(conn, state)

    # 6. Traccar-Datenbank im Hintergrund ebenfalls frisch aufsetzen, damit keine alten 
    # Koordinaten aus der H2-DB von Traccar nachgeladen werden
    def _reset_and_resync_traccar():
        if reset_traccar_process_and_db():
            for _ in range(15):
                time.sleep(1)
                if ensure_traccar_credentials():
                    sync_traccar_devices(state["teams_data"])
                    break
        else:
            trigger_traccar_sync_async(state["teams_data"])

    threading.Thread(target=_reset_and_resync_traccar, daemon=True).start()

    return jsonify({'success': True})


@app.route('/adminconsole-<admin_slug>/randomize_pins', methods=['POST'])
def randomize_pins(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        teams_data = state.get('teams_data', {})

        used_pins = set()
        for t_id, t_data in teams_data.items():
            while True:
                pin = f"{random.randint(1000, 9999)}"
                if pin not in used_pins:
                    used_pins.add(pin)
                    break
            t_data['pin'] = pin
            t_data['slug'] = generate_slug(t_id, pin)

        state['action_log'].insert(0, {
            'id': str(uuid.uuid4()),
            'type': 'system',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'desc': '🔑 Neue Team-PINs wurden manuell ausgewürfelt.',
            'admin_only': True
        })

        save_state(conn, state)
        trigger_traccar_sync_async(state['teams_data'])

    return jsonify({'success': True})







# --- HALTESTELLEN & GTFS MANAGEMENT ---
@app.route('/adminconsole-<admin_slug>/stop_config')
def admin_stop_config(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
    return render_template('stop_config.html', admin_url=ADMIN_SLUG)


@app.route('/api/stop_datasets')
def api_stop_datasets():
    admin_slug = request.args.get('admin_slug')
    if not is_valid_admin_slug(admin_slug):
        return jsonify({"error": "Zugriff verweigert"}), 403
    try:
        if not os.path.exists(STOPS_DIR):
            os.makedirs(STOPS_DIR, exist_ok=True)
        files = [f for f in sorted(os.listdir(STOPS_DIR)) if f.endswith('.json') and os.path.isfile(os.path.join(STOPS_DIR, f))]
        return jsonify(files)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/stop_dataset/<path:filename>')
def api_stop_dataset(filename):
    admin_slug = request.args.get('admin_slug')
    if not is_valid_admin_slug(admin_slug):
        return jsonify({"error": "Zugriff verweigert"}), 403
    safe_filename = os.path.basename(filename)
    if not safe_filename.endswith('.json'):
        return jsonify({"error": "Ungültiges Dateiformat. Nur .json erlaubt."}), 400
    filepath = os.path.join(STOPS_DIR, safe_filename)
    if not os.path.exists(filepath):
        base_fallback = os.path.join(BASE_DIR, safe_filename)
        if os.path.exists(base_fallback):
            filepath = base_fallback
        else:
            return jsonify({"error": "Datei nicht gefunden."}), 404
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = json.load(f)
        return jsonify(content)
    except Exception as e:
        return jsonify({"error": f"Fehler beim Lesen der Datei: {str(e)}"}), 500


@app.route('/api/save_stop_dataset', methods=['POST'])
def api_save_stop_dataset():
    req_json = request.get_json(silent=True) or {}
    admin_slug = req_json.get('admin_slug') or request.args.get('admin_slug')
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    req_data = req_json
    filename = req_data.get('filename', '').strip()

    if not filename:
        return jsonify({"success": False, "error": "Kein Dateiname angegeben."}), 400

    if not filename.endswith('.json'):
        filename += '.json'
    safe_filename = os.path.basename(filename)

    if 'data' in req_data and isinstance(req_data['data'], dict):
        stops = req_data['data']
    elif 'stops' in req_data and isinstance(req_data['stops'], dict):
        stops = req_data['stops']
    elif 'dataset' in req_data and isinstance(req_data['dataset'], dict):
        stops = req_data['dataset']
    else:
        stops = {k: v for k, v in req_data.items() if k != 'filename'}

    if not isinstance(stops, dict):
        return jsonify({"success": False, "error": "Ungültiges Format für Haltestellen-Datensatz."}), 400

    os.makedirs(STOPS_DIR, exist_ok=True)
    target_path = os.path.join(STOPS_DIR, safe_filename)

    try:
        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(stops, f, indent=4, ensure_ascii=False)
    except Exception as e:
        return jsonify({"success": False, "error": f"Fehler beim Speichern: {str(e)}"}), 500

    active_dataset = "Catch_Haltestellen.json"
    try:
        with sqlite3.connect(DB_FILE, timeout=10) as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM settings WHERE key = 'active_stops_dataset'")
            row = c.fetchone()
            if row and row[0]:
                active_dataset = row[0]
    except Exception:
        pass

    if safe_filename == active_dataset:
        load_active_stops_data(safe_filename)
        threading.Thread(target=check_game_gps_rules, daemon=True).start()

    return jsonify({"success": True, "filename": safe_filename, "count": len(stops)})



# --- PLAYER ACTIONS API ---


@app.route('/api/report_catch', methods=['POST'])
def report_catch():
    data = request.json or {}
    reporter_slug = data.get('reporter_slug')
    hunter_id = data.get('hunter')
    prey_id = data.get('prey')

    if not reporter_slug or not hunter_id or not prey_id:
        return jsonify({'success': False, 'error': 'Ungültige Daten übermittelt.'}), 400

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, _, _, _, _ = update_game_time_state(conn, state)

        if phase != "game":
            return jsonify({'success': False, 'error': 'Fänge können nur während des aktiven Spiels gemeldet werden!'}), 403

        reporter_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == reporter_slug), None)
        if not reporter_id:
            return jsonify({'success': False, 'error': 'Reporter-Team nicht gefunden.'}), 404

        if hunter_id not in state["teams_data"] or prey_id not in state["teams_data"]:
            return jsonify({'success': False, 'error': 'Eines der übermittelten Teams existiert nicht.'}), 400

        hunter_data = state["teams_data"][hunter_id]
        prey_data = state["teams_data"][prey_id]

        if hunter_data.get("deactivated", False) or prey_data.get("deactivated", False):
            return jsonify({'success': False, 'error': 'Eines der Teams ist deaktiviert.'}), 403

        if hunter_data.get("role") != "fänger" or prey_data.get("role") != "mister_x":
            return jsonify({'success': False, 'error': 'Ungültige Rollen! Detektive fangen Mister X.'}), 400

        # STRIKTE SPERR-REGEL: Prüfen, welcher Mister X von diesem Jäger als letztes gefangen wurde
        c = conn.cursor()
        c.execute("SELECT prey_id FROM team_catches WHERE hunter_id = ? ORDER BY rowid DESC LIMIT 1", (hunter_id,))
        lc_row = c.fetchone()
        last_prey_id = lc_row["prey_id"] if lc_row else (hunter_data.get("caught")[-1] if hunter_data.get("caught") else None)
        if last_prey_id == prey_id:
            return jsonify({'success': False, 'error': 'Diesen Mister X habt ihr zuletzt gefangen. Fangt erst einen anderen!'}), 400

        now_utc = datetime.now(timezone.utc)
        now_iso = now_utc.isoformat()

        # Prüfen, ob bereits eine passende Anfrage der Gegenseite vorliegt
        matching_req = next((r for r in state["pending_requests"] if
                             r.get("type") == "catch" and r.get("hunter") == hunter_id and r.get("prey") == prey_id and r.get("reporter") != reporter_id), None)

        h_name = hunter_data["display_name"]
        p_name = prey_data["display_name"]

        if matching_req:
            state["pending_requests"] = [r for r in state["pending_requests"] if r.get("id") != matching_req.get("id")]
            sub_ts = matching_req.get("timestamp") or now_iso
            closest_catch_iso = find_closest_teams_approach(conn, hunter_id, prey_id, ref_dt=now_utc)
            actual_ts = closest_catch_iso or None
            execute_team_catch(state, hunter_id, prey_id, submitted_ts=sub_ts, actual_ts=actual_ts, conn=conn)
            save_state(conn, state)
            return jsonify({'success': True, 'message': f'Beidseitig bestätigt! Fang von {p_name} durch {h_name} eingetragen.'})

        # Falls Mister X selbst meldet (reporter_id == prey_id): Sofortige beidseitige Wertung ohne Bestätigung nötig
        if reporter_id == prey_id:
            closest_catch_iso = find_closest_teams_approach(conn, hunter_id, prey_id, ref_dt=now_utc)
            actual_ts = closest_catch_iso or None
            execute_team_catch(state, hunter_id, prey_id, submitted_ts=now_iso, actual_ts=actual_ts, conn=conn)
            save_state(conn, state)
            return jsonify({'success': True, 'message': f'Fang erfolgreich bestätigt! {h_name} erhält die Punkte.'})

        # Falls Fänger meldet: Offene Anfrage prüfen und anlegen
        already_pending = next((r for r in state["pending_requests"] if
                                r.get("type") == "catch" and r.get("hunter") == hunter_id and r.get("prey") == prey_id and r.get("reporter") == reporter_id), None)
        if already_pending:
            return jsonify({'success': False, 'error': 'Es gibt bereits eine offene Fanganfrage für diesen Mister X.'}), 400

        req_id = str(uuid.uuid4())
        state["pending_requests"].append({
            "id": req_id,
            "type": "catch",
            "reporter": reporter_id,
            "reporter_name": h_name,
            "hunter": hunter_id,
            "hunter_name": h_name,
            "prey": prey_id,
            "prey_name": p_name,
            "timestamp": now_iso
        })
        save_state(conn, state)
        send_telegram_msg(f"📬 NEUE FANG-ANFRAGE: '{h_name}' meldet, Mister X ({p_name}) gefangen zu haben. Warte auf Bestätigung.")
        return jsonify({'success': True, 'message': 'Fanganfrage an Mister X gesendet! Warte auf Bestätigung.'})


@app.route('/api/respond_catch', methods=['POST'])
def respond_catch():
    data = request.json or {}
    slug = data.get('slug')
    req_id = data.get('request_id')
    hunter_id = data.get('hunter')
    prey_id = data.get('prey')
    action = str(data.get('action', '')).lower()

    if not slug:
        return jsonify({'success': False, 'error': 'Slug fehlt.'}), 400

    if action not in ('approve', 'reject'):
        return jsonify({'success': False, 'error': 'Ungültige Aktion (approve oder reject).'}), 400

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)

        team_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == slug), None)
        if not team_id:
            return jsonify({'success': False, 'error': 'Team nicht gefunden.'}), 404

        req = None
        if req_id:
            req = next((r for r in state["pending_requests"] if r.get("id") == req_id), None)
        elif hunter_id and prey_id:
            req = next((r for r in state["pending_requests"] if r.get("type") == "catch" and r.get("hunter") == hunter_id and r.get("prey") == prey_id), None)

        if not req:
            return jsonify({'success': False, 'error': 'Anfrage nicht gefunden oder bereits bearbeitet.'}), 404

        if team_id not in (req.get("hunter"), req.get("prey")):
            return jsonify({'success': False, 'error': 'Nicht berechtigt für diese Anfrage.'}), 403

        # RÜCKSTANDSLOSE LÖSCHUNG aus pending_requests
        state["pending_requests"] = [r for r in state["pending_requests"] if r.get("id") != req.get("id")]

        if action == 'reject':
            save_state(conn, state)
            return jsonify({'success': True, 'message': 'Anfrage abgelehnt und restlos entfernt.'})

        elif action == 'approve':
            h_id = req["hunter"]
            p_id = req["prey"]
            now_utc = datetime.now(timezone.utc)
            sub_ts = req.get("timestamp") or now_utc.isoformat()
            closest_catch_iso = find_closest_teams_approach(conn, h_id, p_id, ref_dt=now_utc)
            actual_ts = closest_catch_iso or None

            execute_team_catch(state, h_id, p_id, submitted_ts=sub_ts, actual_ts=actual_ts, conn=conn)
            save_state(conn, state)

            h_name = state["teams_data"][h_id]["display_name"]
            p_name = state["teams_data"][p_id]["display_name"]
            return jsonify({'success': True, 'message': f'Fang bestätigt! {h_name} hat {p_name} gefangen.'})


# --- ADMIN ACTIONS API ---
@app.route('/api/resolve_request', methods=['POST'])
def resolve_request():
    req_json = request.get_json(silent=True) or {}
    admin_slug = req_json.get('admin_slug') or request.args.get('admin_slug')
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    data = req_json
    req_id = data.get('id')
    action = data.get('action')

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)

        req = next((r for r in state["pending_requests"] if r["id"] == req_id), None)
        if not req:
            return jsonify({'success': False, 'error': 'Anfrage nicht gefunden.'}), 404

        state["pending_requests"] = [r for r in state["pending_requests"] if r["id"] != req_id]
        is_late = req.get("is_late", False)
        sub_ts = req.get("timestamp") or datetime.now(timezone.utc).isoformat()

        if action == 'approve':
            if req["type"] == "catch":
                state["pending_requests"] = [r for r in state["pending_requests"] if not (
                            r["type"] == "catch" and r["hunter"] == req["hunter"] and r["prey"] == req["prey"])]
                ref_dt = parse_iso_utc(sub_ts) or datetime.now(timezone.utc)
                closest_catch_iso = find_closest_teams_approach(conn, req["hunter"], req["prey"], ref_dt=ref_dt)
                actual_ts = closest_catch_iso or None
                execute_team_catch(
                    state, req["hunter"], req["prey"],
                    is_late=is_late,
                    submitted_ts=sub_ts,
                    actual_ts=actual_ts,
                    conn=conn
                )

        save_state(conn, state)

    return jsonify({'success': True})


@app.route('/api/undo_request', methods=['POST'])
def api_undo_request():
    data = request.get_json(silent=True) or {}
    slug = data.get("slug")
    request_id = data.get("request_id")
    if not slug or not request_id:
        return jsonify({'success': False, 'error': 'Slug oder Request-ID fehlt.'}), 400

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)

        team_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == slug), None)
        if not team_id:
            return jsonify({'success': False, 'error': 'Team nicht gefunden.'}), 404

        req = next((r for r in state.get("pending_requests", []) if r.get("id") == request_id), None)
        if not req:
            return jsonify({'success': False, 'error': 'Anfrage nicht gefunden oder bereits bearbeitet.'}), 404

        if not (req.get("type") == "catch" and req.get("reporter") == team_id):
            return jsonify({'success': False, 'error': 'Nicht berechtigt.'}), 403

        state["pending_requests"].remove(req)
        save_state(conn, state)

    return jsonify({'success': True})


@app.route('/api/edit_team_profile', methods=['POST'])
@app.route('/api/update_team_profile', methods=['POST'])
def api_update_team_profile():
    data = request.get_json(silent=True) or {}
    slug = data.get("slug")
    new_name = (data.get("new_name") or "").replace('<', '').replace('>', '').strip()
    new_pin_input = data.get("new_pin")

    if not slug:
        return jsonify({'success': False, 'error': 'Slug fehlt.'}), 400

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)

        team_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == slug), None)
        if not team_id:
            return jsonify({'success': False, 'error': 'Team nicht gefunden.'}), 404

        team_role = state["teams_data"][team_id].get("role", "fänger")
        old_pin = state["teams_data"][team_id].get("pin", "")

        if new_pin_input is not None and str(new_pin_input).strip() != "":
            new_pin = str(new_pin_input).replace('<', '').replace('>', '').strip()
        else:
            new_pin = old_pin

        # Mister X darf NUR die PIN ändern, der Teamname bleibt fest
        if team_role == "mister_x":
            effective_name = state["teams_data"][team_id].get("display_name", team_id)
        else:
            if not new_name:
                return jsonify({'success': False, 'error': 'Teamname darf nicht leer sein.'}), 400
            if len(new_name) > 40:
                return jsonify({'success': False, 'error': 'Teamname darf maximal 40 Zeichen lang sein.'}), 400
            effective_name = new_name

        state["teams_data"][team_id]["display_name"] = effective_name
        state["teams_data"][team_id]["pin"] = new_pin
        new_slug = generate_slug(team_id, new_pin)
        state["teams_data"][team_id]["slug"] = new_slug

        # Update in pending_requests
        for req in state.get("pending_requests", []):
            if req.get("team") == team_id:
                req["team_name"] = effective_name
            if req.get("reporter") == team_id:
                req["reporter_name"] = effective_name
            if req.get("hunter") == team_id:
                req["hunter_name"] = effective_name
            if req.get("prey") == team_id:
                req["prey_name"] = effective_name

        # Update in action_log
        for entry in state.get("action_log", []):
            if entry.get("team") == team_id:
                entry["team_name"] = effective_name
            if entry.get("reporter") == team_id:
                entry["reporter_name"] = effective_name
            if entry.get("hunter") == team_id:
                entry["hunter_name"] = effective_name
            if entry.get("prey") == team_id:
                entry["prey_name"] = effective_name

        save_state(conn, state)
        trigger_traccar_sync_async(state["teams_data"])

    return jsonify({
        'success': True,
        'new_slug': new_slug,
        'pin_changed': (old_pin != new_pin)
    })


@app.route('/api/undo_action', methods=['POST'])
def undo_action():
    req_json = request.get_json(silent=True) or {}
    admin_slug = req_json.get('admin_slug') or request.args.get('admin_slug')
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    data = req_json
    action_id = data.get('id')

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        action = next((a for a in state["action_log"] if a["id"] == action_id), None)
        if not action:
            return jsonify({'success': False, 'error': 'Aktion im Log nicht gefunden.'}), 404

        c = conn.cursor()
        if action["type"] == "catch":
            hunter_id = action.get("hunter")
            prey_id = action.get("prey")
            sub_ts = action.get("timestamp")
            if hunter_id and hunter_id in state["teams_data"] and prey_id in state["teams_data"][hunter_id]["caught"]:
                state["teams_data"][hunter_id]["caught"].remove(prey_id)
                ts_list = state["teams_data"][hunter_id].get("catch_timestamps", [])
                removed = False
                for idx, entry in enumerate(ts_list):
                    if len(entry) >= 4 and entry[3] == action_id:
                        ts_list.pop(idx)
                        removed = True
                        break
                if not removed:
                    for idx, entry in enumerate(ts_list):
                        if entry[0] == prey_id and (sub_ts is None or entry[1] == sub_ts):
                            ts_list.pop(idx)
                            break
                c.execute("DELETE FROM team_catches WHERE action_id = ?", (action_id,))
                if c.rowcount == 0 and sub_ts:
                    c.execute("DELETE FROM team_catches WHERE rowid IN (SELECT rowid FROM team_catches WHERE hunter_id = ? AND prey_id = ? AND submitted_at = ? LIMIT 1)", (hunter_id, prey_id, sub_ts))
            if prey_id and prey_id in state["teams_data"] and hunter_id in state["teams_data"][prey_id]["caught_by"]:
                state["teams_data"][prey_id]["caught_by"].remove(hunter_id)

        elif action["type"] == "manual_points":
            team_id = action.get("team")
            points = 0
            if action.get("stop") is not None:
                try:
                    points = int(float(action.get("stop")))
                except (ValueError, TypeError):
                    points = 0
            if points == 0 and action.get("desc"):
                m = re.search(r'([+-]?\d+(?:\.\d+)?)', str(action.get("desc")))
                if m:
                    try:
                        points = int(float(m.group(1)))
                    except (ValueError, TypeError):
                        points = 0
            if team_id and team_id in state["teams_data"]:
                current = state["teams_data"][team_id].get("manual_points", 0)
                state["teams_data"][team_id]["manual_points"] = current - points

        state["action_log"].remove(action)
        save_state(conn, state)

    return jsonify({'success': True})


@app.route('/api/catch_team', methods=['POST'])
def catch_team():
    req_json = request.get_json(silent=True) or {}
    admin_slug = req_json.get('admin_slug') or request.args.get('admin_slug')
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    data = req_json
    hunter_id = data.get('hunter')
    prey_id = data.get('prey')

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        if hunter_id and prey_id:
            if hunter_id == prey_id:
                return jsonify({'success': False, 'error': 'Ein Team kann sich nicht selbst fangen!'}), 400
            now_utc = datetime.now(timezone.utc)
            now_iso = now_utc.isoformat()
            closest_catch_iso = find_closest_teams_approach(conn, hunter_id, prey_id, ref_dt=now_utc)
            actual_ts = closest_catch_iso or None
            if execute_team_catch(state, hunter_id, prey_id, submitted_ts=now_iso, actual_ts=actual_ts, conn=conn):
                save_state(conn, state)
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Fehler bei der Zuweisung.'}), 400

    return jsonify({'success': False, 'error': 'Ungültige Teams übergeben.'}), 400


@app.route('/api/add_manual_points', methods=['POST'])
def add_manual_points():
    req_json = request.get_json(silent=True) or {}
    admin_slug = req_json.get('admin_slug') or request.args.get('admin_slug')
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    data = req_json
    team_id = data.get('team_id')
    points_str = data.get('points', 0)

    try:
        points = int(points_str)
    except ValueError:
        return jsonify({'success': False, 'error': 'Ungültige Punktzahl.'}), 400

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        if team_id in state["teams_data"]:
            current = state["teams_data"][team_id].get("manual_points", 0)
            state["teams_data"][team_id]["manual_points"] = current + points

            display_name = state["teams_data"][team_id]["display_name"]
            desc = f"🔧 Manuelle Punktekorrektur für {display_name}: {points:+} Punkte."

            now_iso = datetime.now(timezone.utc).isoformat()
            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()),
                "type": "manual_points",
                "team": team_id,
                "team_name": display_name,
                "stop": str(points),
                "timestamp": now_iso,
                "actual_timestamp": None,
                "desc": desc,
                "admin_only": False
            })
            save_state(conn, state)
            return jsonify({'success': True})

    return jsonify({'success': False, 'error': 'Team nicht gefunden.'}), 400




@app.route('/api/game_state')
@app.route('/api/ticker_data')
def api_ticker_data():
    slug = request.args.get('slug')
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        config = state["config"]

        leaderboard = []
        for t_id, d in state["teams_data"].items():
            if d.get("deactivated", False) or d.get("role") != "fänger":
                continue

            score = calculate_score(d, config)

            leaderboard.append({
                "id": t_id, "display_name": d["display_name"], "score": score,
                "role": "fänger"
            })

        leaderboard.sort(key=lambda x: (-x["score"], x["display_name"].lower()))
        public_log = [
            e for e in state.get("action_log", [])
            if not e.get("admin_only") and e.get("type") not in ("broadcast", "broadcast_update", "mister_broadcast")
        ]

        resp_data = {
            'success': True,
            'action_log': public_log,
            'leaderboard': leaderboard
        }

        if slug:
            team_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == slug), None)
            if team_id:
                resp_data["my_pending_requests"] = [
                    r for r in state.get("pending_requests", [])
                    if (r.get("type") == "stop" and r.get("team") == team_id) or (r.get("type") == "catch" and r.get("reporter") == team_id)
                ]
                resp_data["open_opponent_catches"] = [
                    r for r in state.get("pending_requests", [])
                    if r.get("type") == "catch" and (r.get("hunter") == team_id or r.get("prey") == team_id) and r.get("reporter") != team_id
                ]

        return jsonify(resp_data)


@app.route('/api/replay_data')
def api_replay_data():
    admin_param = request.args.get('admin', '')
    is_admin = False
    if admin_param:
        if is_valid_admin_slug(admin_param):
            is_admin = True
        else:
            return jsonify({"error": "Zugriff verweigert"}), 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

        if not start_iso or not end_iso:
            return jsonify({"error": "Keine Spielzeiten konfiguriert."})

        start_dt = parse_iso_utc(start_iso)
        end_dt = parse_iso_utc(end_iso)
        now_dt = parse_iso_utc(now_iso)
        mid_dt = parse_iso_utc(mid_iso) if mid_iso else None

        is_second_half = bool(mid_dt and now_dt >= mid_dt)
        replay_start = start_dt

        # --- LOGIK FÜR SICHTBARKEIT & ZEITFENSTER ---
        if not is_admin:
            # Spieler / Öffentlich: Replay erst nach Spielende verfügbar
            if phase != "finished":
                return jsonify({"error": "Das Replay ist erst nach Spielende verfügbar."}), 403
            replay_end = end_dt + timedelta(minutes=3)
        else:
            # Admin (Live-Modus mit 30 Sekunden Verzögerung)
            if phase == "inactive":
                replay_end = start_dt
            elif phase == "game":
                replay_end = now_dt - timedelta(seconds=30)
                if replay_end > end_dt:
                    replay_end = end_dt
            else:
                replay_end = end_dt + timedelta(minutes=3)

        if replay_end < replay_start:
            replay_end = replay_start

        c = conn.cursor()

        # 1. GPS-Punkte
        sql_start = (replay_start - timedelta(minutes=1)).isoformat()
        sql_end = (replay_end + timedelta(minutes=1)).isoformat()
        c.execute("""
            SELECT team_id, lat, lon, timestamp
            FROM location_history
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
        """, (sql_start, sql_end))

        locations = {t_id: [] for t_id in state["teams_data"]}
        for row in c.fetchall():
            t_id = row["team_id"]
            pt_dt = parse_iso_utc(row["timestamp"])
            if pt_dt and replay_start <= pt_dt <= replay_end:
                if t_id not in locations:
                    locations[t_id] = []
                locations[t_id].append({"lat": row["lat"], "lon": row["lon"], "ts": int(pt_dt.timestamp() * 1000)})

        # 2. Events (Aktionen)
        c.execute("""
            SELECT id, type, team, team_name, stop, hunter, hunter_name, prey, prey_name, timestamp, actual_timestamp, desc, is_late, admin_only
            FROM action_logs ORDER BY timestamp ASC
        """)

        def find_closest_coords(pts, target_ts_ms, max_diff_ms=10 * 60 * 1000):
            if not pts: return None, None
            closest = min(pts, key=lambda p: abs(p["ts"] - target_ts_ms))
            if abs(closest["ts"] - target_ts_ms) <= max_diff_ms:
                return closest["lat"], closest["lon"]
            return None, None

        events = []
        for row in c.fetchall():
            if row["admin_only"] and row["type"] not in ("role_change", "manual_points"): continue
            if row["type"] == "stop" and not row["team"]: continue

            act_dt = parse_iso_utc(row["actual_timestamp"]) or parse_iso_utc(row["timestamp"])
            if not act_dt: continue

            act_ts_ms = int(act_dt.timestamp() * 1000)
            lat, lon = None, None

            if replay_start <= act_dt <= replay_end:
                if row["type"] == "stop":
                    stop_name = row["stop"]
                    if stop_name and stop_name in stops_data:
                        lat = stops_data[stop_name]["lat"]
                        lon = stops_data[stop_name]["lon"]
                    else:
                        lat, lon = find_closest_coords(locations.get(row["team"], []), act_ts_ms)
                elif row["type"] in ("catch", "team_caught"):
                    lat, lon = find_closest_coords(locations.get(row["prey"], []), act_ts_ms)
                    if lat is None:
                        lat, lon = find_closest_coords(locations.get(row["hunter"], []), act_ts_ms)

                events.append({
                    "id": row["id"], "type": row["type"], "ts": act_ts_ms, "desc": row["desc"],
                    "team": row["team"], "team_name": row["team_name"], "stop": row["stop"],
                    "hunter": row["hunter"], "hunter_name": row["hunter_name"], "prey": row["prey"],
                    "prey_name": row["prey_name"], "lat": lat, "lon": lon
                })

        # Broadcasts von Mister X abfragen
        c.execute("""
            SELECT id, team_id, interval_iso, published_at, lat, lon, line, direction, station
            FROM mister_broadcasts ORDER BY published_at ASC
        """)
        for b in c.fetchall():
            b_dt = parse_iso_utc(b["published_at"])
            if b_dt and replay_start <= b_dt <= replay_end:
                b_ms = int(b_dt.timestamp() * 1000)
                desc = f"📢 Standort Mister X: {b['line'] or ''} {b['station'] or ''}".strip()
                events.append({
                    "id": f"broadcast-{b['id']}",
                    "type": "mister_broadcast",
                    "ts": b_ms,
                    "desc": desc,
                    "team": b["team_id"],
                    "lat": b["lat"],
                    "lon": b["lon"],
                    "line": b["line"],
                    "direction": b["direction"],
                    "station": b["station"]
                })

        events.sort(key=lambda e: e["ts"])

        # 3. Teams Grunddaten (inklusive Rolle)
        teams_dict = {
            t_id: {
                "id": t_id,
                "display_name": d.get("display_name", t_id),
                "role": d.get("role", "fänger"),
                "deactivated": bool(d.get("deactivated", False))
            }
            for t_id, d in state["teams_data"].items()
        }

        # 4. Stop & Role Lifecycle (Haltestellen und Rollenwechsel entfernt)
        stop_lifecycle = []
        role_lifecycle = []

        enable_gf = bool(state["config"].get("enable_geofences", False))
        return jsonify({
            "start_ts": int(replay_start.timestamp() * 1000),
            "end_ts": int(replay_end.timestamp() * 1000),
            "config": state["config"],
            "teams": teams_dict,
            "locations": locations,
            "events": events,
            "stop_lifecycle": stop_lifecycle,
            "role_lifecycle": role_lifecycle,
            "geofence_outer": GEOFENCE_OUTER,
            "geofence_exclusion": GEOFENCE_EXCLUSION if enable_gf else []
        })


@app.route('/adminconsole-<admin_slug>/replay')
def admin_replay(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

    return render_template('replay.html', phase=phase, start_time=start_iso, end_time=end_iso, is_admin=True, admin_url=ADMIN_SLUG, geofence_outer=GEOFENCE_OUTER)


@app.route('/replay')
def replay():
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

    return render_template('replay.html', phase=phase, start_time=start_iso, end_time=end_iso, geofence_outer=GEOFENCE_OUTER)


# --- TRACCAR WEBHOOK ROUTE ---
@app.route('/api/traccar', methods=['GET', 'POST'])
def traccar_webhook():
    payload = request.get_json(silent=True) or {}
    
    # NEU: Traccar API in log.json schreiben (falls aktiviert)
    log_data = {
        "method": request.method,
        "args": request.args.to_dict(),
        "form": request.form.to_dict(),
        "json": payload
    }
    write_debug_log("traccar_api", log_data)

    identifier = (
        request.args.get('id')
        or request.args.get('deviceid')
        or request.args.get('deviceId')
        or request.args.get('uniqueId')
        or request.form.get('id')
        or request.form.get('deviceId')
        or request.form.get('uniqueId')
        or payload.get('uniqueId')
        or payload.get('deviceId')
        or payload.get('id')
    )

    lat = (
        request.args.get('lat')
        or request.args.get('latitude')
        or request.form.get('lat')
        or request.form.get('latitude')
    )

    lon = (
        request.args.get('lon')
        or request.args.get('longitude')
        or request.form.get('lon')
        or request.form.get('longitude')
    )

    raw_ts = (
        request.args.get('timestamp')
        or request.args.get('fixTime')
        or request.form.get('timestamp')
        or request.form.get('fixTime')
        or payload.get('fixTime')
        or payload.get('deviceTime')
        or payload.get('serverTime')
        or payload.get('timestamp')
    )

    if lat is None:
        lat = payload.get('latitude')
        if lat is None:
            lat = payload.get('lat')

    if lon is None:
        lon = payload.get('longitude')
        if lon is None:
            lon = payload.get('lon')

    device = payload.get('device')
    position = payload.get('position')

    if isinstance(device, dict) and not identifier:
        identifier = (
            device.get('uniqueId')
            or device.get('id')
            or device.get('deviceId')
        )

    if isinstance(position, dict):
        if lat is None:
            lat = position.get('latitude')
        if lon is None:
            lon = position.get('longitude')
        if not raw_ts:
            raw_ts = (
                position.get('fixTime')
                or position.get('deviceTime')
                or position.get('serverTime')
            )

    if identifier is None or lat is None or lon is None:
        app.logger.warning(
            "Traccar-Webhook: Pflichtfelder fehlen. JSON-Felder: %s",
            list(payload.keys()) if isinstance(payload, dict) else []
        )
        return jsonify({
            'success': False,
            'error': 'Fehlende Parameter',
            'received': {
                'identifier': identifier is not None,
                'latitude': lat is not None,
                'longitude': lon is not None
            }
        }), 400

    try:
        lat = float(lat)
        lon = float(lon)

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError("Koordinaten außerhalb des gültigen Bereichs")

    except (ValueError, TypeError):
        return jsonify({
            'success': False,
            'error': 'Ungültige Koordinaten'
        }), 400

    if lat == 0.0 and lon == 0.0:
        return jsonify({
            'success': True,
            'ignored': 'GPS 0.0/0.0 ignoriert'
        }), 200

    packet_time = time.time()

    if raw_ts is not None and str(raw_ts).strip():
        try:
            ts_string = str(raw_ts).strip()

            if ts_string.replace('.', '', 1).isdigit():
                ts_value = float(ts_string)
                packet_time = (
                    ts_value / 1000.0
                    if ts_value > 10000000000
                    else ts_value
                )
            else:
                dt = datetime.fromisoformat(
                    ts_string.replace('Z', '+00:00')
                )

                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

                packet_time = dt.timestamp()

        except (ValueError, TypeError, OverflowError):
            app.logger.warning(
                "Traccar-Webhook: Zeitstempel nicht lesbar: %r",
                raw_ts
            )

    # Zukunftspakete / vorgehende Smartphone-Uhrzeit deckeln
    packet_time = min(packet_time, time.time() + 5)

    # Zusätzliche Telemetrie-Daten auslesen (position bzw. position.attributes)
    pos_dict = position if isinstance(position, dict) else {}
    pos_attrs = pos_dict.get('attributes') if isinstance(pos_dict.get('attributes'), dict) else {}
    payload_attrs = payload.get('attributes') if isinstance(payload.get('attributes'), dict) else {}

    # batteryLevel (REAL)
    raw_battery = (
        pos_attrs.get('batteryLevel')
        if pos_attrs.get('batteryLevel') is not None
        else pos_attrs.get('battery')
        if pos_attrs.get('battery') is not None
        else payload_attrs.get('batteryLevel')
        if payload_attrs.get('batteryLevel') is not None
        else payload.get('batteryLevel')
        if payload.get('batteryLevel') is not None
        else request.args.get('batteryLevel')
        or request.args.get('batt')
        or request.args.get('battery')
        or request.form.get('batteryLevel')
        or request.form.get('batt')
    )
    battery_level = None
    if raw_battery is not None:
        try:
            battery_level = float(raw_battery)
        except (ValueError, TypeError):
            battery_level = None

    # charge (INTEGER/BOOLEAN)
    raw_charge = (
        pos_attrs.get('charge')
        if pos_attrs.get('charge') is not None
        else pos_attrs.get('charging')
        if pos_attrs.get('charging') is not None
        else payload_attrs.get('charge')
        if payload_attrs.get('charge') is not None
        else payload.get('charge')
        if payload.get('charge') is not None
        else request.args.get('charge')
        or request.form.get('charge')
    )
    charge = None
    if raw_charge is not None:
        if isinstance(raw_charge, bool):
            charge = 1 if raw_charge else 0
        elif isinstance(raw_charge, (int, float)):
            charge = 1 if int(raw_charge) != 0 else 0
        elif isinstance(raw_charge, str):
            charge = 1 if raw_charge.strip().lower() in ('true', '1', 'yes') else 0

    # motion (INTEGER/BOOLEAN)
    raw_motion = (
        pos_attrs.get('motion')
        if pos_attrs.get('motion') is not None
        else payload_attrs.get('motion')
        if payload_attrs.get('motion') is not None
        else payload.get('motion')
        if payload.get('motion') is not None
        else request.args.get('motion')
        or request.form.get('motion')
    )
    motion = None
    if raw_motion is not None:
        if isinstance(raw_motion, bool):
            motion = 1 if raw_motion else 0
        elif isinstance(raw_motion, (int, float)):
            motion = 1 if int(raw_motion) != 0 else 0
        elif isinstance(raw_motion, str):
            motion = 1 if raw_motion.strip().lower() in ('true', '1', 'yes') else 0

    # altitude (REAL)
    raw_altitude = (
        pos_dict.get('altitude')
        if pos_dict.get('altitude') is not None
        else payload.get('altitude')
        if payload.get('altitude') is not None
        else request.args.get('altitude')
        or request.args.get('alt')
        or request.form.get('altitude')
        or request.form.get('alt')
    )
    altitude = None
    if raw_altitude is not None:
        try:
            altitude = float(raw_altitude)
        except (ValueError, TypeError):
            altitude = None

    # speed (REAL)
    raw_speed = (
        pos_dict.get('speed')
        if pos_dict.get('speed') is not None
        else payload.get('speed')
        if payload.get('speed') is not None
        else request.args.get('speed')
        or request.form.get('speed')
    )
    speed = None
    if raw_speed is not None:
        try:
            speed = float(raw_speed)  * 0.51444
        except (ValueError, TypeError):
            speed = None

    # course (REAL)
    raw_course = (
        pos_dict.get('course')
        if pos_dict.get('course') is not None
        else payload.get('course')
        if payload.get('course') is not None
        else request.args.get('course')
        or request.args.get('bearing')
        or request.form.get('course')
        or request.form.get('bearing')
    )
    course = None
    if raw_course is not None:
        try:
            course = float(raw_course)
        except (ValueError, TypeError):
            course = None

    # accuracy (REAL)
    raw_accuracy = (
        pos_dict.get('accuracy')
        if pos_dict.get('accuracy') is not None
        else payload.get('accuracy')
        if payload.get('accuracy') is not None
        else request.args.get('accuracy')
        or request.form.get('accuracy')
    )
    accuracy = None
    if raw_accuracy is not None:
        try:
            accuracy = float(raw_accuracy)
        except (ValueError, TypeError):
            accuracy = None

    clean_id = str(identifier).strip()

    if not clean_id:
        return jsonify({
            'success': False,
            'error': 'Leere Gerätekennung'
        }), 400

    time_str = datetime.now().strftime("%H:%M:%S")

    if clean_id.startswith("debug-koblenz-"):
        debug_test_events[clean_id] = {
            "lat": lat,
            "lon": lon,
            "timestamp": packet_time
        }

        return jsonify({
            'success': True,
            'debug_test': True
        }), 200

    try:
        with sqlite3.connect(
            DB_FILE,
            timeout=10,
            isolation_level='IMMEDIATE'
        ) as conn:
            c = conn.cursor()

            # NEU: Alte GPS-Pakete ignorieren, die zeitlich vor dem letzten Spiel-Reset liegen
            c.execute("SELECT value FROM settings WHERE key = 'last_reset_ts'")
            reset_row = c.fetchone()
            if reset_row and reset_row[0]:
                try:
                    if packet_time < float(reset_row[0]):
                        return jsonify({
                            'success': True,
                            'ignored': 'Paket stammt aus der Zeit vor dem letzten Spiel-Reset'
                        }), 200
                except (ValueError, TypeError):
                    pass

            c.execute("""
                SELECT id, display_name
                FROM teams
                WHERE slug = ?
                LIMIT 1
            """, (clean_id,))

            team_row = c.fetchone()

            if not team_row:
                app.logger.warning(
                    "Traccar-Webhook: Unbekannte Kennung %r",
                    clean_id
                )

                return jsonify({
                    'success': True,
                    'ignored': f"Unbekannte Kennung '{clean_id}'"
                }), 200

            resolved_team_id, resolved_name = team_row

            packet_time_iso = datetime.fromtimestamp(packet_time, tz=timezone.utc).isoformat()

            # Nur in location_history schreiben, wenn der letzte Eintrag mehr als 1 Sekunde her ist
            c.execute("""
                SELECT timestamp
                FROM location_history
                WHERE team_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (resolved_team_id,))
            last_hist = c.fetchone()

            save_history = False
            if last_hist is None or last_hist[0] is None:
                save_history = True
            else:
                last_hist_dt = parse_iso_utc(last_hist[0])
                if last_hist_dt is None or (packet_time - last_hist_dt.timestamp()) > 1:
                    save_history = True

            if save_history:
                c.execute("""
                    INSERT INTO location_history
                        (team_id, lat, lon, timestamp, batteryLevel, charge, motion, altitude, speed, course, accuracy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    resolved_team_id,
                    lat,
                    lon,
                    packet_time_iso,
                    battery_level,
                    charge,
                    motion,
                    altitude,
                    speed,
                    course,
                    accuracy
                ))

            c.execute("""
                SELECT updated_at
                FROM team_locations
                WHERE team_id = ?
            """, (resolved_team_id,))

            existing_loc = c.fetchone()



            update_live = False
            if existing_loc is None or existing_loc[0] is None:
                update_live = True
            else:
                existing_dt = parse_iso_utc(existing_loc[0])
                if existing_dt is None or datetime.fromtimestamp(packet_time, tz=timezone.utc) >= existing_dt:
                    update_live = True

            if update_live:
                c.execute("""
                    INSERT INTO team_locations
                        (team_id, lat, lon, updated_at, batteryLevel, charge, motion, altitude, speed, course, accuracy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(team_id) DO UPDATE SET
                        lat = excluded.lat,
                        lon = excluded.lon,
                        updated_at = excluded.updated_at,
                        batteryLevel = excluded.batteryLevel,
                        charge = excluded.charge,
                        motion = excluded.motion,
                        altitude = excluded.altitude,
                        speed = excluded.speed,
                        course = excluded.course,
                        accuracy = excluded.accuracy
                    WHERE team_locations.updated_at IS NULL
                       OR excluded.updated_at >= team_locations.updated_at
                """, (
                    resolved_team_id,
                    lat,
                    lon,
                    packet_time_iso,
                    battery_level,
                    charge,
                    motion,
                    altitude,
                    speed,
                    course,
                    accuracy
                ))



            # Sobald ein Standort eintrifft, GPS-Ausfall-Stufe für dieses Team zurücksetzen
            gps_inactivity_notified_stage[resolved_team_id] = 0

            conn.commit()

    except sqlite3.Error:
        app.logger.exception(
            "Traccar-Webhook: Datenbankfehler für %r",
            clean_id
        )

        return jsonify({
            'success': False,
            'error': 'Datenbankfehler'
        }), 500

    recent_traccar_webhooks.appendleft({
        "time_str": time_str,
        "identifier": clean_id,
        "lat": lat,
        "lon": lon,
        "batteryLevel": battery_level,
        "charge": charge,
        "motion": motion,
        "altitude": altitude,
        "speed": speed,
        "course": course,
        "accuracy": accuracy,
        "matched": True,
        "status": f"OK ({resolved_name})"
    })

    #app.logger.info(f'{resolved_team_id} ({resolved_name}): {lat}, {lon} um {packet_time} ')

    return jsonify({
        'success': True,
        'team_id': resolved_team_id,
        'team_name': resolved_name,
        'history_saved': save_history,
        'live_updated': update_live,
        'timestamp': packet_time
    }), 200


# --- TRACCAR DEBUG DASHBOARD & E2E-TEST ROUTEN (MIT -{adminpin} UND _{adminpin} UNTERSTÜTZUNG) ---
@app.route('/api/traccar_debug/<admin_slug>/log_settings', methods=['GET', 'POST'])
def traccar_debug_log_settings(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403
    
    global LOG_SETTINGS
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        new_settings = data.get('settings')
        if isinstance(new_settings, dict):
            for k in LOG_SETTINGS:
                if k in new_settings:
                    LOG_SETTINGS[k] = bool(new_settings[k])
        elif 'category' in data and 'enabled' in data:
            cat = data['category']
            if cat in LOG_SETTINGS:
                LOG_SETTINGS[cat] = bool(data['enabled'])
        else:
            for k in LOG_SETTINGS:
                if k in data:
                    LOG_SETTINGS[k] = bool(data[k])
    
    return jsonify({
        'success': True,
        'settings': LOG_SETTINGS,
        'file_exists': os.path.exists(TRACCAR_LOG_FILE)
    })

@app.route('/api/traccar_debug/<admin_slug>/download_log')
def traccar_debug_download_log(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403

    flush_debug_logs()
    if os.path.exists(TRACCAR_LOG_FILE):
        return send_file(TRACCAR_LOG_FILE, as_attachment=True, download_name='log.json')
    
    # Falls noch keine Logdatei existiert, eine leere JSON-Liste zurückgeben
    return jsonify([]), 200


@app.route('/traccar_debug-<admin_slug>')
@app.route('/traccar_debug_<admin_slug>')
def traccar_debug_page(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403
    return render_template('traccar_debug.html', admin_url=ADMIN_SLUG)


@app.route('/api/traccar_debug/<admin_slug>/status')
def traccar_debug_status(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    api_online = False
    api_error = None
    devices = []

    try:
        with traccar_lock:
            if ensure_traccar_credentials():
                resp = traccar_session.get(TRACCAR_API_URL, timeout=4)
                if resp.status_code == 200:
                    api_online = True
                    devices = resp.json()
                else:
                    api_error = f"HTTP {resp.status_code} auf /api/devices"
            else:
                api_error = traccar_last_error or "Auth fehlgeschlagen / Offline"
    except Exception as e:
        api_error = str(e)

    osmand_open = is_port_in_use(5055)

    global traccar_process
    proc_running = (traccar_process is not None and traccar_process.poll() is None)
    proc_pid = traccar_process.pid if proc_running else None

    java_bin = get_java_executable()
    java_installed = bool(java_bin)

    jar_exists = os.path.isfile(TRACCAR_JAR_PATH)
    xml_exists = os.path.isfile(TRACCAR_XML_PATH)
    traccar_db_exists = os.path.isfile(os.path.join(TRACCAR_DATA_DIR, "database.mv.db"))

    game_db_size = os.path.getsize(DB_FILE) if os.path.exists(DB_FILE) else 0
    game_db_size_kb = round(game_db_size / 1024, 2)
    active_threads = threading.active_count()
    python_version = sys.version.split()[0]

    try:
        with sqlite3.connect(DB_FILE, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            state = get_state(conn)
            phase, _, _, _, _ = update_game_time_state(conn, state)
            num_teams = len(state.get("teams_data", {}))
    except Exception:
        phase = "Fehler beim Lesen"
        num_teams = 0

    # --- SYSTEM-METRIKEN (Robust & Erweitert) ---
    sys_info = {
        "node": "Unbekannt",
        "os": "Unbekannt",
        "cpu_percent": 0.0,
        "cpu_cores": 0,
        "ram_percent": 0.0,
        "ram_total_gb": 0.0,
        "disk_percent": 0.0,
        "disk_total_gb": 0.0,
        "battery_percent": None,
        "battery_plugged": None,
        "wifi_ssid": "Nicht verfügbar",
        "wifi_signal": "",
        "temperatures": [],
        "fans": [],
        "net_io": {"sys_sent_mb": 0.0, "sys_recv_mb": 0.0, "app_sent_mb": 0.0, "app_recv_mb": 0.0},
        "top_processes_ram": [],
        "top_processes_cpu": []
    }

    try:
        sys_info["node"] = platform.node() or "Unbekannt"
    except Exception:
        pass

    try:
        sys_info["os"] = f"{platform.system()} {platform.release()}"
    except Exception:
        pass

    try:
        sys_info["cpu_percent"] = psutil.cpu_percent(interval=None)
        sys_info["cpu_cores"] = psutil.cpu_count(logical=True)
    except Exception:
        pass

    try:
        ram = psutil.virtual_memory()
        sys_info["ram_percent"] = ram.percent
        sys_info["ram_total_gb"] = round(ram.total / (1024**3), 2)
    except Exception:
        pass

    try:
        disk = psutil.disk_usage('/')
        sys_info["disk_percent"] = disk.percent
        sys_info["disk_total_gb"] = round(disk.total / (1024**3), 2)
    except Exception:
        pass

    try:
        batt = psutil.sensors_battery()
        if batt:
            sys_info["battery_percent"] = round(batt.percent, 1)
            sys_info["battery_plugged"] = batt.power_plugged
    except Exception:
        pass

    # Temperaturen & Lüfter
    try:
        if hasattr(psutil, "sensors_temperatures"):
            temps = psutil.sensors_temperatures()
            for name, entries in temps.items():
                for entry in entries:
                    sys_info["temperatures"].append({
                        "label": entry.label or name,
                        "current": entry.current,
                        "high": entry.high
                    })
    except Exception:
        pass

    try:
        if hasattr(psutil, "sensors_fans"):
            fans = psutil.sensors_fans()
            for name, entries in fans.items():
                for entry in entries:
                    sys_info["fans"].append({
                        "label": entry.label or name,
                        "current": entry.current
                    })
    except Exception:
        pass

    # Netzwerk-Traffic (App-spezifisch vs. System-weit)
    try:
        net = psutil.net_io_counters()
        sys_info["net_io"] = {
            "sys_sent_mb": round(net.bytes_sent / (1024 ** 2), 1),
            "sys_recv_mb": round(net.bytes_recv / (1024 ** 2), 1),
            "app_sent_mb": round(flask_traffic_tx / (1024 ** 2), 3),
            "app_recv_mb": round(flask_traffic_rx / (1024 ** 2), 3)
        }
    except Exception:
        pass

    # Top 10 Prozesse nach RAM und CPU
    try:
        procs = []
        for p in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
            try:
                pinfo = p.info
                pinfo['cpu_percent'] = p.cpu_percent(interval=None) or 0.0
                pinfo['memory_percent'] = round(p.memory_percent() or 0.0, 1)
                procs.append(pinfo)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        sys_info["top_processes_ram"] = sorted(procs, key=lambda x: x['memory_percent'], reverse=True)[:10]
        sys_info["top_processes_cpu"] = sorted(procs, key=lambda x: x['cpu_percent'], reverse=True)[:10]
    except Exception:
        pass

    try:
        if platform.system() == "Linux":
            env = os.environ.copy()
            env["LC_ALL"] = "C"
            env["LANG"] = "C"

            try:
                out = subprocess.check_output(
                    ["nmcli", "-t", "-f", "ACTIVE,SSID,SIGNAL", "dev", "wifi"],
                    text=True, stderr=subprocess.DEVNULL, env=env, timeout=2
                )
                for line in out.splitlines():
                    if line.lower().startswith(("yes:", "ja:")):
                        parts = line.rsplit(":", 2)
                        if len(parts) == 3 and parts[1]:
                            sys_info["wifi_ssid"] = parts[1].replace("\\:", ":")
                            sys_info["wifi_signal"] = f"{parts[2]}%"
                            break
            except Exception:
                pass

            if sys_info["wifi_ssid"] == "Nicht verfügbar":
                try:
                    ssid = subprocess.check_output(
                        ["iwgetid", "-r"], text=True, stderr=subprocess.DEVNULL, timeout=2
                    ).strip()
                    if ssid:
                        sys_info["wifi_ssid"] = ssid
                except Exception:
                    pass

                if os.path.exists("/proc/net/wireless"):
                    with open("/proc/net/wireless", "r") as f:
                        lines = f.readlines()
                        if len(lines) > 2:
                            parts = lines[2].split()
                            if len(parts) >= 3:
                                qual = float(parts[2].rstrip("."))
                                pct = min(100, max(0, int((qual / 70.0) * 100)))
                                sys_info["wifi_signal"] = f"{pct}%"
                                if sys_info["wifi_ssid"] == "Nicht verfügbar":
                                    iface = parts[0].rstrip(":")
                                    sys_info["wifi_ssid"] = f"WLAN ({iface})"

            if sys_info["wifi_ssid"] == "Nicht verfügbar":
                stats = psutil.net_if_stats()
                for iface, st in stats.items():
                    if st.isup and not iface.startswith(("lo", "docker", "veth", "br-", "virbr", "tun", "tap", "wg")):
                        sys_info["wifi_ssid"] = f"LAN ({iface})"
                        sys_info["wifi_signal"] = f"{st.speed} Mbit/s" if st.speed > 0 else "Verbunden"
                        break
    except Exception:
        pass

    return jsonify({
        "api_online": api_online,
        "api_error": api_error,
        "osmand_port_open": osmand_open,
        "traccar_user": TRACCAR_USER,
        "devices": devices,
        "recent_webhooks": list(recent_traccar_webhooks),
        "process_running": proc_running,
        "process_pid": proc_pid,
        "java_installed": java_installed,
        "jar_exists": jar_exists,
        "xml_exists": xml_exists,
        "db_exists": traccar_db_exists,
        "game_db_size_kb": game_db_size_kb,
        "active_threads": active_threads,
        "python_version": python_version,
        "game_phase": phase,
        "num_teams": num_teams,
        "sys_info": sys_info
    })


@app.route('/api/traccar_debug/<admin_slug>/sync', methods=['POST'])
def traccar_debug_sync(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403
    ok = sync_traccar_devices()
    return jsonify({'success': ok, 'error': traccar_last_error})


@app.route('/api/traccar_debug/<admin_slug>/hard_reset', methods=['POST'])
def traccar_debug_hard_reset(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    ok = reset_traccar_process_and_db()
    if not ok:
        return jsonify({'success': False, 'error': traccar_last_error})

    for _ in range(15):
        time.sleep(2)
        if ensure_traccar_credentials():
            sync_traccar_devices()
            return jsonify({'success': True})

    return jsonify({'success': False, 'error': f"Java-Server neu gestartet, bootet noch ({traccar_last_error}). Bitte 5s warten."})


import subprocess
import sys
import os


@app.route(f'/adminconsole-<admin_slug>/api/restart_traccar', methods=['POST'])
def api_restart_traccar(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    try:
        stop_traccar_process()
        success = start_traccar_server()
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route(f'/adminconsole-<admin_slug>/api/restart_flask', methods=['POST'])
def api_restart_flask(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    # Wichtig: Funktioniert zuverlässig, wenn systemd oder ein Service-Manager die App neu startet
    def delayed_restart():
        time.sleep(1)
        stop_traccar_process()
        python = sys.executable
        os.execl(python, python, *sys.argv)

    threading.Thread(target=delayed_restart, daemon=True).start()
    return jsonify({'success': True, 'message': 'Flask startet neu...'})


@app.route(f'/api/traccar_debug/<admin_slug>/run_update', methods=['POST'])
def api_run_update(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    update_script_path = os.path.join(BASE_DIR, 'update.py')
    
    if not os.path.exists(update_script_path):
        return jsonify({'success': False, 'error': f"Die Datei {update_script_path} wurde nicht gefunden."})

    try:
        # Führt die update.py aus und fängt den Output (stdout + stderr) ab
        result = subprocess.run(
            [sys.executable, update_script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=BASE_DIR,
            timeout=300 # Maximal 5 Minuten Ausführungszeit
        )
        return jsonify({'success': True, 'output': result.stdout, 'returncode': result.returncode})
    except subprocess.TimeoutExpired:
        return jsonify({'success': False, 'error': 'Das Skript hat zu lange gebraucht und wurde abgebrochen (Timeout).'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# Hintergrund-Worker auf Modul-Ebene starten (für Gunicorn & Flask-Entwicklungsserver)
threading.Thread(target=traccar_background_worker, daemon=True).start()
threading.Thread(target=game_rules_background_worker, daemon=True).start()
threading.Thread(target=server_status_logger_worker, daemon=True).start()


if __name__ == '__main__':
    print(f"🔑 Admin-Konsole: http://127.0.0.1:5000/adminconsole-{ADMIN_SLUG}")
    print(f"🛰️ Traccar-Debug: http://127.0.0.1:5000/traccar_debug-{ADMIN_SLUG}")
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5000)
