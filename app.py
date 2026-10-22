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
GTFS_STOPS_FILE = os.path.join(STOPS_DIR, 'stops.txt')
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
    "target_stops": 12,
    "max_history": 30,
    "num_teams": 4,
    "team_names": [],
    "points_stop_reached": 2,
    "points_was_caught": -3,
    "points_caught_team": 5,
    "start_time": "",
    "end_time": "",
    "timezone": "Europe/Berlin",
    "enable_mid_game_swap": True,
    "shuffle_stops_on_swap": True,
    "auto_approve_stops": False,
    "stop_radius_meters": 75,
    "auto_calculate_stops": True
}

stops_data = {}
ALL_STOPS = []
ALTERNATIVE_PAIRS = {}
current_active_dataset = None


def load_active_stops_data(filename=None):
    global stops_data, ALL_STOPS, ALTERNATIVE_PAIRS, current_active_dataset
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

    loaded = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
        except Exception as e:
            app.logger.error(f"Fehler beim Laden von {filepath}: {e}")
            loaded = {}

    stops_data.clear()
    ALL_STOPS.clear()
    ALTERNATIVE_PAIRS.clear()

    # In ALL_STOPS dürfen weiterhin nur die Haupt-Haltestellen landen (oberste Keys im JSON)
    ALL_STOPS.extend(list(loaded.keys()))

    for main_name, stop_info in loaded.items():
        if not isinstance(stop_info, dict):
            continue
        stops_data[main_name] = {
            "lat": stop_info.get("lat"),
            "lon": stop_info.get("lon"),
            "factor": stop_info.get("factor", 1) if stop_info.get("factor") is not None else 1
        }
        alt = stop_info.get("alternative")
        if alt and isinstance(alt, dict) and alt.get("name"):
            alt_name = alt["name"].strip()
            if alt_name:
                stops_data[alt_name] = {
                    "lat": alt.get("lat"),
                    "lon": alt.get("lon"),
                    "factor": alt.get("factor", 1) if alt.get("factor") is not None else 1
                }
                ALTERNATIVE_PAIRS[main_name] = alt_name
                ALTERNATIVE_PAIRS[alt_name] = main_name

    current_active_dataset = safe_filename
    return stops_data

# --- TRACCAR KONFIGURATION & SYNC ---
TRACCAR_BASE_URL = os.getenv("TRACCAR_BASE_URL", "http://127.0.0.1:8082/api")
TRACCAR_OSMAND_URL = os.getenv("TRACCAR_OSMAND_URL", "http://127.0.0.1:5055")
TRACCAR_API_URL = f"{TRACCAR_BASE_URL}/devices"

TRACCAR_USER = os.getenv("TRACCAR_API_USER", "admin").strip()
TRACCAR_PASS = os.getenv("TRACCAR_API_PASSWORD", "admin").strip()

# --- TRACCAR LOGGING KONFIGURATION ---
TRACCAR_LOG_ENABLED = False
TRACCAR_LOG_FILE = os.path.join(BASE_DIR, 'log.json')
traccar_log_lock = threading.Lock()

def write_traccar_api_log(data):
    """Schreibt Traccar-API-Anfragen thread-sicher in die log.json."""
    if not TRACCAR_LOG_ENABLED:
        return
    
    with traccar_log_lock:
        try:
            logs = []
            if os.path.exists(TRACCAR_LOG_FILE):
                with open(TRACCAR_LOG_FILE, 'r', encoding='utf-8') as f:
                    try:
                        logs = json.load(f)
                        if not isinstance(logs, list):
                            logs = []
                    except json.JSONDecodeError:
                        logs = []
            
            # Maximal 1000 Einträge im Log behalten, um die Datei nicht explodieren zu lassen
            if len(logs) >= 1000:
                logs = logs[-999:]
                
            logs.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "payload": data
            })
            
            with open(TRACCAR_LOG_FILE, 'w', encoding='utf-8') as f:
                json.dump(logs, f, indent=2, ensure_ascii=False)
        except Exception as e:
            app.logger.error(f"Fehler beim Schreiben der log.json: {e}")

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

# Globaler Replay-Cache für /api/replay_data
replay_cache = {"data": None, "timestamp": None}


# --- DATENBANK SETUP & MIGRATION ---
def ensure_column(cursor, table_name, column_name, column_type):
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_cols = [row[1] for row in cursor.fetchall()]
    if column_name not in existing_cols:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")


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

        c.execute('''CREATE TABLE IF NOT EXISTS team_stops (
                        team_id TEXT,
                        stop_name TEXT,
                        submitted_at TEXT,
                        actual_timestamp TEXT,
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS team_catches (
                        hunter_id TEXT,
                        prey_id TEXT,
                        submitted_at TEXT,
                        actual_timestamp TEXT,
                        FOREIGN KEY(hunter_id) REFERENCES teams(id),
                        FOREIGN KEY(prey_id) REFERENCES teams(id)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS active_stops (
                        stop_name TEXT
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS stop_history (
                        history_order INTEGER PRIMARY KEY AUTOINCREMENT,
                        stop_name TEXT
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
                        notified_admin INTEGER
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

        c.execute('''CREATE TABLE IF NOT EXISTS replay_stop_lifecycle (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        stop_name TEXT,
                        active_from TEXT,
                        active_until TEXT
                    )''')

        # NEU: Tabelle für den Rollen-Lebenszyklus
        c.execute('''CREATE TABLE IF NOT EXISTS replay_role_lifecycle (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        team_id TEXT,
                        role TEXT,
                        active_from TEXT,
                        active_until TEXT
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS gps_outages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        team_id TEXT,
                        offline_from TEXT,
                        offline_until TEXT,
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')


        # Sicherstellen, dass bestehende Datenbanken nahtlos um die neuen Zeitspalten erweitert werden
        ensure_column(c, "team_stops", "submitted_at", "TEXT")
        ensure_column(c, "team_stops", "actual_timestamp", "TEXT")
        ensure_column(c, "team_catches", "submitted_at", "TEXT")
        ensure_column(c, "team_catches", "actual_timestamp", "TEXT")
        ensure_column(c, "pending_requests", "gps_check_until", "TEXT")
        ensure_column(c, "pending_requests", "notified_admin", "INTEGER")
        ensure_column(c, "action_logs", "actual_timestamp", "TEXT")
        ensure_column(c, "action_logs", "admin_only", "INTEGER")

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


def find_closest_stop_approach(conn, team_id, stop_name, start_dt, end_dt):
    """
    Sucht in location_history (sowie aktuellem Live-Standort) im Zeitfenster [start_dt, end_dt] den Punkt,
    an dem team_id der Haltestelle stop_name am nächsten war.
    Gibt (min_distance_meters, closest_timestamp_iso) oder (None, None) zurück.
    """
    if stop_name not in stops_data:
        return None, None
    stop_lat = stops_data[stop_name]["lat"]
    stop_lon = stops_data[stop_name]["lon"]

    c = conn.cursor()
    c.execute("""
        SELECT lat, lon, timestamp
        FROM location_history
        WHERE team_id = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp ASC
    """, (team_id, start_dt.isoformat(), end_dt.isoformat()))
    rows = list(c.fetchall())

    c.execute("""
        SELECT lat, lon, updated_at
        FROM team_locations
        WHERE team_id = ? AND updated_at >= ? AND updated_at <= ?
    """, (team_id, start_dt.isoformat(), end_dt.isoformat()))
    live_row = c.fetchone()
    if live_row:
        rows.append(live_row)

    if not rows:
        return None, None

    min_dist = None
    best_ts = None
    for r in rows:
        d = haversine_meters(r[0], r[1], stop_lat, stop_lon)
        if min_dist is None or d < min_dist:
            min_dist = d
            best_ts = r[2]

    return min_dist, best_ts


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


def stop_legacy_docker_container():
    if shutil.which("docker"):
        for prefix in ([], ["sudo", "-n"]):
            try:
                res = subprocess.run(
                    prefix + ["docker", "ps", "-q", "-f", "name=traccar-server"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=3
                )
                if res.stdout.strip():
                    print("🧹 Alter Docker-Container 'traccar-server' erkannt. Stoppe und entferne ihn...")
                    subprocess.run(prefix + ["docker", "rm", "-f", "traccar-server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    time.sleep(1)
            except Exception:
                pass


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

    stop_legacy_docker_container()

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


def trigger_traccar_sync_async(teams_data):
    copy_data = json.loads(json.dumps(teams_data))
    threading.Thread(target=sync_traccar_devices, args=(copy_data,), daemon=True).start()


# --- HINTERGRUND-PRÜFUNG FÜR GPS-INAKTIVITÄT & 1-MINUTEN-HALTESTELLEN-CHECKS ---
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

    try:
        with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
            conn.row_factory = sqlite3.Row
            state = get_state(conn)
            phase, start_iso, end_iso, _, _ = update_game_time_state(conn, state)
            config = state["config"]
            state_changed = False

            # 1. 1-Minuten-Nachprüfung für offene Haltestellen-Anfragen
            if config.get("auto_approve_stops", False):
                radius_m = float(config.get("stop_radius_meters", 100))
                remaining_requests = []
                for req in state["pending_requests"]:
                    if req.get("type") == "stop" and req.get("gps_check_until"):
                        if req.get("stop") not in state["active_stops"]:
                            # Haltestelle ist nicht mehr aktiv (z.B. von anderem Team geholt oder neu gewürfelt) -> verwerfen
                            state_changed = True
                            continue

                        req_dt = parse_iso_utc(req.get("timestamp")) or now_utc
                        until_dt = parse_iso_utc(req.get("gps_check_until"))
                        window_start = req_dt - timedelta(minutes=5)
                        window_end = min(now_utc, until_dt) if until_dt else now_utc

                        min_dist, closest_iso = find_closest_stop_approach(
                            conn, req["team"], req["stop"], window_start, window_end
                        )

                        if min_dist is not None and min_dist <= radius_m:
                            execute_stop_catch(
                                state, req["stop"], req["team"],
                                is_late=req.get("is_late", False),
                                submitted_ts=req.get("timestamp"),
                                actual_ts=closest_iso or req.get("timestamp"),
                                auto_approved=True
                            )
                            state_changed = True
                            t_name = req.get("team_name", req["team"])
                            telegram_messages.append(
                                f"✅ AUTO-GPS-FREIGABE (Nachprüfung): Team '{t_name}' hat Haltestelle '{req['stop']}' ({int(min_dist)}m) erreicht."
                            )
                            continue
                        elif until_dt and now_utc >= until_dt:
                            # 1 Minute ist abgelaufen -> wird zur regulären manuellen Admin-Anfrage
                            req.pop("gps_check_until", None)
                            if not req.get("notified_admin"):
                                req["notified_admin"] = True
                                t_name = req.get("team_name", req["team"])
                                telegram_messages.append(
                                    f"📬 MANUELLE FREIGABE NÖTIG: Team '{t_name}' möchte Haltestelle '{req['stop']}' eintragen (kein GPS-Punkt im {int(radius_m)}m Radius gefunden)."
                                )
                            state_changed = True
                    remaining_requests.append(req)
                state["pending_requests"] = remaining_requests

            # 2. 5 Minuten Standort-Ausfall-Historie über robuste SQLite-Tabelle
            if phase in ("pre_game", "game", "post_game", "finished"):
                c = conn.cursor()
                c.execute("SELECT team_id, updated_at FROM team_locations")
                loc_map = {r["team_id"]: parse_iso_utc(r["updated_at"]) for r in c.fetchall()}
                start_dt_utc = parse_iso_utc(start_iso)
                pre_start_utc = start_dt_utc - timedelta(minutes=5) if start_dt_utc else None
                end_dt_utc = parse_iso_utc(end_iso)

                for t_id, t_data in state["teams_data"].items():
                    if t_data.get("deactivated", False) or t_data.get("role") not in ("läufer", "fänger"):
                        continue

                    last_dt = loc_map.get(t_id)
                    ref_dt = pre_start_utc if (last_dt and pre_start_utc and last_dt < pre_start_utc) else (last_dt or pre_start_utc)
                    
                    if not ref_dt:
                        continue

                    c.execute("SELECT id FROM gps_outages WHERE team_id = ? AND offline_until IS NULL", (t_id,))
                    open_outage = c.fetchone()

                    if phase in ("post_game", "finished"):
                        if open_outage:
                            close_time = end_dt_utc.isoformat() if end_dt_utc else now_utc.isoformat()
                            c.execute("UPDATE gps_outages SET offline_until = ? WHERE id = ?", (close_time, open_outage["id"]))
                        continue

                    missing_sec = (now_utc - ref_dt).total_seconds()

                    if missing_sec >= 300:
                        if not open_outage:
                            c.execute("INSERT INTO gps_outages (team_id, offline_from) VALUES (?, ?)", (t_id, ref_dt.isoformat()))
                            t_name = t_data.get("display_name", t_id)
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
                            state_changed = True

            if state_changed:
                save_state(conn, state)

    except Exception as e:
        print(f"⚠️ Fehler in check_game_gps_rules: {e}")
    finally:
        gps_rules_lock.release()

    for msg in telegram_messages:
        send_telegram_msg(msg)


def game_rules_background_worker():
    while True:
        time.sleep(5)
        check_game_gps_rules()


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
def send_telegram_msg(text):
    TOKEN = os.getenv("TELEGRAM_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    if not TOKEN or not CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        requests.post(url, data={'chat_id': CHAT_ID, 'text': text}, timeout=5)
    except Exception as e:
        print(f'Telegram error: {e}')


def generate_slug(display_name, pin):
    clean_name = re.sub(r'[^a-z0-9]+', '-', display_name.lower()).strip('-')
    return f"{clean_name}_{pin}"


def calculate_next_roles(teams_data):
    for t_id, d in teams_data.items():
        if d.get("deactivated", False):
            d["next_role"] = d.get("role", "keine")
            continue
        current = d.get("role", "keine")
        if current == "fänger":
            d["next_role"] = "läufer"
        elif current == "läufer":
            d["next_role"] = "fänger"
        else:
            d["next_role"] = "keine"


def calculate_score(team_data, config):
    stops_points = sum(stops_data.get(stop, {}).get("factor", 1) * config["points_stop_reached"] for stop in team_data.get("stops", []))
    raw_score = stops_points + (len(team_data.get("caught", [])) * config["points_caught_team"]) + (len(team_data.get("caught_by", [])) * config["points_was_caught"]) + team_data.get("manual_points", 0)
    return int(raw_score) if raw_score == int(raw_score) else float(raw_score)


def save_state(conn, state):
    c = conn.cursor()

    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('config', ?)", (json.dumps(state['config']),))
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('roles_assigned', ?)", (1 if state.get('roles_assigned') else 0,))
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('roles_swapped', ?)", (1 if state.get('roles_swapped') else 0,))
    if 'active_stops_dataset' in state:
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_stops_dataset', ?)", (state['active_stops_dataset'],))

    c.execute("DELETE FROM active_stops")
    for stop in state.get('active_stops', []):
        c.execute("INSERT INTO active_stops (stop_name) VALUES (?)", (stop,))

    c.execute("DELETE FROM stop_history")
    max_history = state["config"].get("max_history", 30)
    history = state['history'][-max_history:] if len(state['history']) > max_history else state['history']
    for stop in history:
        c.execute("INSERT INTO stop_history (stop_name) VALUES (?)", (stop,))

    # Bestehende Zeitstempel aus der Datenbank sichern, damit sie auch nach > 50 action_log-Einträgen erhalten bleiben
    stop_times_by_team = {}
    catch_times_by_pair = {}

    c.execute("SELECT team_id, stop_name, submitted_at, actual_timestamp FROM team_stops")
    for r in c.fetchall():
        if r[2] is not None or r[3] is not None:
            stop_times_by_team.setdefault((r[0], r[1]), []).append((r[2], r[3] or r[2]))

    c.execute("SELECT hunter_id, prey_id, submitted_at, actual_timestamp FROM team_catches")
    for r in c.fetchall():
        if r[2] is not None or r[3] is not None:
            catch_times_by_pair.setdefault((r[0], r[1]), []).append((r[2], r[3] or r[2]))

    # Falls im State bereits stop_timestamps / catch_timestamps mitgeführt wurden, diese berücksichtigen
    for t_id, data in state.get('teams_data', {}).items():
        for entry in data.get('stop_timestamps', []):
            if len(entry) >= 3 and (entry[1] is not None or entry[2] is not None):
                key = (t_id, entry[0])
                pair = (entry[1], entry[2] or entry[1])
                if pair not in stop_times_by_team.get(key, []):
                    stop_times_by_team.setdefault(key, []).append(pair)
        for entry in data.get('catch_timestamps', []):
            if len(entry) >= 3 and (entry[1] is not None or entry[2] is not None):
                key = (t_id, entry[0])
                pair = (entry[1], entry[2] or entry[1])
                if pair not in catch_times_by_pair.get(key, []):
                    catch_times_by_pair.setdefault(key, []).append(pair)

    # Zeitstempel aus action_log für team_stops und team_catches ergänzen
    for log in reversed(state.get('action_log', [])):
        if log.get('type') == 'stop' and log.get('team') and log.get('stop'):
            key = (log['team'], log['stop'])
            pair = (log.get('timestamp'), log.get('actual_timestamp') or log.get('timestamp'))
            if pair[0] is not None or pair[1] is not None:
                if pair not in stop_times_by_team.get(key, []):
                    stop_times_by_team.setdefault(key, []).append(pair)
        elif log.get('type') == 'catch' and log.get('hunter') and log.get('prey'):
            key = (log['hunter'], log['prey'])
            pair = (log.get('timestamp'), log.get('actual_timestamp') or log.get('timestamp'))
            if pair[0] is not None or pair[1] is not None:
                if pair not in catch_times_by_pair.get(key, []):
                    catch_times_by_pair.setdefault(key, []).append(pair)

    c.execute("DELETE FROM teams")
    c.execute("DELETE FROM team_stops")
    c.execute("DELETE FROM team_catches")
    for t_id, data in state['teams_data'].items():
        c.execute("""
            INSERT OR REPLACE INTO teams (id, display_name, pin, role, next_role, slug, deactivated, manual_points)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (t_id, data['display_name'], data['pin'], data.get('role', 'keine'), data.get('next_role', 'keine'), data['slug'], 1 if data.get('deactivated') else 0, data.get('manual_points', 0)))

        for stop in data.get('stops', []):
            times_list = stop_times_by_team.get((t_id, stop), [])
            sub_ts, act_ts = times_list.pop(0) if times_list else (None, None)
            c.execute("INSERT INTO team_stops (team_id, stop_name, submitted_at, actual_timestamp) VALUES (?, ?, ?, ?)", (t_id, stop, sub_ts, act_ts))
        for prey_id in data.get('caught', []):
            times_list = catch_times_by_pair.get((t_id, prey_id), [])
            sub_ts, act_ts = times_list.pop(0) if times_list else (None, None)
            c.execute("INSERT INTO team_catches (hunter_id, prey_id, submitted_at, actual_timestamp) VALUES (?, ?, ?, ?)", (t_id, prey_id, sub_ts, act_ts))

    c.execute("DELETE FROM pending_requests")
    for req in state['pending_requests']:
        c.execute("""
            INSERT INTO pending_requests (id, type, team, team_name, stop, reporter, reporter_name, hunter, hunter_name, prey, prey_name, timestamp, is_late, gps_check_until, notified_admin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            req['id'], req['type'], req.get('team'), req.get('team_name'), req.get('stop'),
            req.get('reporter'), req.get('reporter_name'), req.get('hunter'), req.get('hunter_name'),
            req.get('prey'), req.get('prey_name'), req.get('timestamp'),
            1 if req.get('is_late') else 0, req.get('gps_check_until'),
            1 if req.get('notified_admin') else 0
        ))

    c.execute("DELETE FROM action_logs")
    logs = state['action_log'][:50] if len(state['action_log']) > 50 else state['action_log']
    for log in logs:
        c.execute("""
            INSERT INTO action_logs (id, type, team, team_name, stop, hunter, hunter_name, prey, prey_name, timestamp, actual_timestamp, desc, is_late, admin_only)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            log['id'], log['type'], log.get('team'), log.get('team_name'), log.get('stop'),
            log.get('hunter'), log.get('hunter_name'), log.get('prey'), log.get('prey_name'),
            log.get('timestamp'), log.get('actual_timestamp'), log.get('desc'),
            1 if log.get('is_late') else 0, 1 if log.get('admin_only') else 0
        ))

    # --- REPLAY STOP LIFECYCLE SYNC ---
    now_iso = datetime.now(timezone.utc).isoformat()
    current_active = set(state.get('active_stops', []))

    # 1. Hole alle aktuell in der DB als "offen" markierten Haltestellen
    c.execute("SELECT id, stop_name FROM replay_stop_lifecycle WHERE active_until IS NULL")
    open_records = c.fetchall()

    open_stops_in_db = set()
    for record in open_records:
        record_id, s_name = record[0], record[1]
        open_stops_in_db.add(s_name)
        # Wenn ein offener Stop nicht mehr im State aktiv ist -> schließen!
        if s_name not in current_active:
            c.execute("UPDATE replay_stop_lifecycle SET active_until = ? WHERE id = ?", (now_iso, record_id))

    # 2. Öffne neue Lebenszyklen für Haltestellen, die aktiv sind, aber noch keinen offenen DB-Eintrag haben
    for s_name in current_active:
        if s_name not in open_stops_in_db:
            c.execute("INSERT INTO replay_stop_lifecycle (stop_name, active_from) VALUES (?, ?)", (s_name, now_iso))
    # ----------------------------------

    # --- NEU: REPLAY ROLE LIFECYCLE SYNC ---
    c.execute("SELECT id, team_id, role FROM replay_role_lifecycle WHERE active_until IS NULL")
    open_roles = {}
    for r in c.fetchall():
        open_roles[r[1]] = {"id": r[0], "role": r[2]}

    for t_id, data in state["teams_data"].items():
        current_role = data.get("role", "keine")
        if t_id in open_roles:
            if open_roles[t_id]["role"] != current_role:
                # Rolle hat sich geändert -> alten Zyklus schließen, neuen öffnen
                c.execute("UPDATE replay_role_lifecycle SET active_until = ? WHERE id = ?", (now_iso, open_roles[t_id]["id"]))
                c.execute("INSERT INTO replay_role_lifecycle (team_id, role, active_from) VALUES (?, ?, ?)", (t_id, current_role, now_iso))
        else:
            # Bisher kein offener Zyklus -> neu öffnen
            c.execute("INSERT INTO replay_role_lifecycle (team_id, role, active_from) VALUES (?, ?, ?)", (t_id, current_role, now_iso))
    # ---------------------------------------

    conn.commit()


def get_state(conn):
    c = conn.cursor()
    c.execute("SELECT key, value FROM settings WHERE key != 'admin_slug'")
    rows = c.fetchall()

    if not rows:
        load_active_stops_data("Catch_Haltestellen.json")
        initial_active = select_random_stops(DEFAULT_CONFIG["target_stops"])
        num_teams = DEFAULT_CONFIG["num_teams"]
        team_names = DEFAULT_CONFIG["team_names"]

        teams_data = {}
        for i in range(num_teams):
            team_id = f"Team {i + 1}"
            display_name = team_names[i].strip() if i < len(team_names) and team_names[i].strip() else team_id
            pin = f"{random.randint(0, 9999):04d}"

            teams_data[team_id] = {
                "display_name": display_name,
                "stops": [], "stop_timestamps": [], "caught": [], "catch_timestamps": [], "caught_by": [], "pin": pin, "role": "keine",
                "slug": generate_slug(team_id, pin), "deactivated": False,
                "next_role": "keine", "manual_points": 0
            }

        calculate_next_roles(teams_data)
        state = {
            "config": DEFAULT_CONFIG.copy(),
            "active_stops_dataset": "Catch_Haltestellen.json",
            "active_stops": initial_active,
            "history": initial_active.copy(),
            "teams_data": teams_data,
            "pending_requests": [],
            "action_log": [],
            "roles_swapped": False,
            "roles_assigned": False
        }
        c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_stops_dataset', ?)", ("Catch_Haltestellen.json",))
        save_state(conn, state)
        return state

    state = {}
    settings_dict = {row['key']: row['value'] for row in rows}

    state['config'] = json.loads(settings_dict.get('config', '{}'))
    state['roles_assigned'] = bool(int(settings_dict.get('roles_assigned', 0)))
    state['roles_swapped'] = bool(int(settings_dict.get('roles_swapped', 0)))
    state['active_stops_dataset'] = settings_dict.get('active_stops_dataset', 'Catch_Haltestellen.json')

    if current_active_dataset != state['active_stops_dataset'] or not stops_data:
        load_active_stops_data(state['active_stops_dataset'])

    c.execute("SELECT stop_name FROM active_stops")
    state['active_stops'] = [r['stop_name'] for r in c.fetchall()]

    c.execute("SELECT stop_name FROM stop_history ORDER BY history_order ASC")
    state['history'] = [r['stop_name'] for r in c.fetchall()]

    state['teams_data'] = {}
    c.execute("SELECT * FROM teams")
    for t in c.fetchall():
        t_id = t['id']
        state['teams_data'][t_id] = {
            'display_name': t['display_name'],
            'pin': t['pin'],
            'role': t['role'],
            'next_role': t['next_role'],
            'slug': t['slug'],
            'deactivated': bool(t['deactivated']),
            'manual_points': t['manual_points'],
            'stops': [],
            'stop_timestamps': [],
            'caught': [],
            'catch_timestamps': [],
            'caught_by': []
        }

    c.execute("SELECT team_id, stop_name, submitted_at, actual_timestamp FROM team_stops")
    for r in c.fetchall():
        if r['team_id'] in state['teams_data']:
            state['teams_data'][r['team_id']]['stops'].append(r['stop_name'])
            if r['submitted_at'] is not None or r['actual_timestamp'] is not None:
                state['teams_data'][r['team_id']]['stop_timestamps'].append(
                    (r['stop_name'], r['submitted_at'], r['actual_timestamp'])
                )

    c.execute("SELECT hunter_id, prey_id, submitted_at, actual_timestamp FROM team_catches")
    for r in c.fetchall():
        h_id, p_id = r['hunter_id'], r['prey_id']
        if h_id in state['teams_data']:
            state['teams_data'][h_id]['caught'].append(p_id)
            if r['submitted_at'] is not None or r['actual_timestamp'] is not None:
                state['teams_data'][h_id]['catch_timestamps'].append(
                    (p_id, r['submitted_at'], r['actual_timestamp'])
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
    c.execute("SELECT * FROM action_logs ORDER BY timestamp DESC")
    for r in c.fetchall():
        log = dict(r)
        log['is_late'] = bool(log.get('is_late'))
        log['admin_only'] = bool(log.get('admin_only'))
        state['action_log'].append({k: v for k, v in log.items() if v is not None})

    for k, v in DEFAULT_CONFIG.items():
        if k not in state["config"]:
            state["config"][k] = v

    return state


def roll_stop_choice(base_stop):
    if base_stop in ALTERNATIVE_PAIRS and random.random() < 0.5:
        return ALTERNATIVE_PAIRS[base_stop]
    return base_stop


def get_new_random_stop(active_stops, history):
    if not ALL_STOPS:
        return ""

    blocked = set(active_stops or []) | set(history or [])
    for stop in list(blocked):
        if stop in ALTERNATIVE_PAIRS:
            blocked.add(ALTERNATIVE_PAIRS[stop])

    available = [s for s in ALL_STOPS if s not in blocked]
    if not available:
        blocked_active = set(active_stops or [])
        for stop in list(blocked_active):
            if stop in ALTERNATIVE_PAIRS:
                blocked_active.add(ALTERNATIVE_PAIRS[stop])
        available = [s for s in ALL_STOPS if s not in blocked_active]

    if not available:
        base_stop = random.choice(ALL_STOPS)
    else:
        base_stop = random.choice(available)

    return roll_stop_choice(base_stop)


def select_random_stops(count):
    if not ALL_STOPS:
        return []
    sample_size = min(count, len(ALL_STOPS))
    return [roll_stop_choice(s) for s in random.sample(ALL_STOPS, sample_size)]


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

        if hasattr(tz, 'localize'):
            start_dt = tz.localize(start_dt.replace(tzinfo=None))
            end_dt = tz.localize(end_dt.replace(tzinfo=None))
        else:
            start_dt = start_dt.replace(tzinfo=tz)
            end_dt = end_dt.replace(tzinfo=tz)

    except ValueError:
        return "inactive", None, None, None, now.isoformat()

    role_assign_dt = start_dt - timedelta(minutes=30)
    if now >= role_assign_dt and not state.get("roles_assigned", False):
        active_teams = [t_id for t_id, d in state["teams_data"].items() if not d.get("deactivated", False)]
        num_active = len(active_teams)
        if num_active > 0:
            if random.choice([True, False]):
                num_laeufer = (num_active + 1) // 2
                num_faenger = num_active - num_laeufer
            else:
                num_faenger = (num_active + 1) // 2
                num_laeufer = num_active - num_faenger

            roles = ["fänger"] * num_faenger + ["läufer"] * num_laeufer
            random.shuffle(roles)

            now_iso = datetime.now(timezone.utc).isoformat()
            for i, t_id in enumerate(active_teams):
                state["teams_data"][t_id]["role"] = roles[i]
                state["action_log"].insert(0, {
                    "id": str(uuid.uuid4()),
                    "type": "role_change",
                    "team": t_id,
                    "timestamp": now_iso,
                    "actual_timestamp": now_iso,
                    "desc": roles[i],
                    "admin_only": True
                })

            calculate_next_roles(state["teams_data"])
            state["roles_assigned"] = True
            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()), "type": "system", "timestamp": now_iso,
                "desc": "🎲 Die Rollen wurden zugewiesen!"
            })
            save_state(conn, state)

    mid_dt = None
    if config.get("enable_mid_game_swap", True):
        mid_dt = start_dt + (end_dt - start_dt) / 2
        if now >= mid_dt and not state.get("roles_swapped", False):
            now_iso = datetime.now(timezone.utc).isoformat()
            for t_id, data in state["teams_data"].items():
                if not data.get("deactivated", False):
                    current_role = data.get("role", "keine")
                    new_role = None
                    if current_role == "läufer":
                        new_role = "fänger"
                        data["role"] = "fänger"
                    elif current_role == "fänger":
                        new_role = "läufer"
                        data["role"] = "läufer"
                    if new_role:
                        state["action_log"].insert(0, {
                            "id": str(uuid.uuid4()),
                            "type": "role_change",
                            "team": t_id,
                            "timestamp": now_iso,
                            "actual_timestamp": now_iso,
                            "desc": new_role,
                            "admin_only": True
                        })

            calculate_next_roles(state["teams_data"])
            state["roles_swapped"] = True

            # Beim Rollentausch offene Haltestellen-Anfragen von ehemaligen Läufern bereinigen
            state["pending_requests"] = [
                r for r in state["pending_requests"]
                if not (r.get("type") == "stop" and state["teams_data"].get(r.get("team"), {}).get("role") != "läufer")
            ]

            if config.get("shuffle_stops_on_swap", True):
                target_stops = config.get("target_stops", 12)
                state["history"] = []
                new_active_stops = select_random_stops(target_stops)
                state["active_stops"] = new_active_stops
                state["history"] = new_active_stops.copy()
                desc = "🔄 HALBZEIT! Die Rollen wurden getauscht. Haltestellen sind neu gewürfelt und die Historie wurde gelöscht."
            else:
                desc = "🔄 HALBZEIT! Die Rollen der aktiven Teams wurden getauscht."

            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()), "type": "system", "timestamp": datetime.now(timezone.utc).isoformat(),
                "desc": desc
            })
            save_state(conn, state)

    pre_start = start_dt - timedelta(minutes=5)
    post_end = end_dt + timedelta(minutes=5)

    if now < pre_start:
        phase = "inactive"
    elif pre_start <= now < start_dt:
        phase = "pre_game"
    elif start_dt <= now <= end_dt:
        phase = "game"
    elif end_dt < now <= post_end:
        phase = "post_game"
    else:
        phase = "finished"

    return phase, start_dt.isoformat(), end_dt.isoformat(), (mid_dt.isoformat() if mid_dt else None), now.isoformat()


# --- SPIEL-AKTIONEN MIT TATSÄCHLICHER ZEIT ---
def execute_stop_catch(state, stop_name, team_id, is_late=False, submitted_ts=None, actual_ts=None, auto_approved=False):
    now_iso = datetime.now(timezone.utc).isoformat()
    sub_iso = submitted_ts or now_iso
    act_iso = actual_ts or sub_iso

    is_neutral = not team_id
    display_name = None if is_neutral else "Unbekannt"
    if team_id and team_id in state["teams_data"]:
        state["teams_data"][team_id]["stops"].append(stop_name)
        state["teams_data"][team_id].setdefault("stop_timestamps", []).append((stop_name, sub_iso, act_iso))
        display_name = state["teams_data"][team_id]["display_name"]

    if stop_name in state['active_stops']:
        state['active_stops'].remove(stop_name)
        new_stop = get_new_random_stop(state['active_stops'], state['history'])
        state['active_stops'].append(new_stop)
        state['history'].append(new_stop)

    if is_neutral:
        desc = f"🔄 Haltestelle '{stop_name}' wurde vom System neutral ausgetauscht."
        admin_only = True
    else:
        desc = f"📍 {display_name} hat die Haltestelle '{stop_name}' erreicht."
        if auto_approved:
            desc += " (GPS Auto-Freigabe)"
        if is_late:
            desc += "   (Nachtrag nach Spielende)"
        admin_only = False

    state["action_log"].insert(0, {
        "id": str(uuid.uuid4()), "type": "stop", "team": team_id if not is_neutral else None, "team_name": display_name, "stop": stop_name,
        "timestamp": sub_iso, "actual_timestamp": act_iso, "desc": desc, "is_late": is_late,
        "admin_only": admin_only
    })
    return True


def execute_team_catch(state, hunter_id, prey_id, is_late=False, submitted_ts=None, actual_ts=None):
    now_iso = datetime.now(timezone.utc).isoformat()
    sub_iso = submitted_ts or now_iso
    act_iso = actual_ts or sub_iso

    if hunter_id in state['teams_data'] and prey_id in state['teams_data']:
        state['teams_data'][hunter_id]['caught'].append(prey_id)
        state['teams_data'][hunter_id].setdefault('catch_timestamps', []).append((prey_id, sub_iso, act_iso))
        state['teams_data'][prey_id]['caught_by'].append(hunter_id)

        hunter_name = state['teams_data'][hunter_id]['display_name']
        prey_name = state['teams_data'][prey_id]['display_name']

        desc = f"⚔️ {hunter_name} (Fänger) hat {prey_name} (Läufer) gefangen."
        if is_late:
            desc += "   (Nachtrag nach Spielende)"

        state["action_log"].insert(0, {
            "id": str(uuid.uuid4()), "type": "catch", "hunter": hunter_id, "prey": prey_id,
            "hunter_name": hunter_name, "prey_name": prey_name,
            "timestamp": sub_iso, "actual_timestamp": act_iso, "desc": desc, "is_late": is_late
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

        teams_list.append({"id": t_id, "name": d["display_name"]})
        score = calculate_score(d, config)

        leaderboard.append({
            "id": t_id, "display_name": d["display_name"], "score": score,
            "role": d.get("role", "keine"), "next_role": d.get("next_role", "")
        })

    leaderboard.sort(key=lambda x: x["score"], reverse=True)
    teams_list.sort(key=lambda x: x["name"].lower())

    public_log = [e for e in state.get("action_log", []) if not e.get("admin_only")]

    return render_template('index.html', teams=teams_list, leaderboard=leaderboard,
                           phase=phase, start_time=start_iso, end_time=end_iso,
                           mid_time=mid_iso, now_time=now_iso,
                           enable_swap=config.get("enable_mid_game_swap", True),
                           roles_swapped=state.get("roles_swapped", False),
                           action_log=public_log)


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
    laeufer_teams = []
    faenger_teams = []

    for t_id_iter, d in state["teams_data"].items():
        if d.get("deactivated", False):
            continue

        t_role = d.get("role", "keine")
        if t_role == "läufer" and t_id_iter != team_id:
            laeufer_teams.append({"id": t_id_iter, "name": d["display_name"]})
        elif t_role == "fänger" and t_id_iter != team_id:
            faenger_teams.append({"id": t_id_iter, "name": d["display_name"]})

        score = calculate_score(d, config)

        leaderboard.append({
            "id": t_id_iter, "display_name": d["display_name"], "score": score,
            "role": t_role, "next_role": d.get("next_role", "")
        })

    leaderboard.sort(key=lambda x: x["score"], reverse=True)

    team_log = [
        entry for entry in state["action_log"]
        if not entry.get("admin_only") and (
            entry.get("team") == team_id or entry.get("hunter") == team_id or entry.get("prey") == team_id or entry.get("type") == "system"
        )
    ]

    host_ip = request.host.split(':')[0]

    return render_template('team.html', team_id=team_id, team_name=team_data["display_name"],
                           role=role, next_role=next_role, slug=slug,
                           laeufer_teams=laeufer_teams, faenger_teams=faenger_teams,
                           leaderboard=leaderboard, log=team_log, phase=phase,
                           start_time=start_iso, end_time=end_iso, mid_time=mid_iso,
                           now_time=now_iso, enable_swap=config.get("enable_mid_game_swap", True),
                           roles_swapped=state.get("roles_swapped", False),
                           is_deactivated=is_deactivated, host_ip=host_ip)


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

        c = conn.cursor()
        c.execute("SELECT team_id, lat, lon, updated_at FROM team_locations")
        all_locs = {r["team_id"]: r for r in c.fetchall()}

        visible_locations = []
        # Ab 5 Min vor Spielbeginn (pre_game, game, post_game) sehen Fänger die Läufer
        catcher_can_see_runners = (my_role == "fänger" and phase in ("pre_game", "game", "post_game"))

        for t_id, loc_row in all_locs.items():
            t_data = state["teams_data"].get(t_id)
            if not t_data or t_data.get("deactivated", False):
                continue

            t_role = t_data.get("role", "keine")
            is_self = (t_id == my_team_id)

            if is_self or (catcher_can_see_runners and t_role == "läufer"):
                dt = parse_iso_utc(loc_row["updated_at"])
                ts_epoch = dt.timestamp() if dt else time.time()
                visible_locations.append({
                    "id": t_id,
                    "name": t_data["display_name"],
                    "role": t_role,
                    "lat": loc_row["lat"],
                    "lon": loc_row["lon"],
                    "updated_at": ts_epoch,
                    "is_self": is_self
                })

        # Prüfung auf > 2 Minuten keinen Standort (für Läufer und Fänger im Spiel / Pre-Game)
        gps_warning = False
        missing_minutes = 0
        now_utc = datetime.now(timezone.utc)

        if phase in ("pre_game", "game") and my_role in ("läufer", "fänger") and not my_team_data.get("deactivated", False):
            my_loc = all_locs.get(my_team_id)
            if my_loc and my_loc["updated_at"]:
                last_dt = parse_iso_utc(my_loc["updated_at"])
                if last_dt:
                    diff_sec = (now_utc - last_dt).total_seconds()
                    if diff_sec >= 120:
                        gps_warning = True
                        missing_minutes = max(2, int(diff_sec // 60))
            else:
                # Noch gar kein Standort gesendet seit Pre-Game Beginn
                start_dt = parse_iso_utc(start_iso)
                pre_start_dt = (start_dt - timedelta(minutes=5)) if start_dt else None
                if pre_start_dt:
                    diff_sec = (now_utc - pre_start_dt).total_seconds()
                    if diff_sec >= 120:
                        gps_warning = True
                        missing_minutes = max(2, int(diff_sec // 60))

    return jsonify({
        "success": True,
        "role": my_role,
        "phase": phase,
        "locations": visible_locations,
        "gps_warning": gps_warning,
        "missing_minutes": missing_minutes
    })


@app.route('/api/login_team', methods=['POST'])
def login_team():
    data = request.json
    team_id = data.get('team_id')
    pin = data.get('pin')

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)

    team_data = state["teams_data"].get(team_id)

    if team_data and str(team_data.get("pin")) == str(pin).strip():
        return jsonify({'success': True, 'slug': team_data["slug"]})

    return jsonify({'success': False, 'error': 'Falscher PIN oder Team nicht gefunden!'})


@app.route('/api/login_admin', methods=['POST'])
def login_admin():
    data = request.json
    pw = str(data.get('password', '')).strip()
    if pw == ADMIN_PASSWORD:
        return jsonify({'success': True, 'redirect_url': f"/adminconsole-{ADMIN_SLUG}"})
    return jsonify({'success': False, 'error': 'Falsches Admin-Passwort!'})


@app.route('/adminconsole-<admin_slug>')
def admin(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

        c = conn.cursor()
        c.execute("SELECT team_id, offline_from, offline_until FROM gps_outages")
        all_outages = c.fetchall()

    config = state["config"]
    tz = ZoneInfo(config.get("timezone", "Europe/Berlin"))
    now_utc = datetime.now(timezone.utc)
    
    start_dt_utc = parse_iso_utc(start_iso)
    game_end_utc = parse_iso_utc(end_iso)
    pre_start_utc = start_dt_utc - timedelta(minutes=5) if start_dt_utc else None

    leaderboard = []
    team_urls = {}
    teams_list = []
    base_url = request.host_url.rstrip('/')
    team_names_map = {t_id: d["display_name"] for t_id, d in state["teams_data"].items()}

    c = conn.cursor()
    c.execute("""
        SELECT team_id, lat, lon, updated_at, batteryLevel, charge, motion, altitude, speed, course, accuracy
        FROM team_locations
    """)
    locations_by_team = {row["team_id"]: dict(row) for row in c.fetchall()}

    for t_id, data in state["teams_data"].items():
        team_urls[data["display_name"]] = f"{base_url}/team/{data['slug']}"
        teams_list.append({"id": t_id, "name": data["display_name"]})

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
            "telemetry": telemetry
        })

    leaderboard.sort(key=lambda x: x["score"], reverse=True)
    teams_list.sort(key=lambda x: x["name"].lower())

    return render_template('admin.html', active_stops=state['active_stops'], teams=teams_list,
                           leaderboard=leaderboard, admin_url=ADMIN_SLUG, team_urls=team_urls,
                           pending_requests=state["pending_requests"], action_log=state["action_log"],
                           phase=phase, start_time=start_iso, end_time=end_iso, mid_time=mid_iso,
                           now_time=now_iso, team_names_map=team_names_map,
                           enable_swap=config.get("enable_mid_game_swap", True),
                           roles_swapped=state.get("roles_swapped", False))


@app.route('/api/admin_data/<admin_slug>')
def api_admin_data(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

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
                    "role": team_data.get("role", "keine"),
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

        c.execute("""
            SELECT team_id, lat, lon, timestamp, batteryLevel, charge, motion, altitude, speed, course, accuracy
            FROM location_history
            ORDER BY timestamp ASC
        """)
        location_hist = []
        for h in c.fetchall():
            location_hist.append({
                "team_id": h["team_id"],
                "lat": h["lat"],
                "lon": h["lon"],
                "timestamp": h["timestamp"],
                "batteryLevel": h["batteryLevel"],
                "charge": bool(h["charge"]) if h["charge"] is not None else None,
                "motion": bool(h["motion"]) if h["motion"] is not None else None,
                "altitude": h["altitude"],
                "speed": h["speed"],
                "course": h["course"],
                "accuracy": h["accuracy"]
            })

    return jsonify({
        'success': True,
        'pending_requests': state.get("pending_requests", []),
        'action_log': state.get("action_log", []),
        'phase': phase,
        'now_time': now_iso,
        'active_stops': state.get("active_stops", []),
        'team_locations': team_locs,
        'location_history': location_hist
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
            sorted_teams = sorted(state["teams_data"].items(), key=lambda x: int(x[0].replace("Team ", "")) if "Team " in x[0] else 999)
            available_datasets = [f for f in sorted(os.listdir(STOPS_DIR)) if f.endswith('.json') and os.path.isfile(os.path.join(STOPS_DIR, f))]
            active_dataset = state.get("active_stops_dataset", "Catch_Haltestellen.json")
            if not available_datasets:
                available_datasets = [active_dataset]
            return render_template(
                'settings.html',
                config=config,
                sorted_teams=sorted_teams,
                admin_url=ADMIN_SLUG,
                available_datasets=available_datasets,
                active_stops_dataset=active_dataset
            )

        data = request.json
        old_start = config.get("start_time")
        old_end = config.get("end_time")
        old_swap = config.get("enable_mid_game_swap")

        # Datensatz-Änderung prüfen
        new_dataset = data.get("active_stops_dataset")
        old_dataset = state.get("active_stops_dataset", "Catch_Haltestellen.json")
        if new_dataset and new_dataset != old_dataset:
            state["active_stops_dataset"] = new_dataset
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('active_stops_dataset', ?)", (new_dataset,))
            load_active_stops_data(new_dataset)

            target = int(data.get("target_stops", config["target_stops"]))
            state["history"] = []
            new_active = select_random_stops(target)
            state["active_stops"] = new_active
            state["history"] = new_active.copy()

            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()),
                "type": "system",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "desc": f"📁 Haltestellen-Datensatz geändert auf '{new_dataset}'. Haltestellen wurden neu ausgewürfelt."
            })

        config["start_time"] = data.get("start_time", config.get("start_time"))
        config["end_time"] = data.get("end_time", config.get("end_time"))
        config["timezone"] = data.get("timezone", "Europe/Berlin")
        config["enable_mid_game_swap"] = data.get("enable_mid_game_swap", True)
        config["shuffle_stops_on_swap"] = data.get("shuffle_stops_on_swap", True)

        config["auto_approve_stops"] = bool(data.get("auto_approve_stops", config.get("auto_approve_stops", False)))
        config["stop_radius_meters"] = float(data.get("stop_radius_meters", config.get("stop_radius_meters", 100)))

        if old_start != config["start_time"] or old_end != config["end_time"] or old_swap != config["enable_mid_game_swap"]:
            state["roles_swapped"] = False

        team_list = data.get("teams_list", [])
        old_teams_data = state.get("teams_data", {})
        new_teams_data = {}

        now_iso = datetime.now(timezone.utc).isoformat()
        for t in team_list:
            team_id = str(t["team_id"]).strip()
            display_name = str(t["new_name"]).strip() or team_id
            role = t["role"]
            pin = str(t["pin"]).strip()
            active = bool(t["active"])

            if team_id and team_id in old_teams_data:
                team_data = old_teams_data[team_id]
                old_role = team_data.get("role", "keine")
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
                if role != "keine":
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

        calculate_next_roles(new_teams_data)
        state["teams_data"] = new_teams_data

        active_has_keine = any(d["role"] == "keine" for d in new_teams_data.values() if not d["deactivated"])
        if not active_has_keine and not state.get("roles_assigned"):
            state["roles_assigned"] = True

        config["num_teams"] = len(new_teams_data)
        config["points_stop_reached"] = int(data.get("points_stop_reached", config["points_stop_reached"]))
        config["points_was_caught"] = int(data.get("points_was_caught", config["points_was_caught"]))
        config["points_caught_team"] = int(data.get("points_caught_team", config["points_caught_team"]))

        config["auto_calculate_stops"] = data.get("auto_calculate_stops", True)
        tot_hst = len(ALL_STOPS)

        if config["auto_calculate_stops"] and tot_hst > 0:
            teams_count = len(new_teams_data)

            # Formel 1: Aktive Haltestellen
            aktiv = math.ceil(min(tot_hst * 0.6, (tot_hst * 0.3) * max(math.sqrt(teams_count) / 2, 1)))
            aktiv = max(2 * teams_count, aktiv)
            aktiv = min(tot_hst - 6, aktiv)
            config["target_stops"] = max(1, aktiv)

            # Formel 2: Historien-Limit
            history = math.ceil(min(config["target_stops"] * 1.75, tot_hst * 0.8))
            history = max(config["target_stops"] + 3, history)
            history = min(tot_hst - 3, history)
            config["max_history"] = max(1, history)
        else:
            config["target_stops"] = int(data.get("target_stops", config["target_stops"]))
            config["max_history"] = int(data.get("max_history", config["max_history"]))

        # Warnung prüfen (Spielfeld zu klein)
        warning_msg = None
        if config["target_stops"] > tot_hst * 0.75:
            warning_msg = f"Achtung: Die Anzahl der aktiven Haltestellen ({config['target_stops']}) überschreitet 75% der Gesamthaltestellen ({tot_hst}). Bitte wähle ein größeres Spielfeld!"

        current_active_count = len(state["active_stops"])
        target = config["target_stops"]
        if current_active_count < target:
            needed = target - current_active_count
            for _ in range(needed):
                new_stop = get_new_random_stop(state["active_stops"], state["history"])
                if new_stop and new_stop not in state["active_stops"]:
                    state["active_stops"].append(new_stop)
                    state["history"].append(new_stop)
        elif current_active_count > target:
            state["active_stops"] = state["active_stops"][:target]

        save_state(conn, state)
        trigger_traccar_sync_async(state["teams_data"])

    return jsonify({'success': True, 'new_admin_url': ADMIN_SLUG, 'warning': warning_msg})


@app.route('/adminconsole-<admin_slug>/reset', methods=['POST'])
def reset_game(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    gps_inactivity_notified_stage.clear()
    replay_cache["data"] = None
    replay_cache["timestamp"] = None

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        config = state["config"]
        target = config.get("target_stops", 12)

        initial_active = select_random_stops(target)
        state["active_stops"] = initial_active
        state["history"] = initial_active.copy()

        state["pending_requests"] = []
        state["action_log"] = []
        state["roles_swapped"] = False

        teams_ids = list(state["teams_data"].keys())
        for t_id in teams_ids:
            state["teams_data"][t_id]["stops"] = []
            state["teams_data"][t_id]["stop_timestamps"] = []
            state["teams_data"][t_id]["caught"] = []
            state["teams_data"][t_id]["catch_timestamps"] = []
            state["teams_data"][t_id]["caught_by"] = []
            state["teams_data"][t_id]["role"] = "keine"
            state["teams_data"][t_id]["manual_points"] = 0

        calculate_next_roles(state["teams_data"])
        state["roles_assigned"] = False

        c = conn.cursor()
        c.execute("DELETE FROM team_locations")
        c.execute("DELETE FROM location_history")
        c.execute("DELETE FROM team_stops")
        c.execute("DELETE FROM team_catches")
        c.execute("DELETE FROM replay_stop_lifecycle")
        c.execute("DELETE FROM replay_role_lifecycle")

        save_state(conn, state)
        trigger_traccar_sync_async(state["teams_data"])

    return jsonify({'success': True})


@app.route('/adminconsole-<admin_slug>/randomize_roles', methods=['POST'])
def randomize_roles(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        active_teams = [t_id for t_id, d in state["teams_data"].items() if not d.get("deactivated", False)]
        num_active = len(active_teams)

        if num_active > 0:
            if random.choice([True, False]):
                num_laeufer = (num_active + 1) // 2
                num_faenger = num_active - num_laeufer
            else:
                num_faenger = (num_active + 1) // 2
                num_laeufer = num_active - num_faenger

            roles = ["fänger"] * num_faenger + ["läufer"] * num_laeufer
            random.shuffle(roles)

            for i, t_id in enumerate(active_teams):
                state["teams_data"][t_id]["role"] = roles[i]

            calculate_next_roles(state["teams_data"])
            state["roles_assigned"] = True
            save_state(conn, state)

    return jsonify({'success': True})


@app.route('/adminconsole-<admin_slug>/shuffle_stops', methods=['POST'])
def shuffle_stops(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        config = state["config"]
        target = config.get("target_stops", 12)

        new_active_stops = select_random_stops(target)
        state["active_stops"] = new_active_stops
        state["history"] = new_active_stops.copy()

        state["action_log"].insert(0, {
            "id": str(uuid.uuid4()), "type": "system",
            "timestamp": datetime.now(timezone.utc).isoformat(), "desc": "🔀 Haltestellen wurden manuell neu gewürfelt und die Historie zurückgesetzt."
        })
        save_state(conn, state)

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


@app.route('/adminconsole-<admin_slug>/clear_replay_cache', methods=['GET', 'POST'])
def clear_replay_cache(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return jsonify({'success': False, 'error': 'Verboten'}), 403

    global replay_cache
    replay_cache["data"] = None
    replay_cache["timestamp"] = None

    if request.method == 'POST' or request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.is_json:
        return jsonify({'success': True, 'message': 'Replay-Cache wurde erfolgreich geleert.'})

    return redirect(f"/adminconsole-{admin_slug}")




# --- HALTESTELLEN & GTFS MANAGEMENT ---
@app.route('/adminconsole-<admin_slug>/stop_config')
def admin_stop_config(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403
    return render_template('stop_config.html', admin_url=ADMIN_SLUG)


@app.route('/api/stop_datasets')
def api_stop_datasets():
    try:
        if not os.path.exists(STOPS_DIR):
            os.makedirs(STOPS_DIR, exist_ok=True)
        files = [f for f in sorted(os.listdir(STOPS_DIR)) if f.endswith('.json') and os.path.isfile(os.path.join(STOPS_DIR, f))]
        return jsonify(files)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/stop_dataset/<path:filename>')
def api_stop_dataset(filename):
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
    req_data = request.get_json(silent=True) or {}
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

    return jsonify({"success": True, "filename": safe_filename, "count": len(stops)})


# --- GLOBALE CACHE VARIABLE FÜR GTFS ---
gtfs_cache = {
    "data": None,
    "timestamp": 0
}
gtfs_cache_lock = threading.Lock()

@app.route('/api/search_gtfs', methods=['POST'])
def api_search_gtfs():
    if not os.path.exists(GTFS_STOPS_FILE):
        return jsonify({"error": "GTFS-Datei 'stops.txt' wurde im Ordner 'stops_data' nicht gefunden.", "results": []}), 404

    req_data = request.get_json(silent=True) or {}
    if not req_data and request.form:
        req_data = request.form

    stadt = req_data.get('stadt', '').strip()
    stadtteil = req_data.get('stadtteil', '').strip()
    name = req_data.get('name', '').strip()

    if not name and not stadt and not stadtteil:
        return jsonify([])

    global gtfs_cache
    current_time = time.time()

    # Cache-Prüfung (120 Sekunden = 2 Minuten)
    with gtfs_cache_lock:
        if gtfs_cache["data"] is None or (current_time - gtfs_cache["timestamp"]) > 120:
            try:
                with open(GTFS_STOPS_FILE, 'r', encoding='utf-8-sig', errors='replace') as f:
                    sample = f.read(4096)
                    f.seek(0)
                    delimiter = ';' if ';' in sample and sample.count(';') > sample.count(',') else ','
                    reader = csv.DictReader(f, delimiter=delimiter)

                    parsed_data = []
                    for row in reader:
                        s_name = row.get('stop_name') or row.get('STOP_NAME') or ''
                        s_desc = row.get('stop_desc') or row.get('STOP_DESC') or ''
                        s_lat = row.get('stop_lat') or row.get('STOP_LAT') or ''
                        s_lon = row.get('stop_lon') or row.get('STOP_LON') or ''

                        if not s_name or not s_lat or not s_lon:
                            continue

                        try:
                            lat = float(str(s_lat).replace(',', '.'))
                            lon = float(str(s_lon).replace(',', '.'))
                        except ValueError:
                            continue

                        parsed_data.append({
                            "name": s_name,
                            "lat": lat,
                            "lon": lon,
                            "combined": f"{s_name} {s_desc}".lower()
                        })
                    
                    gtfs_cache["data"] = parsed_data
                    gtfs_cache["timestamp"] = current_time
            except Exception as e:
                return jsonify({"error": f"Fehler beim Lesen der GTFS-Datei: {str(e)}"}), 500

        cached_stops = gtfs_cache["data"]

    # Suche im (nun garantiert geladenen) Cache
    results = []
    seen = set()

    q_name = name.lower()
    q_stadt = stadt.lower()
    q_stadtteil = stadtteil.lower()

    tokens_both = [t for t in f"{q_stadt} {q_name}".split() if len(t) > 0]
    tokens_name = [t for t in q_name.split() if len(t) > 0]

    candidates_both = []
    candidates_name = []

    for item in cached_stops:
        s_name = item["name"]
        lat = item["lat"]
        lon = item["lon"]
        combined = item["combined"]

        if tokens_both and all(t in combined for t in tokens_both):
            candidates_both.append((s_name, lat, lon, combined))
        elif tokens_name and all(t in combined for t in tokens_name):
            candidates_name.append((s_name, lat, lon, combined))

    chosen = candidates_both if candidates_both else candidates_name

    if q_stadtteil:
        chosen.sort(key=lambda c: 0 if q_stadtteil in c[3] else 1)

    for s_name, lat, lon, _ in chosen:
        key = (s_name, round(lat, 5), round(lon, 5))
        if key in seen:
            continue
        seen.add(key)
        results.append({
            "name": s_name,
            "lat": lat,
            "lon": lon
        })
        if len(results) >= 100:
            break

    return jsonify(results)


# --- PLAYER ACTIONS API ---
@app.route('/api/request_stop', methods=['POST'])
def request_stop():
    data = request.json
    team_slug = data.get('slug')
    stop_name = data.get('stop')
    msg_to_send = None
    response_msg = f'Anfrage für "{stop_name}" erfolgreich gesendet.'

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, _, _, _, _ = update_game_time_state(conn, state)

        if phase not in ["game", "post_game"]:
            return jsonify({'success': False, 'error': 'Einträge sind nur während des aktiven Spiels oder kurz danach möglich!'}), 403

        is_late = (phase == "post_game")

        team_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == team_slug), None)
        if not team_id or stop_name not in state['active_stops']:
            return jsonify({'success': False, 'error': 'Haltestelle nicht mehr aktiv oder Team ungültig.'}), 400

        if state["teams_data"][team_id].get("deactivated", False):
            return jsonify({'success': False, 'error': 'Dein Team ist deaktiviert.'}), 403

        if state["teams_data"][team_id].get("role") != "läufer":
            return jsonify({'success': False, 'error': 'Nur Läufer können Haltestellen eintragen!'}), 403

        now_utc = datetime.now(timezone.utc)
        now_iso = now_utc.isoformat()
        display_name = state["teams_data"][team_id]["display_name"]
        config = state["config"]

        # Automatische GPS-Prüfung (letzte 5 Minuten + ggf. 1 Minute Nachprüfung)
        if config.get("auto_approve_stops", False):
            radius_m = float(config.get("stop_radius_meters", 100))
            window_start = now_utc - timedelta(minutes=5)
            min_dist, closest_iso = find_closest_stop_approach(conn, team_id, stop_name, window_start, now_utc)

            if min_dist is not None and min_dist <= radius_m:
                # Eventuell offene identische Anfrage entfernen
                state["pending_requests"] = [
                    r for r in state["pending_requests"]
                    if not (r["type"] == "stop" and r["team"] == team_id and r["stop"] == stop_name)
                ]
                execute_stop_catch(
                    state, stop_name, team_id,
                    is_late=is_late,
                    submitted_ts=now_iso,
                    actual_ts=closest_iso or now_iso,
                    auto_approved=True
                )
                save_state(conn, state)
                msg_to_send = f"✅ AUTO-GPS-FREIGABE: Team '{display_name}' hat Haltestelle '{stop_name}' ({int(min_dist)}m Abstand) automatisch gutgeschrieben bekommen."
                response_msg = f"✅ Haltestelle '{stop_name}' wurde per GPS automatisch bestätigt und gutgeschrieben!"
            else:
                already_pending = next((
                    r for r in state["pending_requests"]
                    if r["type"] == "stop" and r["team"] == team_id and r["stop"] == stop_name
                ), None)
                check_until_iso = (now_utc + timedelta(minutes=1)).isoformat()
                if not already_pending:
                    state["pending_requests"].append({
                        "id": str(uuid.uuid4()),
                        "type": "stop",
                        "team": team_id,
                        "team_name": display_name,
                        "stop": stop_name,
                        "timestamp": now_iso,
                        "is_late": is_late,
                        "gps_check_until": check_until_iso,
                        "notified_admin": False
                    })
                    save_state(conn, state)
                elif not already_pending.get("gps_check_until"):
                    already_pending["gps_check_until"] = check_until_iso
                    already_pending["timestamp"] = now_iso
                    already_pending["is_late"] = is_late
                    save_state(conn, state)
                response_msg = (
                    f"⏳ Anfrage für '{stop_name}' eingereicht. Noch kein GPS-Punkt im Radius ({int(radius_m)}m) gefunden – "
                    f"wird für 1 Minute automatisch weitergeprüft, danach kann der Spielleiter manuell freigeben."
                )
        else:
            already_pending = next((
                r for r in state["pending_requests"]
                if r["type"] == "stop" and r["team"] == team_id and r["stop"] == stop_name
            ), None)

            if not already_pending:
                state["pending_requests"].append({
                    "id": str(uuid.uuid4()), "type": "stop", "team": team_id, "team_name": display_name, "stop": stop_name,
                    "timestamp": now_iso, "is_late": is_late, "notified_admin": True
                })
                save_state(conn, state)
                msg_to_send = f"📬 NEUE ANFRAGE: Team '{display_name}' möchte die Haltestelle '{stop_name}' eintragen lassen."
            else:
                response_msg = f'Anfrage für "{stop_name}" liegt bereits zur Prüfung vor.'

    if msg_to_send:
        send_telegram_msg(msg_to_send)

    return jsonify({'success': True, 'message': response_msg})


@app.route('/api/report_catch', methods=['POST'])
def report_catch():
    data = request.json
    reporter_slug = data.get('reporter_slug')
    hunter_id = data.get('hunter')
    prey_id = data.get('prey')

    msg_to_send = None
    result = None

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, _, _, _, _ = update_game_time_state(conn, state)

        if phase not in ["game", "post_game"]:
            return jsonify({'success': False,
                            'error': 'Fänge können nur während des aktiven Spiels oder kurz danach gemeldet werden!'}), 403

        is_late = (phase == "post_game")

        reporter_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == reporter_slug), None)
        if not reporter_id or not hunter_id or not prey_id:
            return jsonify({'success': False, 'error': 'Ungültige Daten übermittelt.'}), 400

        if hunter_id not in state["teams_data"] or prey_id not in state["teams_data"]:
            return jsonify({'success': False, 'error': 'Eines der übermittelten Teams existiert nicht.'}), 400

        if state["teams_data"][hunter_id].get("role") != "fänger" or state["teams_data"][prey_id].get("role") != "läufer":
            return jsonify({'success': False, 'error': 'Ungültige Rollen! Bitte App neu laden, ggf. gab es gerade einen Rollentausch.'}), 400

        if state["teams_data"].get(reporter_id, {}).get("deactivated", False):
            return jsonify({'success': False, 'error': 'Dein Team ist deaktiviert.'}), 403

        recent_catches = [log for log in state["action_log"] if
                          log["type"] == "catch" and log["hunter"] == hunter_id and log["prey"] == prey_id]

        now_utc = datetime.now(timezone.utc)
        if recent_catches:
            last_ts = parse_iso_utc(recent_catches[0]["timestamp"])
            if last_ts and (now_utc - last_ts).total_seconds() < 300:
                return jsonify({'success': True, 'message': 'Fang wurde bereits vor kurzem registriert!'})

        matching_req = next((r for r in state["pending_requests"] if
                             r["type"] == "catch" and r["hunter"] == hunter_id and r["prey"] == prey_id and r[
                                 "reporter"] != reporter_id), None)

        if matching_req:
            state["pending_requests"].remove(matching_req)
            sub_ts = matching_req.get("timestamp") or now_utc.isoformat()
            closest_catch_iso = find_closest_teams_approach(conn, hunter_id, prey_id, ref_dt=now_utc)
            actual_ts = closest_catch_iso or sub_ts

            execute_team_catch(
                state, hunter_id, prey_id,
                is_late=is_late,
                submitted_ts=sub_ts,
                actual_ts=actual_ts
            )
            save_state(conn, state)

            h_name = state['teams_data'][hunter_id]['display_name']
            p_name = state['teams_data'][prey_id]['display_name']
            msg_to_send = f"✅ AUTO-APPROVE: Beidseitig bestätigt! '{h_name}' hat '{p_name}' gefangen."
            result = {'success': True, 'message': 'Beidseitig bestätigt! Der Fang wurde sofort eingetragen.'}
        else:
            already_pending = next((r for r in state["pending_requests"] if
                                    r["type"] == "catch" and r["hunter"] == hunter_id and r["prey"] == prey_id and r[
                                        "reporter"] == reporter_id), None)
            if not already_pending:
                r_name = state['teams_data'][reporter_id]['display_name']
                h_name = state['teams_data'][hunter_id]['display_name']
                p_name = state['teams_data'][prey_id]['display_name']

                state["pending_requests"].append({
                    "id": str(uuid.uuid4()), "type": "catch", "reporter": reporter_id, "reporter_name": r_name,
                    "hunter": hunter_id, "hunter_name": h_name, "prey": prey_id, "prey_name": p_name,
                    "timestamp": now_utc.isoformat(), "is_late": is_late
                })
                save_state(conn, state)
                msg_to_send = f"📬 NEUE FANG-ANFRAGE: '{r_name}' meldet, dass '{h_name}' das Team '{p_name}' gefangen hat."

            result = {'success': True, 'message': 'Anfrage gesendet. Warte auf Bestätigung der Gegenseite oder des Admins.'}

    if msg_to_send:
        send_telegram_msg(msg_to_send)

    return jsonify(result)


# --- ADMIN ACTIONS API ---
@app.route('/api/resolve_request', methods=['POST'])
def resolve_request():
    data = request.json
    req_id = data.get('id')
    action = data.get('action')

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)

        req = next((r for r in state["pending_requests"] if r["id"] == req_id), None)
        if not req:
            return jsonify({'success': False, 'error': 'Anfrage nicht gefunden.'}), 404

        state["pending_requests"].remove(req)
        is_late = req.get("is_late", False)
        sub_ts = req.get("timestamp") or datetime.now(timezone.utc).isoformat()

        if action == 'approve':
            if req["type"] == "stop":
                # Bei manueller Genehmigung durch den Spielleiter wird die Zeit hinterlegt,
                # in der das Erreichen durch das Team eingetragen wurde (sub_ts).
                execute_stop_catch(
                    state, req["stop"], req["team"],
                    is_late=is_late,
                    submitted_ts=sub_ts,
                    actual_ts=sub_ts,
                    auto_approved=False
                )
            elif req["type"] == "catch":
                state["pending_requests"] = [r for r in state["pending_requests"] if not (
                            r["type"] == "catch" and r["hunter"] == req["hunter"] and r["prey"] == req["prey"])]
                ref_dt = parse_iso_utc(sub_ts) or datetime.now(timezone.utc)
                closest_catch_iso = find_closest_teams_approach(conn, req["hunter"], req["prey"], ref_dt=ref_dt)
                actual_ts = closest_catch_iso or sub_ts
                execute_team_catch(
                    state, req["hunter"], req["prey"],
                    is_late=is_late,
                    submitted_ts=sub_ts,
                    actual_ts=actual_ts
                )

        save_state(conn, state)

    return jsonify({'success': True})


@app.route('/api/undo_action', methods=['POST'])
def undo_action():
    data = request.json
    action_id = data.get('id')

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        action = next((a for a in state["action_log"] if a["id"] == action_id), None)
        if not action:
            return jsonify({'success': False, 'error': 'Aktion im Log nicht gefunden.'}), 404

        c = conn.cursor()
        if action["type"] == "stop":
            team_id = action.get("team")
            stop = action["stop"]
            sub_ts = action.get("timestamp")
            if team_id and team_id in state["teams_data"] and stop in state["teams_data"][team_id]["stops"]:
                state["teams_data"][team_id]["stops"].remove(stop)
                ts_list = state["teams_data"][team_id].get("stop_timestamps", [])
                for idx, entry in enumerate(ts_list):
                    if entry[0] == stop and (sub_ts is None or entry[1] == sub_ts):
                        ts_list.pop(idx)
                        break
                if sub_ts:
                    c.execute("DELETE FROM team_stops WHERE rowid IN (SELECT rowid FROM team_stops WHERE team_id = ? AND stop_name = ? AND submitted_at = ? LIMIT 1)", (team_id, stop, sub_ts))

            if stop not in state["active_stops"]:
                state["active_stops"].append(stop)

        elif action["type"] == "catch":
            hunter_id = action.get("hunter")
            prey_id = action.get("prey")
            sub_ts = action.get("timestamp")
            if hunter_id and hunter_id in state["teams_data"] and prey_id in state["teams_data"][hunter_id]["caught"]:
                state["teams_data"][hunter_id]["caught"].remove(prey_id)
                ts_list = state["teams_data"][hunter_id].get("catch_timestamps", [])
                for idx, entry in enumerate(ts_list):
                    if entry[0] == prey_id and (sub_ts is None or entry[1] == sub_ts):
                        ts_list.pop(idx)
                        break
                if sub_ts:
                    c.execute("DELETE FROM team_catches WHERE rowid IN (SELECT rowid FROM team_catches WHERE hunter_id = ? AND prey_id = ? AND submitted_at = ? LIMIT 1)", (hunter_id, prey_id, sub_ts))
            if prey_id and prey_id in state["teams_data"] and hunter_id in state["teams_data"][prey_id]["caught_by"]:
                state["teams_data"][prey_id]["caught_by"].remove(hunter_id)

        state["action_log"].remove(action)
        save_state(conn, state)

    return jsonify({'success': True})


@app.route('/api/toggle', methods=['POST'])
def toggle():
    data = request.json
    stop_name = data.get('name')
    team_id = data.get('team_id')

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        if stop_name in state['active_stops']:
            now_iso = datetime.now(timezone.utc).isoformat()
            if execute_stop_catch(state, stop_name, team_id, submitted_ts=now_iso, actual_ts=now_iso):
                save_state(conn, state)
                return jsonify({'success': True})

    return jsonify({'success': False}), 400


@app.route('/api/catch_team', methods=['POST'])
def catch_team():
    data = request.json
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
            actual_ts = closest_catch_iso or now_iso
            if execute_team_catch(state, hunter_id, prey_id, submitted_ts=now_iso, actual_ts=actual_ts):
                save_state(conn, state)
                return jsonify({'success': True})
            return jsonify({'success': False, 'error': 'Fehler bei der Zuweisung.'}), 400

    return jsonify({'success': False, 'error': 'Ungültige Teams übergeben.'}), 400


@app.route('/api/add_manual_points', methods=['POST'])
def add_manual_points():
    data = request.json
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
                "timestamp": now_iso,
                "actual_timestamp": now_iso,
                "desc": str(points),
                "admin_only": True
            })
            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()),
                "type": "system",
                "team": team_id,
                "timestamp": now_iso,
                "actual_timestamp": now_iso,
                "desc": desc,
                "admin_only": False
            })
            save_state(conn, state)
            return jsonify({'success': True})

    return jsonify({'success': False, 'error': 'Team nicht gefunden.'}), 400


@app.route('/api/stops')
def api_stops():
    team_slug = request.args.get('slug')

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        update_game_time_state(conn, state)

        if not team_slug:
            return jsonify({'error': 'Slug fehlt'}), 400

        team_data = next((d for d in state["teams_data"].values() if d.get("slug") == team_slug), None)

        if not team_data:
            return jsonify({'error': 'Team nicht gefunden'}), 404

        if team_data.get("role") != "läufer":
            return jsonify({'error': 'Nicht berechtigt'}), 403

        result = []
        for name in state['active_stops']:
            if name in stops_data:
                result.append({
                    'name': name,
                    'lat': stops_data[name]['lat'],
                    'lon': stops_data[name]['lon'],
                    'factor': stops_data[name].get('factor', 1),
                    'reached': False
                })

        return jsonify(result)


@app.route('/api/all_stops')
def api_all_stops():
    result = []
    for name, data in stops_data.items():
        result.append({'name': name, 'lat': data['lat'], 'lon': data['lon'], 'factor': data.get('factor', 1)})
    return jsonify(result)


@app.route('/api/ticker_data')
def api_ticker_data():
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        config = state["config"]

        leaderboard = []
        for t_id, d in state["teams_data"].items():
            if d.get("deactivated", False):
                continue

            score = calculate_score(d, config)

            leaderboard.append({
                "id": t_id, "display_name": d["display_name"], "score": score,
                "role": d.get("role", "keine"), "next_role": d.get("next_role", "")
            })

        leaderboard.sort(key=lambda x: x["score"], reverse=True)
        public_log = [e for e in state.get("action_log", []) if not e.get("admin_only")]

        return jsonify({
            'success': True,
            'action_log': public_log,
            'leaderboard': leaderboard
        })


@app.route('/api/replay_data')
def api_replay_data():
    is_admin = request.args.get('admin') == '1'

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
        replay_start = start_dt - timedelta(minutes=5)

        # --- LOGIK FÜR SICHTBARKEIT & ZEITFENSTER ---
        if not is_admin:
            # Spieler / Öffentlich
            if phase in ["inactive", "roles_assigned", "pre_game"] or (phase == "game" and not is_second_half):
                return jsonify({"error": "Das Replay ist während der 1. Halbzeit nicht verfügbar. Es wird zur Halbzeitpause für die erste Spielhälfte freigeschaltet."})
            elif phase == "game" and is_second_half:
                replay_end = mid_dt  # Zeige in der 2. Halbzeit nur das Replay bis zur Halbzeit
            else:
                replay_end = end_dt + timedelta(minutes=3)  # Spiel ist komplett vorbei
        else:
            # Admin (Live-Modus mit 30 Sekunden Verzögerung)
            if phase in ["inactive", "roles_assigned", "pre_game"]:
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
                elif row["type"] == "catch":
                    lat, lon = find_closest_coords(locations.get(row["prey"], []), act_ts_ms)
                    if lat is None:
                        lat, lon = find_closest_coords(locations.get(row["hunter"], []), act_ts_ms)

            events.append({
                "id": row["id"], "type": row["type"], "ts": act_ts_ms, "desc": row["desc"],
                "team": row["team"], "team_name": row["team_name"], "stop": row["stop"],
                "hunter": row["hunter"], "hunter_name": row["hunter_name"], "prey": row["prey"],
                "prey_name": row["prey_name"], "lat": lat, "lon": lon
            })
        events.sort(key=lambda e: e["ts"])

        # 3. Teams Grunddaten
        teams_dict = {t_id: {"id": t_id, "display_name": d.get("display_name", t_id), "deactivated": bool(d.get("deactivated", False))} for t_id, d in state["teams_data"].items()}

        # 4. Stop Lifecycle
        c.execute("SELECT stop_name, active_from, active_until FROM replay_stop_lifecycle")
        stop_lifecycle = []
        for r in c.fetchall():
            f_dt = parse_iso_utc(r["active_from"])
            u_dt = parse_iso_utc(r["active_until"]) if r["active_until"] else None
            if f_dt: stop_lifecycle.append({"stop_name": r["stop_name"], "from": int(f_dt.timestamp() * 1000), "until": int(u_dt.timestamp() * 1000) if u_dt else None})

        # 5. NEU: Role Lifecycle
        c.execute("SELECT team_id, role, active_from, active_until FROM replay_role_lifecycle")
        role_lifecycle = []
        for r in c.fetchall():
            f_dt = parse_iso_utc(r["active_from"])
            u_dt = parse_iso_utc(r["active_until"]) if r["active_until"] else None
            if f_dt: role_lifecycle.append({"team_id": r["team_id"], "role": r["role"], "from": int(f_dt.timestamp() * 1000), "until": int(u_dt.timestamp() * 1000) if u_dt else None})

        return jsonify({
            "start_ts": int(replay_start.timestamp() * 1000),
            "end_ts": int(replay_end.timestamp() * 1000),
            "config": state["config"],
            "teams": teams_dict,
            "locations": locations,
            "events": events,
            "stop_lifecycle": stop_lifecycle,
            "role_lifecycle": role_lifecycle
        })


@app.route('/adminconsole-<admin_slug>/replay')
def admin_replay(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

    return render_template('replay.html', phase=phase, start_time=start_iso, end_time=end_iso, is_admin=True)


@app.route('/replay')
def replay():
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='DEFERRED') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

    return render_template('replay.html', phase=phase, start_time=start_iso, end_time=end_iso)


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
    write_traccar_api_log(log_data)

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
            speed = float(raw_speed)
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

            c.execute("""
                SELECT id, display_name
                FROM teams
                WHERE slug = ?
                   OR CAST(id AS TEXT) = ?
                   OR display_name = ?
                LIMIT 1
            """, (clean_id, clean_id, clean_id))

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
    
    global TRACCAR_LOG_ENABLED
    if request.method == 'POST':
        data = request.get_json(silent=True) or {}
        TRACCAR_LOG_ENABLED = bool(data.get('enabled', False))
    
    return jsonify({
        'success': True,
        'enabled': TRACCAR_LOG_ENABLED,
        'file_exists': os.path.exists(TRACCAR_LOG_FILE)
    })

@app.route('/api/traccar_debug/<admin_slug>/download_log')
def traccar_debug_download_log(admin_slug):
    if not is_valid_admin_slug(admin_slug):
        return "Zugriff verweigert", 403
    
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
        "top_processes": []
    }

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

    # Top 10 Prozesse nach RAM/CPU
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
        procs.sort(key=lambda x: x['memory_percent'], reverse=True)
        sys_info["top_processes"] = procs[:10]
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



if __name__ == '__main__':
    print(f"🔑 Admin-Konsole: http://127.0.0.1:5000/adminconsole-{ADMIN_SLUG}")
    print(f"🛰️ Traccar-Debug: http://127.0.0.1:5000/traccar_debug-{ADMIN_SLUG}")
    threading.Thread(target=traccar_background_worker, daemon=True).start()
    threading.Thread(target=game_rules_background_worker, daemon=True).start()

    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5000)
