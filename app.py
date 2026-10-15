import os
import json
import random
import uuid
import time
import re
import shutil
import socket
import sqlite3
import secrets
import subprocess
import threading
from collections import deque
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

# Lädt die Variablen aus der .env-Datei (falls lokal vorhanden)
load_dotenv()

try:
    from zoneinfo import ZoneInfo
except ImportError:
    import pytz
    def ZoneInfo(tz_str):
        return pytz.timezone(tz_str)

app = Flask(__name__)

# --- ADMIN PASSWORT & SICHERER URL-SLUG --- #
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "1234-5")
ADMIN_SLUG = secrets.token_urlsafe(16)

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
    "shuffle_stops_on_swap": True        
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, 'Catch_Haltestellen.json')
DB_FILE = os.path.join(BASE_DIR, 'game.db')
TRACCAR_DATA_DIR = os.path.join(BASE_DIR, 'traccar_data')

with open(DATA_FILE, 'r', encoding='utf-8') as f:
    stops_data = json.load(f)

ALL_STOPS = list(stops_data.keys())

# --- TRACCAR KONFIGURATION & SYNC ---
TRACCAR_BASE_URL = os.getenv("TRACCAR_BASE_URL", "http://127.0.0.1:8082/api")
TRACCAR_OSMAND_URL = os.getenv("TRACCAR_OSMAND_URL", "http://127.0.0.1:5055")
TRACCAR_API_URL = f"{TRACCAR_BASE_URL}/devices"

TRACCAR_USER = os.getenv("TRACCAR_API_USER", "admin").strip()
TRACCAR_PASS = os.getenv("TRACCAR_API_PASSWORD", "admin").strip()

traccar_lock = threading.Lock()
traccar_session = requests.Session()
traccar_ready = False
traccar_last_error = "Initialisiere..."

# In-Memory Log für das Debug-Dashboard (/traccar_debug_<admin_slug>)
recent_traccar_webhooks = deque(maxlen=25)
debug_test_events = {}


def get_compose_cmd():
    """Ermittelt, ob 'docker compose' oder 'docker-compose' auf dem System verfügbar ist."""
    for cmd in (["docker", "compose"], ["docker-compose"]):
        try:
            subprocess.run(cmd + ["version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            return cmd
        except Exception:
            continue
    return None


def start_traccar_container():
    """Startet den Traccar Docker-Container automatisch."""
    print("🐳 Prüfe / Starte Docker-Container für Traccar...")
    base_cmd = get_compose_cmd()
    if not base_cmd:
        print("⚠️ Weder 'docker compose' noch 'docker-compose' gefunden.")
        return False

    try:
        subprocess.run(
            base_cmd + ["up", "-d", "--force-recreate"],
            cwd=BASE_DIR,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"✅ Traccar via Docker gestartet ({' '.join(base_cmd)} up -d).")
        return True
    except subprocess.CalledProcessError as e:
        err_msg = e.stderr.decode('utf-8', errors='ignore') if e.stderr else str(e)
        print(f"⚠️ Docker-Warnung beim Start: {err_msg.strip()}")
        return False
    except Exception as e:
        print(f"⚠️ Unerwarteter Fehler beim Docker-Start: {e}")
        return False


def reset_traccar_container_and_db():
    """
    Löscht den alten Traccar-Container inklusive kaputter/gesperrter H2-Datenbank
    und startet ihn sauber neu, damit 'admin/admin' wieder frisch initialisiert wird.
    """
    global traccar_ready, traccar_last_error, traccar_session
    print("🧨 Setze Traccar-Container und interne H2-Datenbank komplett zurück...")
    traccar_ready = False
    traccar_last_error = "Container wird zurückgesetzt..."
    traccar_session = requests.Session()

    base_cmd = get_compose_cmd()
    if not base_cmd:
        traccar_last_error = "Docker Compose nicht gefunden"
        return False

    try:
        # 1. Container stoppen und anonyme Volumes entfernen
        subprocess.run(base_cmd + ["down", "-v"], cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Falls noch ein alter Container namens traccar-server hängt:
        subprocess.run(["docker", "rm", "-f", "traccar-server"], cwd=BASE_DIR, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # 2. Lokalen traccar_data Ordner leeren (falls durch Root/Docker angelegt, zur Not via Alpine-Container)
        if os.path.exists(TRACCAR_DATA_DIR):
            try:
                shutil.rmtree(TRACCAR_DATA_DIR)
            except PermissionError:
                subprocess.run(
                    ["docker", "run", "--rm", "-v", f"{TRACCAR_DATA_DIR}:/data", "alpine", "sh", "-c", "rm -rf /data/*"],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )
        os.makedirs(TRACCAR_DATA_DIR, exist_ok=True)

        # 3. Frisch starten
        subprocess.run(base_cmd + ["up", "-d"], cwd=BASE_DIR, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("✅ Traccar-Container frisch erstellt. Warte auf Bootvorgang...")
        return True
    except Exception as e:
        traccar_last_error = f"Reset-Fehler: {e}"
        print(f"❌ Fehler beim Zurücksetzen von Traccar: {e}")
        return False


def ensure_traccar_credentials():
    """
    Meldet sich an der Traccar-API an (speichert Session-Cookie + BasicAuth).
    Falls noch die Standard-Zugangsdaten (admin/admin) aktiv sind, werden diese
    automatisch auf TRACCAR_API_USER und TRACCAR_API_PASSWORD aus der .env umgestellt.
    """
    global traccar_ready, traccar_last_error
    try:
        # 1. Prüfen, ob Login mit den .env-Zugangsdaten klappt (POST /api/session)
        resp = traccar_session.post(
            f"{TRACCAR_BASE_URL}/session",
            data={"email": TRACCAR_USER, "password": TRACCAR_PASS},
            timeout=4
        )
        if resp.status_code == 200:
            traccar_session.auth = HTTPBasicAuth(TRACCAR_USER, TRACCAR_PASS)
            traccar_ready = True
            traccar_last_error = None
            return True

        # 2. Falls 401: Versuchen, mit Traccar-Default (admin / admin) einzuloggen
        resp_default = traccar_session.post(
            f"{TRACCAR_BASE_URL}/session",
            data={"email": "admin", "password": "admin"},
            timeout=4
        )
        if resp_default.status_code == 200:
            user_data = resp_default.json()
            user_id = user_data.get("id")
            user_data["email"] = TRACCAR_USER
            user_data["name"] = TRACCAR_USER
            user_data["password"] = TRACCAR_PASS

            # Benutzer über aktive Session + BasicAuth aktualisieren
            update_resp = traccar_session.put(
                f"{TRACCAR_BASE_URL}/users/{user_id}",
                json=user_data,
                auth=HTTPBasicAuth("admin", "admin"),
                timeout=5
            )
            if update_resp.status_code == 200:
                print(f"🔐 Traccar Standard-Login erfolgreich auf '{TRACCAR_USER}' geändert!")
                # Neue Session mit den neuen Credentials öffnen
                traccar_session.post(
                    f"{TRACCAR_BASE_URL}/session",
                    data={"email": TRACCAR_USER, "password": TRACCAR_PASS},
                    timeout=4
                )
                traccar_session.auth = HTTPBasicAuth(TRACCAR_USER, TRACCAR_PASS)
                traccar_ready = True
                traccar_last_error = None
                return True
            else:
                traccar_last_error = f"Admin-Update fehlgeschlagen (HTTP {update_resp.status_code})"
        else:
            traccar_last_error = f"Login abgelehnt (HTTP {resp.status_code}) – Alte DB im Container?"

    except requests.exceptions.ConnectionError:
        traccar_last_error = "Port 8082 nicht erreichbar (Traccar bootet oder DB-Crash)"
    except requests.exceptions.Timeout:
        traccar_last_error = "Timeout auf Port 8082"
    except Exception as e:
        traccar_last_error = f"Fehler: {e}"

    traccar_ready = False
    return False


def sync_traccar_devices(teams_data=None):
    """
    Synchronisiert die Teams resilient mit Traccar:
    - Legt fehlende aktive Teams (anhand des Slugs = Name + PIN) an.
    - Benennt Teams um, falls sich der Anzeigename geändert hat.
    - Löscht veraltete Geräte (z.B. nach PIN-Wechsel, Löschung oder Deaktivierung).
    """
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

            # Soll-Zustand: Alle aktiven Teams mit ihrem aktuellen Slug (beinhaltet PIN)
            desired_devices = {}
            for t_id, t_data in teams_data.items():
                if not t_data.get("deactivated", False):
                    slug = t_data.get("slug")
                    name = t_data.get("display_name", t_id)
                    if slug:
                        desired_devices[slug] = name

            # 1. Neue Geräte anlegen oder bestehende umbenennen
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

            # 2. Veraltete Geräte (z.B. alte PIN / deaktiviertes Team) aus Traccar entfernen
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
    """Führt den Traccar-Sync asynchron im Hintergrund aus, damit Requests nicht blockieren."""
    copy_data = json.loads(json.dumps(teams_data))
    threading.Thread(target=sync_traccar_devices, args=(copy_data,), daemon=True).start()


def traccar_background_worker():
    """
    Hintergrund-Thread: Wartet auf Traccar beim Start.
    Falls die alte Datenbank im Container gesperrt ist oder einen Login-Konflikt (401)
    aus einem früheren Lauf hat, setzt er den Container einmalig automatisch zurück!
    """
    print("⏳ Verbinde mit Traccar API...")
    failed_attempts = 0
    auto_reset_done = False

    while True:
        try:
            if ensure_traccar_credentials():
                sync_traccar_devices()
                break
            else:
                failed_attempts += 1
                # Wenn nach 5 Versuchen (~15s) immer noch 401 oder DB-Crash vorliegt -> Auto-Self-Heal!
                if failed_attempts >= 5 and not auto_reset_done:
                    print(f"⚠️ Traccar antwortet nicht korrekt ({traccar_last_error}). Starte automatische Selbstheilung...")
                    auto_reset_done = True
                    reset_traccar_container_and_db()
                    failed_attempts = 0
                    time.sleep(6)
                    continue
        except Exception as e:
            print(f"Fehler im Traccar-Worker: {e}")
        time.sleep(3)

    print("🚀 Traccar API verbunden und synchronisiert!")
    while True:
        time.sleep(30)
        try:
            sync_traccar_devices()
        except Exception:
            pass


# --- DATENBANK SETUP ---
def init_db():
    with sqlite3.connect(DB_FILE, timeout=10) as conn:
        c = conn.cursor()
        
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
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')
                    
        c.execute('''CREATE TABLE IF NOT EXISTS team_catches (
                        hunter_id TEXT,
                        prey_id TEXT,
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
                        timestamp REAL,
                        is_late INTEGER
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
                        timestamp REAL,
                        desc TEXT,
                        is_late INTEGER
                    )''')
                    
        c.execute('''CREATE TABLE IF NOT EXISTS team_locations (
                        team_id TEXT PRIMARY KEY,
                        lat REAL,
                        lon REAL,
                        updated_at REAL,
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')
                    
        c.execute('''CREATE TABLE IF NOT EXISTS location_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        team_id TEXT,
                        lat REAL,
                        lon REAL,
                        timestamp REAL,
                        FOREIGN KEY(team_id) REFERENCES teams(id)
                    )''')
                    
        conn.commit()

init_db()


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
    """Schreibt den Dictionary-State in die relationalen DB-Tabellen"""
    c = conn.cursor()

    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('config', ?)", (json.dumps(state['config']),))
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('roles_assigned', ?)", (1 if state.get('roles_assigned') else 0,))
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('roles_swapped', ?)", (1 if state.get('roles_swapped') else 0,))

    c.execute("DELETE FROM active_stops")
    for stop in state.get('active_stops', []):
        c.execute("INSERT INTO active_stops (stop_name) VALUES (?)", (stop,))

    c.execute("DELETE FROM stop_history")
    max_history = state["config"].get("max_history", 30)
    history = state['history'][-max_history:] if len(state['history']) > max_history else state['history']
    for stop in history:
        c.execute("INSERT INTO stop_history (stop_name) VALUES (?)", (stop,))

    c.execute("DELETE FROM teams")
    c.execute("DELETE FROM team_stops")
    c.execute("DELETE FROM team_catches")
    for t_id, data in state['teams_data'].items():
        c.execute("""
            INSERT OR REPLACE INTO teams (id, display_name, pin, role, next_role, slug, deactivated, manual_points)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (t_id, data['display_name'], data['pin'], data.get('role', 'keine'), data.get('next_role', 'keine'), data['slug'], 1 if data.get('deactivated') else 0, data.get('manual_points', 0)))

        for stop in data.get('stops', []):
            c.execute("INSERT INTO team_stops (team_id, stop_name) VALUES (?, ?)", (t_id, stop))
        for prey_id in data.get('caught', []):
            c.execute("INSERT INTO team_catches (hunter_id, prey_id) VALUES (?, ?)", (t_id, prey_id))

    c.execute("DELETE FROM pending_requests")
    for req in state['pending_requests']:
        c.execute("""
            INSERT INTO pending_requests (id, type, team, team_name, stop, reporter, reporter_name, hunter, hunter_name, prey, prey_name, timestamp, is_late)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (req['id'], req['type'], req.get('team'), req.get('team_name'), req.get('stop'), req.get('reporter'), req.get('reporter_name'), req.get('hunter'), req.get('hunter_name'), req.get('prey'), req.get('prey_name'), req.get('timestamp'), 1 if req.get('is_late') else 0))

    c.execute("DELETE FROM action_logs")
    logs = state['action_log'][:50] if len(state['action_log']) > 50 else state['action_log']
    for log in logs:
        c.execute("""
            INSERT INTO action_logs (id, type, team, team_name, stop, hunter, hunter_name, prey, prey_name, timestamp, desc, is_late)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (log['id'], log['type'], log.get('team'), log.get('team_name'), log.get('stop'), log.get('hunter'), log.get('hunter_name'), log.get('prey'), log.get('prey_name'), log.get('timestamp'), log.get('desc'), 1 if log.get('is_late') else 0))

    conn.commit()


def get_state(conn):
    """Baut den State-Dictionary aus den relationalen Tabellen auf"""
    c = conn.cursor()
    c.execute("SELECT key, value FROM settings")
    rows = c.fetchall()

    if not rows:
        initial_active = random.sample(ALL_STOPS, min(DEFAULT_CONFIG["target_stops"], len(ALL_STOPS)))
        num_teams = DEFAULT_CONFIG["num_teams"]
        team_names = DEFAULT_CONFIG["team_names"]

        teams_data = {}
        for i in range(num_teams):
            team_id = f"Team {i + 1}"
            display_name = team_names[i].strip() if i < len(team_names) and team_names[i].strip() else team_id
            pin = f"{random.randint(0, 9999):04d}"
            
            teams_data[team_id] = {
                "display_name": display_name,
                "stops": [], "caught": [], "caught_by": [], "pin": pin, "role": "keine",
                "slug": generate_slug(team_id, pin), "deactivated": False,
                "next_role": "keine", "manual_points": 0
            }
        
        calculate_next_roles(teams_data)
        state = {
            "config": DEFAULT_CONFIG.copy(),
            "active_stops": initial_active,
            "history": initial_active.copy(),
            "teams_data": teams_data,
            "pending_requests": [],
            "action_log": [],
            "roles_swapped": False,
            "roles_assigned": False
        }
        save_state(conn, state)
        return state

    state = {}
    settings_dict = {row['key']: row['value'] for row in rows}
    
    state['config'] = json.loads(settings_dict.get('config', '{}'))
    state['roles_assigned'] = bool(int(settings_dict.get('roles_assigned', 0)))
    state['roles_swapped'] = bool(int(settings_dict.get('roles_swapped', 0)))

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
            'caught': [],
            'caught_by': []
        }

    c.execute("SELECT team_id, stop_name FROM team_stops")
    for r in c.fetchall():
        if r['team_id'] in state['teams_data']:
            state['teams_data'][r['team_id']]['stops'].append(r['stop_name'])

    c.execute("SELECT hunter_id, prey_id FROM team_catches")
    for r in c.fetchall():
        h_id, p_id = r['hunter_id'], r['prey_id']
        if h_id in state['teams_data']:
            state['teams_data'][h_id]['caught'].append(p_id)
        if p_id in state['teams_data']:
            state['teams_data'][p_id]['caught_by'].append(h_id)

    state['pending_requests'] = []
    c.execute("SELECT * FROM pending_requests")
    for r in c.fetchall():
        req = dict(r)
        req['is_late'] = bool(req['is_late'])
        state['pending_requests'].append({k: v for k, v in req.items() if v is not None})

    state['action_log'] = []
    c.execute("SELECT * FROM action_logs ORDER BY timestamp DESC")
    for r in c.fetchall():
        log = dict(r)
        log['is_late'] = bool(log['is_late'])
        state['action_log'].append({k: v for k, v in log.items() if v is not None})

    for k, v in DEFAULT_CONFIG.items():
        if k not in state["config"]:
            state["config"][k] = v

    return state


def get_new_random_stop(active_stops, history):
    available = set(ALL_STOPS) - set(active_stops) - set(history)
    if not available:
        available = set(ALL_STOPS) - set(active_stops)
    if not available:
        return random.choice(ALL_STOPS)
    return random.choice(list(available))


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

            for i, t_id in enumerate(active_teams):
                state["teams_data"][t_id]["role"] = roles[i]

            calculate_next_roles(state["teams_data"])
            state["roles_assigned"] = True
            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()), "type": "system", "timestamp": time.time(),
                "desc": "🎲 Die Rollen wurden zugewiesen!"
            })
            save_state(conn, state)

    mid_dt = None
    if config.get("enable_mid_game_swap", True):
        mid_dt = start_dt + (end_dt - start_dt) / 2
        if now >= mid_dt and not state.get("roles_swapped", False):
            for t_id, data in state["teams_data"].items():
                if not data.get("deactivated", False):
                    current_role = data.get("role", "keine")
                    if current_role == "läufer":
                        data["role"] = "fänger"
                    elif current_role == "fänger":
                        data["role"] = "läufer"
            
            calculate_next_roles(state["teams_data"])
            state["roles_swapped"] = True
            
            if config.get("shuffle_stops_on_swap", True):
                target_stops = config.get("target_stops", 12)
                state["history"] = []
                new_active_stops = random.sample(ALL_STOPS, min(target_stops, len(ALL_STOPS)))
                state["active_stops"] = new_active_stops
                state["history"] = new_active_stops.copy()
                desc = "🔄 HALBZEIT! Die Rollen wurden getauscht. Haltestellen sind neu gewürfelt und die Historie wurde gelöscht."
            else:
                desc = "🔄 HALBZEIT! Die Rollen der aktiven Teams wurden getauscht."

            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()), "type": "system", "timestamp": time.time(),
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


# --- SPIEL-AKTIONEN ---
def execute_stop_catch(state, stop_name, team_id, is_late=False):
    display_name = "Unbekannt"
    if team_id and team_id in state["teams_data"]:
        state["teams_data"][team_id]["stops"].append(stop_name)
        display_name = state["teams_data"][team_id]["display_name"]
        
    if stop_name in state['active_stops']:
        state['active_stops'].remove(stop_name)
        new_stop = get_new_random_stop(state['active_stops'], state['history'])
        state['active_stops'].append(new_stop)
        state['history'].append(new_stop)

    desc = f"📍 {display_name} hat die Haltestelle '{stop_name}' erreicht."
    if is_late: desc += "   (Nachtrag nach Spielende)"
    
    state["action_log"].insert(0, {
        "id": str(uuid.uuid4()), "type": "stop", "team": team_id, "team_name": display_name, "stop": stop_name,
        "timestamp": time.time(), "desc": desc, "is_late": is_late
    })
    return True


def execute_team_catch(state, hunter_id, prey_id, is_late=False):
    if hunter_id in state['teams_data'] and prey_id in state['teams_data']:
        state['teams_data'][hunter_id]['caught'].append(prey_id)
        state['teams_data'][prey_id]['caught_by'].append(hunter_id)

        hunter_name = state['teams_data'][hunter_id]['display_name']
        prey_name = state['teams_data'][prey_id]['display_name']

        desc = f"⚔️ {hunter_name} (Fänger) hat {prey_name} (Läufer) gefangen."
        if is_late: desc += "   (Nachtrag nach Spielende)"

        state["action_log"].insert(0, {
            "id": str(uuid.uuid4()), "type": "catch", "hunter": hunter_id, "prey": prey_id,
            "hunter_name": hunter_name, "prey_name": prey_name,
            "timestamp": time.time(), "desc": desc, "is_late": is_late
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

    return render_template('index.html', teams=teams_list, leaderboard=leaderboard,
                           phase=phase, start_time=start_iso, end_time=end_iso,
                           mid_time=mid_iso, now_time=now_iso,
                           enable_swap=config.get("enable_mid_game_swap", True),
                           roles_swapped=state.get("roles_swapped", False),
                           action_log=state.get("action_log", []))


@app.route('/rules.html')
def rules():
    return render_template('rules.html')


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

    if not team_id: return "Team nicht gefunden", 404

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

    team_log = [entry for entry in state["action_log"] if
                entry.get("team") == team_id or entry.get("hunter") == team_id or entry.get("prey") == team_id or entry.get("type") == "system"]

    host_ip = request.host.split(':')[0]

    return render_template('team.html', team_id=team_id, team_name=team_data["display_name"],
                           role=role, next_role=next_role, slug=slug,
                           laeufer_teams=laeufer_teams, faenger_teams=faenger_teams,
                           leaderboard=leaderboard, log=team_log, phase=phase,
                           start_time=start_iso, end_time=end_iso, mid_time=mid_iso,
                           now_time=now_iso, enable_swap=config.get("enable_mid_game_swap", True),
                           roles_swapped=state.get("roles_swapped", False),
                           is_deactivated=is_deactivated, host_ip=host_ip)


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
    if data.get('password') == ADMIN_PASSWORD:
        return jsonify({'success': True, 'redirect_url': f"/adminconsole-{ADMIN_SLUG}"})
    return jsonify({'success': False, 'error': 'Falsches Admin-Passwort!'})


@app.route('/adminconsole-<admin_slug>')
def admin(admin_slug):
    if admin_slug != ADMIN_SLUG: return "Zugriff verweigert", 403
    
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)

    config = state["config"]
    leaderboard = []
    team_urls = {}
    teams_list = []
    base_url = request.host_url.rstrip('/')
    
    team_names_map = {t_id: d["display_name"] for t_id, d in state["teams_data"].items()}

    for t_id, data in state["teams_data"].items():
        team_urls[data["display_name"]] = f"{base_url}/team/{data['slug']}"
        teams_list.append({"id": t_id, "name": data["display_name"]})

        score = calculate_score(data, config)

        leaderboard.append({
            "id": t_id, "display_name": data["display_name"], "score": score, "role": data.get("role", "keine"),
            "next_role": data.get("next_role", ""),
            "stops": data.get("stops", []), "caught": data.get("caught", []), "caught_by": data.get("caught_by", []),
            "deactivated": data.get("deactivated", False),
            "manual_points": data.get("manual_points", 0)
        })
        
    leaderboard.sort(key=lambda x: x["score"], reverse=True)
    teams_list.sort(key=lambda x: x["name"].lower())

    return render_template('admin.html', active_stops=state['active_stops'], teams=teams_list,
                           leaderboard=leaderboard, admin_url=admin_slug, team_urls=team_urls,
                           pending_requests=state["pending_requests"], action_log=state["action_log"],
                           phase=phase, start_time=start_iso, end_time=end_iso, mid_time=mid_iso, 
                           now_time=now_iso, team_names_map=team_names_map,
                           enable_swap=config.get("enable_mid_game_swap", True),
                           roles_swapped=state.get("roles_swapped", False))


@app.route('/api/admin_data/<admin_slug>')
def api_admin_data(admin_slug):
    if admin_slug != ADMIN_SLUG: return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403
    
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(conn, state)
        
        c = conn.cursor()
        c.execute("SELECT team_id, lat, lon, updated_at FROM team_locations")
        locations = c.fetchall()
        
        team_locs = []
        for loc in locations:
            t_id = loc['team_id']
            team_data = state["teams_data"].get(t_id)
            if team_data and not team_data.get("deactivated", False):
                team_locs.append({
                    "id": t_id,
                    "name": team_data["display_name"],
                    "role": team_data.get("role", "keine"),
                    "lat": loc['lat'],
                    "lon": loc['lon'],
                    "updated_at": loc['updated_at']
                })
        
    return jsonify({
        'success': True,
        'pending_requests': state.get("pending_requests", []),
        'action_log': state.get("action_log", []),
        'phase': phase,
        'now_time': now_iso,
        'active_stops': state.get("active_stops", []),
        'team_locations': team_locs
    })


@app.route('/adminconsole-<admin_slug>/settings', methods=['GET', 'POST'])
def admin_settings(admin_slug):
    if admin_slug != ADMIN_SLUG: return "Zugriff verweigert", 403
    
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        config = state["config"]

        if request.method == 'GET':
            sorted_teams = sorted(state["teams_data"].items(), key=lambda x: int(x[0].replace("Team ", "")) if "Team " in x[0] else 999)
            return render_template('settings.html', config=config, sorted_teams=sorted_teams, admin_url=admin_slug)

        data = request.json
        old_start = config.get("start_time")
        old_end = config.get("end_time")
        old_swap = config.get("enable_mid_game_swap")

        config["target_stops"] = int(data.get("target_stops", config["target_stops"]))
        config["max_history"] = int(data.get("max_history", config["max_history"]))

        config["start_time"] = data.get("start_time", config.get("start_time"))
        config["end_time"] = data.get("end_time", config.get("end_time"))
        config["timezone"] = data.get("timezone", "Europe/Berlin")
        config["enable_mid_game_swap"] = data.get("enable_mid_game_swap", True)
        config["shuffle_stops_on_swap"] = data.get("shuffle_stops_on_swap", True)

        if old_start != config["start_time"] or old_end != config["end_time"] or old_swap != config["enable_mid_game_swap"]:
            state["roles_swapped"] = False

        team_list = data.get("teams_list", [])
        old_teams_data = state.get("teams_data", {})
        new_teams_data = {}
        
        for t in team_list:
            team_id = str(t["team_id"]).strip()
            display_name = str(t["new_name"]).strip() or team_id
            role = t["role"]
            pin = str(t["pin"]).strip()
            active = bool(t["active"])
            
            if team_id and team_id in old_teams_data:
                team_data = old_teams_data[team_id]
                team_data["display_name"] = display_name
                team_data["role"] = role
                team_data["pin"] = pin
                team_data["slug"] = generate_slug(team_id, pin)
                team_data["deactivated"] = not active
                new_teams_data[team_id] = team_data
            else:
                new_teams_data[team_id] = {
                    "display_name": display_name,
                    "stops": [], "caught": [], "caught_by": [], "pin": pin,
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

        current_active_count = len(state["active_stops"])
        target = config["target_stops"]
        if current_active_count < target:
            needed = target - current_active_count
            available = list(set(ALL_STOPS) - set(state["active_stops"]) - set(state["history"]))
            if not available: available = list(set(ALL_STOPS) - set(state["active_stops"]))
            if available:
                added = random.sample(available, min(needed, len(available)))
                state["active_stops"].extend(added)
                state["history"].extend(added)
        elif current_active_count > target:
            state["active_stops"] = state["active_stops"][:target]

        save_state(conn, state)
        trigger_traccar_sync_async(state["teams_data"])
        
    return jsonify({'success': True, 'new_admin_url': admin_slug})


@app.route('/adminconsole-<admin_slug>/reset', methods=['POST'])
def reset_game(admin_slug):
    if admin_slug != ADMIN_SLUG: return jsonify({'success': False, 'error': 'Verboten'}), 403
    
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        config = state["config"]
        target = config.get("target_stops", 12)

        initial_active = random.sample(ALL_STOPS, min(target, len(ALL_STOPS)))
        state["active_stops"] = initial_active
        state["history"] = initial_active.copy()

        state["pending_requests"] = []
        state["action_log"] = []
        state["roles_swapped"] = False

        teams_ids = list(state["teams_data"].keys())
        for t_id in teams_ids:
            state["teams_data"][t_id]["stops"] = []
            state["teams_data"][t_id]["caught"] = []
            state["teams_data"][t_id]["caught_by"] = []
            state["teams_data"][t_id]["role"] = "keine"
            state["teams_data"][t_id]["manual_points"] = 0
            
        calculate_next_roles(state["teams_data"])
        state["roles_assigned"] = False

        save_state(conn, state)
        trigger_traccar_sync_async(state["teams_data"])
        
    return jsonify({'success': True})


@app.route('/adminconsole-<admin_slug>/randomize_roles', methods=['POST'])
def randomize_roles(admin_slug):
    if admin_slug != ADMIN_SLUG: return jsonify({'success': False, 'error': 'Verboten'}), 403
    
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
    if admin_slug != ADMIN_SLUG: return jsonify({'success': False, 'error': 'Verboten'}), 403
    
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        config = state["config"]
        target = config.get("target_stops", 12)

        new_active_stops = random.sample(ALL_STOPS, min(target, len(ALL_STOPS)))
        state["active_stops"] = new_active_stops
        state["history"] = new_active_stops.copy()
        
        state["action_log"].insert(0, {
            "id": str(uuid.uuid4()), "type": "system", 
            "timestamp": time.time(), "desc": "🔀 Haltestellen wurden manuell neu gewürfelt und die Historie zurückgesetzt."
        })
        save_state(conn, state)
        
    return jsonify({'success': True})


# --- PLAYER ACTIONS API ---
@app.route('/api/request_stop', methods=['POST'])
def request_stop():
    data = request.json
    team_slug = data.get('slug')
    stop_name = data.get('stop')
    msg_to_send = None
    
    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        conn.row_factory = sqlite3.Row
        state = get_state(conn)
        phase, _, _, _, _ = update_game_time_state(conn, state)

        if phase not in ["game", "post_game"]:
            return jsonify({'success': False, 'error': 'Einträge sind nur während des aktiven Spiels oder kurz danach möglich!'}), 403

        is_late = (phase == "post_game")

        team_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == team_slug), None)
        if not team_id or stop_name not in state['active_stops']: return jsonify({'success': False}), 400
        
        if state["teams_data"][team_id].get("deactivated", False):
            return jsonify({'success': False, 'error': 'Dein Team ist deaktiviert.'}), 403

        already_pending = next((r for r in state["pending_requests"] if
                                r["type"] == "stop" and r["team"] == team_id and r["stop"] == stop_name), None)

        if not already_pending:
            display_name = state["teams_data"][team_id]["display_name"]
            state["pending_requests"].append({
                "id": str(uuid.uuid4()), "type": "stop", "team": team_id, "team_name": display_name, "stop": stop_name,
                "timestamp": time.time(), "is_late": is_late
            })
            save_state(conn, state)
            msg_to_send = f"📬 NEUE ANFRAGE: Team '{display_name}' möchte die Haltestelle '{stop_name}' eintragen lassen."

    if msg_to_send:
        send_telegram_msg(msg_to_send)

    return jsonify({'success': True})


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
        if recent_catches and (time.time() - recent_catches[0]["timestamp"]) < 300:
            return jsonify({'success': True, 'message': 'Fang wurde bereits vor kurzem registriert!'})

        matching_req = next((r for r in state["pending_requests"] if
                             r["type"] == "catch" and r["hunter"] == hunter_id and r["prey"] == prey_id and r[
                                 "reporter"] != reporter_id), None)

        if matching_req:
            state["pending_requests"].remove(matching_req)
            execute_team_catch(state, hunter_id, prey_id, is_late=is_late)
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
                    "timestamp": time.time(), "is_late": is_late
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
        if not req: return jsonify({'success': False, 'error': 'Anfrage nicht gefunden.'}), 404

        state["pending_requests"].remove(req)
        is_late = req.get("is_late", False)

        if action == 'approve':
            if req["type"] == "stop":
                execute_stop_catch(state, req["stop"], req["team"], is_late=is_late)
            elif req["type"] == "catch":
                state["pending_requests"] = [r for r in state["pending_requests"] if not (
                            r["type"] == "catch" and r["hunter"] == req["hunter"] and r["prey"] == req["prey"])]
                execute_team_catch(state, req["hunter"], req["prey"], is_late=is_late)

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
        if not action: return jsonify({'success': False, 'error': 'Aktion im Log nicht gefunden.'}), 404

        if action["type"] == "stop":
            team_id = action.get("team")
            stop = action["stop"]
            if team_id and team_id in state["teams_data"] and stop in state["teams_data"][team_id]["stops"]:
                state["teams_data"][team_id]["stops"].remove(stop)
            
            if stop not in state["active_stops"]:
                state["active_stops"].append(stop)
                
        elif action["type"] == "catch":
            hunter_id = action.get("hunter")
            prey_id = action.get("prey")
            if hunter_id and hunter_id in state["teams_data"] and prey_id in state["teams_data"][hunter_id]["caught"]:
                state["teams_data"][hunter_id]["caught"].remove(prey_id)
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
            if execute_stop_catch(state, stop_name, team_id):
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
            if hunter_id == prey_id: return jsonify({'success': False, 'error': 'Ein Team kann sich nicht selbst fangen!'}), 400
            if execute_team_catch(state, hunter_id, prey_id):
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
            
            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()), "type": "system", 
                "timestamp": time.time(), "desc": desc
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

        return jsonify({
            'success': True,
            'action_log': state.get("action_log", []),
            'leaderboard': leaderboard
        })


# --- TRACCAR WEBHOOK ROUTE ---
@app.route('/api/traccar', methods=['GET', 'POST'])
def traccar_webhook():
    """
    Nimmt Positions-Updates von Traccar entgegen.
    Unterstützt URL-Forwarding (?id=...&lat=...&lon=...), JSON-Forwarding und OsmAnd.
    """
    identifier = request.args.get('id') or request.args.get('deviceid') or request.form.get('id')
    lat = request.args.get('lat') or request.args.get('latitude') or request.form.get('lat')
    lon = request.args.get('lon') or request.args.get('longitude') or request.form.get('lon')

    if request.is_json:
        payload = request.get_json(silent=True) or {}
        if not identifier:
            identifier = payload.get('id') or payload.get('uniqueId') or (payload.get('device') or {}).get('uniqueId')
        if lat is None:
            lat = payload.get('lat') or payload.get('latitude') or (payload.get('position') or {}).get('latitude')
        if lon is None:
            lon = payload.get('lon') or payload.get('longitude') or (payload.get('position') or {}).get('longitude')

    if not identifier or lat is None or lon is None:
        return jsonify({'success': False, 'error': 'Fehlende Parameter (id, lat, lon)'}), 400

    try:
        lat = float(lat)
        lon = float(lon)
    except (ValueError, TypeError):
        return jsonify({'success': False, 'error': 'Ungültige Koordinaten'}), 400

    if lat == 0.0 and lon == 0.0:
        return jsonify({'success': True, 'ignored': 'GPS 0.0/0.0 ignoriert'}), 200

    timestamp = time.time()
    clean_id = str(identifier).strip()
    time_str = datetime.now().strftime("%H:%M:%S")

    # Spezialbehandlung für den Koblenz End-to-End Debug-Testuser
    if clean_id.startswith("debug-koblenz-"):
        debug_test_events[clean_id] = {
            "lat": lat,
            "lon": lon,
            "timestamp": timestamp
        }
        recent_traccar_webhooks.appendleft({
            "time_str": time_str,
            "identifier": clean_id,
            "lat": lat,
            "lon": lon,
            "matched": True,
            "status": "E2E-TEST EMPFANGEN (Koblenz)"
        })
        print(f"🧪 Debug-Loopback empfangen: {clean_id} -> {lat:.5f}, {lon:.5f}")
        return jsonify({'success': True, 'debug_test': True}), 200

    with sqlite3.connect(DB_FILE, timeout=10, isolation_level='IMMEDIATE') as conn:
        c = conn.cursor()

        c.execute(
            "SELECT id, display_name FROM teams WHERE slug = ? OR id = ? OR display_name = ?",
            (clean_id, clean_id, clean_id)
        )
        team_row = c.fetchone()

        if not team_row:
            recent_traccar_webhooks.appendleft({
                "time_str": time_str,
                "identifier": clean_id,
                "lat": lat,
                "lon": lon,
                "matched": False,
                "status": "Unbekanntes Team"
            })
            print(f"⚠️ Traccar Webhook: Unbekannte Kennung '{clean_id}' empfangen.")
            return jsonify({'success': False, 'error': f"Kein Team für Kennung '{clean_id}' gefunden"}), 404

        resolved_team_id, resolved_name = team_row[0], team_row[1]

        c.execute('''INSERT OR REPLACE INTO team_locations (team_id, lat, lon, updated_at) 
                     VALUES (?, ?, ?, ?)''', (resolved_team_id, lat, lon, timestamp))

        c.execute('''INSERT INTO location_history (team_id, lat, lon, timestamp)
                     VALUES (?, ?, ?, ?)''', (resolved_team_id, lat, lon, timestamp))

        conn.commit()

    recent_traccar_webhooks.appendleft({
        "time_str": time_str,
        "identifier": clean_id,
        "lat": lat,
        "lon": lon,
        "matched": True,
        "status": f"OK ({resolved_name})"
    })
    print(f"📡 GPS-Update: {resolved_name} ({resolved_team_id}) -> {lat:.5f}, {lon:.5f}")
    return jsonify({'success': True, 'team_id': resolved_team_id}), 200


# --- TRACCAR DEBUG DASHBOARD & E2E-TEST ROUTEN ---
@app.route('/traccar_debug_<admin_slug>')
def traccar_debug_page(admin_slug):
    if admin_slug != ADMIN_SLUG:
        return "Zugriff verweigert", 403
    return render_template('traccar_debug.html', admin_url=admin_slug)


@app.route('/api/traccar_debug/<admin_slug>/status')
def traccar_debug_status(admin_slug):
    if admin_slug != ADMIN_SLUG:
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    api_online = False
    api_error = None
    devices = []

    try:
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

    # Prüfen ob Port 5055 (OsmAnd Eingang) offen ist
    osmand_open = False
    try:
        with socket.create_connection(("127.0.0.1", 5055), timeout=2):
            osmand_open = True
    except Exception:
        osmand_open = False

    return jsonify({
        "api_online": api_online,
        "api_error": api_error,
        "osmand_port_open": osmand_open,
        "traccar_user": TRACCAR_USER,
        "devices": devices,
        "recent_webhooks": list(recent_traccar_webhooks)
    })


@app.route('/api/traccar_debug/<admin_slug>/sync', methods=['POST'])
def traccar_debug_sync(admin_slug):
    if admin_slug != ADMIN_SLUG:
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403
    ok = sync_traccar_devices()
    return jsonify({'success': ok, 'error': traccar_last_error})


@app.route('/api/traccar_debug/<admin_slug>/hard_reset', methods=['POST'])
def traccar_debug_hard_reset(admin_slug):
    """Setzt den Traccar-Docker-Container + H2-Datenbank per Klick sauber zurück."""
    if admin_slug != ADMIN_SLUG:
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    ok = reset_traccar_container_and_db()
    if not ok:
        return jsonify({'success': False, 'error': traccar_last_error})

    # Bis zu 25 Sekunden warten, bis der frisch erstellte Container hochgefahren ist
    for _ in range(12):
        time.sleep(2)
        if ensure_traccar_credentials():
            sync_traccar_devices()
            return jsonify({'success': True})

    return jsonify({'success': False, 'error': f"Container neu gestartet, bootet aber noch ({traccar_last_error}). Bitte 5s warten."})


@app.route('/api/traccar_debug/<admin_slug>/test', methods=['POST'])
def traccar_debug_e2e_test(admin_slug):
    """
    Führt einen echten End-to-End Loopback-Test durch:
    1. Prüft Traccar-API Login
    2. Erstellt einen temporären Test-User (debug-koblenz-<zufall>) in Traccar
    3. Sendet die Koordinaten von Koblenz (Deutsches Eck: 50.3644, 7.6061) an Port 5055
    4. Wartet darauf, dass Traccar die Daten an /api/traccar weiterleitet
    5. Löscht den temporären Test-User wieder aus Traccar
    """
    if admin_slug != ADMIN_SLUG:
        return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403

    steps = []
    test_unique_id = f"debug-koblenz-{random.randint(1000, 9999)}"
    target_lat = round(50.3644 + random.uniform(-0.0005, 0.0005), 6)
    target_lon = round(7.6061 + random.uniform(-0.0005, 0.0005), 6)
    created_device_id = None

    # Schritt 1: Authentifizierung prüfen
    if not ensure_traccar_credentials():
        steps.append({
            "step": "1. Traccar API Login",
            "ok": False,
            "detail": f"Login als '{TRACCAR_USER}' fehlgeschlagen ({traccar_last_error}). Tipp: Klicke links auf 'Traccar-Container & DB zurücksetzen'!"
        })
        return jsonify({"success": False, "steps": steps})

    steps.append({
        "step": "1. Traccar API Login",
        "ok": True,
        "detail": f"Erfolgreich als '{TRACCAR_USER}' authentifiziert."
    })

    try:
        # Schritt 2: Test-User in Traccar anlegen
        create_resp = traccar_session.post(
            TRACCAR_API_URL,
            json={"name": "Koblenz Debug Testuser", "uniqueId": test_unique_id},
            timeout=5
        )
        if create_resp.status_code not in (200, 201):
            steps.append({
                "step": "2. Test-Gerät in Traccar erstellen",
                "ok": False,
                "detail": f"Fehler beim Anlegen (HTTP {create_resp.status_code}): {create_resp.text}"
            })
            return jsonify({"success": False, "steps": steps})

        created_device_id = create_resp.json().get("id")
        steps.append({
            "step": "2. Test-Gerät in Traccar erstellen",
            "ok": True,
            "detail": f"Test-User 'Koblenz Debug Testuser' (uniqueId: {test_unique_id}, ID: {created_device_id}) angelegt."
        })

        # Schritt 3: Koblenz-Koordinaten an Port 5055 (OsmAnd-Protokoll) senden
        debug_test_events.pop(test_unique_id, None)
        osmand_params = {
            "id": test_unique_id,
            "lat": target_lat,
            "lon": target_lon,
            "timestamp": int(time.time()),
            "speed": 5.0
        }
        ping_resp = requests.post(TRACCAR_OSMAND_URL, params=osmand_params, timeout=5)
        if ping_resp.status_code != 200:
            steps.append({
                "step": "3. Koblenz-Koordinaten an Traccar (Port 5055) senden",
                "ok": False,
                "detail": f"OsmAnd-Port antwortete mit HTTP {ping_resp.status_code}"
            })
            return jsonify({"success": False, "steps": steps})

        steps.append({
            "step": "3. Koblenz-Koordinaten an Traccar (Port 5055) senden",
            "ok": True,
            "detail": f"Koordinaten Koblenz ({target_lat}, {target_lon}) erfolgreich an {TRACCAR_OSMAND_URL} übergeben."
        })

        # Schritt 4: Warten auf Rückmeldung am Python-Webhook (/api/traccar)
        received = None
        start_wait = time.time()
        while time.time() - start_wait < 5.0:
            if test_unique_id in debug_test_events:
                received = debug_test_events.pop(test_unique_id)
                break
            time.sleep(0.2)

        if received:
            delay_ms = int((received["timestamp"] - start_wait) * 1000)
            steps.append({
                "step": "4. Empfang am Python-Webhook (/api/traccar)",
                "ok": True,
                "detail": f"Paket nach ~{max(delay_ms, 10)}ms in Python eingetroffen! Empfangene Pos: ({received['lat']}, {received['lon']})."
            })
        else:
            steps.append({
                "step": "4. Empfang am Python-Webhook (/api/traccar)",
                "ok": False,
                "detail": "Timeout nach 5s: Traccar hat den Ping angenommen, aber er kam nicht bei /api/traccar an. Prüfe 'forward.url' in traccar.xml."
            })

    except Exception as e:
        steps.append({
            "step": "Unerwarteter Fehler während des Tests",
            "ok": False,
            "detail": str(e)
        })
    finally:
        # Schritt 5: Test-Gerät wieder sauber entfernen
        if created_device_id is not None:
            try:
                del_resp = traccar_session.delete(f"{TRACCAR_API_URL}/{created_device_id}", timeout=5)
                steps.append({
                    "step": "5. Test-Gerät aufräumen",
                    "ok": del_resp.status_code in (200, 204),
                    "detail": f"Test-User '{test_unique_id}' wieder aus Traccar entfernt."
                })
            except Exception as e:
                steps.append({
                    "step": "5. Test-Gerät aufräumen",
                    "ok": False,
                    "detail": f"Konnte Test-User nicht löschen: {e}"
                })

    all_ok = all(s["ok"] for s in steps)
    return jsonify({"success": all_ok, "steps": steps})


if __name__ == '__main__':
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'true' or not app.debug:
        print(f"🔑 Admin-Konsole: http://127.0.0.1:5000/adminconsole-{ADMIN_SLUG}")
        print(f"🛰️ Traccar-Debug: http://127.0.0.1:5000/traccar_debug_{ADMIN_SLUG}")
        start_traccar_container()
        threading.Thread(target=traccar_background_worker, daemon=True).start()

    app.run(debug=True, host='0.0.0.0', port=5000)