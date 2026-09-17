import os
import json
import random
import uuid
import time
import re
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify
import requests
from filelock import FileLock
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

# --- ADMIN PASSWORT ---
# Zieht das Passwort aus den Umgebungsvariablen. Fallback: "1234-5"
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "1234-5")

# --- STANDARD-KONFIGURATION ---
DEFAULT_CONFIG = {
    "target_stops": 12,
    "max_history": 30,
    "num_teams": 4,
    "team_names": [],
    "points_stop_reached": 2,
    "points_was_caught": -3,
    "points_caught_team": 4,
    "start_time": "",
    "end_time": "",
    "timezone": "Europe/Berlin",
    "enable_mid_game_swap": False
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(BASE_DIR, 'Catch_Haltestellen.json')
STATE_FILE = os.path.join(BASE_DIR, 'state.json')
STATE_LOCK_FILE = os.path.join(BASE_DIR, 'state.json.lock')

# Initialisierung des Locks mit 10 Sekunden Timeout
STATE_LOCK = FileLock(STATE_LOCK_FILE, timeout=10)

with open(DATA_FILE, 'r', encoding='utf-8') as f:
    stops_data = json.load(f)
ALL_STOPS = list(stops_data.keys())


# --- HILFSFUNKTIONEN FÜR STATUS UND AKTIONEN ---
def send_telegram_msg(text):
    TOKEN = os.getenv("TELEGRAM_TOKEN")
    CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    if not TOKEN or not CHAT_ID:
        print('No telegram token or chat_id provided')
        return
        
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        requests.post(url, data={'chat_id': CHAT_ID, 'text': text})
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


def get_state():
    if not os.path.exists(STATE_FILE):
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
                "next_role": "keine",
                "manual_points": 0
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
        save_state(state)
        return state

    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        state = json.load(f)

    if "config" not in state:
        state["config"] = DEFAULT_CONFIG.copy()
    else:
        for k, v in DEFAULT_CONFIG.items():
            if k not in state["config"]:
                state["config"][k] = v

    if "roles_assigned" not in state:
        active_has_keine = any(d.get("role", "keine") == "keine" for d in state.get("teams_data", {}).values() if not d.get("deactivated", False))
        state["roles_assigned"] = not active_has_keine

    return state


def save_state(state):
    max_history = state["config"].get("max_history", 30)
    if len(state['history']) > max_history:
        state['history'] = state['history'][-max_history:]
    if len(state['action_log']) > 50:
        state['action_log'] = state['action_log'][:50]

    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f)


def get_new_random_stop(active_stops, history):
    available = set(ALL_STOPS) - set(active_stops) - set(history)
    if not available:
        available = set(ALL_STOPS) - set(active_stops)
    if not available:
        return random.choice(ALL_STOPS)
    return random.choice(list(available))


# --- ZEIT- UND PHASENLOGIK ---
def update_game_time_state(state):
    config = state["config"]
    tz_str = config.get("timezone", "Europe/Berlin")
    tz = ZoneInfo(tz_str)
    now = datetime.now(tz)

    start_str = config.get("start_time", "")
    end_str = config.get("end_time", "")

    if not start_str or not end_str:
        return "inactive", None, None, None, now.isoformat()

    try:
        start_dt = datetime.fromisoformat(start_str).replace(tzinfo=tz)
        end_dt = datetime.fromisoformat(end_str).replace(tzinfo=tz)
    except ValueError:
        return "inactive", None, None, None, now.isoformat()

    # Rollenzuweisung genau 30 Minuten vor Spielbeginn
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
            save_state(state)

    mid_dt = None
    if config.get("enable_mid_game_swap"):
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
            
            # --- Haltestellen durchwürfeln und Historie leeren ---
            target_stops = config.get("target_stops", 12)
            state["history"] = []
            new_active_stops = random.sample(ALL_STOPS, min(target_stops, len(ALL_STOPS)))
            state["active_stops"] = new_active_stops
            state["history"] = new_active_stops.copy()
            # -----------------------------------------------------------

            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()), "type": "system", "timestamp": time.time(),
                "desc": "🔄 HALBZEIT! Die Rollen wurden getauscht. Haltestellen sind neu gewürfelt und die Historie wurde gelöscht."
            })
            save_state(state)

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
    if stop_name in state['active_stops']:
        state['active_stops'].remove(stop_name)
        
        display_name = "Unbekannt"
        if team_id and team_id in state["teams_data"]:
            state["teams_data"][team_id]["stops"].append(stop_name)
            display_name = state["teams_data"][team_id]["display_name"]
            
        new_stop = get_new_random_stop(state['active_stops'], state['history'])
        state['active_stops'].append(new_stop)
        state['history'].append(new_stop)

        desc = f"📍 {display_name} hat die Haltestelle '{stop_name}' erreicht."
        if is_late: desc += " ⚠️ (Nachtrag nach Spielende)"

        state["action_log"].insert(0, {
            "id": str(uuid.uuid4()), "type": "stop", "team": team_id, "team_name": display_name, "stop": stop_name,
            "timestamp": time.time(), "desc": desc, "is_late": is_late
        })
        return True
    return False


def execute_team_catch(state, hunter_id, prey_id, is_late=False):
    if hunter_id in state['teams_data'] and prey_id in state['teams_data']:
        state['teams_data'][hunter_id]['caught'].append(prey_id)
        state['teams_data'][prey_id]['caught_by'].append(hunter_id)

        hunter_name = state['teams_data'][hunter_id]['display_name']
        prey_name = state['teams_data'][prey_id]['display_name']

        desc = f"⚔️ {hunter_name} (Fänger) hat {prey_name} (Läufer) gefangen."
        if is_late: desc += " ⚠️ (Nachtrag nach Spielende)"

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
    with STATE_LOCK:
        state = get_state()
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(state)

    config = state["config"]
    leaderboard = []
    teams_list = []
    
    for t_id, d in state["teams_data"].items():
        if d.get("deactivated", False):
            continue
        teams_list.append({"id": t_id, "name": d["display_name"]})
        score = (len(d.get("stops", [])) * config["points_stop_reached"] +
                 len(d.get("caught", [])) * config["points_caught_team"] +
                 len(d.get("caught_by", [])) * config["points_was_caught"] +
                 d.get("manual_points", 0))
        leaderboard.append({
            "id": t_id, "display_name": d["display_name"], "score": score, 
            "role": d.get("role", "keine"), "next_role": d.get("next_role", "")
        })
        
    leaderboard.sort(key=lambda x: x["score"], reverse=True)
    teams_list.sort(key=lambda x: x["name"].lower())

    return render_template('index.html', teams=teams_list, leaderboard=leaderboard,
                           phase=phase, start_time=start_iso, end_time=end_iso,
                           mid_time=mid_iso, now_time=now_iso,
                           enable_swap=config.get("enable_mid_game_swap", False),
                           roles_swapped=state.get("roles_swapped", False),
                           action_log=state.get("action_log", []))


@app.route('/rules.html')
def rules():
    return render_template('rules.html')


@app.route('/team/<slug>')
def team_dashboard(slug):
    with STATE_LOCK:
        state = get_state()
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(state)

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

        score = (len(d.get("stops", [])) * config["points_stop_reached"] +
                 len(d.get("caught", [])) * config["points_caught_team"] +
                 len(d.get("caught_by", [])) * config["points_was_caught"] +
                 d.get("manual_points", 0))
        leaderboard.append({
            "id": t_id_iter, "display_name": d["display_name"], "score": score, 
            "role": t_role, "next_role": d.get("next_role", "")
        })

    leaderboard.sort(key=lambda x: x["score"], reverse=True)
    team_log = [entry for entry in state["action_log"] if
                entry.get("team") == team_id or entry.get("hunter") == team_id or entry.get("prey") == team_id or entry.get("type") == "system"]

    return render_template('team.html', team_id=team_id, team_name=team_data["display_name"], 
                           role=role, next_role=next_role, slug=slug,
                           laeufer_teams=laeufer_teams, faenger_teams=faenger_teams,
                           leaderboard=leaderboard, log=team_log, phase=phase,
                           start_time=start_iso, end_time=end_iso, mid_time=mid_iso,
                           now_time=now_iso, enable_swap=config.get("enable_mid_game_swap", False),
                           roles_swapped=state.get("roles_swapped", False),
                           is_deactivated=is_deactivated)


@app.route('/api/login_team', methods=['POST'])
def login_team():
    data = request.json
    team_id = data.get('team_id')
    pin = data.get('pin')
    
    with STATE_LOCK:
        state = get_state()
        
    team_data = state["teams_data"].get(team_id)
    if team_data and str(team_data.get("pin")) == str(pin).strip():
        return jsonify({'success': True, 'slug': team_data["slug"]})
            
    return jsonify({'success': False, 'error': 'Falscher PIN oder Team nicht gefunden!'})


@app.route('/api/login_admin', methods=['POST'])
def login_admin():
    data = request.json
    if data.get('password') == ADMIN_PASSWORD:
        return jsonify({'success': True, 'redirect_url': f"/adminconsole-{ADMIN_PASSWORD}"})
    return jsonify({'success': False, 'error': 'Falsches Admin-Passwort!'})


@app.route('/adminconsole-<admin_slug>')
def admin(admin_slug):
    if admin_slug != ADMIN_PASSWORD: return "Zugriff verweigert", 403
    
    with STATE_LOCK:
        state = get_state()
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(state)

    config = state["config"]
    leaderboard = []
    team_urls = {}
    teams_list = []
    base_url = request.host_url.rstrip('/')
    
    team_names_map = {t_id: d["display_name"] for t_id, d in state["teams_data"].items()}

    for t_id, data in state["teams_data"].items():
        team_urls[data["display_name"]] = f"{base_url}/team/{data['slug']}"
        teams_list.append({"id": t_id, "name": data["display_name"]})

        score = (len(data.get("stops", [])) * config["points_stop_reached"] +
                 len(data.get("caught", [])) * config["points_caught_team"] +
                 len(data.get("caught_by", [])) * config["points_was_caught"] +
                 data.get("manual_points", 0))
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
                           enable_swap=config.get("enable_mid_game_swap", False),
                           roles_swapped=state.get("roles_swapped", False))


@app.route('/api/admin_data/<admin_slug>')
def api_admin_data(admin_slug):
    if admin_slug != ADMIN_PASSWORD: return jsonify({'success': False, 'error': 'Zugriff verweigert'}), 403
    
    with STATE_LOCK:
        state = get_state()
        phase, start_iso, end_iso, mid_iso, now_iso = update_game_time_state(state)
        
    return jsonify({
        'success': True,
        'pending_requests': state.get("pending_requests", []),
        'action_log': state.get("action_log", []),
        'phase': phase,
        'now_time': now_iso,
        'active_stops': state.get("active_stops", [])
    })


@app.route('/adminconsole-<admin_slug>/settings', methods=['GET', 'POST'])
def admin_settings(admin_slug):
    if admin_slug != ADMIN_PASSWORD: return "Zugriff verweigert", 403
    
    with STATE_LOCK:
        state = get_state()
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
        config["enable_mid_game_swap"] = data.get("enable_mid_game_swap", False)

        if old_start != config["start_time"] or old_end != config["end_time"] or old_swap != config["enable_mid_game_swap"]:
            state["roles_swapped"] = False

        team_list = data.get("teams_list", [])
        old_teams_data = state.get("teams_data", {})
        new_teams_data = {}
        
        for t in team_list:
            team_id = t["team_id"]
            display_name = t["new_name"].strip() or team_id
            role = t["role"]
            pin = t["pin"]
            active = t["active"]
            
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
        state["roles_assigned"] = not active_has_keine
        
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

        save_state(state)
        
    return jsonify({'success': True, 'new_admin_url': admin_slug})


@app.route('/adminconsole-<admin_slug>/reset', methods=['POST'])
def reset_game(admin_slug):
    if admin_slug != ADMIN_PASSWORD: return jsonify({'success': False, 'error': 'Verboten'}), 403
    
    with STATE_LOCK:
        state = get_state()
        config = state["config"]

        target = config.get("target_stops", 12)
        initial_active = random.sample(ALL_STOPS, min(target, len(ALL_STOPS)))
        state["active_stops"] = initial_active
        state["history"] = initial_active.copy()
        state["pending_requests"] = []
        state["action_log"] = []
        state["roles_swapped"] = False

        teams_ids = list(state["teams_data"].keys())

        for i, t_id in enumerate(teams_ids):
            state["teams_data"][t_id]["stops"] = []
            state["teams_data"][t_id]["caught"] = []
            state["teams_data"][t_id]["caught_by"] = []
            state["teams_data"][t_id]["role"] = "keine"
            state["teams_data"][t_id]["manual_points"] = 0
            
        calculate_next_roles(state["teams_data"])
        state["roles_assigned"] = False
        save_state(state)
        
    return jsonify({'success': True})


@app.route('/adminconsole-<admin_slug>/randomize_roles', methods=['POST'])
def randomize_roles(admin_slug):
    if admin_slug != ADMIN_PASSWORD: return jsonify({'success': False, 'error': 'Verboten'}), 403
    
    with STATE_LOCK:
        state = get_state()
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
            save_state(state)

    return jsonify({'success': True})


# --- PLAYER ACTIONS API ---
@app.route('/api/request_stop', methods=['POST'])
def request_stop():
    data = request.json
    team_slug = data.get('slug')
    stop_name = data.get('stop')
    msg_to_send = None
    
    with STATE_LOCK:
        state = get_state()
        phase, _, _, _, _ = update_game_time_state(state)
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
            save_state(state)
            msg_to_send = f"📍 NEUE ANFRAGE: Team '{display_name}' möchte die Haltestelle '{stop_name}' eintragen lassen."

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
    
    with STATE_LOCK:
        state = get_state()
        phase, _, _, _, _ = update_game_time_state(state)
        if phase not in ["game", "post_game"]:
            return jsonify({'success': False,
                            'error': 'Fänge können nur während des aktiven Spiels oder kurz danach gemeldet werden!'}), 403

        is_late = (phase == "post_game")
        reporter_id = next((t_id for t_id, d in state["teams_data"].items() if d.get("slug") == reporter_slug), None)

        if not reporter_id or not hunter_id or not prey_id: return jsonify(
            {'success': False, 'error': 'Ungültige Daten übermittelt.'}), 400
            
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
            save_state(state)
            
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
                save_state(state)
                msg_to_send = f"⚔️ NEUE FANG-ANFRAGE: '{r_name}' meldet, dass '{h_name}' das Team '{p_name}' gefangen hat."
            
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
    
    with STATE_LOCK:
        state = get_state()
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

        save_state(state)
        
    return jsonify({'success': True})


@app.route('/api/undo_action', methods=['POST'])
def undo_action():
    data = request.json
    action_id = data.get('id')
    
    with STATE_LOCK:
        state = get_state()
        action = next((a for a in state["action_log"] if a["id"] == action_id), None)

        if not action: return jsonify({'success': False, 'error': 'Aktion im Log nicht gefunden.'}), 404

        if action["type"] == "stop":
            team_id = action.get("team")
            stop = action["stop"]
            if team_id and team_id in state["teams_data"] and stop in state["teams_data"][team_id]["stops"]:
                state["teams_data"][team_id]["stops"].remove(stop)
        elif action["type"] == "catch":
            hunter_id = action.get("hunter")
            prey_id = action.get("prey")
            if hunter_id and hunter_id in state["teams_data"] and prey_id in state["teams_data"][hunter_id]["caught"]:
                state["teams_data"][hunter_id]["caught"].remove(prey_id)
            if prey_id and prey_id in state["teams_data"] and hunter_id in state["teams_data"][prey_id]["caught_by"]:
                state["teams_data"][prey_id]["caught_by"].remove(hunter_id)

        state["action_log"].remove(action)
        save_state(state)
        
    return jsonify({'success': True})


@app.route('/api/toggle', methods=['POST'])
def toggle():
    data = request.json
    stop_name = data.get('name')
    team_id = data.get('team_id')
    
    with STATE_LOCK:
        state = get_state()
        if stop_name in state['active_stops']:
            if execute_stop_catch(state, stop_name, team_id):
                save_state(state)
                return jsonify({'success': True})
                
    return jsonify({'success': False}), 400


@app.route('/api/catch_team', methods=['POST'])
def catch_team():
    data = request.json
    hunter_id = data.get('hunter')
    prey_id = data.get('prey')
    
    with STATE_LOCK:
        state = get_state()
        if hunter_id and prey_id:
            if hunter_id == prey_id: return jsonify({'success': False, 'error': 'Ein Team kann sich nicht selbst fangen!'}), 400
            if execute_team_catch(state, hunter_id, prey_id):
                save_state(state)
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

    with STATE_LOCK:
        state = get_state()
        if team_id in state["teams_data"]:
            current = state["teams_data"][team_id].get("manual_points", 0)
            state["teams_data"][team_id]["manual_points"] = current + points
            
            display_name = state["teams_data"][team_id]["display_name"]
            desc = f"🔧 Manuelle Punktekorrektur für {display_name}: {points:+} Punkte."
            
            state["action_log"].insert(0, {
                "id": str(uuid.uuid4()), "type": "system", 
                "timestamp": time.time(), "desc": desc
            })
            save_state(state)
            return jsonify({'success': True})
            
    return jsonify({'success': False, 'error': 'Team nicht gefunden.'}), 400


@app.route('/api/stops')
def api_stops():
    team_slug = request.args.get('slug')
    
    with STATE_LOCK:
        state = get_state()
        # Stelle sicher, dass der Spielzeit-Status und Rollentausch aktuell sind 
        update_game_time_state(state)
        
        if not team_slug:
            return jsonify({'error': 'Slug fehlt'}), 400
            
        team_data = next((d for d in state["teams_data"].values() if d.get("slug") == team_slug), None)
        
        if not team_data:
            return jsonify({'error': 'Team nicht gefunden'}), 404
            
        # Nur aktive Läufer dürfen die aktiven Haltestellen abfragen
        if team_data.get("role") != "läufer":
            return jsonify({'error': 'Nicht berechtigt'}), 403
            
        result = []
        for name in state['active_stops']:
            if name in stops_data:
                result.append(
                    {'name': name, 'lat': stops_data[name]['lat'], 'lon': stops_data[name]['lon'], 'reached': False})
                    
        return jsonify(result)


@app.route('/api/all_stops')
def api_all_stops():
    result = []
    for name, data in stops_data.items():
        result.append({'name': name, 'lat': data['lat'], 'lon': data['lon']})
    return jsonify(result)


# TICKER API ENDPUNKT (MIT LEADERBOARD)
@app.route('/api/ticker_data')
def api_ticker_data():
    with STATE_LOCK:
        state = get_state()
        config = state["config"]
        
        leaderboard = []
        for t_id, d in state["teams_data"].items():
            if d.get("deactivated", False):
                continue
            score = (len(d.get("stops", [])) * config["points_stop_reached"] +
                     len(d.get("caught", [])) * config["points_caught_team"] +
                     len(d.get("caught_by", [])) * config["points_was_caught"] +
                     d.get("manual_points", 0))
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')