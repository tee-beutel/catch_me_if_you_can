import os
import re
import requests
import time
import math
import threading
import random
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Lädt die Variablen aus der .env-Datei
load_dotenv()

# --- KONFIGURATION ---
BASE_URL = "http://127.0.0.1:5000"
OSMAND_URL = "http://127.0.0.1:5055"
ADMIN_PASS = os.getenv("ADMIN_PASSWORD", "1234-5").strip()
GAME_DURATION_MINUTES = 10
UPDATE_INTERVAL = 3  # Sekunden zwischen den GPS-Updates
LOG_FILE = "log.txt"

PLATZ_DER_DEUTSCHEN_EINHEIT = (52.2664, 10.5245)

# --- HILFSFUNKTIONEN ---
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0)**2
    return 2.0 * R * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))

def move_towards(lat, lon, target_lat, target_lon, speed_m_per_s, dt):
    dist = haversine(lat, lon, target_lat, target_lon)
    if dist <= speed_m_per_s * dt:
        return target_lat, target_lon
    
    ratio = (speed_m_per_s * dt) / dist
    new_lat = lat + (target_lat - lat) * ratio
    new_lon = lon + (target_lon - lon) * ratio
    return new_lat, new_lon

def generate_slug(display_name, pin):
    clean_name = re.sub(r'[^a-z0-9]+', '-', display_name.lower()).strip('-')
    return f"{clean_name}_{pin}"

# --- SPIEL-STEUERUNG ---
class CatchSimulator:
    def __init__(self):
        self.session = requests.Session()
        self.teams = {}
        self.all_stops = {}
        self.running = True
        
        self.stats_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.stats = {
            "api_calls": 0,
            "errors": 0,
            "gps_sent": 0,
            "stops_reached": 0,
            "catches_initiated": 0,
            "catches_confirmed": 0,
            "edge_cases_tested": 0
        }
        
        # Log-Datei initialisieren (leert alte Inhalte)
        with open(LOG_FILE, "w", encoding="utf-8") as f:
            f.write(f"--- SIMULATION LOG STARTED AT {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---\n\n")

    def log(self, message):
        """Schreibt eine Nachricht in die Konsole UND in die log.txt Datei"""
        time_prefix = datetime.now().strftime("[%H:%M:%S] ")
        full_message = time_prefix + message
        
        print(full_message)
        
        with self.log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(full_message + "\n")

    def _track_request(self, is_error=False):
        with self.stats_lock:
            self.stats["api_calls"] += 1
            if is_error:
                self.stats["errors"] += 1

    def api_get(self, endpoint, params=None):
        url = f"{BASE_URL}{endpoint}"
        try:
            res = self.session.get(url, params=params, timeout=5)
            self._track_request(res.status_code >= 500)
            self.log(f"📡 [GET] {endpoint} -> HTTP {res.status_code}")
            return res
        except Exception as e:
            self._track_request(is_error=True)
            self.log(f"📡 [GET] {endpoint} -> FEHLER: {e}")
            return None

    def api_post(self, endpoint, json_data=None):
        url = f"{BASE_URL}{endpoint}"
        try:
            res = self.session.post(url, json=json_data, timeout=5)
            # 4xx Fehler sind bei Edge Cases Absicht, daher tracken wir hier nur 5xx als kritischen Error
            self._track_request(res.status_code >= 500)
            self.log(f"📡 [POST] {endpoint} -> HTTP {res.status_code}")
            if res.status_code != 200:
                self.log(f"   ↳ Response: {res.text.strip()}")
            return res
        except Exception as e:
            self._track_request(is_error=True)
            self.log(f"📡 [POST] {endpoint} -> FEHLER: {e}")
            return None

    def send_gps(self, team):
        # Für den "GPS Ausfall" Edge-Case prüfen wir, ob das Team pausiert ist
        if time.time() < team.get("pause_gps_until", 0):
            return
            
        params = {
            "id": team["slug"],
            "lat": round(team["lat"], 6),
            "lon": round(team["lon"], 6),
            "timestamp": int(time.time())
        }
        try:
            res = requests.post(OSMAND_URL, params=params, timeout=2)
            if res.status_code == 200:
                with self.stats_lock:
                    self.stats["gps_sent"] += 1
                self.log(f"🛰️ [GPS] {team['name']} gesendet -> HTTP 200")
            else:
                self._track_request(is_error=True)
                self.log(f"🛰️ [GPS] {team['name']} gesendet -> HTTP {res.status_code}")
        except Exception as e:
            self._track_request(is_error=True)
            self.log(f"🛰️ [GPS] {team['name']} FEHLER -> {e}")

    def setup_game(self):
        self.log(f"🔧 [ADMIN] Verbinde mit Server (Admin-Passwort: {ADMIN_PASS})...")
        
        # --- EDGE CASE 1: ADMIN RESET ---
        self.log("🧨 [ADMIN] Setze das Spiel komplett zurück...")
        reset_res = self.api_post(f"/adminconsole-{ADMIN_PASS}/reset")
        if not reset_res or not reset_res.json().get('success'):
            self.log("❌ Fehler beim Reset! Falsches Admin-Passwort?")
            return False
            
        # Haltestellen abrufen
        res = self.api_get("/api/all_stops")
        if res and res.status_code == 200:
            for stop in res.json():
                self.all_stops[stop['name']] = stop

        # Einstellungen auslesen
        settings_html_res = self.api_get(f"/adminconsole-{ADMIN_PASS}/settings")
        html = settings_html_res.text
        
        try:
            target_stops = int(re.search(r'id="target_stops" value="(\d+)"', html).group(1))
            max_history = int(re.search(r'id="max_history" value="(\d+)"', html).group(1))
        except:
            target_stops = 12
            max_history = 30
            
        teams_list = []
        needs_roles = False
        
        team_rows = re.findall(r'<div class="team-row">(.*?)</div>', html, re.DOTALL)
        for row in team_rows:
            t_id = re.search(r'class="t-id" value="([^"]+)"', row).group(1)
            t_name = re.search(r'class="t-new-name" value="([^"]+)"', row).group(1)
            
            role_match = re.search(r'<option value="([^"]+)"[^>]*selected', row)
            role = role_match.group(1) if role_match else "keine"
            
            pin = re.search(r'class="t-pin" value="([^"]+)"', row).group(1)
            active = 'checked' in re.search(r'class="t-active"([^>]*)>', row).group(1)
            
            if active and role == "keine":
                needs_roles = True
                
            teams_list.append({
                "team_id": t_id,
                "new_name": t_name,
                "role": role,
                "pin": pin,
                "active": active
            })

        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=GAME_DURATION_MINUTES)
        
        settings_payload = {
            "target_stops": target_stops,
            "max_history": max_history,
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "timezone": "Europe/Berlin",
            "enable_mid_game_swap": True,
            "shuffle_stops_on_swap": True,
            "auto_approve_stops": True,
            "stop_radius_meters": 150,
            "teams_list": teams_list
        }
        
        res = self.api_post(f"/adminconsole-{ADMIN_PASS}/settings", json_data=settings_payload)
        if not res or not res.json().get('success'):
            self.log("❌ Fehler beim Setzen der Einstellungen!")
            return False

        time.sleep(1) 

        # Rollen zuweisen falls nötig
        if needs_roles:
            self.log("🎲 [ADMIN] Weise Rollen zu...")
            self.api_post(f"/adminconsole-{ADMIN_PASS}/randomize_roles")
            time.sleep(1)
            
            html = self.api_get(f"/adminconsole-{ADMIN_PASS}/settings").text
            team_rows = re.findall(r'<div class="team-row">(.*?)</div>', html, re.DOTALL)
            for row in team_rows:
                t_id = re.search(r'class="t-id" value="([^"]+)"', row).group(1)
                role_match = re.search(r'<option value="([^"]+)"[^>]*selected', row)
                role = role_match.group(1) if role_match else "keine"
                for t in teams_list:
                    if t["team_id"] == t_id:
                        t["role"] = role

        for t in teams_list:
            if t["active"]:
                slug = generate_slug(t["new_name"], t["pin"])
                self.teams[t["team_id"]] = {
                    "id": t["team_id"],
                    "name": t["new_name"],
                    "slug": slug,
                    "role": t["role"],
                    "lat": PLATZ_DER_DEUTSCHEN_EINHEIT[0] + random.uniform(-0.0002, 0.0002),
                    "lon": PLATZ_DER_DEUTSCHEN_EINHEIT[1] + random.uniform(-0.0002, 0.0002),
                    "pause_gps_until": 0
                }
            
        self.log(f"✅ [ADMIN] Spiel für {GAME_DURATION_MINUTES} Minuten gestartet! Start am Platz der Deutschen Einheit.")
        return True

    def run_edge_cases(self):
        self.log("\n🧪 [TEST] Starte Edge-Case Belastungstests...")
        
        runners = [t for t in self.teams.values() if t["role"] == "läufer"]
        catchers = [t for t in self.teams.values() if t["role"] == "fänger"]
        
        if not runners or not catchers:
            self.log("   ↳ Nicht genug Teams für Edge-Cases (Benötige mind. 1 Läufer, 1 Fänger).")
            return
            
        runner_1 = runners[0]
        catcher_1 = catchers[0]

        # --- EDGE CASE 2: Role Violation (Läufer meldet Catch) ---
        self.log(f"   ↳ Test: Rollen-Verstoß ({runner_1['name']} versucht Catch zu melden)")
        res = self.api_post("/api/report_catch", json_data={
            "reporter_slug": runner_1["slug"],
            "hunter": runner_1["id"],
            "prey": catcher_1["id"]
        })
        if res and res.status_code == 400:
            self.log("     ✅ Richtig abgewehrt (HTTP 400).")
            self.stats["edge_cases_tested"] += 1
        else:
            self.log(f"     ❌ Fehler: App hat den Request nicht mit HTTP 400 abgelehnt! (Status: {res.status_code if res else 'None'})")

        # --- EDGE CASE 3: Self-Catch ---
        self.log(f"   ↳ Test: Eigen-Catch ({catcher_1['name']} versucht sich selbst zu fangen)")
        res = self.api_post("/api/report_catch", json_data={
            "reporter_slug": catcher_1["slug"],
            "hunter": catcher_1["id"],
            "prey": catcher_1["id"]
        })
        if res and res.status_code == 400:
            self.log("     ✅ Richtig abgewehrt (HTTP 400).")
            self.stats["edge_cases_tested"] += 1
        else:
            self.log(f"     ❌ Fehler: App hat Eigen-Catch nicht mit HTTP 400 abgelehnt!")

        # --- EDGE CASE 4: Race Condition (Zwei Läufer wollen gleichzeitig dieselbe Haltestelle) ---
        if len(runners) >= 2:
            runner_2 = runners[1]
            stop = random.choice(list(self.all_stops.values()))
            
            self.log(f"   ↳ Test: Race Condition (Zwei Läufer feuern parallel auf '{stop['name']}')")
            
            # Beide an denselben Ort teleportieren für GPS-Auto-Approve
            runner_1["lat"], runner_1["lon"] = stop["lat"], stop["lon"]
            runner_2["lat"], runner_2["lon"] = stop["lat"], stop["lon"]
            self.send_gps(runner_1)
            self.send_gps(runner_2)
            time.sleep(1) # GPS im Backend verarbeiten lassen
            
            def req_stop(team):
                res = self.api_post("/api/request_stop", json_data={"slug": team["slug"], "stop": stop["name"]})
                self.log(f"       -> Antwort für {team['name']}: {res.status_code if res else 'None'}")
                
            t1 = threading.Thread(target=req_stop, args=(runner_1,))
            t2 = threading.Thread(target=req_stop, args=(runner_2,))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            self.log("     ✅ Parallele Anfragen verarbeitet. (App sollte nicht abstürzen).")
            self.stats["edge_cases_tested"] += 1

        # --- EDGE CASE 5: GPS Ausfall / Timeout ---
        self.log(f"   ↳ Test: GPS-Warnung ({runner_1['name']} stellt GPS für 2,5 Minuten ab)")
        # Blockiert das Senden in der send_gps Funktion
        runner_1["pause_gps_until"] = time.time() + 150
        self.stats["edge_cases_tested"] += 1
        
        self.log("🧪 [TEST] Edge-Cases abgeschlossen.\n")


    def delayed_stop_report(self, team, stop_name):
        delay = random.uniform(0, 10.0)
        time.sleep(delay)
        if not self.running:
            return
            
        self.log(f"✅ [LÄUFER] {team['name']} meldet Haltestelle '{stop_name}' (Verzögerung: {delay:.1f}s)")
        res = self.api_post("/api/request_stop", json_data={"slug": team["slug"], "stop": stop_name})
        if res and res.status_code == 200:
            with self.stats_lock:
                self.stats["stops_reached"] += 1

    def delayed_runner_catch_confirm(self, runner_slug, runner_name, hunter_id, prey_id):
        time.sleep(10.0)
        if not self.running:
            return
            
        self.log(f"🤝 [LÄUFER] {runner_name} bestätigt, gefangen worden zu sein (10s später)!")
        res = self.api_post("/api/report_catch", json_data={
            "reporter_slug": runner_slug,
            "hunter": hunter_id,
            "prey": prey_id
        })
        if res and res.status_code == 200:
            with self.stats_lock:
                self.stats["catches_confirmed"] += 1

    def runner_behavior(self, team):
        target_stop = None
        speed = 200  
        
        while self.running:
            loc_res = self.api_get("/api/team_locations", params={"slug": team['slug']})
            if loc_res and loc_res.status_code == 200:
                loc_data = loc_res.json()
                if loc_data.get("role") != "läufer":
                    team["role"] = loc_data.get("role")
                    if team["role"] == "fänger":
                        self.catcher_behavior(team)
                    return

            if not target_stop:
                stops_req = self.api_get("/api/stops", params={"slug": team['slug']})
                if stops_req and stops_req.status_code == 200:
                    stops = stops_req.json()
                    if stops:
                        target_stop = random.choice(stops)

            if target_stop:
                team["lat"], team["lon"] = move_towards(team["lat"], team["lon"], target_stop["lat"], target_stop["lon"], speed, UPDATE_INTERVAL)
                dist = haversine(team["lat"], team["lon"], target_stop["lat"], target_stop["lon"])
                
                if dist <= 50:
                    threading.Thread(target=self.delayed_stop_report, args=(team, target_stop["name"])).start()
                    target_stop = None
                    time.sleep(3)
            
            self.send_gps(team)
            time.sleep(UPDATE_INTERVAL)

    def catcher_behavior(self, team):
        self.log(f"⏳ [FÄNGER] {team['name']} wartet 7 Sekunden am Start...")
        time.sleep(7)
        speed = 220
        
        while self.running:
            loc_res = self.api_get("/api/team_locations", params={"slug": team['slug']})
            loc_data = {}
            if loc_res and loc_res.status_code == 200:
                loc_data = loc_res.json()
                if loc_data.get("role") != "fänger":
                    team["role"] = loc_data.get("role")
                    if team["role"] == "läufer":
                        self.runner_behavior(team)
                    return

            visible = loc_data.get("locations", [])
            runners = [r for r in visible if r["role"] == "läufer"]
            
            if runners:
                closest = None
                min_dist = float('inf')
                for r in runners:
                    d = haversine(team["lat"], team["lon"], r["lat"], r["lon"])
                    if d < min_dist:
                        min_dist = d
                        closest = r
                
                if closest:
                    team["lat"], team["lon"] = move_towards(team["lat"], team["lon"], closest["lat"], closest["lon"], speed, UPDATE_INTERVAL)
                    new_dist = haversine(team["lat"], team["lon"], closest["lat"], closest["lon"])
                    
                    if new_dist <= 25:
                        self.log(f"⚔️ [FÄNGER] {team['name']} initiiert Catch gegen {closest['name']} (Distanz: {int(new_dist)}m).")
                        res = self.api_post("/api/report_catch", json_data={
                            "reporter_slug": team["slug"],
                            "hunter": team["id"],
                            "prey": closest["id"]
                        })
                        
                        if res and res.status_code == 200:
                            with self.stats_lock:
                                self.stats["catches_initiated"] += 1
                                
                            runner_slug = self.teams[closest["id"]]["slug"]
                            threading.Thread(target=self.delayed_runner_catch_confirm, 
                                             args=(runner_slug, closest["name"], team["id"], closest["id"])).start()
                                             
                        time.sleep(10)

            self.send_gps(team)
            time.sleep(UPDATE_INTERVAL)

    def print_final_report(self):
        self.log("\n" + "="*50)
        self.log("📊 SIMULATIONS-ABSCHLUSSBERICHT")
        self.log("="*50)
        self.log(f"📡 API Aufrufe gesamt: {self.stats['api_calls']}")
        self.log(f"🛰️ GPS Updates gesendet: {self.stats['gps_sent']}")
        self.log(f"🚩 Haltestellen erreicht: {self.stats['stops_reached']}")
        self.log(f"⚔️ Catches initiiert (Fänger): {self.stats['catches_initiated']}")
        self.log(f"🤝 Catches bestätigt (Läufer): {self.stats['catches_confirmed']}")
        self.log(f"🧪 Edge Cases geprüft: {self.stats['edge_cases_tested']}")
        self.log("-" * 50)
        
        if self.stats["errors"] > 0:
            self.log(f"❌ KRITISCH: {self.stats['errors']} API-Fehler (Server HTTP 500) sind aufgetreten! Schau in die Logs der App.")
        else:
            self.log("✅ Keine Server-Abstürze oder interne Fehler (HTTP 500) während der Simulation.")

        res = self.api_get("/api/ticker_data")
        if res and res.status_code == 200:
            data = res.json()
            leaderboard = data.get("leaderboard", [])
            self.log("\n🏆 FINALES LEADERBOARD:")
            total_score = 0
            for pos, t in enumerate(leaderboard, 1):
                self.log(f"  {pos}. {t['display_name']} ({t['role']}) - {t['score']} Punkte")
                total_score += t['score']
                
            if total_score == 0 and self.stats['stops_reached'] > 0:
                self.log("\n⚠️ WARNUNG: Es wurden Haltestellen gemeldet, aber niemand hat Punkte bekommen! Bitte Scoring-Logik in app.py prüfen.")
            elif total_score > 0:
                self.log("\n✅ Punktevergabe (Scoring) funktioniert.")
        else:
            self.log("\n❌ Fehler beim Abrufen des Leaderboards!")
        self.log("="*50 + "\n")

    def start(self):
        if not self.setup_game():
            return
            
        # Erst die Edge-Cases durchspielen
        self.run_edge_cases()
            
        # Dann den Spiel-Loop starten
        threads = []
        for t_id, team in self.teams.items():
            if team["role"] == "keine":
                continue
                
            if team["role"] == "läufer":
                th = threading.Thread(target=self.runner_behavior, args=(team,))
            else:
                th = threading.Thread(target=self.catcher_behavior, args=(team,))
            th.start()
            threads.append(th)

        self.log(f"⏳ Simulation läuft für {GAME_DURATION_MINUTES} Minuten...")
        time.sleep(GAME_DURATION_MINUTES * 60)
        
        self.log("🛑 Beende Simulation, warte auf asynchrone Bestätigungen (bis zu 12s)...")
        self.running = False
        time.sleep(12) 
        
        for th in threads:
            th.join()
            
        self.print_final_report()

if __name__ == '__main__':
    sim = CatchSimulator()
    sim.start()