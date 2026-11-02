import asyncio
import aiohttp
import time
import random
import sys
import threading
import csv
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from collections import defaultdict

# --- KONFIGURATION ---
SERVER_URL = ""
ADMIN_PW = "20045658"
RUN_TIME_SECONDS = None  
TEAM_COUNT = 10

# --- GLOBALE METRIKEN & HISTORIE FÜR DIAGRAMME ---
metrics = {
    "total_requests": 0,
    "total_errors": 0,
    "start_time": 0.0,
    "endpoints": defaultdict(lambda: {"count": 0, "total_time": 0.0, "max_time": 0.0, "errors": 0, "last_time": 0.0}),
    "debug_data": {}
}

# Live-Historie für das GUI (wird auf 120 Einträge limitiert)
hist_time = []
hist_rps = []
hist_latency = []
hist_cpu = []
hist_ram = []
hist_net_dl = []
hist_net_ul = []

# Vollständige Historie für den CSV-Export am Ende
full_history = []

running = True
end_time = 0.0

async def fetch(session, method, url, name, json_data=None):
    """Führt einen HTTP-Request aus und misst die Zeit."""
    start_time = time.time()
    try:
        if method == 'GET':
            async with session.get(url) as resp:
                await resp.read()
                status = resp.status
        else:
            async with session.post(url, json=json_data) as resp:
                await resp.read()
                status = resp.status
                
        elapsed = time.time() - start_time
        
        metrics["total_requests"] += 1
        metrics["endpoints"][name]["count"] += 1
        metrics["endpoints"][name]["total_time"] += elapsed
        metrics["endpoints"][name]["last_time"] = elapsed
        metrics["endpoints"][name]["max_time"] = max(metrics["endpoints"][name]["max_time"], elapsed)
        
        if status >= 400:
            metrics["total_errors"] += 1
            metrics["endpoints"][name]["errors"] += 1
            
        return status
    except Exception:
        metrics["total_errors"] += 1
        metrics["endpoints"][name]["errors"] += 1
        return 0

# --- WORKER TASKS ---

async def team_gps_worker(session, base_url, slug):
    lat, lon = 52.268, 10.521 
    while running:
        lat += random.uniform(-0.0001, 0.0001)
        lon += random.uniform(-0.0001, 0.0001)
        await fetch(session, 'POST', f"{base_url}/api/traccar", "[API] Traccar GPS", json_data={
            "id": slug, "lat": lat, "lon": lon
        })
        await asyncio.sleep(3)

async def team_browser_worker(session, base_url, slug):
    while running:
        if random.random() < 0.15:
            await fetch(session, 'GET', f"{base_url}/team/{slug}", "[HTML] Team Ansicht")
            await fetch(session, 'GET', f"{base_url}/rules.html", "[HTML] Rules")
        
        for _ in range(random.randint(2, 4)):
            if not running: break
            await fetch(session, 'GET', f"{base_url}/api/team_locations?slug={slug}", "[API] Team Locations")
            await fetch(session, 'GET', f"{base_url}/api/ticker_data?slug={slug}", "[API] Team Ticker")
            await asyncio.sleep(random.uniform(3, 6))

async def admin_browser_worker(session, base_url, admin_pw):
    while running:
        if random.random() < 0.15:
            await fetch(session, 'GET', f"{base_url}/adminconsole-{admin_pw}", "[HTML] Admin Dashboard")
            await fetch(session, 'GET', f"{base_url}/adminconsole-{admin_pw}/replay", "[HTML] Replay Dashboard")

        await fetch(session, 'GET', f"{base_url}/api/admin_data/{admin_pw}", "[API] Admin Data")
        await fetch(session, 'GET', f"{base_url}/api/replay_data?admin=1", "[API] Replay Data")
        await asyncio.sleep(random.uniform(4, 7))

async def debug_fetcher_worker(session, base_url, admin_pw):
    """Fragt die Server-Leistung alle 120 Sekunden ab."""
    while running:
        try:
            async with session.get(f"{base_url}/api/traccar_debug/{admin_pw}/status") as resp:
                if resp.status == 200:
                    metrics["debug_data"] = await resp.json()
        except Exception:
            pass
        await asyncio.sleep(120)

async def metrics_aggregator():
    """Sammelt jede Sekunde die Datenpunkte für die Diagramme und Logs."""
    last_req_count = 0
    while running:
        await asyncio.sleep(1)
        current_reqs = metrics["total_requests"]
        rps = current_reqs - last_req_count
        last_req_count = current_reqs
        
        # Sichert Kopie der Endpunkte gegen Iterationsfehler im Threading
        eps = list(metrics["endpoints"].values())
        total_count = sum(d["count"] for d in eps)
        total_time = sum(d["total_time"] for d in eps)
        avg_latency = (total_time / total_count) if total_count > 0 else 0
        
        sys_info = metrics.get("debug_data", {}).get("sys_info", {})
        net_io = sys_info.get("net_io", {})
        
        t = time.time() - metrics["start_time"]
        cpu_val = sys_info.get("cpu_percent", 0.0)
        ram_val = sys_info.get("ram_percent", 0.0)
        dl_val = net_io.get("sys_recv_mb", 0.0)
        ul_val = net_io.get("sys_sent_mb", 0.0)
        
        # Für das Live-Diagramm
        hist_time.append(t)
        hist_rps.append(rps)
        hist_latency.append(avg_latency * 1000)
        hist_cpu.append(cpu_val)
        hist_ram.append(ram_val)
        hist_net_dl.append(dl_val)
        hist_net_ul.append(ul_val)
        
        # Für den permanenten Log-Export
        full_history.append({
            "time_s": round(t, 1),
            "rps": rps,
            "avg_latency_ms": round(avg_latency * 1000, 2),
            "cpu_percent": cpu_val,
            "ram_percent": ram_val,
            "dl_mb": dl_val,
            "ul_mb": ul_val
        })
        
        if len(hist_time) > 120:
            hist_time.pop(0)
            hist_rps.pop(0)
            hist_latency.pop(0)
            hist_cpu.pop(0)
            hist_ram.pop(0)
            hist_net_dl.pop(0)
            hist_net_ul.pop(0)

# --- SETUP TEAMS ---

async def setup_teams(session, base_url, admin_pw, team_count):
    teams_list = [{"team_id": f"Team {i}", "new_name": f"Stress Team {i}", "role": "fänger" if i % 2 == 0 else "läufer", "pin": "0000", "active": True} for i in range(1, team_count + 1)]
    await session.post(f"{base_url}/adminconsole-{admin_pw}/settings", json={"teams_list": teams_list})
    
    team_slugs = []
    for i in range(1, team_count + 1):
        async with session.post(f"{base_url}/api/login_team", json={"team_id": f"Team {i}", "pin": "0000"}) as resp:
            data = await resp.json()
            if data.get("success"):
                team_slugs.append(data["slug"])
    return team_slugs

# --- ASYNC MAIN LOOP ---

async def run_stress_test(url, admin_pw, seconds, teams):
    global running, end_time
    metrics["start_time"] = time.time()
    end_time = time.time() + seconds

    async with aiohttp.ClientSession() as session:
        print("[Hintergrund] Initialisiere Stresstest und lege Teams an...")
        slugs = await setup_teams(session, url, admin_pw, teams)
        print(f"[Hintergrund] Erfolgreich {len(slugs)} Teams vorbereitet. Starte Traffic...")
        
        tasks = []
        for slug in slugs:
            tasks.append(asyncio.create_task(team_gps_worker(session, url, slug)))
            tasks.append(asyncio.create_task(team_browser_worker(session, url, slug)))
            tasks.append(asyncio.create_task(team_browser_worker(session, url, slug)))
            
        tasks.append(asyncio.create_task(admin_browser_worker(session, url, admin_pw)))
        tasks.append(asyncio.create_task(debug_fetcher_worker(session, url, admin_pw)))
        tasks.append(asyncio.create_task(metrics_aggregator()))

        while time.time() < end_time and running:
            await asyncio.sleep(0.5)
            
        running = False
        await asyncio.gather(*tasks, return_exceptions=True)
        print("[Hintergrund] Stresstest beendet.")

def start_background_loop(url, admin_pw, seconds, teams):
    asyncio.run(run_stress_test(url, admin_pw, seconds, teams))

# --- TKINTER GUI & MATPLOTLIB ---

class StressDashboard(tk.Tk):
    def __init__(self, url, admin_pw, seconds, teams):
        super().__init__()
        self.title("Catch Traccar Stresstest Dashboard")
        self.geometry("1100x800")
        self.summary_shown = False
        
        self.info_frame = ttk.Frame(self)
        self.info_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)
        
        self.lbl_status = ttk.Label(self.info_frame, text="Starte...", font=("Helvetica", 14, "bold"))
        self.lbl_status.pack(side=tk.LEFT)
        
        self.lbl_temp = ttk.Label(self.info_frame, text="Temperatur: N/A", font=("Helvetica", 12))
        self.lbl_temp.pack(side=tk.RIGHT)
        
        self.lbl_errors = ttk.Label(self.info_frame, text="Fehler: 0", font=("Helvetica", 12), foreground="red")
        self.lbl_errors.pack(side=tk.RIGHT, padx=20)
        
        self.fig, self.axs = plt.subplots(2, 2, figsize=(10, 6))
        self.fig.tight_layout(pad=4.0)
        
        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)
        
        self.line_rps, = self.axs[0, 0].plot([], [], 'b-', label='RPS')
        self.axs[0, 0].set_title('Requests per Second (RPS)')
        
        self.line_lat, = self.axs[0, 1].plot([], [], 'g-', label='Ø Latenz')
        self.axs[0, 1].set_title('Ø Latenz aller Anfragen (ms)')
        
        self.line_cpu, = self.axs[1, 0].plot([], [], 'r-', label='CPU %')
        self.line_ram, = self.axs[1, 0].plot([], [], 'm-', label='RAM %')
        self.axs[1, 0].set_title('Server Auslastung (CPU & RAM)')
        self.axs[1, 0].legend()
        
        self.line_dl, = self.axs[1, 1].plot([], [], 'c-', label='Download (MB)')
        self.line_ul, = self.axs[1, 1].plot([], [], 'y-', label='Upload (MB)')
        self.axs[1, 1].set_title('Server Netzwerk Traffic')
        self.axs[1, 1].legend()

        self.update_gui()
        
        self.thread = threading.Thread(target=start_background_loop, args=(url, admin_pw, seconds, teams), daemon=True)
        self.thread.start()
        
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def on_closing(self):
        global running
        running = False
        self.destroy()
        sys.exit(0)

    def show_summary(self):
        """Öffnet ein Fenster mit der Abschlusstabelle, Flaschenhals-Analyse und CSV Button."""
        win = tk.Toplevel(self)
        win.title("Stresstest Auswertung & Flaschenhals-Analyse")
        win.geometry("1000x550")  # Breiter gegen Überlappung

        # Bereite Daten für die Tabelle vor und sortiere nach Durchschnittszeit
        ep_data = []
        for ep, d in metrics["endpoints"].items():
            avg = d["total_time"] / d["count"] if d["count"] > 0 else 0
            ep_data.append((ep, d["count"], d["errors"], avg, d.get("max_time", 0.0)))
        ep_data.sort(key=lambda x: x[3], reverse=True)

        # Flaschenhals-Metriken berechnen
        max_cpu = max(hist_cpu) if hist_cpu else 0
        max_ram = max(hist_ram) if hist_ram else 0
        max_dl = max(hist_net_dl) if hist_net_dl else 0
        worst_ep = ep_data[0][0] if ep_data else "Keine Daten"
        worst_lat = ep_data[0][3] if ep_data else 0

        summary_text = (
            f"🔥 FLASCHENHALS-ANALYSE:\n"
            f"• CPU-Spitze: {max_cpu:.1f}% | RAM-Spitze: {max_ram:.1f}%\n"
            f"• Netzwerk-Spitze: {max_dl:.1f} MB (Download)\n"
            f"• Stärkster Rückstau: '{worst_ep}' (Ø Latenz: {worst_lat:.2f}s)\n"
            f"• HTTP-Fehler: {metrics['total_errors']} (Hohe Fehler deuten meist auf blockierte SQLite-Datenbank hin)"
        )

        lbl_bottleneck = ttk.Label(win, text=summary_text, font=("Helvetica", 11, "bold"), foreground="#b71c1c",
                                   justify=tk.LEFT)
        lbl_bottleneck.pack(pady=10, padx=20, anchor="w")

        cols = ('Endpoint', 'Aufrufe', 'Fehler', 'Ø Latenz (s)', 'Max Latenz (s)')
        tree = ttk.Treeview(win, columns=cols, show='headings')
        for col in cols:
            tree.heading(col, text=col)

        # Breitere Spalte für den Endpunkt-Namen
        tree.column('Endpoint', width=300)
        tree.column('Aufrufe', width=100, anchor=tk.E)
        tree.column('Fehler', width=100, anchor=tk.E)
        tree.column('Ø Latenz (s)', width=100, anchor=tk.E)
        tree.column('Max Latenz (s)', width=100, anchor=tk.E)

        for item in ep_data:
            tree.insert("", "end", values=(item[0], item[1], item[2], f"{item[3]:.3f}", f"{item[4]:.3f}"))

        tree.pack(fill=tk.BOTH, expand=True, padx=20, pady=5)

        btn_export = ttk.Button(win, text="Log & Report herunterladen (CSV)", command=self.export_csv)
        btn_export.pack(pady=15)

    def export_csv(self):
        """Exportiert die Endpunktdaten und den zeitlichen Verlauf in eine CSV-Datei."""
        filepath = filedialog.asksaveasfilename(
            defaultextension=".csv", 
            initialfile="stresstest_report.csv",
            filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
        )
        
        if not filepath:
            return
            
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                
                # Sektion 1: Endpunkte
                writer.writerow(["--- FLASCHENHALS ZUSAMMENFASSUNG ---"])
                writer.writerow(['Endpoint', 'Aufrufe', 'Fehler', 'Avg Zeit (s)', 'Max Zeit (s)'])
                
                ep_data = []
                for ep, d in list(metrics["endpoints"].items()):
                    avg = d["total_time"] / d["count"] if d["count"] > 0 else 0
                    ep_data.append((ep, d["count"], d["errors"], avg, d.get("max_time", 0.0)))
                ep_data.sort(key=lambda x: x[3], reverse=True)
                
                for item in ep_data:
                    writer.writerow([item[0], item[1], item[2], f"{item[3]:.4f}".replace('.', ','), f"{item[4]:.4f}".replace('.', ',')])
                
                writer.writerow([])
                
                # Sektion 2: Zeitverlauf für Excel-Graphen
                writer.writerow(["--- ZEITLICHER VERLAUF ---"])
                writer.writerow(["Sekunde", "RPS", "Ø Latenz (ms)", "CPU (%)", "RAM (%)", "Download (MB)", "Upload (MB)"])
                
                for row in full_history:
                    writer.writerow([
                        str(row["time_s"]).replace('.', ','),
                        row["rps"], 
                        str(row["avg_latency_ms"]).replace('.', ','), 
                        str(row["cpu_percent"]).replace('.', ','), 
                        str(row["ram_percent"]).replace('.', ','), 
                        str(row["dl_mb"]).replace('.', ','), 
                        str(row["ul_mb"]).replace('.', ',')
                    ])
                    
            messagebox.showinfo("Export Erfolgreich", f"Der Report wurde erfolgreich unter\n{filepath}\ngespeichert.")
        except Exception as e:
            messagebox.showerror("Fehler beim Export", f"Die Datei konnte nicht gespeichert werden:\n{e}")

    def update_gui(self):
        if not running:
            if not self.summary_shown:
                self.lbl_status.config(text="Stresstest abgeschlossen!")
                self.show_summary()
                self.summary_shown = True
            return
            
        time_left = max(0, int(end_time - time.time()))
        self.lbl_status.config(text=f"Stresstest läuft | Verbleibend: {time_left}s | Gesamtanfragen: {metrics['total_requests']}")
        self.lbl_errors.config(text=f"Fehler gesamt: {metrics['total_errors']}")
        
        temps = metrics.get("debug_data", {}).get("sys_info", {}).get("temperatures", [])
        if temps:
            t_str = " | ".join([f"{t.get('label', 'Core')}: {t.get('current', 0)}°C" for t in temps[:2]])
            self.lbl_temp.config(text=f"Temperatur: {t_str}")
            
        if hist_time:
            self.line_rps.set_data(hist_time, hist_rps)
            self.axs[0, 0].relim()
            self.axs[0, 0].autoscale_view()
            
            self.line_lat.set_data(hist_time, hist_latency)
            self.axs[0, 1].relim()
            self.axs[0, 1].autoscale_view()
            
            self.line_cpu.set_data(hist_time, hist_cpu)
            self.line_ram.set_data(hist_time, hist_ram)
            self.axs[1, 0].relim()
            self.axs[1, 0].autoscale_view(scaley=False)
            self.axs[1, 0].set_ylim(0, 105)
            
            self.line_dl.set_data(hist_time, hist_net_dl)
            self.line_ul.set_data(hist_time, hist_net_ul)
            self.axs[1, 1].relim()
            self.axs[1, 1].autoscale_view()
            
            self.canvas.draw()
            
        self.after(1000, self.update_gui)

# --- STARTPUNKT ---

if __name__ == "__main__":
    print("\n--- STRESSTEST SETUP ---")
    url = SERVER_URL or input("Server URL (z.B. http://127.0.0.1:5000): ").strip()
    admin_pw = ADMIN_PW or input("Admin Passwort: ").strip()
    seconds_str = str(RUN_TIME_SECONDS) if RUN_TIME_SECONDS else input("Laufzeit in Sekunden: ").strip()
    seconds = int(seconds_str)
    teams_str = str(TEAM_COUNT) if TEAM_COUNT else input("Anzahl Teams: ").strip()
    teams = int(teams_str)

    url = url.rstrip('/')

    print("\n🚀 Starte das grafische Dashboard...")
    app = StressDashboard(url, admin_pw, seconds, teams)
    app.mainloop()