#!/usr/bin/env python3
import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

print("=== Catch-Me Update-Prozess gestartet ===")
print(f"Projektverzeichnis: {BASE_DIR}")
sys.stdout.flush()

# 1. Git pull
print("\n[1/2] Rufe neuesten Code via Git ab (git pull)...")
sys.stdout.flush()
try:
    git_res = subprocess.run(
        ["git", "pull"],
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=120
    )
    print(git_res.stdout)
    if git_res.returncode != 0:
        print(f"⚠️ git pull beendete mit Exit-Code {git_res.returncode}")
except Exception as e:
    print(f"❌ Fehler bei git pull: {e}")
sys.stdout.flush()

# 2. Python-Abhängigkeiten aktualisieren
req_file = os.path.join(BASE_DIR, "requirements.txt")
print("\n[2/2] Aktualisiere Python-Abhängigkeiten...")
sys.stdout.flush()
if os.path.exists(req_file):
    try:
        pip_res = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", req_file],
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=180
        )
        print(pip_res.stdout)
        if pip_res.returncode != 0:
            print(f"⚠️ pip install beendete mit Exit-Code {pip_res.returncode}")
    except Exception as e:
        print(f"❌ Fehler bei pip install: {e}")
else:
    print(f"ℹ️ Keine requirements.txt unter {req_file} gefunden.")
sys.stdout.flush()

print("\n=== Update-Prozess erfolgreich beendet! ===")
print("💡 Tipp: Starte die Dienste bei Bedarf über den Button '🔄 Flask & Traccar neustarten' neu.")
sys.stdout.flush()
