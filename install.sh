#!/bin/bash

# Bei Fehlern sofort abbrechen
set -e

echo "=== Starte Bereitstellung des Catch-Me-Servers auf Hetzner ==="

# 1. System aktualisieren & Basis-Pakete installieren (Python, Venv, Docker)
echo "Aktualisiere Paketlisten und installiere Abhängigkeiten..."
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git curl docker.io docker-compose-v2

# 2. Projektverzeichnis ermitteln
PROJECT_DIR="$(pwd)"
echo "Aktuelles Projektverzeichnis: $PROJECT_DIR"

# 3. Python Virtual Environment einrichten und Python-Pakete installieren
echo "Richte Python Virtual Environment ein..."
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip

if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    pip install Flask requests filelock pytz python-dotenv
fi
deactivate

# 4. Docker-Dienst aktivieren und starten
echo "Konfiguriere und starte Docker..."
sudo systemctl enable docker
sudo systemctl start docker

# 5. Traccar Container via Docker Compose starten
echo "Starte Traccar-Container..."
if [ -f "docker-compose.yml" ]; then
    sudo docker compose up -d
else
    echo "Fehler: docker-compose.yml wurde im Verzeichnis nicht gefunden!"
    exit 1
fi

# 6. Systemd-Dienst für Flask (app.py) für automatische Neustarts bei Fehlern erstellen
SERVICE_FILE="/etc/systemd/system/catchme.service"
echo "Erstelle Systemd-Dienst für den Flask-Server..."

sudo bash -cat <<EOF > "$SERVICE_FILE"
[Unit]
Description=Catch Me Flask Webserver
After=network.target docker.service

[Service]
User=root
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/venv/bin/python $PROJECT_DIR/app.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Systemd aktualisieren und Dienst starten
sudo systemctl daemon-reload
sudo systemctl enable catchme.service
sudo systemctl restart catchme.service

echo "=== Einrichtung erfolgreich abgeschlossen! ==="
echo "Der Flask-Server läuft im Hintergrund und wird bei Fehlern automatisch neu gestartet."
echo "Traccar läuft über Docker und ist konfiguriert[cite: 6, 7]."
sudo systemctl status catchme.service --no-pager