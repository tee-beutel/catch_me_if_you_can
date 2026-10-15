#!/bin/bash

# Bei Fehlern sofort abbrechen
set -e

echo "=== Starte Bereitstellung des Catch-Me-Servers (Nativ via Java, ohne Docker) ==="

# 1. Projektverzeichnis ermitteln (alles bleibt strikt in diesem Ordner!)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRACCAR_DIR="$PROJECT_DIR/traccar_runtime"
TRACCAR_VERSION="5.12"
echo "Aktuelles Projektverzeichnis: $PROJECT_DIR"

# 2. System aktualisieren & Basis-Pakete installieren (Python, Venv, Java, Unzip)
echo "Aktualisiere Paketlisten und installiere Abhängigkeiten (inkl. Java)..."
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git curl unzip default-jre-headless psmisc

# 3. Alte Docker-Container stoppen, falls vorhanden (verhindert Port-Blockaden auf 8082/5055)
if command -v docker >/dev/null 2>&1; then
    if sudo docker ps -a --format '{{.Names}}' | grep -q "^traccar-server$"; then
        echo "Stoppe und entferne alten Docker-Container 'traccar-server'..."
        sudo docker rm -f traccar-server || true
    fi
fi

# 4. Portables Traccar (traccar-other) lokal in $PROJECT_DIR/traccar_runtime herunterladen & entpacken
if [ ! -f "$TRACCAR_DIR/tracker-server.jar" ]; then
    echo "Lade Traccar $TRACCAR_VERSION direkt in den Projektordner herunter..."
    mkdir -p "$TRACCAR_DIR"
    TMP_ZIP="$TRACCAR_DIR/traccar-other.zip"
    DOWNLOAD_URL="https://github.com/traccar/traccar/releases/download/v${TRACCAR_VERSION}/traccar-other-${TRACCAR_VERSION}.zip"

    curl -L "$DOWNLOAD_URL" -o "$TMP_ZIP"
    unzip -o "$TMP_ZIP" -d "$TRACCAR_DIR"
    rm -f "$TMP_ZIP"
    mkdir -p "$TRACCAR_DIR/data" "$TRACCAR_DIR/logs"
    echo "Traccar wurde erfolgreich lokal in $TRACCAR_DIR bereitgestellt."
else
    echo "Traccar ist bereits in $TRACCAR_DIR vorhanden."
fi

# Sicherstellen, dass Dateirechte dem aktuellen Nutzer gehören
sudo chown -R "$(whoami)":"$(whoami)" "$TRACCAR_DIR"

# 5. Python Virtual Environment im Projektordner einrichten
echo "Richte Python Virtual Environment ein..."
python3 -m venv "$PROJECT_DIR/venv"
source "$PROJECT_DIR/venv/bin/activate"
pip install --upgrade pip

if [ -f "$PROJECT_DIR/requirements.txt" ]; then
    pip install -r "$PROJECT_DIR/requirements.txt"
else
    pip install Flask requests filelock pytz python-dotenv
fi
deactivate

# 6. Systemd-Dienst für Flask (app.py startet und überwacht den lokalen Java-Traccar-Server selbstständig)
SERVICE_FILE="/etc/systemd/system/catchme.service"
CURRENT_USER="$(whoami)"
echo "Erstelle Systemd-Dienst für den Catch-Me-Server (User: $CURRENT_USER)..."

sudo bash -c "cat <<EOF > '$SERVICE_FILE'
[Unit]
Description=Catch Me Flask & Local Java Traccar Server
After=network.target

[Service]
User=$CURRENT_USER
WorkingDirectory=$PROJECT_DIR
ExecStart=$PROJECT_DIR/venv/bin/python $PROJECT_DIR/app.py
Restart=always
RestartSec=5
KillMode=control-group

[Install]
WantedBy=multi-user.target
EOF"

# Systemd aktualisieren und Dienst starten
sudo systemctl daemon-reload
sudo systemctl enable catchme.service
sudo systemctl restart catchme.service

echo "=== Einrichtung erfolgreich abgeschlossen! ==="
echo "Alle Dateien liegen isoliert in: $PROJECT_DIR"
echo "Flask und der lokale Java-Traccar-Server laufen im Hintergrund."
sudo systemctl status catchme.service --no-pager