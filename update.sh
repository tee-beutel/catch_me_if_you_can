#!/bin/bash

# Bei Fehlern sofort abbrechen
set -e

echo "=== Starte Update des Catch-Me-Servers ==="

# 1. Projektverzeichnis ermitteln
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"
echo "Projektverzeichnis: $PROJECT_DIR"

# 2. Catch-Me-Dienst stoppen
echo ""
echo "Stoppe catchme.service..."
sudo systemctl stop catchme.service

# 3. Neuesten Code aus dem Git-Repository abrufen
echo ""
echo "Führe git pull aus..."
git pull

# 4. Virtual Environment aktivieren & Abhängigkeiten aktualisieren
echo ""
echo "Aktualisiere Python-Abhängigkeiten in der virtuellen Umgebung..."
if [ -d "$PROJECT_DIR/venv" ]; then
    VENV_PATH="$PROJECT_DIR/venv"
elif [ -d "$PROJECT_DIR/.venv" ]; then
    VENV_PATH="$PROJECT_DIR/.venv"
else
    echo "Virtuelle Umgebung nicht gefunden, erstelle neues venv..."
    python3 -m venv "$PROJECT_DIR/venv"
    VENV_PATH="$PROJECT_DIR/venv"
fi

source "$VENV_PATH/bin/activate"
pip install --upgrade pip
if [ -f "$PROJECT_DIR/requirements.txt" ]; then
    pip install -r "$PROJECT_DIR/requirements.txt"
fi
deactivate

# 5. Systemd neu laden und Dienst starten
echo ""
echo "Lade Systemd-Konfiguration neu und starte den Dienst..."
sudo systemctl daemon-reload
sudo systemctl start catchme.service

# 6. Servicestatus ausgeben
echo ""
echo "=== Update erfolgreich abgeschlossen! ==="
sudo systemctl status catchme.service --no-pager
