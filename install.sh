#!/bin/bash

# Bei Fehlern sofort abbrechen
set -e

echo "=== Starte Bereitstellung des Catch-Me-Servers (Nativ via Java, ohne Docker) ==="

# 1. Projektverzeichnis ermitteln (alles bleibt strikt in diesem Ordner!)
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRACCAR_DIR="$PROJECT_DIR/traccar_runtime"
TRACCAR_VERSION="5.12"
echo "Aktuelles Projektverzeichnis: $PROJECT_DIR"

# 2. Interaktive Abfragen für Zugangsdaten & .env Generierung
echo ""
echo "=== Konfiguration der Zugangsdaten (.env) ==="
ENV_FILE="$PROJECT_DIR/.env"

DEFAULT_ADMIN="admin123"
DEFAULT_TRACCAR_USER="admin"
DEFAULT_TRACCAR_PASS="admin"

if [ -f "$ENV_FILE" ]; then
    EXISTING_ADMIN=$(grep -E '^ADMIN_PASSWORD=' "$ENV_FILE" | head -n1 | cut -d '=' -f2- || true)
    EXISTING_TUSER=$(grep -E '^TRACCAR_API_USER=' "$ENV_FILE" | head -n1 | cut -d '=' -f2- || true)
    EXISTING_TPASS=$(grep -E '^TRACCAR_API_PASSWORD=' "$ENV_FILE" | head -n1 | cut -d '=' -f2- || true)
    [ -n "$EXISTING_ADMIN" ] && DEFAULT_ADMIN="$EXISTING_ADMIN"
    [ -n "$EXISTING_TUSER" ] && DEFAULT_TRACCAR_USER="$EXISTING_TUSER"
    [ -n "$EXISTING_TPASS" ] && DEFAULT_TRACCAR_PASS="$EXISTING_TPASS"
fi

read -p "Admin-Passwort für die Konsole (ADMIN_PASSWORD) [$DEFAULT_ADMIN]: " INPUT_ADMIN_PW
ADMIN_PASSWORD="${INPUT_ADMIN_PW:-$DEFAULT_ADMIN}"

read -p "Traccar API-Benutzername (TRACCAR_API_USER) [$DEFAULT_TRACCAR_USER]: " INPUT_TRACCAR_USER
TRACCAR_API_USER="${INPUT_TRACCAR_USER:-$DEFAULT_TRACCAR_USER}"

read -p "Traccar API-Passwort (TRACCAR_API_PASSWORD) [$DEFAULT_TRACCAR_PASS]: " INPUT_TRACCAR_PASS
TRACCAR_API_PASSWORD="${INPUT_TRACCAR_PASS:-$DEFAULT_TRACCAR_PASS}"

echo "Erstelle .env-Datei..."
cat <<EOF > "$ENV_FILE"
ADMIN_PASSWORD=$ADMIN_PASSWORD
TRACCAR_API_USER=$TRACCAR_API_USER
TRACCAR_API_PASSWORD=$TRACCAR_API_PASSWORD
EOF
chmod 600 "$ENV_FILE"
echo "✅ .env-Datei erfolgreich erstellt."

# 3. System aktualisieren & Basis-Pakete installieren (Python, Venv, Java, Unzip)
echo ""
echo "Aktualisiere Paketlisten und installiere Abhängigkeiten (inkl. Java)..."
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git curl unzip default-jre-headless psmisc

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

# 7. Tailscale & Funneling
echo ""
echo "=== Tailscale & Funneling ==="
read -p "Soll Tailscale installiert und eingerichtet werden? (j/n): " INSTALL_TAILSCALE
if [[ "$INSTALL_TAILSCALE" =~ ^[jJyY] ]]; then
    if ! command -v tailscale >/dev/null 2>&1; then
        echo "Installiere Tailscale..."
        curl -fsSL https://tailscale.com/install.sh | sh
    else
        echo "Tailscale ist bereits installiert."
    fi

    echo "Tailscale Authentifizierung:"
    read -p "Bitte Tailscale Auth-Key eingeben (Enter drücken für interaktiven Web-Login via URL): " TS_AUTHKEY
    if [ -n "$TS_AUTHKEY" ]; then
        sudo tailscale up --authkey="$TS_AUTHKEY"
    else
        sudo tailscale up
    fi

    echo "Richte Tailscale Funnel ein..."
    # Flask (lokal 5000 -> öffentlich 443)
    sudo tailscale funnel --bg --https=443 --yes 5000 || echo "⚠️ Hinweis: Tailscale Funnel für Port 443 konnte nicht aktiviert werden. Bitte Funnel-Berechtigungen im Tailscale-Admin prüfen."
    # Traccar (lokal 8082 -> öffentlich 10000)
    sudo tailscale funnel --bg --https=10000 --yes 8082 || echo "⚠️ Hinweis: Tailscale Funnel für Port 10000 konnte nicht aktiviert werden. Bitte Funnel-Berechtigungen im Tailscale-Admin prüfen."

    echo "Aktueller Tailscale Funnel Status:"
    sudo tailscale funnel status || true
fi

echo "=== Einrichtung erfolgreich abgeschlossen! ==="
echo "Alle Dateien liegen isoliert in: $PROJECT_DIR"
echo "Flask und der lokale Java-Traccar-Server laufen im Hintergrund."
sudo systemctl status catchme.service --no-pager
