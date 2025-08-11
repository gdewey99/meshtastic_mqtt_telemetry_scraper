# Meshtastic MQTT Telemetry Scraper

This script subscribes to the public Meshtastic MQTT broker, logs telemetry to CSV/JSONL, and shows a live view with:
- Colored environment values (temp, RH, pressure, IAQ, wind, lux, radiation)
- Trend arrows (↑ green / ↓ red / → yellow) for environment and RF (RSSI/SNR)
- Derived live-only fields: relative humidity, barometric pressure
- Daily log rotation
- Optional per-node split (one file per node)

CSV/JSONL outputs always have clean values — colors/arrows are live view only.

---

## Installation

Clone the repo:
```bash
git clone https://github.com/gdewey99/meshtastic_mqtt_telemetry_scraper.git
cd meshtastic_mqtt_telemetry_scraper

Install dependencies (choose one):

Method A — System Python (Debian/Ubuntu/Raspberry Pi OS):

bash
Copy
Edit
sudo apt update
sudo apt install -y python3 python3-pip
pip3 install --break-system-packages -r requirements.txt
Method B — Virtual environment (recommended):

bash
Copy
Edit
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

Usage
Common flags:

lua
Copy
Edit
--live                  Live terminal view
--live-color env        Color environment values by type
--live-trend            Add colored trend arrows for env + RSSI/SNR
--nodes <id ...>        Only capture these nodes (!hex or decimal)
--split-by-node         Write separate CSV/JSONL per node
--keep-combined         Keep combined file along with per-node files
Example:

bash
Copy
Edit
./meshtastic_stream_to_csv.py --live --live-color env --live-trend
Split per node and keep combined:

bash
Copy
Edit
./meshtastic_stream_to_csv.py --nodes !a2eb420c !db77dcd8 \
  --live --live-trend --split-by-node --keep-combined
Dependencies
Python 3.9+

paho-mqtt Python package

On Debian-based systems:

bash
Copy
Edit
sudo apt install python3 python3-pip
pip3 install --break-system-packages paho-mqtt
MQTT Defaults
Host: mqtt.meshtastic.org

Port: 1883

User: meshdev

Password: large4cats

Topic: msh/US/#

Raspberry Pi Tips
Using a virtual environment avoids conflicts with system packages:

bash
Copy
Edit
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
To run at boot, create a systemd service file pointing to your script.


