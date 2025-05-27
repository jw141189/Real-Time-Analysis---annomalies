import json
import threading
from kafka import KafkaConsumer
import pandas as pd
import smtplib
from email.mime.text import MIMEText
import folium
from flask import Flask, render_template
from datetime import datetime

# Ustawienia
SERVER = "broker:9092"
TOPIC = "sobota"
file_path = "data.json"
log_file = "log_anomalii.txt"

# Flask
app = Flask(__name__)

# Wczytanie danych referencyjnych
df = pd.read_json(file_path)
df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

# Lista anomalii dla mapy
anomaly_map_data = []

# 🔍 Najbliższa lokalizacja na podstawie współrzędnych
def find_closest_location(lat, lon):
    df["distance_score"] = abs(df["Latitude"] - lat) + abs(df["Longitude"] - lon)
    closest_row = df.loc[df["distance_score"].idxmin()]
    return closest_row["Name"], closest_row["District"]

# ✉️ Wysyłanie maila
def send_email_alert(subject, body):
    sender = "mietelskakarolina@gmail.com"
    recipient = "mietelskakarolina@gmail.com"
    password = "odnuqmrsdttguvqo"  # UWAGA: nie zostawiaj hasła na produkcji!

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())
        print("📧 Wysłano alert e-mail.")
    except Exception as e:
        print(f"Błąd wysyłki e-maila: {e}")

# 📝 Logowanie do pliku
def log_to_file(entry):
    with open(log_file, "a", encoding="utf-8") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {entry}\n")

# 🗺️ Aktualizacja mapy
def update_map():
    m = folium.Map(location=[52.0, 19.0], zoom_start=6)
    for lat, lon, desc, color in anomaly_map_data:
        folium.Marker([lat, lon], popup=desc, icon=folium.Icon(color=color)).add_to(m)
    m.save("mapa.html")

# 🧠 Konsumpcja wiadomości z Kafki
def consume():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[SERVER],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='anomaly_detection_group'
    )

    for message in consumer:
        data = message.value
        temp = data.get("temperature")
        hum = data.get("humidity")
        pres = data.get("pressure")
        if pres: pres = pres / 100
        lat = data.get("latitude")
        lon = data.get("longitude")
        sensor_id = data.get("sensor_id")

        if lat is not None and lon is not None:
            try:
                lat = float(lat)
                lon = float(lon)
            except ValueError:
                continue

            name, district = find_closest_location(lat, lon)

            temp_anomaly = temp is not None and (temp < -5 or temp > 12)
            hum_anomaly = hum is not None and (hum < 0 or hum > 100)
            pres_anomaly = pres is not None and (pres < 960 or pres > 1054)

            desc, color = None, None

            if (temp_anomaly and hum_anomaly) or (hum_anomaly and pres_anomaly) or (pres_anomaly and temp_anomaly):
                desc = (
                    f"⚠️ POTENCJALNA AWARIA CZUJNIKA\n"
                    f"Sensor ID: {sensor_id}\n"
                    f"Temperatura: {temp}°C\n"
                    f"Wilgotność: {hum}%\n"
                    f"Ciśnienie: {pres}hPa\n"
                    f"Lokalizacja: {name} ({district})"
                )
                color = "red"
                send_email_alert("⚠️ ALERT: Możliwa awaria czujnika", desc)
            elif temp_anomaly:
                desc = f"🌡️ Anomalia temperatury - ID: {sensor_id} - {temp}°C - {name} ({district})"
                color = "blue"
            elif hum_anomaly:
                desc = f"💧 Anomalia wilgotności - ID: {sensor_id} - {hum}% - {name} ({district})"
                color = "green"
            elif pres_anomaly:
                desc = f"💨 Anomalia ciśnienia - ID: {sensor_id} - {pres}hPa - {name} ({district})"
                color = "green"

            if desc:
                print(desc)
                log_to_file(desc)
                anomaly_map_data.append((lat, lon, desc, color))
                update_map()

# 🌍 Wyświetlanie mapy przez Flask
@app.route("/")
def mapa():
    return render_template("mapa.html")

# ▶️ Start systemu
if __name__ == "__main__":
    t = threading.Thread(target=consume, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5001)
