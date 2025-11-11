import paho.mqtt.client as mqtt
import json
import mysql.connector
from datetime import datetime
import time

# --- Konfigurasi Database ---
try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="iot_db"
    )
    cursor = db.cursor()
    print("✅ (SUBS) Berhasil terhubung ke database MySQL 'iot_db'")
except mysql.connector.Error as err:
    print(f"❌ (SUBS) Gagal terhubung ke MySQL: {err}")
    exit(1)

# --- Logika MQTT ---
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print("✅ (SUBS) Terhubung ke broker MQTT!")
        client.subscribe("iot/sensor")
        print("Subscribed ke topik 'iot/sensor'")
    else:
        print(f"❌ (SUBS) Gagal terhubung ke broker, kode: {reason_code}")


def on_message(client, userdata, msg):
    # 1. Menampilkan pesan "Receive data"
    print(f"\n--- Menerima data dari topik: {msg.topic} ---")
    payload_str = msg.payload.decode()
    
    try:
        data = json.loads(payload_str)
        
        suhu = float(data.get("suhu", 0))
        humidity = float(data.get("humidity", 0))
        lux = float(data.get("lux", 0))
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sql = "INSERT INTO data_sensor (suhu, humidity, lux, timestamp) VALUES (%s, %s, %s, %s)"
        val = (suhu, humidity, lux, ts)
        
        cursor.execute(sql, val)
        db.commit()

        # 2. Menampilkan "Data dimasukkan"
        print("✅ Data dimasukkan ke dalam DB")
        # 3. Menampilkan "Data apa saja"
        print(f"   Suhu     : {suhu} °C")
        print(f"   Humidity : {humidity} %")
        print(f"   Lux      : {lux}")
        print(f"   Timestamp: {ts}")
        print("------------------------------------------")

    except json.JSONDecodeError:
        print(f"❌ Error: Payload bukan JSON valid: {payload_str}")
        print("------------------------------------------")
    except Exception as e:
        print(f"❌ (SUBS) Error parsing atau menyimpan data: {e}")
        print("------------------------------------------")



# --- Inisialisasi MQTT Client ---
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "RAEL_UTS")
client.on_connect = on_connect
client.on_message = on_message

print("(SUBS) Menghubungkan ke broker...")
client.connect("broker.hivemq.com", 1883, 60) 

print("(SUBS) Menjalankan listener MQTT...")
client.loop_forever()