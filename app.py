from flask import Flask, jsonify, render_template
import mysql.connector
from datetime import datetime

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False 

DB_CONFIG = {
    'host': "localhost",
    'user': "root",
    'password': "",
    'database': "iot_db"
}

@app.route("/")
def dashboard():
    return render_template('index.html')

@app.route("/api/data", methods=["GET"])
def get_data():
    db = None
    cursor = None
    
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor(dictionary=True) 
        
        # 1. Ambil statistik dasar 
        cursor.execute("""
            SELECT 
                MAX(suhu) as suhumax, 
                MIN(suhu) as suhumin, 
                AVG(suhu) as suhurata
            FROM data_sensor
        """)
        stats = cursor.fetchone()

        if not stats or stats['suhumax'] is None:
            if cursor: cursor.close()
            if db: db.close()
            return jsonify({"message": "Belum ada data sensor"}), 200

        cursor.execute("SELECT suhu, humidity, lux, timestamp FROM data_sensor ORDER BY timestamp DESC LIMIT 1")
        data_terbaru = cursor.fetchone()
        # ------------------------------------

        # 'nilai_suhu_max_humid_max' (
        cursor.execute("""
            SELECT id, suhu, humidity, lux, timestamp
            FROM data_sensor
            WHERE suhu = (SELECT MAX(suhu) FROM data_sensor)
            AND humidity = (SELECT MAX(humidity) FROM data_sensor)
        """)
        nilai_max_rows = cursor.fetchall()

        nilai_list = []
        month_year_list = []
        
        for row in nilai_max_rows:
            nilai_list.append({
                "idx": row['id'],
                "suhun": row['suhu'],
                "humid": row['humidity'],
                "kecerahan": row['lux'],
                "timestamp": row['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
            })
            t = row['timestamp']
            month_year_str = f"{t.month}-{t.year}" 
            if {"month_year": month_year_str} not in month_year_list:
                month_year_list.append({"month_year": month_year_str})


        hasil = {
            "data_terbaru": data_terbaru if data_terbaru else {},
            "suhumax": stats['suhumax'],
            "suhumin": stats['suhumin'],
            "suhurata": round(stats['suhurata'], 2),
            "nilai_suhu_max_humid_max": nilai_list, 
            "month_year_max": month_year_list      
        }

        return jsonify(hasil)

    except Exception as e:
        print(f"Error di /api/data: {e}")
        return jsonify({"error": str(e)}), 500
    
    finally:
        if cursor: cursor.close()
        if db: db.close()

if __name__ == "__main__":
    print("Menjalankan Server Flask ")
    print("Akses JSON DI: http://127.0.0.1:5000/ ")
    print("(Soal 2) DI: http://127.0.0.1:5000/api/data")
    app.run(debug=True)