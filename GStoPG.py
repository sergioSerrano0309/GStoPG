from flask import Flask, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import psycopg2
import os
import json
import urllib.parse as urlparse

app = Flask(__name__)

# CONFIGURACIÓN
SPREADSHEET_ID = "1uGiW4rKszKszkeL-TSlpMGwjQg92YUuqR8gTXJxGHvw"
TABLE_NAME = "monos" 

# Leer variables de entorno
DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

# Validación
if not GOOGLE_CREDENTIALS_JSON or not DATABASE_URL:
    raise Exception("Las variables de entorno 'GOOGLE_CREDENTIALS_JSON' y 'DATABASE_URL' deben estar configuradas.")

# Parsear conexión a PostgreSQL
url = urlparse.urlparse(DATABASE_URL)
DB_CONFIG = {
    'host': url.hostname,
    'port': url.port,
    'dbname': url.path[1:],
    'user': url.username,
    'password': url.password
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

@app.route("/ver-datos-google", methods=["GET"])
def ver_datos_google():
    try:
        credentials_info = json.loads(GOOGLE_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES)
        sheets_service = build('sheets', 'v4', credentials=credentials)

        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()

        rows = result.get('values', [])
        if not rows:
            return jsonify({"status": "vacio", "mensaje": "La hoja está vacía."})

        df = pd.DataFrame(rows[1:], columns=rows[0])
        return jsonify(df.to_dict(orient="records"))

    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/actualizar-empleados", methods=["GET"])
def actualizar_empleados():
    try:
        # Autenticación con Google Sheets
        credentials_info = json.loads(GOOGLE_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES)
        sheets_service = build('sheets', 'v4', credentials=credentials)

        # Leer datos
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()

        rows = result.get('values', [])
        if not rows:
            return jsonify({"status": "error", "message": "La hoja está vacía."}), 400

        df = pd.DataFrame(rows[1:], columns=rows[0])

        # Conexión a PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Crear tabla si no existe
        columns_sql = ', '.join([f'"{col}" TEXT' for col in df.columns])
        create_sql = f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns_sql});'
        cur.execute(create_sql)

        # Borrar datos anteriores
        cur.execute(f'DELETE FROM {TABLE_NAME};')

        # Insertar nuevos datos
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join([f'"{col}"' for col in row.index])
            sql = f'INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders});'
            cur.execute(sql, tuple(row))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "success", "message": "Datos actualizados correctamente."}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Ejecutar la app
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
