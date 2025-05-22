from flask import Flask, jsonify
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import psycopg2
import os
import json

app = Flask(__name__)

# CONFIGURACIÓN
SPREADSHEET_ID = "1uGiW4rKszKszkeL-TSlpMGwjQg92YUuqR8gTXJxGHvw"
TABLE_NAME = "empleados"

DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'mi_base',
    'user': 'postgres',
    'password': '1234'
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

@app.route("/actualizar-empleados", methods=["GET"])
def actualizar_empleados():
    try:
        # CARGAR CREDENCIALES DESDE VARIABLE DE ENTORNO
        google_credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
        if not google_credentials_json:
            return jsonify({"status": "error", "message": "Variable de entorno 'GOOGLE_CREDENTIALS_JSON' no configurada."}), 500

        credentials_info = json.loads(google_credentials_json)
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES)

        sheets_service = build('sheets', 'v4', credentials=credentials)

        # LEER DATOS DE GOOGLE SHEETS
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()

        rows = result.get('values', [])
        if not rows:
            return jsonify({"status": "error", "message": "La hoja está vacía."}), 400

        df = pd.DataFrame(rows[1:], columns=rows[0])

        # CONECTAR E INSERTAR EN POSTGRESQL
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute(f"DELETE FROM {TABLE_NAME}")

        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
            cur.execute(sql, tuple(row))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "success", "message": "Datos actualizados correctamente."}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# Ejecutar la app
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
