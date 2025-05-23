from flask import Flask, render_template_string
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
TABLE_NAME = "empleados"

# Leer variables de entorno
database_url = os.getenv("DATABASE_URL")
google_credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")

# Validación
if not google_credentials_json or not database_url:
    raise Exception("Las variables de entorno 'GOOGLE_CREDENTIALS_JSON' y 'DATABASE_URL' deben estar configuradas.")

# Parsear conexión a PostgreSQL
url = urlparse.urlparse(database_url)
DB_CONFIG = {
    'host': url.hostname,
    'port': url.port,
    'dbname': url.path[1:],
    'user': url.username,
    'password': url.password
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

HTML_TEMPLATE = """
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
    <title>{{ title }}</title>
  </head>
  <body>
    <div class="container mt-4">
      <h1>{{ title }}</h1>
      <div class="table-responsive">{{ table|safe }}</div>
      {% if message %}
        <p class="alert alert-info mt-3">{{ message }}</p>
      {% endif %}
    </div>
  </body>
</html>
"""

@app.route("/ver-datos-google", methods=["GET"])
def ver_datos_google():
    try:
        creds_info = json.loads(google_credentials_json)
        credentials = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)

        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()

        rows = result.get('values', [])
        if not rows:
            return render_template_string(
                HTML_TEMPLATE,
                title="Ver datos - Google Sheet",
                table="",
                message="La hoja está vacía."
            )

        df = pd.DataFrame(rows[1:], columns=rows[0])
        html_table = df.to_html(classes='table table-striped', index=False)
        return render_template_string(
            HTML_TEMPLATE,
            title="Datos de Google Sheet",
            table=html_table,
            message=None
        )

    except Exception as e:
        return render_template_string(
            HTML_TEMPLATE,
            title="Error al obtener datos",
            table="",
            message=str(e)
        )

@app.route("/actualizar-empleados", methods=["GET"])
def actualizar_empleados():
    try:
        creds_info = json.loads(google_credentials_json)
        credentials = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)

        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()

        rows = result.get('values', [])
        if not rows:
            return render_template_string(
                HTML_TEMPLATE,
                title="Actualizar empleados",
                table="",
                message="La hoja está vacía."
            ), 400

        df = pd.DataFrame(rows[1:], columns=rows[0])

        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Crear tabla si no existe
        cols_sql = ', '.join([f'"{col}" TEXT' for col in df.columns])
        cur.execute(f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({cols_sql});')
        cur.execute(f'DELETE FROM {TABLE_NAME};')

        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            cols = ', '.join([f'"{col}"' for col in row.index])
            sql = f'INSERT INTO {TABLE_NAME} ({cols}) VALUES ({placeholders});'
            cur.execute(sql, tuple(row))

        conn.commit()
        cur.close()
        conn.close()

        return render_template_string(
            HTML_TEMPLATE,
            title="Actualizar empleados",
            table="",
            message="Datos actualizados correctamente."
        ), 200

    except Exception as e:
        return render_template_string(
            HTML_TEMPLATE,
            title="Error al actualizar",
            table="",
            message=str(e)
        ), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
