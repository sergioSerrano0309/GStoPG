from flask import Flask, render_template_string
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import psycopg2
import os
import json
import urllib.parse as urlparse

app = Flask(__name__)

# Configuración a través de variables de entorno
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TABLE_NAME = os.getenv("TABLE_NAME")
DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

# Plantilla HTML profesional y genérica
HTML_TEMPLATE = """
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <title>{{ title }}</title>
    <style>
      body { padding-top: 56px; }
      tr { text-align: center; }
    </style>
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-primary fixed-top">
      <div class="container-fluid">
        <span class="navbar-brand mb-0 h1">{{ title }}</span>
      </div>
    </nav>

    <main class="container">
      <div class="py-4">
        {% if message %}
          <div class="alert alert-{{ alert_type }}" role="alert">{{ message }}</div>
        {% endif %}
        {% if table %}
        <div class="card shadow-sm">
          <div class="card-body">
            <div class="table-responsive">
              {{ table|safe }}
            </div>
          </div>
        </div>
        {% endif %}
      </div>
    </main>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  </body>
</html>
"""

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

@app.route("/datos", methods=["GET"])
def datos():
    # Verificar configuración
    missing = [name for name, val in {
        'SPREADSHEET_ID': SPREADSHEET_ID,
        'TABLE_NAME': TABLE_NAME,
        'DATABASE_URL': DATABASE_URL,
        'GOOGLE_CREDENTIALS_JSON': GOOGLE_CREDENTIALS_JSON
    }.items() if not val]
    if missing:
        return render_template_string(
            HTML_TEMPLATE,
            title="Error de Configuración",
            table=None,
            message=f"Falta configurar: {', '.join(missing)}",
            alert_type="danger"
        ), 500

    try:
        # Autenticación Google Sheets
        creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        sheets = build('sheets', 'v4', credentials=credentials)

        # Leer datos de la hoja
        result = sheets.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()
        rows = result.get('values', [])
        if not rows:
            return render_template_string(
                HTML_TEMPLATE,
                title=TABLE_NAME,
                table=None,
                message="La hoja está vacía.",
                alert_type="warning"
            )

        df = pd.DataFrame(rows[1:], columns=rows[0])

        # Parsear URL de la BD
        url = urlparse.urlparse(DATABASE_URL)
        db_conf = {
            'host': url.hostname,
            'port': url.port,
            'dbname': url.path[1:],
            'user': url.username,
            'password': url.password
        }

        # Actualizar base de datos genéricamente
        conn = psycopg2.connect(**db_conf)
        cur = conn.cursor()
        cols_sql = ', '.join([f'"{col}" TEXT' for col in df.columns])
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" ({cols_sql});')
        cur.execute(f'DELETE FROM "{TABLE_NAME}";')
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            cols = ', '.join([f'"{col}"' for col in row.index])
            cur.execute(
                f'INSERT INTO "{TABLE_NAME}" ({cols}) VALUES ({placeholders});',
                tuple(row)
            )
        conn.commit()
        cur.close()
        conn.close()

        # Renderizar tabla actualizada
        html_table = df.to_html(classes='table table-hover table-striped text-center mb-0', index=False)
        return render_template_string(
            HTML_TEMPLATE,
            title=TABLE_NAME,
            table=html_table,
            message="Datos actualizados correctamente.",
            alert_type="success"
        )

    except Exception as e:
        return render_template_string(
            HTML_TEMPLATE,
            title=TABLE_NAME,
            table=None,
            message=f"Error: {e}",
            alert_type="danger"
        ), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
