from flask import Flask, render_template_string
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import psycopg2
import os
import urllib.parse as urlparse

app = Flask(__name__)

# Configuración desde variables de entorno
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TABLE_NAME = os.getenv("TABLE_NAME", "datos_hoja")
DATABASE_URL = os.getenv("DATABASE_URL")
CRED_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Scopes de API de Google
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Plantilla HTML
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{{ title }}</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" />
  </head>
  <body>
    <nav class="navbar navbar-expand-lg navbar-dark bg-primary fixed-top">
      <div class="container-fluid">
        <a class="navbar-brand" href="#">Gestión de Datos</a>
      </div>
    </nav>
    <main class="container mt-5 pt-4">
      {% if message %}
      <div class="alert alert-{{ alert_type }}" role="alert">
        {{ message }}
      </div>
      {% endif %}
      <div class="card shadow-sm">
        <div class="card-body">
          <div class="table-responsive">
            {{ table|safe }}
          </div>
        </div>
      </div>
    </main>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  </body>
</html>
"""

@app.route("/datos", methods=["GET"])
def datos():
    # Verificar configuración
    missing = [
        name for name, val in {
            "SPREADSHEET_ID": SPREADSHEET_ID,
            "TABLE_NAME": TABLE_NAME,
            "DATABASE_URL": DATABASE_URL,
            "GOOGLE_APPLICATION_CREDENTIALS": CRED_PATH
        }.items() if not val
    ]
    if missing:
        return render_template_string(HTML_TEMPLATE,
            title="Error de Configuración",
            table=None,
            message=f"Falta configurar: {', '.join(missing)}",
            alert_type="danger"
        ), 500

    try:
        # Autenticación Google
        creds = Credentials.from_service_account_file(CRED_PATH, scopes=SCOPES)
        sheets = build("sheets", "v4", credentials=creds)

        # Leer datos de Google Sheets
        result = sheets.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()
        rows = result.get("values", [])
        if not rows or len(rows) < 2:
            return render_template_string(HTML_TEMPLATE,
                title=TABLE_NAME,
                table=None,
                message="No hay datos en la hoja de cálculo.",
                alert_type="info"
            )
        df = pd.DataFrame(rows[1:], columns=rows[0])

        # Añadir columna DB para seguimiento si no existe
        if "DB" not in df.columns:
            df["DB"] = None

        # Conectar a PostgreSQL
        url = urlparse.urlparse(DATABASE_URL)
        db_conf = {
            "host": url.hostname,
            "port": url.port,
            "dbname": url.path[1:],
            "user": url.username,
            "password": url.password
        }
        conn = psycopg2.connect(**db_conf)
        cur = conn.cursor()

        # Crear tabla si no existe
        cols_sql = ", ".join([f"{col.lower()} TEXT" for col in df.columns])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({cols_sql});")
        conn.commit()

        # Solo insertamos filas nuevas
        def normalizar(v): return "" if v is None else str(v).strip()
        mask_insert = df["DB"].apply(lambda v: normalizar(v) in ["", "0"])
        nuevos_df = df[mask_insert].copy()
        if not nuevos_df.empty:
            # Marcar en DB
            nuevos_df["DB"] = "1"
            for _, row in nuevos_df.iterrows():
                cols = ", ".join([c.lower() for c in row.index])
                placeholders = ", ".join(["%s"] * len(row))
                cur.execute(
                    f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({placeholders});",
                    tuple(row.values)
                )
            conn.commit()

            # Marcar en Sheets
            idx_db = df.columns.get_loc("DB")
            def idx_to_letter(n):
                s = ""
                while n >= 0:
                    s = chr(n % 26 + ord("A")) + s
                    n = n // 26 - 1
                return s
            letra_db = idx_to_letter(idx_db)
            requests = []
            for fila in nuevos_df.index:
                numero = fila + 2
                requests.append({"range": f"{letra_db}{numero}", "values": [["1"]]})
            sheets.spreadsheets().values().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"valueInputOption": "RAW", "data": requests}
            ).execute()

            mensaje = f"Se han insertado {len(nuevos_df)} fila(s)."
        else:
            mensaje = "No hay registros nuevos para insertar."

        # Leer datos actualizados
        df_db = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
        cur.close()
        conn.close()
        html_table = df_db.to_html(classes="table table-hover table-striped text-center mb-0", index=False)

        return render_template_string(
            HTML_TEMPLATE,
            title=TABLE_NAME,
            table=html_table,
            message=mensaje,
            alert_type="success" if nuevos_df.empty is False else "info"
        )

    except Exception:
        import traceback; traceback.print_exc()
        return render_template_string(
            HTML_TEMPLATE,
            title="Error",
            table=None,
            message="Ha ocurrido un error interno.",
            alert_type="danger"
        ), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
