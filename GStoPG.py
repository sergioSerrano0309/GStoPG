from flask import Flask, render_template_string
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import psycopg2
import os
import json
import urllib.parse as urlparse

app = Flask(__name__)

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TABLE_NAME = os.getenv("TABLE_NAME", "datos_hoja")
DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Plantilla HTML
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{{ title }}</title>
    <link
      rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css"
    />
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
            'SPREADSHEET_ID': SPREADSHEET_ID,
            'TABLE_NAME': TABLE_NAME,
            'DATABASE_URL': DATABASE_URL,
            'GOOGLE_CREDENTIALS_JSON': GOOGLE_CREDENTIALS_JSON
        }.items() if not val
    ]
    if missing:
        return render_template_string(
            HTML_TEMPLATE,
            title="Error de Configuración",
            debug_msg="",
            table=None,
            message=f"Falta configurar: {', '.join(missing)}",
            alert_type="danger"
        ), 500

    try:
        # Leer datos de Google Sheets
        creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
        credentials = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        sheets = build('sheets', 'v4', credentials=credentials)
        result = sheets.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()
        rows = result.get('values', [])
        if not rows or len(rows) < 2:
            return render_template_string(
                HTML_TEMPLATE,
                title=TABLE_NAME,
                debug_msg="",
                table=None,
                message="No hay datos en la hoja de cálculo.",
                alert_type="info"
            )
        df = pd.DataFrame(rows[1:], columns=rows[0])
        if 'DB' not in df.columns:
            df['DB'] = None

        # Conectar a PostgreSQL
        url = urlparse.urlparse(DATABASE_URL)
        db_conf = {
            'host': url.hostname,
            'port': url.port,
            'dbname': url.path[1:],
            'user': url.username,
            'password': url.password
        }
        debug_msg = f"Conectando a BD → host: {url.hostname}, puerto: {url.port}, db: {url.path[1:]}, user: {url.username}"
        conn = psycopg2.connect(**db_conf)
        cur = conn.cursor()

        # Crear tabla si no existe
        cols_sql = ", ".join([f"{col.lower()} TEXT" for col in df.columns])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({cols_sql});")
        conn.commit()

        # Determinar filas a insertar o actualizar
        def normalizar(val):
            return "" if val is None else str(val).strip()
        mask_insert = df['DB'].apply(lambda v: normalizar(v) in ["", "0"])
        mask_update = df['DB'].apply(lambda v: normalizar(v) == "2")
        nuevos_df = df[mask_insert].copy()
        actualizar_df = df[mask_update].copy()

        # Si no hay acciones pendientes, mostrar contenido de la BD
        if nuevos_df.empty and actualizar_df.empty:
            df_db = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
            cur.close()
            conn.close()
            html_table = df_db.to_html(classes="table table-hover table-striped text-center mb-0", index=False)
            return render_template_string(
                HTML_TEMPLATE,
                title=TABLE_NAME,
                debug_msg=debug_msg,
                table=html_table,
                message="No hay registros para insertar o actualizar.",
                alert_type="info"
            )

        # Insertar nuevas filas
        print(f"Nuevos registros a insertar: {len(nuevos_df)}", flush=True)
        for _, row in nuevos_df.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            cols = ", ".join([col.lower() for col in row.index])
            values = tuple(row.values)
            cur.execute(f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({placeholders});", values)

        # Actualizar filas existentes
        print(f"Registros a actualizar: {len(actualizar_df)}", flush=True)
        for _, row in actualizar_df.iterrows():
            cols_upd = [col.lower() for col in row.index if col not in ["ID", "DB"]]
            set_clause = ", ".join([f"{col} = %s" for col in cols_upd])
            values = tuple(row[col] for col in row.index if col not in ["ID", "DB"] ) + (row["ID"],)
            cur.execute(f"UPDATE {TABLE_NAME} SET {set_clause} WHERE id = %s;", values)

        # Confirmar transacciones y leer la BD
        conn.commit()
        df_db = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
        cur.close()
        conn.close()

        # Marcar en la hoja las filas procesadas
        idx_db = df.columns.get_loc("DB")
        def idx_to_letter(n):
            s = ""
            while n >= 0:
                s = chr(n % 26 + ord("A")) + s
                n = n // 26 - 1
            return s
        letra_db = idx_to_letter(idx_db)
        requests = []
        for fila in list(nuevos_df.index) + list(actualizar_df.index):
            numero = fila + 2  # Ajuste por encabezados de Sheets
            requests.append({"range": f"{letra_db}{numero}", "values": [["1"]]})
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": requests}
        ).execute()

        # Renderizar la tabla de la BD
        html_table = df_db.to_html(classes="table table-hover table-striped text-center mb-0", index=False)
        mensaje = ""
        if not nuevos_df.empty:
            mensaje += f"Se han insertado {len(nuevos_df)} fila(s). "
        if not actualizar_df.empty:
            mensaje += f"Se han actualizado {len(actualizar_df)} fila(s)."
        return render_template_string(
            HTML_TEMPLATE,
            title=TABLE_NAME,
            debug_msg=debug_msg,
            table=html_table,
            message=mensaje.strip(),
            alert_type="success"
        )

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
