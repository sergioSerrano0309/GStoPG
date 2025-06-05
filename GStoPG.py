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
TABLE_NAME = os.getenv("TABLE_NAME")
DATABASE_URL = os.getenv("DATABASE_URL")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

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

    <main class="container" style="padding-top: 80px;">
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

@app.route("/datos", methods=["GET"])
def datos():
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
            table=None,
            message=f"Falta configurar: {', '.join(missing)}",
            alert_type="danger"
        ), 500

    try:
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
                table=None,
                message="La hoja está vacía o solo tiene encabezados.",
                alert_type="warning"
            )

        encabezados = rows[0]
        num_cols = len(encabezados)
        datos_completos = []
        for fila in rows[1:]:
            if len(fila) < num_cols:
                fila = fila + [""] * (num_cols - len(fila))
            if len(fila) > num_cols:
                fila = fila[:num_cols]
            datos_completos.append(fila)

        df = pd.DataFrame(datos_completos, columns=encabezados)

        url = urlparse.urlparse(DATABASE_URL)
        db_conf = {
            'host': url.hostname,
            'port': url.port,
            'dbname': url.path[1:],
            'user': url.username,
            'password': url.password
        }
        conn = psycopg2.connect(**db_conf)
        cur = conn.cursor()

        cols_sql = ', '.join([f'"{col}" TEXT' for col in df.columns])
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" ({cols_sql});')

        def es_por_insertar(val):
            if val is None:
                return True
            txt = str(val).strip()
            return (txt == "") or (txt == "0")

        def es_por_actualizar(val):
            if val is None:
                return False
            return str(val).strip() == "2"

        mask_insert = df['DB'].apply(es_por_insertar)
        mask_update = df['DB'].apply(es_por_actualizar)

        nuevos_df = df.loc[mask_insert].copy()
        actualizar_df = df.loc[mask_update].copy()

        if nuevos_df.empty and actualizar_df.empty:
            conn.commit()
            cur.close()
            conn.close()
            html_table = df.to_html(classes='table table-hover table-striped text-center mb-0', index=False)
            return render_template_string(
                HTML_TEMPLATE,
                title=TABLE_NAME,
                table=html_table,
                message="No hay registros para insertar o actualizar.",
                alert_type="info"
            )

        for idx, row in nuevos_df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            cols = ', '.join([f'"{col}"' for col in row.index])
            valores = tuple(row.values)
            sql = f'INSERT INTO "{TABLE_NAME}" ({cols}) VALUES ({placeholders});'
            cur.execute(sql, valores)

        for idx, row in actualizar_df.iterrows():
            set_clauses = ', '.join([f'"{col}" = %s' for col in row.index if col != 'ID' and col != 'DB'])
            valores = tuple(row[col] for col in row.index if col != 'ID' and col != 'DB')
            sql = f'UPDATE "{TABLE_NAME}" SET {set_clauses} WHERE "ID" = %s;'
            valores = valores + (row['ID'],)
            cur.execute(sql, valores)

        conn.commit()
        cur.close()
        conn.close()

        df_columns = list(df.columns)
        idx_db = df_columns.index('DB')

        def idx_to_letter(n):
            s = ""
            while n >= 0:
                s = chr(n % 26 + ord('A')) + s
                n = n // 26 - 1
            return s

        letra_db = idx_to_letter(idx_db)
        requests = []
        filas_a_actualizar = list(nuevos_df.index) + list(actualizar_df.index)
        for fila_original in filas_a_actualizar:
            numero_fila_sheets = fila_original + 2
            celda_actualizar = f"{letra_db}{numero_fila_sheets}"
            requests.append({
                "range": celda_actualizar,
                "values": [["1"]]
            })
        body = {
            "valueInputOption": "RAW",
            "data": requests
        }
        sheets.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body=body
        ).execute()

        df.loc[filas_a_actualizar, 'DB'] = "1"
        html_table = df.to_html(classes='table table-hover table-striped text-center mb-0', index=False)

        mensaje = ""
        if not nuevos_df.empty:
            mensaje += f"Se han insertado {len(nuevos_df)} fila(s). "
        if not actualizar_df.empty:
            mensaje += f"Se han actualizado {len(actualizar_df)} fila(s)."

        return render_template_string(
            HTML_TEMPLATE,
            title=TABLE_NAME,
            table=html_table,
            message=mensaje.strip(),
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
