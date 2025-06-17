from flask import Flask, render_template_string
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
import psycopg2
import os
import urllib.parse as urlparse

app = Flask(__name__)

#Configuración desde entorno
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TABLE_NAME    = os.getenv("TABLE_NAME", "datos_hoja")
DATABASE_URL  = os.getenv("DATABASE_URL")
CRED_PATH     = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <title>Estado de la Importación</title>
</head>
<body>
<div class="container mt-5">
    {% if message %}
    <div class="alert alert-{{ alert_type }}" role="alert">
        {{ message }}
    </div>
    {% endif %}
    {% if table %}
    {{ table|safe }}
    {% endif %}
</div>
</body>
</html>
"""

@app.route("/importar")
def importar():
    try:
        # 1. Autenticación con Google Sheets
        creds = Credentials.from_service_account_file(CRED_PATH, scopes=SCOPES)

        # 2. Conexión a la API de Sheets
        sheets = build("sheets", "v4", credentials=creds)

        # 3. Leer datos de Google Sheets y normalizar filas
        result = sheets.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range="A1:Z100"
        ).execute()
        rows = result.get("values", [])
        if not rows or len(rows) < 2:
            headers = rows[0] if rows else []
            sheet_df = pd.DataFrame([], columns=headers)
        else:
            headers = rows[0]
            raw_rows = rows[1:]
            fixed_rows = []
            for row in raw_rows:
                # Rellenar con None si faltan columnas
                if len(row) < len(headers):
                    row = row + [None] * (len(headers) - len(row))
                fixed_rows.append(row)
            sheet_df = pd.DataFrame(fixed_rows, columns=headers)

        # Asegurar columna DB
        if "DB" not in sheet_df.columns:
            sheet_df["DB"] = None

        # Función para convertir porcentajes a valor numérico entre 0 y 1
        def convert_val(v):
            if pd.isna(v):
                return v
            s = str(v).strip()
            if s.endswith("%"):
                num = s[:-1].replace(",", ".")
                try:
                    return float(num) / 100
                except ValueError:
                    return v
            return v

        # Aplicar conversión a la columna "Valor"
        sheet_df["Valor"] = sheet_df["Valor"].apply(convert_val)

        # 4. Conectar a PostgreSQL
        url = urlparse.urlparse(DATABASE_URL)
        conf = {
            "host":     url.hostname,
            "port":     url.port,
            "dbname":   url.path.lstrip("/"),
            "user":     url.username,
            "password": url.password
        }
        conn = psycopg2.connect(**conf)
        cur = conn.cursor()

        # 5. Crear tabla si no existe
        cols_sql = ", ".join([f"{c.lower()} TEXT" for c in sheet_df.columns])
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({cols_sql});"
        )
        conn.commit()

        # 6. Normalizar valores para decidir INSERT/UPDATE
        def norm(x):
            return str(x).strip().lower()

        mask_ins = sheet_df["DB"].apply(lambda v: norm(v) in ["", "0"])
        mask_upd = sheet_df["DB"].apply(lambda v: norm(v) == "2")

        ins_df = sheet_df[mask_ins].copy()
        upd_df = sheet_df[mask_upd].copy()

        # 7. Insertar nuevas filas
        if not ins_df.empty:
            ins_df["DB"] = "1"
            for _, row in ins_df.iterrows():
                cols = [c.lower() for c in row.index]
                vals = tuple(row.values)
                ph = ", ".join(["%s"] * len(vals))
                cur.execute(
                    f"INSERT INTO {TABLE_NAME} ({', '.join(cols)}) VALUES ({ph});",
                    vals
                )

        # 8. Actualizar filas existentes
        if not upd_df.empty:
            for _, row in upd_df.iterrows():
                cols = [c.lower() for c in row.index if c != "DB"]
                vals = [row[c] for c in cols]
                set_clause = ", ".join([f"{c} = %s" for c in cols])
                # Asumimos que la columna 'ID' es la clave primaria
                vals.append(row["ID"])
                cur.execute(
                    f"UPDATE {TABLE_NAME} SET {set_clause} WHERE id = %s;",
                    tuple(vals)
                )

        conn.commit()

        # 9. Generar tabla HTML con el estado final
        cur.execute(f"SELECT * FROM {TABLE_NAME};")
        all_rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        html_table = pd.DataFrame(all_rows, columns=colnames).to_html(classes="table table-striped", index=False)

        return render_template_string(
            HTML_TEMPLATE,
            message="Importación completada con éxito.",
            alert_type="success",
            table=html_table
        )

    except Exception as e:
        import traceback; traceback.print_exc()
        return render_template_string(
            HTML_TEMPLATE,
            message="Error interno: " + str(e),
            alert_type="danger",
            table=None
        ), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
