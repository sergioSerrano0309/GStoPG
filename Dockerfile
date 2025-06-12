# Usa una imagen base ligera de Python
FROM python:3.10-slim

# Evita prompts de instalación
ENV DEBIAN_FRONTEND=noninteractive

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia solo lo necesario e instala dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de la aplicación
COPY . .

# Copiamos tu JSON al contenedor
COPY credentials.json /app/credentials.json

# Le decimos a la librería de Google dónde está
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/credentials.json"

# Expón el puerto que usa Flask (por defecto 5000 en tu código)
EXPOSE 5000

# Comando por defecto para arrancar tu app
CMD ["python", "GStoPG.py"]
