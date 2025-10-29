# 1. Imagen base
# Usamos una imagen "slim" de Python 3.10 para que sea ligera
FROM python:3.10-slim

# 2. Establecer el directorio de trabajo
# Todo lo que hagamos de ahora en adelante será dentro de /app
WORKDIR /app

# 3. Copiar el archivo de requisitos
# Copiamos esto PRIMERO para aprovechar la caché de capas de Docker.
# Si el requirements.txt no cambia, Docker no reinstalará todo cada vez.
COPY requirements.txt .

# 4. Instalar las dependencias
# --no-cache-dir mantiene la imagen un poco más pequeña
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar el código de la aplicación
# Copiamos el script de Python al directorio /app del contenedor
# COPY servicioLLM.py .

# 6. Comando de ejecución
# Este es el comando que se ejecutará cuando el contenedor inicie.
# Usamos "-u" (unbuffered) para que los "print" y "logging"
# aparezcan en los logs de Docker inmediatamente.
CMD ["python", "-u", "servicioLLM.py"]