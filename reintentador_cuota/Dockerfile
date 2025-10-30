FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias
RUN pip install kafka-python

# Copiar el script
COPY reintentador_cuota.py .

# Comando para ejecutar el servicio
CMD ["python", "reintentador_cuota.py"]
