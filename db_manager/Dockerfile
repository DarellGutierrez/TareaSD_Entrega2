FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY db_manager.py .

CMD ["python", "-c", "print('DBManager module listo para importar')"]