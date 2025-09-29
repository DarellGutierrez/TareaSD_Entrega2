FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY score.py .

# Comando: usamos gunicorn para servir la app de Flask (score:app = archivo score.py con variable app)
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "score:app"]