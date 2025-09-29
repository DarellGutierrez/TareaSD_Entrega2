FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY score.py .
COPY ../db_manager/db_manager.py ./db_manager.py

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "5000"]