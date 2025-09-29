FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY score.py .

CMD ["uvicorn", "score:app", "--host", "0.0.0.0", "--port", "5000"]