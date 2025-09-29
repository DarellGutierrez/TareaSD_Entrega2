FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
#Copiar script de administración de la base de datos para importarlo al código de cache 
COPY ../db_manager/db_manager.py ./db_manager.py    

EXPOSE 8000
CMD ["uvicorn", "cache_api:app", "--host", "0.0.0.0", "--port", "8000"]