from fastapi import FastAPI
from pydantic import BaseModel
import redis
import os

#Configuración de redis
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")  #nombre del servicio docker-compose
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_MAXMEMORY = os.environ.get("REDIS_MAXMEMORY", "100mb")  #ejemplo
REDIS_POLICY = os.environ.get("REDIS_POLICY", "allkeys-lru")  #política de remocion

#Conexión a Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

#Configurar límites de memoria y política
r.config_set("maxmemory", REDIS_MAXMEMORY)      #tamaño de cache
r.config_set("maxmemory-policy", REDIS_POLICY)  #política de remoción

#modelo de consulta
class Consulta(BaseModel):
    consulta: str

app = FastAPI()

hit_count = 0   #contador de hits
miss_count = 0  #contador de misses

@app.post("/query") #recibe la consulta POST proveniente del generador de tráfico
def recibir_consulta(req: Consulta):
    global hit_count, miss_count
    texto = req.consulta

    if r.exists(texto):
        hit_count += 1
        r.incr(texto)
        status = "hit"
    else:
        miss_count += 1
        r.set(texto, 1)  # guardar en cache
        status = "miss"
        #aquí se puede deberá configurar el envío de la consulta al módulo scoring más adelante
        #requests.post("http://scoring:8001/score", json={"consulta": texto})

    #retornar métricas parciales junto con status
    return {"status": status, "hit_count": hit_count, "miss_count": miss_count}

#endpoint para consultar tasa de hit/miss, por ahora mediante curl http://localhost:8000/metrics lo cual entrega el json con las métricas
@app.get("/metrics")
def obtener_metricas():
    total = hit_count + miss_count
    hit_rate = hit_count / total if total > 0 else 0
    miss_rate = miss_count / total if total > 0 else 0
    return {
        "hit_count": hit_count,
        "miss_count": miss_count,
        "hit_rate": hit_rate,
        "miss_rate": miss_rate,
        "consultas totales": total
    }