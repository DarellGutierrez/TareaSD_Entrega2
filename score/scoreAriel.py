from fastapi import FastAPI, Request
import pandas as pd
from db_manager import DBManager

app = FastAPI()

# Cargar dataset
df = pd.read_csv("/app/dataset/test.csv", header=None, names=["clase", "titulo", "contenido", "mejor_respuesta"])

# Config DB
db = DBManager(
    host="db",
    port=5432,
    user="postgres",
    password="postgres",
    database="db_consultas"
)


@app.post("/generate")
async def score(request: Request):
    data = await request.json()
    idx = int(data.get("indice_pregunta"))
    consulta = data.get("consulta")

    fila = df.iloc[idx]
    titulo = str(fila["titulo"])
    mejor_respuesta = str(fila["mejor_respuesta"])

    # Placeholder de LLM (respuesta dummy)
    respuesta_llm = f"Respuesta LLM simulada para consulta: {consulta}"

    # Placeholder de score (ejemplo: longitud relativa)
    score_value = len(respuesta_llm) / (len(mejor_respuesta) + 1)

    # Insertar en DB
    db.insertar_pregunta(
        id=idx,
        titulo=titulo,
        mejor_respuesta=mejor_respuesta,
        respuesta_llm=respuesta_llm,
        score=score_value
    )

    return {"respuesta_llm": respuesta_llm, "score": score_value}