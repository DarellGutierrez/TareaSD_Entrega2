import os
import json
from typing import Dict, Any
from db_manager import DBManager

from flask import Flask, request, jsonify
from dotenv import load_dotenv

# LLM client
import google.generativeai as genai

# Data handling
import pandas as pd


# Config DB
db = DBManager(
    host="db",
    port=5432,
    user="postgres",
    password="postgres",
    database="db_consultas"
)

# TF-IDF scorer
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    TFIDF_AVAILABLE = True
except Exception:
    TFIDF_AVAILABLE = False

# Optional: rouge / nltk (still optional but not required)
try:
    from rouge_score import rouge_scorer
    ROUGE_AVAILABLE = True
except Exception:
    ROUGE_AVAILABLE = False

load_dotenv()
API_KEY = os.getenv("GOOGLE_API_KEY")  # debe estar en .env
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-flash-latest")

if not API_KEY:
    raise RuntimeError("Necesitas definir GOOGLE_API_KEY en .env")

if not TFIDF_AVAILABLE:
    raise RuntimeError("TF-IDF (scikit-learn) no está disponible. Instala scikit-learn antes de ejecutar el servicio.")

genai.configure(api_key=API_KEY)
generative_model = genai.GenerativeModel(MODEL_NAME)

app = Flask(__name__)

# Cargar dataset (test.csv) al iniciar si está disponible
df_dataset = pd.read_csv("/app/dataset/test.csv", header=None, names=["clase", "titulo", "contenido", "mejor_respuesta"])



def find_best_answer_column(df: pd.DataFrame) -> str:
    """Intenta identificar la columna que contiene la mejor respuesta."""
    cols = list(df.columns)
    # Prioriza columnas con nombres obvios
    for c in cols:
        lc = c.lower()
        if 'best' in lc and 'answer' in lc:
            return c
    for c in cols:
        lc = c.lower()
        if lc in ('best_answer', 'answer', 'accepted_answer', 'community_answer'):
            return c
    # Si no se reconoce, devuelve la última columna (suponiendo formato clásico de 4 columnas)
    return cols[-1]



# Helper: call Gemini (usa instrucción en prompt para limitar longitud)
def call_gemini(prompt: str) -> Dict[str, Any]:
    """
    Llama a Gemini y devuelve dict con texto y raw response.
    Se antepone la instrucción para limitar la longitud y, por seguridad, se trunca localmente a 150 palabras.
    """
    # Instrucción solicitada (en español)
    length_instruction = "IInstruction: I want the answer to be short, not to exceed 120 words"

    # Construimos el prompt final: instrucción + prompt original (el cache ya envía la consulta completa)
    final_prompt = f"{length_instruction}{prompt}"

    # Llamada simple a la API (sin pasar max_output_tokens para evitar incompatibilidades)
    response = generative_model.generate_content(final_prompt)
    text = getattr(response, "text", None)
    if text is None and isinstance(response, dict):
        text = response.get("text", "")
    text = text.strip() if text else ""

    # Por seguridad, truncamos localmente a 150 palabras si el modelo no respeta la instrucción
    words = text.split()
    if len(words) > 150:
        text = ' '.join(words[:150])

    return {
        "text": text,
        "raw": response
    }


# TF-IDF scoring (único método)
from sklearn.feature_extraction.text import TfidfVectorizer


def tfidf_cosine_score(a: str, b: str) -> float:
    """TF-IDF + cosine similarity (0..1). Versión robusta compatible con csr_matrix."""
    # Vectorizamos las dos cadenas (devuelve matrices sparse)
    vect = TfidfVectorizer().fit_transform([a or "", b or ""])
    # Usamos sklearn.metrics.pairwise.cosine_similarity: acepta sparse matrices y es estable
    sim_matrix = cosine_similarity(vect[0], vect[1])
    # cosine_similarity devuelve una matriz 1x1 en este caso; extraemos el valor
    return float(sim_matrix[0, 0])


def score_response(llm_text: str, best_answer: str) -> Dict[str, Any]:
    """Devuelve {method, value, explanation}. Siempre usa TF-IDF en esta versión."""
    result = {"method": "tfidf", "value": None, "explain": "TF-IDF cosine similarity (0..1)."}
    try:
        val = tfidf_cosine_score(llm_text, best_answer)
        val = max(0.0, min(1.0, float(val)))
        result["value"] = val
    except Exception as e:
        result["explain"] = f"Error computing TF-IDF score: {e}"
        result["value"] = None
    return result


# Endpoints
@app.route("/generate", methods=["POST"])
def generate():
    """
    POST JSON: {"query": "title + content concatenated", "request_id": "... (optional)"}
    Returns: {"llm_response": "...", "raw": {...}}
    """
    payload = request.get_json(force=True)
    query = payload.get("query") or payload.get("text") or ""
    if not query:
        return jsonify({"error": "Missing 'query' in JSON body"}), 400

    gen = call_gemini(query)
    return jsonify({
        "llm_response": gen["text"],
        "raw": str(gen["raw"])
    }), 200


@app.route("/generate_and_score", methods=["POST"])
def generate_and_score():
    """
    Endpoint esperado (desde la caché):
    POST JSON:
    {
      "consulta": "titulo + contenido concatenados (string)",
      "indice_pregunta": 123  # número de fila de la pregunta en el CSV (int)
    }

    El endpoint tomará 'consulta' como prompt para el LLM y buscará la mejor respuesta directamente en la fila indicada por 'indice_pregunta'.
    """
    payload = request.get_json(force=True)

    # Campos que esperamos del cache
    consulta = payload.get("consulta", None)
    indice = payload.get("indice_pregunta", None)

    # Validaciones
    if consulta is None:
        return jsonify({"error": "Se requiere 'consulta' en el JSON body."}), 400
    if indice is None:
        return jsonify({"error": "Se requiere 'indice_pregunta' en el JSON body."}), 400

    # Obtener la best_answer directamente por número de fila
    fila = df_dataset.iloc[indice]
    #titulo = str(fila["titulo"])
    best_answer = str(fila["mejor_respuesta"])

    # Usamos 'consulta' tal cual como prompt
    query = str(consulta).strip()

    # Llamar al LLM (usa instrucción en el prompt para limitar la longitud)
    gen = call_gemini(query)
    llm_text = gen["text"]

    # Calcular score (TF-IDF)
    score = score_response(llm_text, best_answer)

    storage_payload = {
        "indice_pregunta": indice,
        "consulta": consulta,
        "best_answer": best_answer,
        "llm_answer": llm_text,
        "score": score["value"],
        #"score_method": score["method"],
    }

    db.insertar_pregunta(indice, consulta, best_answer, llm_text, score["value"])

    return jsonify({
        "respuesta_llm": llm_text
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False)
