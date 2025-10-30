import os
import json
import logging
import time
from dotenv import load_dotenv

import google.generativeai as genai
# Importamos las excepciones específicas de la API de Google
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable, InternalServerError

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Carga de Variables de Entorno ---
# Carga variables de un archivo .env (útil para desarrollo local)
load_dotenv()

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_PREGUNTAS_NUEVAS = "preguntas_nuevas"
TOPIC_RESPUESTAS_EXITOSAS = "respuestas_exitosas"
TOPIC_REINTENTOS_CUOTA = "reintentos_cuota"
TOPIC_REINTENTOS_SOBRECARGA = "reintentos_sobrecarga"
TOPIC_FALLIDAS_TERMINALES = "fallidas_terminales"
KAFKA_GROUP_ID = "grupo-servicio-llm"

# Configuración de Gemini (usando tu snippet)
API_KEY = os.getenv("GOOGLE_API_KEY")
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite") # Actualizado a un modelo común

if not API_KEY:
    logging.error("No se encontró la variable de entorno GOOGLE_API_KEY.")
    exit(1)

try:
    genai.configure(api_key=API_KEY)
    generative_model = genai.GenerativeModel(MODEL_NAME)
    logging.info(f"Modelo Gemini '{MODEL_NAME}' configurado exitosamente.")
except Exception as e:
    logging.error(f"Error configurando el modelo de Gemini: {e}")
    exit(1)


# --- Funciones Auxiliares ---

def crear_productor_kafka(servidores_bootstrap):
    """Intenta crear un productor de Kafka."""
    try:
        productor = KafkaProducer(
            bootstrap_servers=servidores_bootstrap,
            # Serializa los mensajes de valor a JSON y luego a bytes UTF-8
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logging.info("Productor de Kafka conectado exitosamente.")
        return productor
    except NoBrokersAvailable:
        logging.error(f"No se pudo conectar al broker de Kafka en {servidores_bootstrap}.")
        return None

def crear_consumidor_kafka(servidores_bootstrap, topico, group_id):
    """Intenta crear un consumidor de Kafka."""
    try:
        consumidor = KafkaConsumer(
            topico,
            bootstrap_servers=servidores_bootstrap,
            auto_offset_reset='earliest', # Empieza a leer desde el principio si es un nuevo consumidor
            group_id=group_id,
            # Deserializa los mensajes de valor de bytes UTF-8 a JSON
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logging.info(f"Consumidor de Kafka conectado y suscrito al tópico '{topico}'.")
        return consumidor
    except NoBrokersAvailable:
        logging.error(f"No se pudo conectar al broker de Kafka en {servidores_bootstrap}.")
        return None

def enviar_a_topico_fallido(productor, topico_destino, tipo_error, mensaje_original, detalle_error):
    """Envía un mensaje estructurado a un tópico de fallos específico."""
    mensaje_error = {
        "tipo_error": tipo_error,
        "mensaje_original": mensaje_original,
        "error_detalle": str(detalle_error)
    }
    try:
        productor.send(topico_destino, value=mensaje_error)
        productor.flush()
        logging.warning(f"Mensaje enrutado a '{topico_destino}' (Tipo: {tipo_error})")
    except Exception as e:
        logging.error(f"Error al producir mensaje en Kafka (fallidas): {e}")

# --- Lógica Principal ---

def main():
    logging.info("Iniciando servicio LLM...")

    # Espera activa por Kafka (útil en Docker Compose al arrancar)
    productor = None
    while productor is None:
        productor = crear_productor_kafka(KAFKA_BOOTSTRAP_SERVERS)
        if productor is None:
            logging.warning("Reintentando conexión con Kafka (Productor) en 5s...")
            time.sleep(5)

    consumidor = None
    while consumidor is None:
        consumidor = crear_consumidor_kafka(KAFKA_BOOTSTRAP_SERVERS, TOPIC_PREGUNTAS_NUEVAS, KAFKA_GROUP_ID)
        if consumidor is None:
            logging.warning("Reintentando conexión con Kafka (Consumidor) en 5s...")
            time.sleep(5)

    logging.info("Servicio LLM listo y esperando mensajes...")

    # Bucle infinito de consumo
    for message in consumidor:
        data = message.value
        logging.info(f"Mensaje recibido (Índice: {data.get('indice_pregunta', 'N/A')})")

        try:
            # 3. Enviar la consulta al LLM
            # Aseguramos que el contador de intentos exista
            if 'intentos_calidad' not in data:
                data['intentos_calidad'] = 0

            # (Opcional) Pequeña variación en el prompt para reintentos
            consulta_llm = data['consulta']
            if data['intentos_calidad'] > 0:
                consulta_llm = f"Reformulación (intento {data['intentos_calidad'] + 1}): {data['consulta']}"

            response = generative_model.generate_content(consulta_llm)
            respuesta_llm = response.text

            # 4. Producir al topic "respuestas_exitosas"
            respuesta_exitosa = data.copy()
            # Reemplazar saltos de línea para mejor visualización en la base de datos
            respuesta_exitosa['respuesta_llm'] = respuesta_llm.replace("\n", "\\n")

            productor.send(TOPIC_RESPUESTAS_EXITOSAS, value=respuesta_exitosa)
            productor.flush()
            logging.info(f"Respuesta exitosa enviada a '{TOPIC_RESPUESTAS_EXITOSAS}' (Índice: {data['indice_pregunta']})")

        # 5. Manejo de errores específicos
        except ResourceExhausted as e:
            # Error 429: Límite de cuota o "Rate Limiting"
            logging.warning(f"Error de Cuota/Rate Limit: {e}")
            enviar_a_topico_fallido(productor, TOPIC_REINTENTOS_CUOTA, "rate_limit", data, e)

        except (ServiceUnavailable, InternalServerError) as e:
            # Error 503 o 500: Sobrecarga temporal del servidor
            logging.warning(f"Error de Sobrecarga del Servidor (LLM): {e}")
            enviar_a_topico_fallido(productor, TOPIC_REINTENTOS_SOBRECARGA, "overload", data, e)

        except Exception as e:
            # Otros errores (ej. prompt bloqueado, error de conexión, etc.)
            logging.error(f"Error inesperado procesando la consulta: {e}")
            enviar_a_topico_fallido(productor, TOPIC_FALLIDAS_TERMINALES, "unknown_error", data, e)

if __name__ == "__main__":
    main()