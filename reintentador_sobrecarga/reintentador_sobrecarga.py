import os
import json
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuración de Kafka y Lógica de Backoff ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_CONSUMO = "reintentos_sobrecarga"
TOPIC_PUBLICACION = "preguntas_nuevas"
TOPIC_FALLIDAS_TERMINALES = "fallidas_terminales"
KAFKA_GROUP_ID = "grupo-reintentos-sobrecarga"

# Parámetros de Exponential Backoff
INITIAL_BACKOFF_SECONDS = int(os.getenv("INITIAL_BACKOFF_SECONDS", 5))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 4)) # 5s, 10s, 20s, 40s

# --- Funciones de Conexión a Kafka (reutilizadas) ---
def crear_productor_kafka():
    while True:
        try:
            productor = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logging.info("Productor de Kafka conectado.")
            return productor
        except NoBrokersAvailable:
            logging.warning("No se pudo conectar al broker de Kafka (Productor). Reintentando en 5s...")
            time.sleep(5)

def crear_consumidor_kafka():
    while True:
        try:
            consumidor = KafkaConsumer(
                TOPIC_CONSUMO,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logging.info(f"Consumidor de Kafka suscrito a '{TOPIC_CONSUMO}'.")
            return consumidor
        except NoBrokersAvailable:
            logging.warning(f"No se pudo conectar al broker de Kafka (Consumidor). Reintentando en 5s...")
            time.sleep(5)

# --- Lógica Principal ---
def main():
    logging.info("Iniciando servicio de reintento por sobrecarga (Exponential Backoff)...")
    productor = crear_productor_kafka()
    consumidor = crear_consumidor_kafka()

    logging.info("Servicio listo para procesar reintentos por sobrecarga...")

    for message in consumidor:
        data = message.value
        mensaje_original = data.get("mensaje_original", {})
        indice_pregunta = mensaje_original.get("indice_pregunta", "N/A")

        # Incrementar el contador de reintentos
        retry_count = mensaje_original.get("retry_count_sobrecarga", 0) + 1
        mensaje_original["retry_count_sobrecarga"] = retry_count

        if retry_count > MAX_RETRIES:
            logging.error(f"Máximo de reintentos ({MAX_RETRIES}) alcanzado para el mensaje (Índice: {indice_pregunta}). Enviando a fallidas terminales.")
            productor.send(TOPIC_FALLIDAS_TERMINALES, value=data)
            productor.flush()
            continue

        # Calcular espera con exponential backoff
        wait_time = INITIAL_BACKOFF_SECONDS * (2 ** (retry_count - 1))
        logging.info(f"Mensaje de reintento por sobrecarga recibido (Índice: {indice_pregunta}, Intento: {retry_count}). Esperando {wait_time}s.")

        # 1. Esperar
        time.sleep(wait_time)

        # 2. Re-publicar el mensaje actualizado en el tópico de preguntas nuevas
        if mensaje_original:
            productor.send(TOPIC_PUBLICACION, value=mensaje_original)
            productor.flush()
            logging.info(f"Mensaje (Índice: {indice_pregunta}) reenviado a '{TOPIC_PUBLICACION}' para nuevo procesamiento.")
        else:
            logging.error(f"No se encontró 'mensaje_original' en el payload: {data}")

if __name__ == "__main__":
    main()
