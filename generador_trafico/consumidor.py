from kafka import KafkaConsumer
import json
import time

print("Intentando conectar con Kafka como consumidor...")

try:
    # 1. Crear el consumidor
    consumer = KafkaConsumer(
        'respuestas_exitosas',  # El topic al que nos suscribimos
        
        # OJO: Usamos el puerto EXTERNO, igual que el productor
        bootstrap_servers=['kafka:9092'], 
        
        # Equivalente a --from-beginning
        auto_offset_reset='earliest', 
        
        # Cierra el script después de 10 seg si no hay mensajes nuevos
        consumer_timeout_ms=10000,
        
        # Deserializador: Convierte los bytes (JSON) de vuelta a un diccionario
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print("Conectado. Escuchando mensajes en 'pruebaPython'...")

    # 2. Iterar sobre los mensajes
    for message in consumer:
        print(f"\n--- Mensaje Recibido ---")
        print(f"Topic:     {message.topic}")
        print(f"Partición: {message.partition}")
        print(f"Offset:    {message.offset}")
        print(f"Valor (JSON):")
        
        # 'message.value' ya es un diccionario gracias al deserializer
        # Usamos json.dumps para imprimirlo bonito (con indentación)
        print(json.dumps(message.value, indent=2))

    print("\n--- No hay más mensajes. Cerrando consumidor. ---")

except Exception as e:
    print(f"Error al consumir mensajes: {e}")

finally:
    if 'consumer' in locals():
        consumer.close()
        print("Conexión del consumidor cerrada.")