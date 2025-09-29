import time
import psycopg2

class DBManager:
    #inicialización de la base de datos en el puerto 5432
    def __init__(self, host="db", port=5432, user="postgres", password="postgres", database="db_consultas", retries=5, delay=5):
        for attempt in range(retries):
            try:
                self.conn = psycopg2.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database
                )
                print("Conectado a la base de datos")
                break
            except psycopg2.OperationalError as e:
                print(f"Fallo de conexión (intento {attempt+1}/{retries}): {e}")
                time.sleep(delay)
        else:
            raise Exception("No se pudo conectar a la base de datos después de varios intentos")
        
        self.conn.autocommit = True
    #función para insertar datos iniciales de consulta que debe llamar el módulo score al obtener una consulta miss del cache
    def insertar_pregunta(self, id, titulo, mejor_respuesta, respuesta_llm, score):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO preguntas (id, titulo, mejor_respuesta, respuesta_llm, score, numero_consultas)
                VALUES (%s, %s, %s, %s, %s, 1)
                ON CONFLICT (id) DO NOTHING;
            """, (id, titulo, mejor_respuesta, respuesta_llm, score))
    #función para incrementar el número de consultas que ha recibido una pregunta en específico que debe llamar el módulo cache al tener un hit
    def incrementar_consulta(self, id):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE preguntas
                SET numero_consultas = numero_consultas + 1
                WHERE id = %s;
            """, (id,))