import time
import argparse
import pandas as pd
import numpy as np
import os
import json
import requests
from kafka import KafkaProducer
from db_manager import DBManager

#Configurar base de datos
db = DBManager(
    host=os.getenv("DB_HOST", "db"),
    port=int(os.getenv("DB_PORT", 5432)),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "postgres"),
    database=os.getenv("DB_NAME", "db_consultas")
)

#Configuración de Kafka
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=json_serializer
)

print("Conectado a Kafka (modo JSON)...")

#Elegir las preguntas para las consultas mediante distribución tanto uniforme como zipf (a elección mediante parámetros)
def seleccionar_preguntas(df, cantidad, distribucion, **kwargs):
    total_filas = len(df)

    if distribucion == "uniforme":
        indices = np.random.randint(0, total_filas, size=cantidad)

    elif distribucion == "zipf":
        a = kwargs.get("a", 1.5)  # parámetro típico de zipf en caso de que no se especifique argumento "a" (parámetro de forma o exponente)
        crudo = np.random.zipf(a, size=cantidad)
        indices = (crudo - 1) % total_filas  # ajusta a rango válido

    else:
        raise ValueError(f"Distribución no soportada: {distribucion}") # en caso de tener un typo en distribución o no introducirla

    return indices


#Elegir intervalos de esperas entre consultas
def generar_esperas(cantidad, modo, **kwargs):
    if modo == "fijo":
        espera = kwargs.get("espera", 1.0)  # parámetro predeterminado de 1 segundo fijo de espera en caso de no especificarse
        return [espera] * cantidad

    elif modo == "uniforme":
        minimo = kwargs.get("minimo", 0.5)  # parámetro predeterminado de 0.5 seg como piso
        maximo = kwargs.get("maximo", 2.0)  # parámetro predeterminado de 2 seg como techo
        return list(np.random.uniform(minimo, maximo, size=cantidad))

    else:
        raise ValueError(f"Modo de espera no soportado: {modo}") # en caso de tener un typo en modo o no introducirlo


#Loop de generación de tráfico
def ejecutar_trafico(cantidad, distribucion, modo_espera, **kwargs):
    # dataframe del dataset indicando columnas y la no existencia de fila header
    df = pd.read_csv("/app/dataset/test.csv", header=None, names=["clase", "titulo", "contenido", "mejor_respuesta"]) 

    indices_preguntas = seleccionar_preguntas(df, cantidad, distribucion, **kwargs) #indices de las preguntas elegidas mediante la distribución a elección
    esperas = generar_esperas(cantidad, modo_espera, **kwargs)                      #esperas fijas o uniformemente distribuidas entre valores min y max (en seg)

    #print para verificar los parámetros a utilizar
    print(f"Parámetros: cantidad={cantidad}, distribucion={distribucion}, modo_espera={modo_espera}, "
      f"a={kwargs.get('a')}, espera={kwargs.get('espera')}, minimo={kwargs.get('minimo')}, maximo={kwargs.get('maximo')}")

    for idx, espera in zip(indices_preguntas, esperas):
        fila = df.iloc[idx]
        titulo = str(fila["titulo"]) if pd.notna(fila["titulo"]) else ""           #evitar que campos vacíos se interpreten por float (nan) por pandas  
        contenido = str(fila["contenido"]) if pd.notna(fila["contenido"]) else ""
        mejor_respuesta = str(fila["mejor_respuesta"]) if pd.notna(fila["mejor_respuesta"]) else ""

        #forma del payload con consulta, indice_pregunta y respuesta_popular
        payload = {"consulta": titulo + " " + contenido,
                    "indice_pregunta": int(idx), #convertir idx a int nativo de python (desde int64 de numpy)
                    "respuesta_popular": mejor_respuesta,
                    "intentos_calidad": 0
        }

        try:
            # Revisar si la pregunta ya está en la base de datos
            with db.conn.cursor() as cur:
                cur.execute("SELECT id FROM preguntas WHERE id = %s", (int(idx),))
                existe = cur.fetchone()
            
            if existe:
                db.incrementar_consulta(int(idx))
                print(f"Consulta existente en la DB con id={idcx} (incrementando número de consultas)")
            else:
                #Si no existe, enviar consulta al tópico preguntas_nuevas por Kafka
                print(f"Nueva pregunta id={idx} enviando a Kafka mediante tópico preguntas_nuevas")
                producer.send("preguntas_nuevas", value=payload)
                producer.flush()

        except Exception as e:
            print(f"Error al enviar idx={idx}: {e}")

        time.sleep(espera)

#CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #parser.add_argument("--url", type=str, default="http://cache:8000/query", help="URL del servicio de cache al que enviar las consultas, valor por defecto: 'http://cache:8000/query'")
    parser.add_argument("--cantidad", type=int, default=100, help="Número total de consultas a generar, valor por defecto: 100")
    parser.add_argument("--distribucion", type=str, choices=["uniforme", "zipf"], default="uniforme", help="Distribución para seleccionar preguntas del dataset, valor por defecto: uniforme")

    #argumento opcional en caso de elegir --distribucion zipf
    parser.add_argument("--a", type=float, default=1.5, help="Parámetro de forma para la distribución Zipf, valor por defecto: 1.5")
    
    parser.add_argument("--modo_espera", type=str, choices=["fijo", "uniforme"], default="fijo", help="Modo de espera entre consultas, valor por defecto: fijo")

    #argumento de cuantos segundos de espera se tienen entre consultas en caso de elegir espera --modo_espera fija
    parser.add_argument("--espera", type=float, default=1.0, help="Tiempo de espera fijo entre consultas (solo si modo_espera=fijo), valor por defecto: 1.0")

    #argumentos opcionales en caso de elegir --modo_espera uniforme
    parser.add_argument("--minimo", type=float, default=0.5, help="Tiempo mínimo de espera aleatoria (solo si modo_espera=uniforme), valor por defecto: 0.5")
    parser.add_argument("--maximo", type=float, default=2.0, help="Tiempo máximo de espera aleatoria (solo si modo_espera=uniforme), valor por defecto: 2.0")

    args = parser.parse_args()

    ejecutar_trafico(
        #url=args.url,
        cantidad=args.cantidad,
        distribucion=args.distribucion,
        modo_espera=args.modo_espera,
        a=args.a,
        espera=args.espera,
        minimo=args.minimo,
        maximo=args.maximo
)