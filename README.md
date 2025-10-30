# TareaSD - Segundo entregable
Repositorio de la Tarea 1 de Sistemas Distribuidos.  
Esta tarea tiene como objetivo implementar un sistema distribuido con generador de tráfico, caché, score de calidad y almacenamiento para comparar respuestas generadas por un LLM con respuestas reales de Yahoo! Answers.

---

## Despliegue

Clonar el repositorio y levantar los contenedores:
```bash
git clone https://github.com/DarellGutierrez/TareaSD_Entrega2.git

cd TareaSD_Entrega2

sudo docker-compose up -d --build
```
El resto de la documentación asume que los contenedores están levantados

## Generador de tráfico
El contenedor generador_trafico ejecuta generador_trafico.py con parámetros de distribución, cantidad de consultas, y cadencia de las consultas.

### Argumentos de generador_trafico.py
- --cantidad N: número de consultas a generar
- --distribucion {uniforme,zipf}: tipo de distribución.
- --modo_espera {fijo,uniforme}: cómo espaciar las consultas
- --espera S: segundos entre consultas (cuando --modo_espera fijo)
- --a α: parámetro alfa de la distribución zipf
- --minimo m: tiempo mínimo en segundos de espera aleatoria (cuando --modo_espera uniforme)
- --maximo M: tiempo máximo en segundos de espera aleatoria (cuando --modo_espera uniforme)

### Ejemplos
```bash
# 30k consultas, distribución zipf con alfa=1.01, espera fija de 4s
sudo docker-compose run generador \
python generador_trafico.py --cantidad 30000 --distribucion zipf --a 1.01 --modo_espera fijo --espera 4

# 50k consultas, distribución uniforme, modo de espera uniforme entre 3 y 5 segundos.
sudo docker-compose run generador \
python generador_trafico.py --cantidad 50000 --distribucion uniforme --modo_espera uniforme --minimo 3 --maximo 5
```
Para consultar las métricas desde el módulo cache, luego de realizar generar el tráfico:
```bash
curl http://localhost:8000/metrics
```
## Base de datos
Se puede entrar a la base de datos PostgreSQL mediante el siguiente comando

```bash
sudo docker exec -it tareasd_entrega2-db-1 psql -U postgres -d db_consultas
```
Una vez dentro se recomienda activar la vista extendida para visualizar las consultas
```sql
\x on
```
Consultas útiles
```sql
SELECT count(*) FROM preguntas; --regresa la cantidad de preguntas únicas
SELECT sum(numero_consultas) FROM preguntas; --regresa la cantidad de consultas realizadas
SELECT id, numero_consultas FROM preguntas ORDER BY numero_consultas DESC; --regresa el número de fila de la pregunta (en el dataset) y la cantidad de veces que se consultó ordenado de mayor a menor
```
## Caché
Parámetros configurables en **docker-compose.yml** (módulo cache):
- Política de remoción:
```yaml
REDIS_POLICY=allkeys-lfu  # o allkeys-lfu
```
- Tamaño de caché:
```yaml
REDIS_MAXMEMORY=4mb
```
### Pruebas de rendimiento para caché
Para analizar hit rate sin gastar tokens de LLM, deshabilitar el módulo score en **docker-compose.yml**
```yaml
MOCK_GEMINI=1
```
Para usar el LLM real:
```yaml
MOCK_GEMINI=0
```
## Servicio LLM
Asegurar configurar el módulo `servicio-llm` en **docker-compose.yml** agregando tu llave API de gemini:
```yaml
- GOOGLE_API_KEY=tu-llave-API
```
Para cambiar el modelo de gemini a utilizar para las consultas se modifica la variable de entorno GEMINI_MODEL:
```yaml
- GEMINI_MODEL=gemini-2.5-flash-lite # reemplazar por modelo a elección
```
## Flink
Se puede configurar el módulo de `flink-job-submitter` en **docker-compose.yml** modificando el umbral de aceptación y el número de reintentos máximo para calidad:
```yaml
- SCORE_THRESHOLD=0.25 # Umbral de aceptación de respuestas para guardar en la db
- MAX_QUALITY_RETRIES=3
```

## Estructura de servicios
- Kafka -> genera tópicos para que los demás servicios se comuniquen de forma asíncrona.
- Generador de tráfico -> simula consultas que no están en la base de datos y las envía mediante tópico preguntas_nuevas de Kafka.
- Servicio LLM -> consume el tópico de preguntas_nuevas y obtiene una respuesta del LLM (gemini),la cuál manda mediante el tópico de respuesta_exitosa, o hacia los tópicos de error reintentos_cuota o reintentos_sobrecarga dependiendo del tipo de error.
- Servicios de reintento -> consumen los tópicos reintentos_cuota y reintentos_sobrecarga y manejan los errores para volver a enviar los mensajes al tópico preguntas_nuevas.
- Flink -> consume el tópico respuestas_existosas, calcula la métrica de score mediante TF-IDF y un umbral definido de 0.25, si el score está sobre el umbral la almacena en la base de datos, si está bajo el umbral reenvia el mensaje al tópico preguntas_nuevas intentando obtener una mejor respuesta, evitando ciclos infinitos de reintento.
- Base de datos PostgreSQL -> persiste preguntas, respuestas y métricas de score.

## Bajar los contenedores
Para bajar los contenedores, asegurando de eliminar servicios huérfanos del módulo generador, se utiliza el siguiente comando:
```bash
sudo docker-compose down --remove-orphans
``` 
Si se busca eliminar el volúmen que alberga la base de datos se adjunta el argumento -v:
```bash
sudo docker-compose down --remove-orphans -v
```
