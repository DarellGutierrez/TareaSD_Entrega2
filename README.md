# TareaSD
Repositorio de la tarea 1 para sistemas distribuidos
### Levantar los contenedores, luego de clonar la repo, adentrandose en el directorio, se levantan los contenedores mediante
sudo docker-compose up --build
### Borrar los contenedores junto con volumen de db para pruebas
sudo docker-compose down -vc
### Generador de tráfico
generador_trafico siempre tiene que ejecutar generador_trafico.py con argumentos de distribución ("uniforme" o "zipf") y cantidad de consultas, como se muestra a continuación:

ejemplos:
sudo docker-compose run generador python generador_trafico.py --cantidad 30000 --distribucion zipf --a 1.01 --modo_espera fijo --espera 4

docker-compose run generador python generador_trafico.py \
  --cantidad 50 \
  --distribucion zipf \
  --modo_espera fijo \
  --espera 2.0 \
  --a 1.2

### Entrar en la base de datos luego de levantar los contenedores
$ sudo docker exec -it tarea1-db-1 psql -U postgres -d db_consultas
Dentro de la base de datos se aconseja utilizar \x on para visualizar de forma extendida los valores de string, que pueden ser bastante largos

### Cómo cambiar parámetros de cache
En docker-compose editar las variables de entorno "REDIS_POLICY=allkeys-lru/lfu" para elegir entre LRU y LFU y "REDIS_MAXMEMORY=xmb" para configurar x mb tamaño de cache, para el módulo cache 

### Cómo realizar pruebas de rendimiento de parámetro de cache
Para realizar las pruebas de rendimiento de parámetro de cache es conveniente poder "deshabilitar" el módulo score, ya que no nos interesa para el hit rate y estaríamos limitando velocidad de consultas y gastando tokens de gemini, para hacer esto se edita en docker-compose en el módulo score la variable de entorno "MOCK_GEMINI" a 1 para pruebas y devuelta a 0 para utilizar el módulo correctamente