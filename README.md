# TareaSD
Repositorio de la tarea 1 para sistemas distribuidos
### Levantar los contenedores, luego de clonar la repo, adentrandose en el directorio, se levantan los contenedores mediante
sudo docker-compose up --build
### Generador de tráfico
generador_trafico siempre tiene que ejecutar generador_trafico.py con argumentos de distribución ("uniforme" o "zipf") y cantidad de consultas, como se muestra a continuación:

ejemplos:
docker-compose run generador python generador_trafico.py --cantidad 50 --distribucion zipf --modo_espera fijo --espera 5

docker-compose run generador python generador_trafico.py \
  --cantidad 50 \
  --distribucion zipf \
  --modo_espera fijo \
  --espera 2.0 \
  --a 1.2

### Entrar en la base de datos luego de levantar los contenedores
$ sudo docker exec -it tarea1-db-1 psql -U postgres -d db_consultas
Dentro de la base de datos se aconseja utilizar \x on para visualizar de forma extendida los valores de string, que pueden ser bastante largos
