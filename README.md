# TareaSD
Repositorio de la tarea 1 para sistemas distribuidos
### Levantar los contenedores, luego de clonar la repo, adentrandose en el directorio, se levantan los contenedores mediante
sudo docker-compose up --build
### Generador de tráfico
generador_trafico siempre tiene que ejecutar generador_trafico.py con argumentos de distribución ("uniforme" o "zipf") y cantidad de consultas, como se muestra a continuación:

ejemplos:
docker-compose run generador python generador_trafico.py --cantidad 50 --distribucion zipf --modo_espera fijo

docker-compose run generador python generador_trafico.py \
  --cantidad 50 \
  --distribucion zipf \
  --modo_espera fijo \
  --espera 2.0 \
  --a 1.2
