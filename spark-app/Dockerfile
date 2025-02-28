# Usa l'immagine ufficiale di Bitnami Spark
FROM bitnami/spark:latest

# Imposta l'utente root per installare pacchetti di sistema
USER root

# Installa pacchetti di sistema necessari (wget incluso)
RUN apt-get update && \
    apt-get install -y curl wget && \
    rm -rf /var/lib/apt/lists/*

# Installa kafka-python manualmente
RUN pip install --no-cache-dir kafka-python

# Installa il connettore Elasticsearch compatibile con Spark 3.5
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.14.0/elasticsearch-spark-30_2.12-8.14.0.jar

# Copia il codice dell'applicazione nella cartella /app
COPY . /app
WORKDIR /app

# Installa le dipendenze Python, incluso numpy, se il file requirements.txt esiste
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Crea uno script di attesa per Elasticsearch
RUN echo -e '#!/bin/bash\n\
until curl -s http://elasticsearch:9200/_cluster/health?pretty | grep -q "green\\|yellow"; do\n\
  echo "Waiting for Elasticsearch..."\n\
  sleep 2\n\
done\n\
echo "Elasticsearch is ready!"' > wait-for-services.sh && chmod +x wait-for-services.sh

# Ritorna all'utente Spark (Bitnami usa UID 1001 per evitare problemi di permessi)
USER 1001

# Avvia lo script di attesa e poi esegui lo script Spark
CMD ["bash", "-c", "./wait-for-services.sh && python spark_fake_news_consumer.py & python spark_topic_modeling_consumer.py"]