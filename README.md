# Fake News Detection

## Descrizione
Questo progetto utilizza una combinazione di tecnologie avanzate per rilevare e analizzare fake news in tempo reale. Il sistema sfrutta **Flask, Logstash, Kafka, Spark Streaming, Spark NLP, Elasticsearch e Kibana**, oltre a **Google Fact Check API** per la verifica delle notizie.

## Tecnologie Utilizzate
- **Flask**: Framework per la creazione di API REST.
- **Kafka**: Sistema di messaggistica distribuito per lo streaming di dati in tempo reale.
- **Spark Streaming**: Elaborazione continua dei dati provenienti da Kafka.
- **Spark NLP**: Elaborazione del linguaggio naturale per l'analisi testuale.
- **Elasticsearch**: Motore di ricerca e database per l'archiviazione dei dati.
- **Logstash**: Strumento per l'acquisizione e l'integrazione dei dati.
- **Kibana**: Dashboard interattiva per la visualizzazione dei dati.
- **Docker e Docker Compose**: Containerizzazione e orchestrazione dei servizi per una gestione più semplice.

## Installazione
### 1. Clona il Repository
```sh
git clone https://github.com/TUO-NOME-UTENTE/fake-news-detection.git
cd fake-news-detection
```

### 2. Avvia i Servizi con Docker Compose
Assicurati di avere **Docker** e **Docker Compose** installati, poi esegui:
```sh
docker-compose up -d
```

### 3. Crea i Topic di Kafka
```sh
docker exec -it fake-news-detection-kafka kafka-topics.sh --create --topic fake-news --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1

docker exec -it fake-news-detection-kafka kafka-topics.sh --create --topic news-topics --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```

### 4. Crea gli Indici Elasticsearch
```sh
docker exec -it fake-news-detection-elasticsearch curl -X PUT "http://localhost:9200/fact-check-results"
docker exec -it fake-news-detection-elasticsearch curl -X PUT "http://localhost:9200/fake-news-index"
docker exec -it fake-news-detection-elasticsearch curl -X PUT "http://localhost:9200/news-topics-index"
```

### 5. Configura Elasticsearch
```sh
Invoke-RestMethod -Uri "http://localhost:9200/_settings" -Method Put -Headers @{"Content-Type"="application/json"} -Body '{"index": {"number_of_replicas": 0}}'

oppure

```
```sh
curl -X PUT "http://localhost:9200/_settings" -H "Content-Type: application/json" -d '{"index": {"number_of_replicas": 0}}'
```

## Accesso ai Servizi
- **API Flask**: `http://localhost:8000`
- **Kibana**: `http://localhost:5601`
- **Elasticsearch**: `http://localhost:9200`

## Contribuire
Fai un fork del progetto, lavora sulle modifiche e invia una pull request!

## Licenza
Questo progetto è distribuito sotto la licenza MIT.

