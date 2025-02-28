from flask import Flask, render_template, request, jsonify
from kafka import KafkaProducer
from kafka.errors import KafkaError
from elasticsearch import Elasticsearch
import json
import logging
import requests

app = Flask(__name__)

# Configurazione del logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FakeNewsApp")

# Configurazione Kafka
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "fact-check-request"

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logger.info("✅ Kafka Producer connesso con successo!")
except KafkaError as e:
    logger.error(f"❌ Errore durante la connessione a Kafka: {e}")
    producer = None

# Configurazione Elasticsearch
try:
    es = Elasticsearch(["http://elasticsearch:9200"])
    if not es.ping():
        raise ValueError("Elasticsearch non è raggiungibile!")
    logger.info("✅ Elasticsearch connesso con successo!")
except Exception as e:
    logger.error(f"❌ Errore nella connessione a Elasticsearch: {e}")
    es = None

# Funzione per interrogare Google Fact Check API
def check_fake_news(title):
    """Interroga Google Fact Check API e restituisce un elenco di risultati di fact-checking."""
    api_url = "https://factchecktools.googleapis.com/v1alpha1/claims:search"
    api_key = "AIzaSyCoUxLPkO4-FQM99eBZhwhJ3L-Gqgsbp7w"  # Sostituisci con una chiave API valida

    params = {"query": title, "key": api_key}
    try:
        response = requests.get(api_url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "claims" in data and len(data["claims"]) > 0:
                fact_checks = []
                for claim in data["claims"]:
                    review = claim.get("claimReview", [{}])[0]
                    fact_checks.append({
                        "title": claim.get("text", "N/A"),
                        "content": claim.get("text", "N/A"),  # Recupera il contenuto reale
                        "rating": review.get("textualRating", "Unknown"),
                        "source": review.get("publisher", {}).get("name", "Unknown Source"),
                        "url": review.get("url", "#"),
                    })
                return fact_checks  # Restituisce un elenco di verifiche
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Errore nella richiesta a Google Fact Check API: {e}")

    return []  # Ritorna un elenco vuoto se non trova nulla

@app.route("/")
def home():
    """Mostra la home page."""
    return render_template("index.html")

@app.route("/verify", methods=["POST"])
def verify():
    """Riceve il titolo, recupera il contenuto reale e lo invia a Kafka."""
    title = request.form.get("title")

    if not title:
        return jsonify({"error": "Il titolo è obbligatorio"}), 400

    # 🔍 1️⃣ Prova a recuperare il contenuto reale da Elasticsearch
    content = None

    if es:
        try:
            response = es.search(index="fake-news-index", body={"query": {"match": {"title": title}}})
            hits = response.get("hits", {}).get("hits", [])
            if hits:
                content = hits[0]["_source"].get("content", None)
                logger.info(f"✅ Contenuto trovato su Elasticsearch: {content}")
        except Exception as e:
            logger.error(f"❌ Errore durante la ricerca su Elasticsearch: {e}")

    # 🧐 2️⃣ Se Elasticsearch non ha il contenuto, prova con Google Fact Check API
    if not content:
        google_results = check_fake_news(title)
        if google_results:
            content = google_results[0]["content"]  # Prende il primo risultato disponibile
            logger.info(f"🔎 Contenuto trovato su Google Fact Check API: {content}")

    # 🛑 3️⃣ Se ancora non trova nulla, assegna un messaggio di fallback
    if not content:
        content = f"⚠ Nessun contenuto disponibile per '{title}'."  # Messaggio di default

    if not producer:
        return jsonify({"error": "Kafka non è disponibile"}), 500

    try:
        message = {"title": title, "content": content}
        producer.send(KAFKA_TOPIC, message)
        producer.flush()
        logger.info(f"📨 Messaggio inviato a Kafka con contenuto REALE: {message}")
    except KafkaError as e:
        logger.error(f"❌ Errore nell'invio del messaggio a Kafka: {e}")
        return jsonify({"error": "Errore nell'invio del messaggio"}), 500

    return render_template("loading.html", title=title)

@app.route("/results", methods=["GET"])
def results():
    """Recupera i risultati da Elasticsearch e Google Fact Check API."""
    title = request.args.get("title")

    if not title:
        return jsonify({"error": "Il titolo è obbligatorio"}), 400

    # Prima prova a recuperare i dati da Elasticsearch
    es_result = None

    if es:
        try:
            response = es.search(index="fake-news-index", body={"query": {"match": {"title": title}}})
            hits = response.get("hits", {}).get("hits", [])
            if hits:
                es_result = hits[0]["_source"]
                logger.info(f"✅ Risultato trovato per '{title}' in Elasticsearch: {es_result}")
        except Exception as e:
            logger.error(f"❌ Errore durante la ricerca su Elasticsearch: {e}")

    # Se Elasticsearch non ha dati, interroga Google Fact Check API
    google_fact_check_results = check_fake_news(title)

    return render_template("results.html", title=title, es_result=es_result, google_results=google_fact_check_results)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
