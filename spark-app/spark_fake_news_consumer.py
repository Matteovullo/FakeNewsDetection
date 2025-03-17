from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType
import requests
import time
import logging
from functools import lru_cache

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FakeNewsConsumer")

spark = SparkSession.builder \
    .appName("FakeNewsConsumer") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.0") \
    .getOrCreate()

schema = StructType([
    StructField("title", StringType(), True),
    StructField("content", StringType(), True)
])

@lru_cache(maxsize=1000)
def check_fake_news(title):
    """Interroga Google Fact Check API per verificare se un titolo è una fake news."""
    api_url = "https://factchecktools.googleapis.com/v1alpha1/claims:search"
    api_key = "AIzaSyCoUxLPkO4-FQM99eBZhwhJ3L-Gqgsbp7w"  

    params = {"query": title, "key": api_key}
    try:
        response = requests.get(api_url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "claims" in data and len(data["claims"]) > 0:
                return data["claims"][0]["claimReview"][0]["textualRating"]
        else:
            logger.warning(f"API request failed ({response.status_code}) for title: {title}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
    return "Unknown"

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "fact-check-request") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logger.info(f"Nessun dato da processare nel batch {batch_id}")
        return

    logger.info(f"📦 Processing batch {batch_id}")

    unique_titles = batch_df.select("title").distinct().rdd.map(lambda row: row.title).collect()
    title_ratings = {title: check_fake_news(title) for title in unique_titles}

    def get_rating(title):
        return title_ratings.get(title, "Unknown")

    rating_udf = udf(get_rating, StringType())
    batch_df = batch_df.withColumn("fake_news_rating", rating_udf(col("title")))

    try:
        batch_df.write.format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "http://elasticsearch:9200") \
            .option("es.port", "9200") \
            .option("es.resource", "fake-news-index") \
            .option("es.nodes.wan.only", "false") \
            .option("es.mapping.id", "title") \
            .mode("append") \
            .save()
        logger.info("✅ Scrittura su Elasticsearch completata con successo!")
    except Exception as e:
        logger.error(f"❌ Errore durante la scrittura su Elasticsearch: {e}")

query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoint-fake-news") \
    .start()

query.awaitTermination()
