from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, udf, struct, to_json
import sparknlp
from sparknlp.base import DocumentAssembler, Finisher
from sparknlp.annotator import Tokenizer, StopWordsCleaner, Normalizer, LemmatizerModel
from pyspark.ml.feature import NGram, CountVectorizer
from pyspark.ml.clustering import LDA
from pyspark.ml import Pipeline
import logging
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TopicModelingConsumer")

spark = SparkSession.builder \
    .appName("TopicModelingConsumer") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0," \
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.1," \
            "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.3") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "120s") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.storage.memoryFraction", "0.6") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.default.parallelism", "100") \
    .getOrCreate()

schema = StructType([
    StructField("title", StringType(), True),
    StructField("content", StringType(), True)
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "fact-check-request") \
    .option("startingOffsets", "earliest") \
    .load()

df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

df = df.filter(col("content").isNotNull() & (col("content") != ""))

df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

document_assembler = DocumentAssembler().setInputCol("content").setOutputCol("document")
tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("tokens")
normalizer = Normalizer().setInputCols(["tokens"]).setOutputCol("normalized")
stopwords_cleaner = StopWordsCleaner().setInputCols(["normalized"]).setOutputCol("cleanTokens")

lemmatizer = LemmatizerModel.pretrained("lemma_antbnc", "en") \
    .setInputCols(["cleanTokens"]) \
    .setOutputCol("lemma")

finisher = Finisher().setInputCols(["lemma"]).setOutputCols(["lemma_result"])
ngram = NGram(n=2, inputCol="lemma_result", outputCol="bigrams")

vectorizer = CountVectorizer(inputCol="bigrams", outputCol="features")
lda = LDA(k=5, maxIter=10, featuresCol="features")

pipeline = Pipeline(stages=[document_assembler, tokenizer, normalizer, stopwords_cleaner, lemmatizer, finisher, ngram, vectorizer, lda])

convert_array_to_string = udf(lambda arr: ", ".join(arr) if isinstance(arr, list) else "", StringType())
convert_vector_to_array = udf(lambda vec: vec.toArray().tolist() if vec is not None else [], ArrayType(DoubleType()))

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        logger.info(f"⚠️ Nessun dato nel batch {batch_id}, salto il processamento.")
        return
    
    logger.info(f"📦 Batch {batch_id} ricevuto con {batch_df.count()} articoli.")

    batch_df = batch_df.cache()
    model = pipeline.fit(batch_df)
    transformed_df = model.transform(batch_df)

    transformed_df.printSchema()

    transformed_df = transformed_df.withColumn("bigrams", convert_array_to_string(col("bigrams")))
    transformed_df = transformed_df.withColumn("topicDistribution", convert_vector_to_array(col("topicDistribution")))

    final_df = transformed_df.select("title", "content", "bigrams", "topicDistribution")

    final_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "http://elasticsearch:9200") \
        .option("es.resource", "news-topics-index") \
        .option("checkpointLocation", "/tmp/spark-checkpoint-news-topics") \
        .mode("append") \
        .save()

    logger.info(f"✅ Batch {batch_id} elaborato con successo e scritto su Elasticsearch!")

query = df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark-checkpoint-news-topics") \
    .start()

query.awaitTermination()