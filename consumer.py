from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# def writeToMongo(df,batchId):
#     df.write \
#         .format("mongo") \
#         .option('uri','mongodb://localhost') \
#         .option('database','spotifydb') \
#         .option('collection','spotifycol1') \
#         .mode('append') \
#         .save()
        
        
        
kafka_topic_name = "spotifyTopic"
kafka_bootstrap_servers = "localhost:29092"

def writeToMongoDB(df, epoch_id):
    # Define MongoDB output options
    mongo_uri = "mongodb://jack:jack@localhost:27017/spotifydb"
    collection='spotifycoll'
    df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri", mongo_uri) \
        .option("collection",collection) \
        .save()        

        
spark = SparkSession \
        .builder \
        .config("spark.jars.packages", '/home/jack/.ivy2/cache/org.apache.spark/spark-sql-kafka-0-10_2.12/jars/spark-sql-kafka-0-10_2.12-3.0.1.jar' + "," +"/home/jack/.ivy2/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-2.4.1.jar") \
        .appName("Spotify streaming") \
        .config("spark.mongodb.input.uri", "mongodb://localhost/spotifydb.spotifycoll") \
        .config("spark.mongodb.output.uri", "mongodb://localhost/spotifydb.spotifycoll") \
        .getOrCreate()
      
streamingDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)\
        .option("subscribe", kafka_topic_name) \
        .load()
streamingDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

stringDF = streamingDF.selectExpr("CAST(value AS STRING)")
schema = "name STRING, album STRING, artist STRING, danceability FLOAT"
csvDF = stringDF.select(from_csv(col("value"), schema).alias("data"))
schemaDF = csvDF.select("data.*")
finalDF = schemaDF.withColumn("partyHard?", when((schemaDF.danceability > 0.8), lit("ABSOLUTELY YESS!!!")) \
                                                .when((schemaDF.danceability >= 0.6) & \
                                                (schemaDF.danceability < 0.8), lit("Yes"))
                                                .when((schemaDF.danceability >= 0.4) & \
                                                (schemaDF.danceability < 0.6), lit("Maybe?")) \
                                                .when((schemaDF.danceability >= 0.3) & \
                                                (schemaDF.danceability < 0.5), lit("No"))
                                                .otherwise(lit("ABSOLUTELY NO!!!")))

query = finalDF \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .format("console") \
        .start()
queryToMongo = finalDF.writeStream.foreachBatch(writeToMongoDB).start()
queryToMongo.awaitTermination()
query.awaitTermination()