# Import Dependencies 
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

spark = SparkSession \
       .builder \
       .appName("Kafka-to-local") \
       .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read Input 
kafka_read = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","de-capstone3")  \
        .option("startingOffsets", "earliest")  \
        .load()

updated_data= kafka_read \
      .withColumn('value_str',kafka_read['value'].cast('string').alias('key_str')).drop('value') \
      .drop('key','topic','partition','offset','timestamp','timestampType')

clickstream_data = updated_data.writeStream \
  .format("json") \
  .outputMode("append") \
  .option("truncate", "false") \
  .option("path", "clickstream_data/") \
  .option("checkpointLocation", "clickstream_data/cp/") \
  .start()

clickstream_data.awaitTermination()