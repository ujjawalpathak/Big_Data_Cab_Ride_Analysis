from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Kafka-to-HDFS").master("local").getOrCreate()
spark

#Reading data from hdfs
df=spark.read.json("clickstream.json")

df.show(10,truncate=False)
#Selecting the columns from the clickstream data set
df=df.select(get_json_object(df['value_str'],"$.customer_id").alias("customer_id"),
            get_json_object(df['value_str'],"$.app_version").alias("app_version"),
            get_json_object(df['value_str'],"$.OS_version").alias("OS_version"),
            get_json_object(df['value_str'],"$.lat").alias("lat"),
            get_json_object(df['value_str'],"$.lon").alias("lon"),
            get_json_object(df['value_str'],"$.page_id").alias("page_id"),
            get_json_object(df['value_str'],"$.button_id").alias("button_id"),
            get_json_object(df['value_str'],"$.is_button_click").alias("is_button_click"),
            get_json_object(df['value_str'],"$.is_page_view").alias("is_page_view"),
            get_json_object(df['value_str'],"$.is_scroll_up").alias("is_scroll_up"),
            get_json_object(df['value_str'],"$.is_scroll_down").alias("is_scroll_down"),
            get_json_object(df['value_str'],"$.timestamp").alias("timestamp")
             )

df.printSchema()

df.show(10)

#Writing the dataset to hdfs
df.coalesce(1).write.format('csv').mode('overwrite').save('/user/hadoop/clickstream_flattened',header='true')