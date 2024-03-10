from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("datewise_bookings_aggregates_spark").master("local").getOrCreate()
spark

#Reading data from HDFS
df=spark.read.csv("/user/hadoop/cab_rides/part-m-00000")

df.show(10)

df.printSchema()

#The count of the dataset
df.count()

#Renaming the columns 
new_col = ["booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon","pickup_timestamp","drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count"]

new_df = df.toDF(*new_col)

new_df.show(truncate=False)

#Converting pickup_timestamp to date by extracting date from pickup_timestamp for aggregation
new_df=new_df.select("booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon",to_date(col('pickup_timestamp')).alias('pickup_date').cast("date"),"drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count")

new_df.show()

#Aggregation on pickup_date
agg_df=new_df.groupBy("pickup_date").count().orderBy("pickup_date")

agg_df.show()

#The count of Bookings aggregates_table
agg_df.count()

agg_df.coalesce(1).write.format('csv').mode('overwrite').save('/user/hadoop/datewise_bookings_agg',header='true')
