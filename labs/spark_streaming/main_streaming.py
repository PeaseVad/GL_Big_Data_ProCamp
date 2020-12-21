from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys


spark = SparkSession.builder.appName('StructuredStreaming').getOrCreate()

if len(sys.argv) == 4:
    bucket = sys.argv[1]
    folder  = sys.argv[2]
    topic  =  sys.argv[3]
    
else: 
    print('Please enter bucket,folder and topic as line arguments')
    sys.exit()

    

sys.argv[1:]

schema = StructType([
    StructField("data",StructType([
    StructField("id", LongType(), True),
    StructField("id_str", StringType(), True),
    StructField("order_type", LongType(), True),
    StructField("datetime", StringType(), True),
    StructField("microtimestamp", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("amount_str", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("price_str", StringType(), True)])),
    StructField("channel", StringType(), True),
    StructField("event", StringType(), True)
])


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", topic ) \
  .load() \
  .selectExpr("CAST(value AS STRING)").withColumn("value",(from_json("value", schema))).select("value.*").withColumn("timestamp", from_unixtime("data.datetime").cast(TimestampType()))

df_result = df \
    .withWatermark("timestamp", "3 minutes") \
    .groupBy(
        window("timestamp", "1 minutes")\
 ).agg(count(col("data.id")).alias("count"), mean(col("data.price")).alias("average_price"), (sum(col("data.price"))* sum(col("data.amount"))).alias("total_sale"))


df_result\
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/bdpc/checkpoint/") \
    .option("path", f"gs://{bucket}/{folder}/") \
    .trigger(processingTime='10 seconds') \
    .outputMode("append")\
    .start() \
    .awaitTermination()