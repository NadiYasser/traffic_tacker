from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Spark Session Config
spark = ( SparkSession.builder
         .appName("TrafficStreamingLakehouse")
         .master("spark://spark-master:7077")
         .config("spark.sql.extensions",
                 "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog",
                 "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .enableHiveSupport()
         .getOrCreate()
         )

spark.sparkContext.setLogLevel("WARN")

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","Kafka:9092")
    .option("subscribe","traffic-topic")
    .option("StartingOffsets","latest")
    .load()
)
