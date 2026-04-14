from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
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

# convert binary to string

json_stream = raw_stream.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "timestamp as kafka_timestamp"
)

# flexible Schema
traffic_schema = StructType([
    StructField("vehicule_id",StringType()),
    StructField("road_id",StringType()),
    StructField("city_zone",StringType()),
    StructField("speed",StringType()),
    StructField("congestion_level",IntegerType()),
    StructField("weather",StringType()),
    StructField("event_time",StringType()),

])

parsed = json_stream.withColumn(
    "data",
    from_json(col("raw_json"),traffic_schema)
)

flattened = parsed.select("raw_json","kafka_timestamp")