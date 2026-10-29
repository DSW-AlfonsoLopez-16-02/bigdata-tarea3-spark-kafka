from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (SparkSession.builder
         .appName("Streaming311")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("unique_key", IntegerType()),
    StructField("complaint_type", StringType()),
    StructField("agency", StringType()),
    StructField("borough", StringType()),
    StructField("created_date", StringType()),
])

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "solicitudes_311")
       .option("startingOffsets", "latest")
       .load())

eventos = (raw.select(from_json(col("value").cast("string"), schema).alias("d"))
           .select("d.*")
           .withColumn("ts", to_timestamp("created_date", "yyyy-MM-dd HH:mm:ss")))

resultado = (eventos
             .groupBy(window(col("ts"), "1 minute"), col("complaint_type"))
             .agg(count("*").alias("solicitudes"))
             .orderBy(col("window").desc(), col("solicitudes").desc()))

query = (resultado.writeStream
         .outputMode("complete")
         .format("console")
         .option("truncate", "false")
         .start())

query.awaitTermination()
