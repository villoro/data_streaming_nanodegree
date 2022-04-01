from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.appName("events").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kdf = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

schema_json = T.StructType(
    [
        T.StructField("customer", T.StringType()),
        T.StructField("score", T.FloatType()),
        T.StructField("riskDate", T.DateType()),
    ]
)

kdf = (
    kdf.withColumn("value", F.col("value").cast("string"))
    .select(F.from_json("value", schema_json))
    .select("value.customer", "value.score")  # , "value.riskDate")
    # No need to create a view, but it would be done with
    # .createOrReplaceTempView("customer_risk")
)

(
    # The view is adding nothing here, but it would work like that:
    # spark.sql("SELECT customer, score FROM customer_risk")
    kdf.writeStream.outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
)
