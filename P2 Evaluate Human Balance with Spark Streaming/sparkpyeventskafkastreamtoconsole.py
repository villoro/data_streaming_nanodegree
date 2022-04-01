from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 1. Declare schemas
schema_events = T.StructType(
    [
        T.StructField("customer", T.StringType()),
        T.StructField("score", T.FloatType()),
        T.StructField("riskDate", T.DateType()),
    ]
)

# 2. Set spark session
spark = SparkSession.builder.appName("events").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 3. Load stream
kdf = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Extract risk score
kdf = (
    kdf.withColumn("value", F.col("value").cast("string"))
    .withColumn("value", F.from_json("value", schema_events))
    .select("value.customer", "value.score")  # , "value.riskDate")
    # No need to create a view, but it would be done with
    # .createOrReplaceTempView("customer_risk")
)

# 5. Show risk score
(
    # The view is adding nothing here, but it would work like that:
    # spark.sql("SELECT customer, score FROM customer_risk")
    kdf.writeStream.outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
)
