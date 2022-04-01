from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

schema_redis = T.StructType(
    [
        T.StructField("key", T.StringType()),
        T.StructField("existType", T.StringType()),
        T.StructField("Ch", T.BooleanType()),
        T.StructField("Incr", T.BooleanType()),
        T.StructField(
            "zSetEntries",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("element", T.StringType()),
                        T.StructField("Score", T.IntegerType()),
                    ]
                )
            ),
        ),
    ]
)
schema_customer = T.StructType(
    [
        T.StructField("name", T.StringType()),
        T.StructField("email", T.StringType()),
        T.StructField("phone", T.StringType()),
        T.StructField("birthDay", T.StringType()),
    ]
)
schema_events = T.StructType(
    [
        T.StructField("customer", T.StringType()),
        T.StructField("score", T.FloatType()),
        T.StructField("riskDate", T.DateType()),
    ]
)

# 2. Set up spark app
spark = SparkSession.builder.appName("join").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 3. Read stream
kdf_redis = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Extract redis_data from the json
kdf_redis = (
    kdf_redis.withColumn("value", F.col("value").cast("string"))
    .withColumn("value", F.from_json("value", schema_redis))
    .select("value.existType", "value.Ch", "value.Incr", "value.zSetEntries")
    # No need to create a view, but it would be done with
    # .createOrReplaceTempView("redis_data")
)

# 5. Retrive customer data
kdf_customers = (
    # The view is adding nothing here, but it would work like that:
    # spark.sql("SELECT zSetEntries[0].element AS b64_customer FROM redis_data")
    kdf_redis.selectExpr("zSetEntries[0].element AS b64_customer")
    .withColumn("customer", F.unbase64("b64_customer").cast("string"))
    .withColumn("customer", F.from_json("customer", schema_customer))
    .select("customer.name", "value.email", "value.phone", "value.birthDay")
    # Extract the data we want
    .where("email IS NOT NULL AND birthDay IS NOT NULL")
    .select("email", F.split("birthDay", "-").getItem(0).alias("birthYear"))
)

# 6. Read events
kdf_events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

# 7. Get scores
kdf_scores = (
    kdf_events.withColumn("value", F.col("value").cast("string"))
    .withColumn("value", F.from_json("value", schema_events))
    .select("value.customer", "value.score")  # , "value.riskDate")
)

# 8. Stream final data
(
    kdf_scores.join(kdf_customers, F.expr("customer = email"))
    .select(F.col("customer").alias("key"), F.to_json(F.struct("*")).alias("value"))
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "stedi-score")
    .option("checkpointLocation", "/tmp/kafkacheckpoint")
    .start()
    .awaitTermination()
)
