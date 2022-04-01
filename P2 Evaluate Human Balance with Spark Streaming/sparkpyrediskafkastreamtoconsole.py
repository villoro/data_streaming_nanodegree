from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 1. Declare schemas
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

# 2. Set up spark app
spark = SparkSession.builder.appName("redis").getOrCreate()
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
)

# 6. Show wanted data from customers
(
    kdf_customers.where("email IS NOT NULL AND birthDay IS NOT NULL")
    .select("email", F.split("birthDay", "-").getItem(0).alias("birthYear"))
    .writeStream.outputMode("append")
    .format("console")
    .start()
    .awaitTermination()
)
