from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import rand


def load_sources(spark):

    # Customers CSV
    customers = spark.read.csv(
        "sample_data/customers.csv", header=True, inferSchema=True
    )

    # Transactions JSON
    tx = spark.read.json("sample_data/transactions_json")

    # Compliance Notes TXT
    notes = spark.read.text("sample_data/compliance_notes.txt") \
        .withColumn("customer_id", split(col("value"), "\\|")[0]) \
        .withColumn("note", split(col("value"), "\\|")[2]) \
        .select("customer_id", "note")

    # Empty devices (not used)
    schema = StructType([
        StructField("device_id", StringType()),
        StructField("typing_score", DoubleType()),
        StructField("motion_score", DoubleType())
    ])
    devices = spark.createDataFrame([], schema)

    # Historical
    history = spark.read.parquet("sample_data/historical_fraud.parquet")

    return customers, tx, notes, devices, history



def enrich(customers, tx, notes, devices, history):

    # Extract lat / lon if exists
    if "location" in tx.columns:
        tx = tx.withColumn("lat", col("location.lat")) \
               .withColumn("lon", col("location.lon")) \
               .drop("location")
    else:
        # ADD SYNTHETIC GEO COORDINATES HERE (CORRECT PLACE)
        tx = tx.withColumn("lat", rand()*10 + 20) \
               .withColumn("lon", rand()*10 + 70)

    # Convert timestamp
    tx = tx.withColumn("ts", to_timestamp(col("timestamp")))

    # Join everything
    df = tx.join(customers, "customer_id", "left") \
           .join(notes, "customer_id", "left")

    # Device mismatch
    df = df.withColumn("device_mismatch",
                       when(col("device_id") != col("registered_device"), 1).otherwise(0))

    # Time based flag
    df = df.withColumn("is_night",
                       when((hour(col("ts")) < 6) | (hour(col("ts")) >= 23), 1).otherwise(0))

    # Risk score
    df = df.withColumn("risk_score",
                       col("device_mismatch") * 2 +
                       col("is_night") * 1 +
                       (col("amount") / 1000))

    # Risk category
    df = df.withColumn("risk_level",
                       when(col("risk_score") >= 4, "HIGH")
                       .when(col("risk_score") >= 2, "MEDIUM")
                       .otherwise("LOW"))

    return df



def save_outputs(df):

    df.write.mode("overwrite").parquet("output/final/fused/")

    alerts = df.filter(col("risk_level") == "HIGH")
    alerts.write.mode("overwrite").json("output/alerts/")

    ag = df.groupBy("customer_id").agg(
        count("*").alias("total_tx"),
        avg("amount").alias("avg_amt")
    )
    ag.write.mode("overwrite").parquet("output/aggregates/")


def main():
    spark = SparkSession.builder \
        .appName("FraudPipelineFixed") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    customers, tx, notes, devices, history = load_sources(spark)

    final_df = enrich(customers, tx, notes, devices, history)

    save_outputs(final_df)

    spark.stop()


if __name__ == "__main__":
    main()
