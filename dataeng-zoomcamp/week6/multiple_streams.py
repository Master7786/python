from pyspark.sql import SparkSession


def main():
    spark = SparkSession \
        .builder \
        .appName("Structured Streaming ") \
        .master("local[1]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from topic
    df_high = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka://localhost:9092") \
        .option("subscribe", "datatalks.yellow_taxi_rides.high_amount") \
        .option("startingOffsets", "latest") \
        .load()

    df_low = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka://localhost:9092") \
        .option("subscribe", "datatalks.yellow_taxi_rides.low_amount") \
        .option("startingOffsets", "latest") \
        .load()

    joined_df = df_high.join(df_low, "vendorId")
    joined_df.show()


if __name__ == '__main__':
    main()
