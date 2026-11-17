import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, dayofweek, from_json, hour, month, to_timestamp, year
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# 1. تشغيل سبارك مع إضافة كافكا
spark = (
    SparkSession.builder.appName("TrafficAccidentsStreaming")
    .config("spark.driver.memory", "1g")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. تعريف شكل الداتا اللي جاية من كافكا
schema = StructType([
    StructField("Accident_ID", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("Severity", IntegerType(), True),
    StructField("Weather_Condition", StringType(), True),
    StructField("Road_Condition", StringType(), True),
    StructField("Contributing_Factor", StringType(), True),
    StructField("Vehicle_Type", StringType(), True),
])

# 3. قراءة البث المباشر من كافكا
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "cleaned-accidents")
    .option("startingOffsets", "earliest")
    .load()
)

parsed_df = (
    raw_stream.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("Timestamp")))
)

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "traffic_db",
    "user": "yahya",
    "password": "yahya2026pass",
}


# 4. دالة الحفظ بنظام Upsert (لو الصف موجود اجمع عليه، لو مش موجود ضيفه)
def write_to_postgres(batch_df, batch_id):
  if batch_df.isEmpty():
    return

  batch_df.cache()

  geo_rows = [
      (r["city"], r["state"], r["accident_count"], r["avg_severity"])
      for r in batch_df.select(
          col("City").alias("city"), col("State").alias("state"), col("Severity")
      )
      .groupBy("city", "state")
      .agg(
          count("*").alias("accident_count"),
          avg("Severity").alias("avg_severity"),
      )
      .collect()
  ]

  time_rows = [
      (
          r["accident_hour"],
          r["day_of_week"],
          r["accident_month"],
          r["accident_year"],
          r["accident_count"],
      )
      for r in batch_df.withColumn("accident_hour", hour("event_time"))
      .withColumn("day_of_week", dayofweek("event_time"))
      .withColumn("accident_month", month("event_time"))
      .withColumn("accident_year", year("event_time"))
      .groupBy(
          "accident_hour", "day_of_week", "accident_month", "accident_year"
      )
      .agg(count("*").alias("accident_count"))
      .collect()
  ]

  weather_rows = [
      (r["weather_condition"], r["road_condition"], r["accident_count"])
      for r in batch_df.select(
          col("Weather_Condition").alias("weather_condition"),
          col("Road_Condition").alias("road_condition"),
      )
      .groupBy("weather_condition", "road_condition")
      .agg(count("*").alias("accident_count"))
      .collect()
  ]

  factor_rows = [
      (r["contributing_factor"], r["accident_count"])
      for r in batch_df.select(
          col("Contributing_Factor").alias("contributing_factor")
      )
      .groupBy("contributing_factor")
      .agg(count("*").alias("accident_count"))
      .collect()
  ]

  vehicle_rows = [
      (r["vehicle_type"], r["accident_count"])
      for r in batch_df.select(col("Vehicle_Type").alias("vehicle_type"))
      .groupBy("vehicle_type")
      .agg(count("*").alias("accident_count"))
      .collect()
  ]

  conn = psycopg2.connect(**DB_CONFIG)
  cur = conn.cursor()

  # 1. geo_accidents Upsert (بيحسب المتوسط المرجح الجديد ويجمع العداد)
  execute_values(
      cur,
      """
        INSERT INTO geo_accidents (city, state, accident_count, avg_severity)
        VALUES %s
        ON CONFLICT (city, state) DO UPDATE SET
            avg_severity = ((geo_accidents.avg_severity * geo_accidents.accident_count) + (EXCLUDED.avg_severity * EXCLUDED.accident_count))
                           / (geo_accidents.accident_count + EXCLUDED.accident_count),
            accident_count = geo_accidents.accident_count + EXCLUDED.accident_count;
    """,
      geo_rows,
  )

  # 2. time_accidents Upsert
  execute_values(
      cur,
      """
        INSERT INTO time_accidents (accident_hour, day_of_week, accident_month, accident_year, accident_count)
        VALUES %s
        ON CONFLICT (accident_hour, day_of_week, accident_month, accident_year) DO UPDATE SET
            accident_count = time_accidents.accident_count + EXCLUDED.accident_count;
    """,
      time_rows,
  )

  # 3. weather_road_accidents Upsert
  execute_values(
      cur,
      """
        INSERT INTO weather_road_accidents (weather_condition, road_condition, accident_count)
        VALUES %s
        ON CONFLICT (weather_condition, road_condition) DO UPDATE SET
            accident_count = weather_road_accidents.accident_count + EXCLUDED.accident_count;
    """,
      weather_rows,
  )

  # 4. factor_accidents Upsert
  execute_values(
      cur,
      """
        INSERT INTO factor_accidents (contributing_factor, accident_count)
        VALUES %s
        ON CONFLICT (contributing_factor) DO UPDATE SET
            accident_count = factor_accidents.accident_count + EXCLUDED.accident_count;
    """,
      factor_rows,
  )

  # 5. vehicle_accidents Upsert
  execute_values(
      cur,
      """
        INSERT INTO vehicle_accidents (vehicle_type, accident_count)
        VALUES %s
        ON CONFLICT (vehicle_type) DO UPDATE SET
            accident_count = vehicle_accidents.accident_count + EXCLUDED.accident_count;
    """,
      vehicle_rows,
  )

  conn.commit()
  cur.close()
  conn.close()
  batch_df.unpersist()
  print(f"Batch {batch_id} successfully upserted to PostgreSQL.")


# 5. تشغيل البث المستمر
query = (
    parsed_df.writeStream.foreachBatch(write_to_postgres)
    .option("checkpointLocation", "./spark-checkpoints")
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()