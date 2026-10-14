"""
Step 3: Spark Structured Streaming job.

Kafka `oil-readings` -> parse JSON -> clean (fill missing values, remove z-score outliers)
-> rolling average TPC per fryer -> status + discard alerts -> CSV files in output/
(the Streamlit dashboard will read these files in a later step).

Spark reads Kafka in micro-batches (every TRIGGER seconds). For each micro-batch we
add the last few clean readings of every fryer ("history"), so the z-score and the
rolling average can look back across batch boundaries.
"""
import csv
import os
import shutil

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# ---------------- Config ----------------
KAFKA_SERVER = "kafka:29092"        # Kafka's address inside the Docker network
TOPIC = "oil-readings"
OUTPUT_DIR = "/opt/app/output"      # mounted to oil-tracker/output on Windows
CLEAN_CSV = os.path.join(OUTPUT_DIR, "clean_readings.csv")
ALERTS_CSV = os.path.join(OUTPUT_DIR, "alerts.csv")
CHECKPOINT_DIR = "/tmp/oil-checkpoint"
TRIGGER = "2 seconds"               # micro-batch interval

# Outlier settings were tuned by simulation: a longer history or smaller std floor made the
# mean lag behind the rising TPC curve and flagged normal readings as outliers.
HISTORY_N = 10       # past clean readings per fryer used for the z-score (= 1 data-hour)
MIN_HISTORY = 3      # need at least 3 past readings before judging outliers
Z_LIMIT = 3.0        # |z| > 3  ->  outlier
MIN_STD = 1.0        # std floor (TPC % points), so tiny normal noise can't give a huge z-score
ROLLING_N = 10       # rolling average over the last 10 readings (= 1 data-hour)
TPC_WARNING = 22.0   # early warning level
TPC_LIMIT = 25.0     # FSSAI limit: oil must be discarded

# Shape of the JSON messages sent by producer.py
SCHEMA = StructType([
    StructField("fryer_id", StringType()),
    StructField("fryer_name", StringType()),
    StructField("temperature", IntegerType()),
    StructField("tpc", DoubleType()),
    StructField("acid_value", DoubleType()),
    StructField("carbonyl_value", DoubleType()),
    StructField("data_hour", DoubleType()),
    StructField("timestamp", StringType()),
])
HISTORY_SCHEMA = "fryer_id string, data_hour double, tpc double, acid_value double, carbonyl_value double"

CLEAN_HEADER = ["timestamp", "fryer_id", "fryer_name", "temperature", "data_hour",
                "tpc_raw", "tpc_clean", "tpc_rolling", "acid_value", "carbonyl_value",
                "z_score", "is_outlier", "was_missing", "status"]
ALERT_HEADER = ["timestamp", "fryer_id", "fryer_name", "data_hour", "tpc_rolling", "message"]

# ---------------- State kept between micro-batches (driver memory) ----------------
history = {}      # fryer_id -> list of recent clean readings (data_hour, tpc, acid, carbonyl)
alerted = set()   # fryers already alerted in the current oil cycle (alert only once)


def append_csv(path, header, rows):
    """Append rows to a CSV file, writing the header the first time."""
    is_new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new:
            writer.writerow(header)
        writer.writerows(rows)


def process_batch(batch_df, batch_id):
    """Called by Spark once per micro-batch. batch_df is a normal (static) DataFrame."""
    if batch_df.isEmpty():
        return
    spark = batch_df.sparkSession

    # 1) Oil change: if a fryer's hours jump back (producer restarted), the oil is fresh,
    #    so forget that fryer's history and alert status.
    for row in batch_df.groupBy("fryer_id").agg(F.min("data_hour").alias("first_hour")).collect():
        past = history.get(row.fryer_id)
        if past and row.first_hour < past[-1][0]:
            print(f"[oil change] {row.fryer_id}: fresh oil, history reset")
            history.pop(row.fryer_id)
            alerted.discard(row.fryer_id)

    # 2) Combine new readings with each fryer's recent clean history
    hist_rows = [(fid, *r) for fid, rows in history.items() for r in rows]
    hist_df = spark.createDataFrame(hist_rows, HISTORY_SCHEMA).withColumn("is_new", F.lit(False))
    df = batch_df.withColumn("is_new", F.lit(True)).unionByName(hist_df, allowMissingColumns=True)

    # Window functions: look at each fryer's readings in time order
    by_fryer = Window.partitionBy("fryer_id").orderBy("data_hour")
    past = by_fryer.rowsBetween(-HISTORY_N, -1)                   # previous readings only
    up_to_now = by_fryer.rowsBetween(Window.unboundedPreceding, 0)
    rolling = by_fryer.rowsBetween(-(ROLLING_N - 1), 0)           # last ROLLING_N readings

    def z_score(col):
        """z = (tpc - mean of past values) / std of past values, using column `col` as the past."""
        mean = F.avg(col).over(past)
        std = F.greatest(F.stddev(col).over(past), F.lit(MIN_STD))
        enough = F.count(col).over(past) >= MIN_HISTORY
        return F.when(enough, (F.col("tpc") - mean) / std)

    def is_outlier(z_col):
        return F.coalesce(F.col("is_new") & (F.abs(z_col) > Z_LIMIT), F.lit(False))

    df = (
        df
        # 3) Outliers, two-pass z-score:
        #    pass 1 finds obvious spikes; pass 2 recomputes mean/std WITHOUT them, so a spike
        #    earlier in the same micro-batch can't inflate the std and hide a second spike.
        .withColumn("spike_pass1", is_outlier(z_score("tpc")))
        .withColumn("tpc_pass1", F.when(F.col("spike_pass1"), None).otherwise(F.col("tpc")))
        .withColumn("z_score", z_score("tpc_pass1"))
        .withColumn("is_outlier", is_outlier(F.col("z_score")))
        # 4) Missing values: remember which rows had a null, then forward-fill
        #    (replace null / outlier with the fryer's last good value)
        .withColumn("was_missing", F.col("tpc").isNull() | F.col("acid_value").isNull()
                    | F.col("carbonyl_value").isNull())
        .withColumn("tpc_valid", F.when(F.col("is_outlier"), None).otherwise(F.col("tpc")))
        .withColumn("tpc_clean", F.last("tpc_valid", ignorenulls=True).over(up_to_now))
        .withColumn("acid_clean", F.last("acid_value", ignorenulls=True).over(up_to_now))
        .withColumn("carbonyl_clean", F.last("carbonyl_value", ignorenulls=True).over(up_to_now))
        # 5) Rolling average of clean TPC + status against the FSSAI limit
        .withColumn("tpc_rolling", F.avg("tpc_clean").over(rolling))
        .withColumn("rolling_n", F.count("tpc_clean").over(rolling))
        # WARMING_UP until the rolling window is full (avoids false alerts from 1-2 readings)
        .withColumn("status", F.when(F.col("rolling_n") < ROLLING_N, "WARMING_UP")
                    .when(F.col("tpc_rolling") >= TPC_LIMIT, "DISCARD")
                    .when(F.col("tpc_rolling") >= TPC_WARNING, "WARNING")
                    .otherwise("OK"))
    )

    rows = (df.filter("is_new")                       # history rows were only for context
              .orderBy("fryer_id", "data_hour")
              .collect())

    # 6) Save clean readings, raise alerts, update history
    clean_out, alert_out = [], []
    for r in rows:
        clean_out.append([r.timestamp, r.fryer_id, r.fryer_name, r.temperature, r.data_hour,
                          r.tpc, r.tpc_clean, round(r.tpc_rolling, 2) if r.tpc_rolling else None,
                          r.acid_clean, r.carbonyl_clean,
                          round(r.z_score, 2) if r.z_score is not None else None,
                          r.is_outlier, r.was_missing, r.status])

        if r.status == "DISCARD" and r.fryer_id not in alerted:
            alerted.add(r.fryer_id)
            msg = (f"{r.fryer_name}: avg TPC {r.tpc_rolling:.1f}% >= {TPC_LIMIT}% FSSAI limit "
                   f"at hour {r.data_hour}. Discard oil and send to RUCO collection.")
            alert_out.append([r.timestamp, r.fryer_id, r.fryer_name, r.data_hour,
                              round(r.tpc_rolling, 2), msg])
            print(f"*** ALERT *** {msg}")

        fryer_hist = history.setdefault(r.fryer_id, [])
        fryer_hist.append((r.data_hour, r.tpc_clean, r.acid_clean, r.carbonyl_clean))
        del fryer_hist[:-HISTORY_N]                     # keep only the last HISTORY_N

    append_csv(CLEAN_CSV, CLEAN_HEADER, clean_out)
    if alert_out:
        append_csv(ALERTS_CSV, ALERT_HEADER, alert_out)

    n_out = sum(r.is_outlier for r in rows)
    n_miss = sum(r.was_missing for r in rows)
    latest = {r.fryer_id: f"{r.tpc_rolling:.1f}" for r in rows if r.tpc_rolling}
    print(f"batch {batch_id}: {len(rows)} readings | outliers removed: {n_out} | "
          f"missing filled: {n_miss} | rolling TPC: {latest}")


def main():
    # Every run starts fresh: old checkpoint and output files are removed.
    shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for path in (CLEAN_CSV, ALERTS_CSV):
        if os.path.exists(path):
            os.remove(path)

    spark = (SparkSession.builder
             .appName("ruco-oil-tracker")
             .config("spark.sql.shuffle.partitions", "1")   # tiny data -> 1 partition is fastest
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Read the Kafka topic as an unbounded streaming DataFrame
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_SERVER)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")              # only readings sent after Spark starts
           .load())

    # Kafka value = bytes -> string -> JSON columns
    readings = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("r"))
                   .select("r.*")
                   .filter(F.col("fryer_id").isNotNull() & F.col("data_hour").isNotNull()))

    query = (readings.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT_DIR)
             .trigger(processingTime=TRIGGER)
             .start())
    print(f"Spark streaming started. Waiting for readings on '{TOPIC}'...", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
