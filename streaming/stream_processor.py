"""
Steps 3 + 5: Spark Structured Streaming job.

Kafka `oil-readings` -> parse JSON -> clean (fill missing values, remove z-score outliers)
-> rolling average TPC per fryer -> status + discard alerts
-> hours left until discard (model rate as prior, fryer's own live slope once available)
-> PostgreSQL tables `readings` and `alerts` (read by the Streamlit dashboard).

Spark reads Kafka in micro-batches (every TRIGGER seconds). For each micro-batch we
add the last few clean readings of every fryer ("history"), so the z-score and the
rolling average can look back across batch boundaries.
"""
import shutil
import time
from datetime import datetime

from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# ---------------- Config ----------------
KAFKA_SERVER = "kafka:29092"        # Kafka's address inside the Docker network
TOPIC = "oil-readings"
CHECKPOINT_DIR = "/tmp/oil-checkpoint"

# PostgreSQL (the `postgres` container). Spark writes with JDBC: df.write.jdbc(...)
JDBC_URL = "jdbc:postgresql://postgres:5432/oiltracker"
JDBC_PROPS = {"user": "oil", "password": "oil123", "driver": "org.postgresql.Driver"}
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

# Hours-to-discard (Step 5)
MODEL_PATH = "/opt/app/model/tpc_model"   # saved by model/train_model.py
MIN_LIVE_HOURS = 3.0   # use the fryer's own slope once it has >= 3 data-hours of clean readings
# Asymmetric guard on the live slope (tuned by replaying the stream):
#  - faster than the model (up to 3x) is accepted: it means "discard sooner" = the safe direction
#  - slower than the model is only accepted down to 0.7x: early readings fall in the oil's slow
#    "induction period", and trusting that low slope over-estimated hours left by up to 22 h
MAX_RATE_RATIO = 3.0
MIN_RATE_RATIO = 0.7
MAX_LAB_HOUR = 30      # predictions past hour 30 are beyond the lab data

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

# Column layout of the database tables (see database/init.sql); `id` is filled by PostgreSQL
READINGS_SCHEMA = ("run_id long, ts timestamp, fryer_id string, fryer_name string, temperature int, "
                   "data_hour double, tpc_raw double, tpc_clean double, tpc_rolling double, "
                   "acid_value double, carbonyl_value double, z_score double, is_outlier boolean, "
                   "was_missing boolean, status string, live_slope double, rate_per_hour double, "
                   "rate_source string, hours_left double, predicted_discard_hour double, "
                   "extrapolated boolean")
ALERTS_SCHEMA = ("run_id long, ts timestamp, fryer_id string, fryer_name string, "
                 "data_hour double, tpc_rolling double, message string")

# ---------------- State kept between micro-batches (driver memory) ----------------
RUN_ID = int(time.time())   # identifies this Spark run in the database (start time, epoch seconds)
history = {}      # fryer_id -> list of recent clean readings (data_hour, tpc, acid, carbonyl)
alerted = set()   # fryers already alerted in the current oil cycle (alert only once)
slope_sums = {}   # fryer_id -> running sums of clean readings this oil cycle (for the live slope)
B_HOUR = B_INT = None   # model coefficients, loaded in main(): rate(T) = B_HOUR + B_INT * T


def choose_rate(fryer_id, temperature):
    """Return (live_slope, rate_used, rate_source) for one fryer."""
    model_rate = B_HOUR + B_INT * temperature        # model prior (% TPC per hour)

    # Least-squares slope from plain sums: slope = cov(hour, tpc) / var(hour)
    #   = (n*Σht - Σh*Σt) / (n*Σh² - (Σh)²)
    live = None
    s = slope_sums.get(fryer_id)
    if s and s["n"] >= 2:
        var = s["n"] * s["shh"] - s["sh"] ** 2
        if var > 0:
            live = (s["n"] * s["sht"] - s["sh"] * s["st"]) / var

    enough_data = s is not None and s["last_h"] - s["first_h"] >= MIN_LIVE_HOURS
    # Guard: zero/negative slopes, or slopes too far from the model, fall back to the model rate
    sane = (live is not None and live > 0
            and model_rate * MIN_RATE_RATIO <= live <= model_rate * MAX_RATE_RATIO)
    if enough_data and sane:
        return live, live, "live"
    return live, model_rate, "model"


def write_to_db(spark, rows, schema, table):
    """Append rows to a PostgreSQL table using Spark's JDBC writer."""
    if rows:
        spark.createDataFrame(rows, schema).write.jdbc(JDBC_URL, table, mode="append",
                                                       properties=JDBC_PROPS)


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
            slope_sums.pop(row.fryer_id, None)

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

    new = df.filter("is_new")                         # history rows were only for context

    # 6) Live slope: add this batch's sums of CLEAN measured readings to the running totals.
    #    tpc_valid excludes outliers and missing values, so one spike can't wreck the slope.
    batch_sums = (new.filter(F.col("tpc_valid").isNotNull())
                     .groupBy("fryer_id")
                     .agg(F.count("*").alias("n"),
                          F.sum("data_hour").alias("sh"),
                          F.sum("tpc_valid").alias("st"),
                          F.sum(F.col("data_hour") * F.col("tpc_valid")).alias("sht"),
                          F.sum(F.col("data_hour") * F.col("data_hour")).alias("shh"),
                          F.min("data_hour").alias("first_h"),
                          F.max("data_hour").alias("last_h"))
                     .collect())
    for b in batch_sums:
        s = slope_sums.setdefault(b.fryer_id, {"n": 0, "sh": 0.0, "st": 0.0, "sht": 0.0,
                                               "shh": 0.0, "first_h": b.first_h})
        for k in ("n", "sh", "st", "sht", "shh"):
            s[k] += b[k]
        s["last_h"] = b.last_h

    rows = new.orderBy("fryer_id", "data_hour").collect()
    rates = {r.fryer_id: choose_rate(r.fryer_id, r.temperature) for r in rows}

    # 7) Save clean readings + hours left, raise alerts, update history
    clean_out, alert_out, summary = [], [], {}
    for r in rows:
        live, rate, source = rates[r.fryer_id]
        hours_left = predicted = None
        if r.tpc_rolling is not None:
            # hours left = how far below the limit we are / how fast TPC is rising
            hours_left = max((TPC_LIMIT - r.tpc_rolling) / rate, 0.0)
            predicted = r.data_hour + hours_left
        extrapolated = predicted is not None and predicted > MAX_LAB_HOUR
        ts = datetime.fromisoformat(r.timestamp)       # ISO text from the producer -> timestamp

        clean_out.append([RUN_ID, ts, r.fryer_id, r.fryer_name, r.temperature, r.data_hour,
                          r.tpc, r.tpc_clean, round(r.tpc_rolling, 2) if r.tpc_rolling else None,
                          r.acid_clean, r.carbonyl_clean,
                          round(r.z_score, 2) if r.z_score is not None else None,
                          r.is_outlier, r.was_missing, r.status,
                          round(live, 4) if live is not None else None, round(rate, 4), source,
                          round(hours_left, 2) if hours_left is not None else None,
                          round(predicted, 2) if predicted is not None else None, extrapolated])
        if hours_left is not None:
            summary[r.fryer_id] = f"{hours_left:.1f}h ({source}{', extrap' if extrapolated else ''})"

        if r.status == "DISCARD" and r.fryer_id not in alerted:
            alerted.add(r.fryer_id)
            msg = (f"{r.fryer_name}: avg TPC {r.tpc_rolling:.1f}% >= {TPC_LIMIT}% FSSAI limit "
                   f"at hour {r.data_hour}. Discard oil and send to RUCO collection.")
            alert_out.append([RUN_ID, ts, r.fryer_id, r.fryer_name, r.data_hour,
                              round(r.tpc_rolling, 2), msg])
            print(f"*** ALERT *** {msg}")

        fryer_hist = history.setdefault(r.fryer_id, [])
        fryer_hist.append((r.data_hour, r.tpc_clean, r.acid_clean, r.carbonyl_clean))
        del fryer_hist[:-HISTORY_N]                     # keep only the last HISTORY_N

    # 8) Save to PostgreSQL
    write_to_db(spark, clean_out, READINGS_SCHEMA, "readings")
    write_to_db(spark, alert_out, ALERTS_SCHEMA, "alerts")

    n_out = sum(r.is_outlier for r in rows)
    n_miss = sum(r.was_missing for r in rows)
    latest = {r.fryer_id: f"{r.tpc_rolling:.1f}" for r in rows if r.tpc_rolling}
    print(f"batch {batch_id}: {len(rows)} readings | outliers removed: {n_out} | "
          f"missing filled: {n_miss} | rolling TPC: {latest}")
    print(f"           hours left: {summary}")


def main():
    # Every run starts fresh: the old checkpoint is removed and rows get a new run_id.
    # (Older runs stay in the database, so they can still be queried with SQL.)
    shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)
    print(f"Run id: {RUN_ID}", flush=True)

    spark = (SparkSession.builder
             .appName("ruco-oil-tracker")
             .config("spark.sql.shuffle.partitions", "1")   # tiny data -> 1 partition is fastest
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # Load the MLlib model trained in Step 4 (features: hour, temperature, hour_x_temp).
    # Degradation rate at temperature T = d(tpc)/d(hour) = b_hour + b_int * T
    global B_HOUR, B_INT
    model = LinearRegressionModel.load(MODEL_PATH)
    b_hour, _, b_int = model.coefficients.toArray()
    B_HOUR, B_INT = float(b_hour), float(b_int)     # numpy -> plain Python floats (for the DB rows)
    print(f"Loaded model: rate(T) = {B_HOUR:.4f} + {B_INT:.6f} * T  (% TPC per hour)", flush=True)

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
