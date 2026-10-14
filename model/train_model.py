"""
Step 4: Train the TPC regression model with Spark MLlib.

Model:  tpc = b0 + b_hour*hour + b_temp*temp + b_int*(hour*temp)
The hour x temp term lets hotter oil degrade faster:
    degradation rate at temperature T = d(tpc)/d(hour) = b_hour + b_int*T   (% TPC per hour)

What this script does:
  1. Compares 3 candidate models (linear / interaction / +hour^2), with per-fryer
     leave-one-fryer-out (LOFO) error  -> model/model_comparison.csv
  2. Trains the chosen interaction model on all data and saves it
     -> model/tpc_model/ (MLlib format) and model/coefficients.json
  3. Compares two ways of predicting hours-to-discard  -> model/hours_left_comparison.csv
       clock: solve model for the hour where predicted TPC = 25, minus current hour
       rate : (25 - current measured TPC) / degradation rate at that temperature

Run inside the Spark container:
  docker compose exec spark /opt/spark/bin/spark-submit /opt/app/model/train_model.py
"""
import csv
import json
import os

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_CSV = "/opt/app/data/clean/oil_readings.csv"
MODEL_DIR = "/opt/app/model"
TPC_LIMIT = 25.0
MAX_LAB_HOUR = 30            # lab data covers hours 0..30
CHECK_HOURS = [0, 10, 20]    # points at which we compare the two hours-left methods

NAMES = {"F160": "Bakery", "F175": "Cafe", "F190": "College Canteen",
         "F205": "Restaurant", "F220": "Street Stall"}

# Candidate models = which feature columns go into the regression
MODELS = {
    "linear":      ["hour", "temperature"],
    "interaction": ["hour", "temperature", "hour_x_temp"],
    "quadratic":   ["hour", "temperature", "hour_x_temp", "hour_sq"],
}
CHOSEN = "interaction"


def fit(df, features):
    """Fit ordinary least squares on the given feature columns."""
    data = VectorAssembler(inputCols=features, outputCol="features").transform(df)
    # solver="normal" = exact least-squares solution (same maths as numpy lstsq)
    lr = LinearRegression(featuresCol="features", labelCol="tpc", solver="normal")
    return lr.fit(data)


def evaluate(model, df, features):
    """RMSE and R^2 of a fitted model on a DataFrame."""
    data = VectorAssembler(inputCols=features, outputCol="features").transform(df)
    summary = model.evaluate(data)
    return summary.rootMeanSquaredError, summary.r2


def actual_discard_hour(rows):
    """First hour the lab TPC reaches 25%, interpolated between hourly readings (None if never)."""
    for prev, cur in zip(rows, rows[1:]):
        if cur.tpc >= TPC_LIMIT:
            return prev.hour + (TPC_LIMIT - prev.tpc) / (cur.tpc - prev.tpc)
    return None


def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main():
    spark = SparkSession.builder.appName("train-tpc-model").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (spark.read.csv(DATA_CSV, header=True, inferSchema=True)
          .withColumn("hour", F.col("hour").cast("double"))
          .withColumn("temperature", F.col("temperature").cast("double"))
          .withColumn("hour_x_temp", F.col("hour") * F.col("temperature"))
          .withColumn("hour_sq", F.col("hour") * F.col("hour"))
          .cache())
    fryers = sorted(NAMES)

    # ---------- 1) Model comparison with per-fryer leave-one-fryer-out error ----------
    # LOFO: train on 4 fryers, test on the 5th. F160 and F220 are the coldest/hottest,
    # so their folds are EXTRAPOLATION in temperature and are expected to be worse.
    print("\n=== 1) Model comparison (RMSE in TPC % points) ===")
    print(f"{'model':12} {'R2':>6} {'RMSE':>6} | LOFO RMSE: "
          + " ".join(f"{f:>6}" for f in fryers) + f" {'mean':>6}")
    comparison = []
    for name, features in MODELS.items():
        full = fit(df, features)
        rmse, r2 = evaluate(full, df, features)
        lofo = []
        for f in fryers:
            m = fit(df.filter(F.col("fryer_id") != f), features)
            lofo.append(evaluate(m, df.filter(F.col("fryer_id") == f), features)[0])
        mean = sum(lofo) / len(lofo)
        print(f"{name:12} {r2:6.3f} {rmse:6.2f} |            "
              + " ".join(f"{e:6.2f}" for e in lofo) + f" {mean:6.2f}")
        comparison.append([name, " + ".join(features), round(r2, 4), round(rmse, 3)]
                          + [round(e, 3) for e in lofo] + [round(mean, 3)])
    write_csv(os.path.join(MODEL_DIR, "model_comparison.csv"),
              ["model", "features", "r2", "rmse"] + [f"lofo_rmse_{f}" for f in fryers]
              + ["lofo_rmse_mean"], comparison)
    print("(LOFO for F160 / F220 = extrapolating to an unseen, colder / hotter temperature)")

    # ---------- 2) Train the chosen model on ALL data and save it ----------
    features = MODELS[CHOSEN]
    model = fit(df, features)
    rmse, r2 = evaluate(model, df, features)
    b0 = model.intercept
    b_hour, b_temp, b_int = model.coefficients.toArray()
    print(f"\n=== 2) Chosen model: {CHOSEN} ===")
    print(f"tpc = {b0:.4f} + {b_hour:.4f}*hour + {b_temp:.5f}*temp + {b_int:.6f}*hour*temp")
    print(f"R2 = {r2:.4f}, RMSE = {rmse:.3f}")

    model.write().overwrite().save(os.path.join(MODEL_DIR, "tpc_model"))
    coef = {"intercept": b0, "hour": b_hour, "temperature": b_temp, "hour_x_temp": b_int,
            "r2": r2, "rmse": rmse, "tpc_limit": TPC_LIMIT, "max_lab_hour": MAX_LAB_HOUR}
    with open(os.path.join(MODEL_DIR, "coefficients.json"), "w") as f:
        json.dump(coef, f, indent=2)

    # ---------- 3) Hours-to-discard: clock-based vs rate-based ----------
    print(f"\n=== 3) Hours left until TPC {TPC_LIMIT}%: clock vs rate method ===")
    print(f"{'fryer':16} {'rate %/h':>8} {'hour':>5} {'tpc now':>8} {'actual':>7} "
          f"{'clock':>7} {'rate':>7}  note")
    rows_out, slope_check = [], []
    for f in fryers:
        rows = sorted(df.filter(F.col("fryer_id") == f).collect(), key=lambda r: r.hour)
        temp = rows[0].temperature
        rate = b_hour + b_int * temp                              # % TPC per hour at this temperature
        model_discard = (TPC_LIMIT - b0 - b_temp * temp) / rate   # hour where model predicts 25%
        actual = actual_discard_hour(rows)
        own_slope = (rows[-1].tpc - rows[0].tpc) / (rows[-1].hour - rows[0].hour)
        slope_check.append(f"{f}: model rate {rate:.3f} %/h vs lab average slope {own_slope:.3f} %/h")

        for h in CHECK_HOURS:
            tpc_now = next(r.tpc for r in rows if r.hour == h)
            left_clock = max(model_discard - h, 0.0)
            left_rate = max((TPC_LIMIT - tpc_now) / rate, 0.0)
            left_actual = max(actual - h, 0.0) if actual is not None else None
            # This fryer never reached 25% within the 30 lab hours, so any discard-time
            # prediction lies outside the data the model was trained on.
            note = "extrapolated beyond lab data" if actual is None else ""
            print(f"{f + ' ' + NAMES[f]:16} {rate:8.3f} {h:5d} {tpc_now:8.2f} "
                  f"{(f'{left_actual:.1f}' if left_actual is not None else '>' + str(MAX_LAB_HOUR - h)):>7} "
                  f"{left_clock:7.1f} {left_rate:7.1f}  {note}")
            rows_out.append([f, NAMES[f], temp, round(rate, 4), h, tpc_now,
                             round(left_actual, 2) if left_actual is not None else "",
                             round(left_clock, 2), round(left_rate, 2), note])

    print("\nDegradation rate check (the rate method depends on this being right):")
    print("\n".join(slope_check))

    write_csv(os.path.join(MODEL_DIR, "hours_left_comparison.csv"),
              ["fryer_id", "fryer_name", "temperature", "rate_per_hour", "hour", "tpc_now",
               "hours_left_actual", "hours_left_clock", "hours_left_rate", "note"], rows_out)
    print("\nSaved: model/tpc_model/, model/coefficients.json, "
          "model/model_comparison.csv, model/hours_left_comparison.csv")
    spark.stop()


if __name__ == "__main__":
    main()
