"""
Step 2: Replay data/clean/oil_readings.csv into Kafka topic `oil-readings` as live sensor readings.

The lab data has one reading per hour. We interpolate between hours so each fryer
sends a reading every 0.1 data-hour, then add realistic sensor problems
(noise, outlier spikes, missing values) for Spark to clean later.
"""
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from kafka import KafkaProducer

# ---------------- Config ----------------
KAFKA_SERVER = "localhost:9092"
TOPIC = "oil-readings"
CSV_PATH = Path(__file__).parent.parent / "data" / "clean" / "oil_readings.csv"

SECONDS_PER_DATA_HOUR = 5   # time compression: 1 hour of frying = 5 real seconds
STEP_HOURS = 0.1            # one reading per fryer every 0.1 data-hour (= 0.5 s)
TPC_NOISE_STD = 0.2         # normal sensor noise on TPC (in % points)
OUTLIER_PROB = 0.02         # ~2% of readings get a fake glitch spike
MISSING_PROB = 0.01         # ~1% of readings lose one value

# Each frying temperature in the dataset acts as one restaurant fryer.
FRYER_NAMES = {
    "F160": "Bakery",
    "F175": "Cafe",
    "F190": "College Canteen",
    "F205": "Restaurant",
    "F220": "Street Stall",
}
VALUE_COLS = ["tpc", "acid_value", "carbonyl_value"]


def interpolate(fryer_df, hour):
    """Linear interpolation of tpc/acid/carbonyl at a fractional hour (e.g. 3.4)."""
    return {c: float(np.interp(hour, fryer_df["hour"], fryer_df[c])) for c in VALUE_COLS}


def make_reading(fryer_id, temperature, hour, values):
    """Build one message and apply the simulated sensor problems."""
    tpc = values["tpc"] + random.gauss(0, TPC_NOISE_STD)  # small Gaussian noise

    # FAKE OUTLIER (~2%): sensor glitch spike, TPC jumps by +10..+25 points.
    # Spark's z-score step should detect and remove these.
    if random.random() < OUTLIER_PROB:
        tpc += random.uniform(10, 25)

    reading = {
        "fryer_id": fryer_id,
        "fryer_name": FRYER_NAMES[fryer_id],
        "temperature": temperature,
        "tpc": round(tpc, 2),
        "acid_value": round(values["acid_value"], 2),
        "carbonyl_value": round(values["carbonyl_value"], 2),
        "data_hour": round(hour, 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # FAKE MISSING VALUE (~1%): one measurement is dropped (sent as JSON null).
    # Spark's cleaning step must handle these nulls.
    if random.random() < MISSING_PROB:
        reading[random.choice(VALUE_COLS)] = None

    return reading


def main():
    df = pd.read_csv(CSV_PATH)
    fryers = {fid: g.sort_values("hour") for fid, g in df.groupby("fryer_id")}
    max_hour = df["hour"].max()

    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
    print(f"Connected to Kafka at {KAFKA_SERVER}, sending to '{TOPIC}'...")

    # Simulated clock: at every step, ALL 5 fryers send one reading ("in parallel"),
    # then we wait the real-time equivalent of 0.1 data-hour.
    n_steps = int(round(max_hour / STEP_HOURS))
    for step in range(n_steps + 1):
        hour = step * STEP_HOURS
        for fryer_id, fdf in fryers.items():
            values = interpolate(fdf, hour)
            msg = make_reading(fryer_id, int(fdf["temperature"].iloc[0]), hour, values)
            # Kafka stores raw bytes: key = fryer id (keeps order per fryer), value = JSON text
            producer.send(TOPIC, key=fryer_id.encode("utf-8"), value=json.dumps(msg).encode("utf-8"))
            print(msg, flush=True)
        time.sleep(SECONDS_PER_DATA_HOUR * STEP_HOURS)

    producer.flush()
    print("Replay finished: all 30 data-hours sent.")


if __name__ == "__main__":
    main()
