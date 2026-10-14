"""
Step 1: Merge the 3 Mendeley CSVs (acid value, carbonyl value, TPC) into one long table.

Raw files are "wide": one row per hour (0-30), one column per frying temperature.
We convert each to "long" format (one row per fryer per hour) and join them.
"""
from pathlib import Path

import pandas as pd

RAW = Path(__file__).parent / "raw"
CLEAN = Path(__file__).parent / "clean"

# file name -> column name in the merged output
FILES = {
    "total_polar_compoents.csv": "tpc",  # (typo is in the original file name)
    "acid_value.csv": "acid_value",
    "carbonyl_value.csv": "carbonyl_value",
}


def to_long(path, value_name):
    """Wide (hour x temperature) -> long (temperature, hour, value)."""
    df = pd.read_csv(path)
    df["hour"] = df.index  # row number = hours of frying (0..30)
    long = df.melt(id_vars="hour", var_name="temperature", value_name=value_name)
    long["temperature"] = long["temperature"].str.replace("Tem_", "").astype(int)
    return long


def main():
    merged = None
    for fname, col in FILES.items():
        part = to_long(RAW / fname, col)
        merged = part if merged is None else merged.merge(part, on=["temperature", "hour"])

    # each frying temperature acts as one simulated fryer, e.g. F160, F175 ...
    merged["fryer_id"] = "F" + merged["temperature"].astype(str)
    merged = merged[["fryer_id", "temperature", "hour", "tpc", "acid_value", "carbonyl_value"]]
    merged = merged.sort_values(["fryer_id", "hour"]).reset_index(drop=True)

    CLEAN.mkdir(exist_ok=True)
    out = CLEAN / "oil_readings.csv"
    merged.to_csv(out, index=False)

    print(f"Saved {len(merged)} rows -> {out}")
    print(merged.head(), "\n")
    print("Missing values per column:\n", merged.isna().sum(), "\n")
    print("Hour at which TPC first reaches 25% (FSSAI limit):")
    print(merged[merged["tpc"] >= 25].groupby("fryer_id")["hour"].min())


if __name__ == "__main__":
    main()
