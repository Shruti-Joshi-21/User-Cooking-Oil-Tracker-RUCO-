"""
Quick check before choosing the model: do TPC curves rise in straight lines or bend?

Plots TPC vs hour for all 5 fryers (dots = lab data, dashed = straight-line fit) and prints,
per fryer, how much a curved (quadratic) fit improves on a straight line.
Output: model/tpc_vs_hour.png
"""
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
df = pd.read_csv(ROOT / "data" / "clean" / "oil_readings.csv")

NAMES = {"F160": "Bakery", "F175": "Cafe", "F190": "College Canteen",
         "F205": "Restaurant", "F220": "Street Stall"}
COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]  # fixed order, one per fryer
TPC_LIMIT = 25


def r2(y, pred):
    return 1 - ((y - pred) ** 2).sum() / ((y - y.mean()) ** 2).sum()


fig, ax = plt.subplots(figsize=(9, 5.5))
print(f"{'fryer':6} {'R2 line':>8} {'R2 curve':>9} {'hour^2 coef':>12}")
for color, (fid, g) in zip(COLORS, df.groupby("fryer_id")):
    h, y = g["hour"].values, g["tpc"].values
    line = np.polyfit(h, y, 1)
    quad = np.polyfit(h, y, 2)
    print(f"{fid:6} {r2(y, np.polyval(line, h)):8.3f} {r2(y, np.polyval(quad, h)):9.3f} {quad[0]:12.4f}")

    ax.scatter(h, y, s=18, color=color, zorder=3)
    ax.plot(h, np.polyval(line, h), "--", color=color, linewidth=1.5)
    ax.text(30.4, np.polyval(line, 30), f"{NAMES[fid]} ({g['temperature'].iloc[0]}°C)",
            va="center", fontsize=9, color="#333333")

ax.axhline(TPC_LIMIT, color="#888888", linewidth=1)
ax.text(0.3, TPC_LIMIT + 0.6, "FSSAI discard limit (25% TPC)", fontsize=9, color="#555555")
ax.set_xlabel("Frying time (hours)")
ax.set_ylabel("Total Polar Compounds (%)")
ax.set_title("TPC vs frying hour: dots = lab data, dashed = straight-line fit", loc="left")
ax.set_xlim(0, 39)
ax.grid(color="#e6e6e6", linewidth=0.8)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
fig.tight_layout()

out = Path(__file__).parent / "tpc_vs_hour.png"
fig.savefig(out, dpi=150)
print(f"Saved {out}")
