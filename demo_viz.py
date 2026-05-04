#!/usr/bin/env python3
"""Quick visualization demo to verify the adjusted pipeline outputs."""

import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

OUT = Path("outputs")
OUT.mkdir(exist_ok=True)

df = pd.read_parquet("data/processed/final/zone_date_master_2025.parquet")

print("Master table shape:", df.shape)
print("Columns:", list(df.columns))
print()

# ── 1. POI category counts across all zones ───────────────────────────────────
poi_cols = [c for c in df.columns if c.startswith("poi_count_") and c != "poi_count_total"]
zone_poi = df.drop_duplicates("taxi_zone_id")[poi_cols].sum().sort_values(ascending=False)
zone_poi.index = [c.replace("poi_count_", "").replace("_", " ").title() for c in zone_poi.index]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(zone_poi.index, zone_poi.values, color="steelblue")
for bar in bars:
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 20,
            f"{int(bar.get_height()):,}", ha="center", va="bottom", fontsize=8)
ax.set_title("POI Count by Category (all matched zones)")
ax.set_ylabel("Number of POIs")
ax.set_xlabel("POI Category")
plt.xticks(rotation=30, ha="right")
plt.tight_layout()
plt.savefig(OUT / "poi_category_distribution.png", dpi=120)
plt.close()
print("[OK] Saved poi_category_distribution.png")

# ── 2. Avg daily taxi demand by POI category (top-10 cultural/commercial zones) ─
poi_cat_cols = {
    "poi_count_commercial":   "Commercial",
    "poi_count_cultural":     "Cultural",
    "poi_count_recreational": "Recreational",
    "poi_count_transportation": "Transportation",
}
zone_avg = df.groupby("taxi_zone_id").agg(
    avg_trips=("trips_pickup_yellow", "mean"),
    **{k: (k, "first") for k in poi_cat_cols}
).dropna(subset=["avg_trips"])

fig, axes = plt.subplots(2, 2, figsize=(12, 9))
fig.suptitle("Avg Daily Yellow Taxi Demand vs POI Category Count (per zone)", fontsize=13)

for ax, (col, label) in zip(axes.flatten(), poi_cat_cols.items()):
    sub = zone_avg[[col, "avg_trips"]].dropna()
    ax.scatter(sub[col], sub["avg_trips"], alpha=0.5, s=25, color="steelblue")
    corr = sub[col].corr(sub["avg_trips"])
    ax.set_xlabel(f"{label} POI count")
    ax.set_ylabel("Avg daily trips")
    ax.set_title(f"{label}  (r = {corr:.3f})")

plt.tight_layout()
plt.savefig(OUT / "poi_vs_demand_scatter.png", dpi=120)
plt.close()
print("[OK] Saved poi_vs_demand_scatter.png")

# ── 3. Street-hail column exists and is populated ───────────────────────────
print()
print("taxi_type columns present:", [c for c in df.columns if "street_hail" in c or "booked" in c])
sh = df["trips_pickup_street_hail"].dropna()
print(f"trips_pickup_street_hail  →  {len(sh):,} non-null rows, mean = {sh.mean():.1f}")

# ── 4. Rain effect recheck with new data ────────────────────────────────────
rain = df.groupby("rainy_day")["trips_pickup_yellow"].mean()
fig, ax = plt.subplots(figsize=(5, 4))
bars = ax.bar(["No Rain", "Rain"], rain.values, color=["#4c9be8", "#1a5fa8"])
for bar, val in zip(bars, rain.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 3,
            f"{val:.0f}", ha="center", fontsize=11)
ax.set_title("Avg Yellow Taxi Trips: Rain vs No Rain")
ax.set_ylabel("Average trips per zone per day")
plt.tight_layout()
plt.savefig(OUT / "rain_demand_check.png", dpi=120)
plt.close()
print("[OK] Saved rain_demand_check.png")

print()
print("All checks passed. Plots saved to outputs/")
