# DE-Final-Project-Group-Metropolitan

Revisit the open urban data landscape in NYC through a joinability-driven case study of tourism.

---

## Pipeline Overview

This project builds a **reproducible data engineering pipeline** integrating:

- NYC Taxi Trip Data (TLC)
- Points of Interest (CommonPlace dataset)
- Daily Weather Data (NOAA)

All datasets are standardized to a common analytical unit:

**taxi_zone_id × date**

This enables cross-dataset integration and analysis of urban activity patterns.

---

## Run Full Pipeline (Recommended)

The entire pipeline can be executed end-to-end using a single command:

```bash
python run_pipeline.py --years 2019 2020 2025
```

### This will automatically:

- Download NYC Taxi Trip data (TLC)
- Download Taxi Zone shapefiles
- Fetch daily weather data
- Build POI zone features
- Construct integrated zone × date datasets
- Build the final DuckDB database

---

## ⚠️ Important (POI Dataset Requirement)

Before running the pipeline, you MUST manually place the POI dataset:

data/raw/poi/CommonPlace_20260408.csv

### Source:
NYC Open Data — CommonPlace Dataset  
https://data.cityofnewyork.us/City-Government/CommonPlace/t95h-5fsr/about_data

### Why manual?
This dataset:
- Is not accessed via a stable API
- Is exported manually from the NYC Open Data portal
- Is treated as a static reference dataset

---

## Folder Structure

project/
├── data/
│   ├── raw/
│   │   ├── tlc/
│   │   ├── poi/
│   │   └── taxi_zones/
│   └── processed/
│       ├── tlc/
│       ├── poi/
│       ├── weather/
│       └── final/
├── download_tlc_2025.py
├── build_poi_zone_features.py
├── build_weather_2025.py
├── build_zone_date_master.py
├── build_database.py
├── run_pipeline.py
└── requirements.txt

---

## Setup

### 1. Create environment

```bash
python -m venv .venv
```

### 2. Activate environment

Windows:
```bash
.venv\Scripts\activate
```

Mac/Linux:
```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

---

## Reproducibility (Clean Run)

To simulate a fresh environment:

Windows:
```powershell
Remove-Item -Recurse -Force data\processed\*
Remove-Item -Recurse -Force data\raw\tlc\*
Remove-Item -Recurse -Force data\raw\taxi_zones\*
```

Mac/Linux:
```bash
rm -rf data/processed/*
rm -rf data/raw/tlc/*
rm -rf data/raw/taxi_zones/*
```

Then run:

```bash
python run_pipeline.py --years 2019 2020 2025
```

The pipeline will automatically regenerate all datasets.

---

## Individual Pipeline Components (Optional)

### 1) Download and aggregate TLC data

Yellow taxi only (fastest, recommended for first run):

```bash
python download_tlc_2025.py \
  --year 2025 \
  --modes yellow \
  --aggregate
```

All modes (note: hvfhv is very large and will take significantly longer):

```bash
python download_tlc_2025.py \
  --year 2025 \
  --modes yellow green fhv hvfhv \
  --aggregate
```

If files are already downloaded and you only want to re-aggregate:

```bash
python download_tlc_2025.py \
  --year 2025 \
  --modes yellow green fhv hvfhv \
  --aggregate \
  --skip-download
```

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `--year` | `2025` | Target year to download |
| `--modes` | `yellow` | One or more of: `yellow`, `green`, `fhv`, `hvfhv` |
| `--raw-dir` | `data/raw/tlc` | Directory to save raw monthly parquet files |
| `--processed-dir` | `data/processed/tlc` | Directory to save aggregated zone-date parquet |
| `--aggregate` | *(flag)* | Aggregate monthly files into `taxi_zone_id × date` parquet after download |
| `--skip-download` | *(flag)* | Skip downloading; only re-aggregate already-downloaded files |

Output files follow the pattern: `data/processed/tlc/{mode}_zone_date_{year}.parquet`

---

### 2) Build POI zone features

If the POI CSV has a WKT geometry column (e.g. `the_geom` like `POINT (-73.98 40.75)`):

```bash
python build_poi_zone_features.py \
  --poi-csv data/raw/poi/CommonPlace_20260408.csv \
  --zones-file data/raw/taxi_zones/taxi_zones.shp \
  --output-file data/processed/poi/poi_zone_features.parquet \
  --point-col the_geom
```

If the POI CSV has explicit latitude and longitude columns:

```bash
python build_poi_zone_features.py \
  --poi-csv data/raw/poi/CommonPlace_20260408.csv \
  --zones-file data/raw/taxi_zones/taxi_zones.shp \
  --output-file data/processed/poi/poi_zone_features.parquet \
  --lat-col LATITUDE \
  --lon-col LONGITUDE
```

If neither is specified, the script tries to auto-detect common column names (`latitude`, `lon`, `lng`, `location`, etc.).

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `--poi-csv` | *(required)* | Path to CommonPlace/POI CSV export |
| `--zones-file` | *(required)* | Path to taxi zone shapefile (`.shp`, GeoJSON, or GeoPackage) |
| `--output-file` | *(required)* | Output path for zone-level POI features parquet |
| `--poi-name-col` | `FEATURE NAME` | Column containing POI name |
| `--point-col` | `None` | WKT point geometry column (e.g. `the_geom`) |
| `--lat-col` | `None` | Explicit latitude column |
| `--lon-col` | `None` | Explicit longitude column |
| `--category-cols` | `FACI_DOM FACILITY_T THEME` | Category columns used for POI classification |

The script performs deduplication (normalized name + rounded coordinates) and a spatial point-in-polygon join to assign each POI to a taxi zone.

---

### 3) Build weather data

```bash
python build_weather_2025.py \
  --station USW00094728 \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --output-file data/processed/weather/weather_2025.csv
```

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `--station` | `USW00094728` | NOAA station ID (default = Central Park, NYC) |
| `--start-date` | `2025-01-01` | Start date for weather fetch |
| `--end-date` | `2025-12-31` | End date for weather fetch |
| `--output-file` | *(required)* | Output path (`.csv` or `.parquet`) |
| `--units` | `metric` | Unit system: `metric` (°C, mm, m/s) or `standard` |

**Derived fields produced:**

| Field | Description |
|---|---|
| `tmax` / `tmin` | Max/min temperature (°C) |
| `prcp` | Precipitation (mm) |
| `awnd` | Average wind speed (m/s) |
| `snow` | Snowfall (mm) |
| `temp_avg` | `(tmax + tmin) / 2` |
| `rainy_day` | 1 if `prcp > 0` |
| `heavy_rain_day` | 1 if `prcp >= 10mm` |
| `pleasant_temp_day` | 1 if `temp_avg` between 15–27°C |
| `windy_day` | 1 if `awnd > 3.5 m/s` (~75th pct for NYC) |
| `season` | `winter / spring / summer / fall` (NYC-calibrated: March = winter, September = summer) |

> **Note:** NOAA does not provide wind data for Jan–Mar 2019. Those months will have `awnd = NaN` and `windy_day = NaN`.

---

### 4) Build integrated dataset

Yellow only:

```bash
python build_zone_date_master.py \
  --tlc-files data/processed/tlc/yellow_zone_date_2025.parquet \
  --poi-file data/processed/poi/poi_zone_features.parquet \
  --weather-file data/processed/weather/weather_2025.csv \
  --output-file data/processed/final/zone_date_master_2025.parquet
```

All modes (pass all aggregated TLC files together):

```bash
python build_zone_date_master.py \
  --tlc-files \
    data/processed/tlc/yellow_zone_date_2025.parquet \
    data/processed/tlc/green_zone_date_2025.parquet \
    data/processed/tlc/fhv_zone_date_2025.parquet \
    data/processed/tlc/hvfhv_zone_date_2025.parquet \
  --poi-file data/processed/poi/poi_zone_features.parquet \
  --weather-file data/processed/weather/weather_2025.csv \
  --output-file data/processed/final/zone_date_master_2025.parquet
```

**Parameters:**

| Parameter | Default | Description |
|---|---|---|
| `--tlc-files` | *(required)* | One or more aggregated TLC parquet files (all modes accepted) |
| `--poi-file` | *(required)* | Zone-level POI features parquet |
| `--weather-file` | *(required)* | Daily weather CSV or parquet |
| `--output-file` | *(required)* | Output path for the integrated `taxi_zone_id × date` master table |

The output is keyed by `date × taxi_zone_id` with trip metrics pivoted wide per mode (e.g. `trips_pickup_yellow`, `trips_pickup_hvfhv`), joined with static POI features and daily weather.

---

## What Each Script Does

| Script | Input | Output | Notes |
|---|---|---|---|
| `download_tlc_2025.py` | TLC public parquet URLs | Raw monthly parquets + aggregated zone-date parquet | Standardizes column names across modes |
| `build_poi_zone_features.py` | CommonPlace CSV + taxi zone shapefile | Zone-level POI feature parquet | Static spatial join; treated as fixed reference |
| `build_weather_2025.py` | NOAA NCEI API | Daily weather CSV | Central Park station by default |
| `build_zone_date_master.py` | All of the above | Final `taxi_zone_id × date` master table | Merges all datasets at the analysis grain |

---

## Hypothesis 1: Taxi Demand Drivers Analysis

### Objective

Evaluate how weather conditions and POI density impact taxi trip volume.

---

### Variables

Dependent Variable:
- trips_pickup_yellow

Independent Variables:
- Weather: rainy_day, heavy_rain_day, prcp, temp_avg, awnd, windy_day, snow
- Spatial: poi_count_total

---

### Analytical Approach

- 3-year comparison: 2019 (pre-COVID), 2020 (COVID), 2025 (post-COVID)
- Zone × Date (base level)
- Zone-level aggregation (spatial patterns)
- City-level aggregation (temporal trends)

---

### Key Findings

- Rain has a modest but consistent lift on taxi demand (3–8%) in normal years; heavy rain amplifies this further
- 2020 reversed the pattern — rain suppressed already-low COVID-era demand instead of boosting it
- Snow suppresses demand (-6.6%); people cancel plans rather than switch to taxis
- Wind and temperature show negligible effects
- Zone character matters most: walkable neighborhoods (East Village, Alphabet City) gain on rainy days; parks and outdoor zones (Flushing Meadows-Corona Park, Prospect Park) lose demand
- POI density shows weak correlation with demand

---

## Notebook

hypothesis_1_analysis.ipynb

---

## Run the Notebook

VS Code:
- Open project folder  
- Open notebook  
- Select .venv kernel  
- Run all cells  

Jupyter:
```bash
jupyter notebook
```

---

