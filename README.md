# NYC Tourism Data Integration Starter Scripts

This bundle provides a clean 2025 pilot pipeline for integrating:
- **NYC TLC trip data** (monthly Parquet files)
- **NYC CommonPlace / Points of Interest**
- **NOAA daily weather** for NYC (Central Park station by default)

The target integration grain is **`taxi_zone_id × date`**.

## Folder structure

A simple working layout is:

```text
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
└── requirements.txt
```

## 1) Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Download and aggregate TLC 2025 monthly files

Yellow taxi only:

```bash
python download_tlc_2025.py \
  --year 2025 \
  --modes yellow \
  --aggregate
```

Multiple modes:

```bash
python download_tlc_2025.py \
  --year 2025 \
  --modes yellow green hvfhv fhv \
  --aggregate
```

This creates files such as:

```text
data/processed/tlc/yellow_zone_date_2025.parquet
```

## 3) Build zone-level POI features

Download and export the **CommonPlace / Points of Interest** CSV from NYC Open Data.
Also download the **Taxi Zones** polygon file (shapefile / GeoJSON / GeoPackage).

Example:

```bash
python build_poi_zone_features.py \
  --poi-csv data/raw/poi/commonplace_2026-04-04.csv \
  --zones-file data/raw/taxi_zones/taxi_zones.shp \
  --output-file data/processed/poi/poi_zone_features.parquet \
  --poi-name-col "FEATURE NAME"
```

### Notes on POI geometry

The script supports two common layouts:
- explicit latitude and longitude columns
- a point/WKT-style column such as `POINT (-73.98 40.75)`

If the geometry columns are not inferred automatically, pass them explicitly:

```bash
python build_poi_zone_features.py \
  --poi-csv data/raw/poi/commonplace.csv \
  --zones-file data/raw/taxi_zones/taxi_zones.shp \
  --output-file data/processed/poi/poi_zone_features.parquet \
  --lat-col latitude \
  --lon-col longitude
```

or

```bash
python build_poi_zone_features.py \
  --poi-csv data/raw/poi/commonplace.csv \
  --zones-file data/raw/taxi_zones/taxi_zones.shp \
  --output-file data/processed/poi/poi_zone_features.parquet \
  --point-col the_geom
```

The script:
- parses coordinates
- normalizes `FEATURE NAME`
- removes exact near-duplicates using normalized name + rounded coordinates
- assigns each POI to a taxi zone using a point-in-polygon join
- aggregates zone-level counts by a compact category taxonomy

## 4) Build daily weather for 2025

This downloads NOAA daily summaries for the Central Park station by default.

```bash
python build_weather_2025.py \
  --station USW00094728 \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --output-file data/processed/weather/weather_2025.csv
```

Derived fields include:
- `temp_avg`
- `rainy_day`
- `heavy_rain_day`
- `pleasant_temp_day`

## 5) Build the integrated `zone × date` master table

```bash
python build_zone_date_master.py \
  --tlc-files data/processed/tlc/yellow_zone_date_2025.parquet \
  --poi-file data/processed/poi/poi_zone_features.parquet \
  --weather-file data/processed/weather/weather_2025.csv \
  --output-file data/processed/final/zone_date_master_2025.parquet
```

If you aggregated multiple TLC modes, pass them all:

```bash
python build_zone_date_master.py \
  --tlc-files \
    data/processed/tlc/yellow_zone_date_2025.parquet \
    data/processed/tlc/green_zone_date_2025.parquet \
    data/processed/tlc/hvfhv_zone_date_2025.parquet \
  --poi-file data/processed/poi/poi_zone_features.parquet \
  --weather-file data/processed/weather/weather_2025.csv \
  --output-file data/processed/final/zone_date_master_2025.parquet
```

## What each script assumes

### `download_tlc_2025.py`
- uses the public monthly TLC Parquet files
- standardizes a small set of common fields
- aggregates to pickup-side `taxi_zone_id × date × trip_mode`

### `build_poi_zone_features.py`
- expects a CommonPlace/POI CSV export and a taxi-zone polygon file
- treats POI as a static spatial layer for the 2025 pilot

### `build_weather_2025.py`
- uses NOAA/NCEI daily summaries
- defaults to Central Park station `USW00094728`
- builds one row per date

### `build_zone_date_master.py`
- merges TLC facts, POI zone features, and weather
- outputs a single analytical table keyed by `date` and `taxi_zone_id`

## Recommended first run

For the first proof of concept, start with:
- Yellow Taxi only
- CommonPlace POI features
- Central Park daily weather

That is the fastest path to a working 2025 `zone × date` prototype.
