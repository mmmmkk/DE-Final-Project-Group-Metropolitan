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

python run_pipeline.py --years 2019 2020 2025

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

python -m venv .venv

### 2. Activate environment

Windows:
.venv\Scripts\activate

Mac/Linux:
source .venv/bin/activate

### 3. Install dependencies

pip install -r requirements.txt

---

## Reproducibility (Clean Run)

To simulate a fresh environment:

Windows:
Remove-Item -Recurse -Force data\processed\*
Remove-Item -Recurse -Force data\raw\tlc\*
Remove-Item -Recurse -Force data\raw\taxi_zones\*

Mac/Linux:
rm -rf data/processed/*
rm -rf data/raw/tlc/*
rm -rf data/raw/taxi_zones/*

Then run:

python run_pipeline.py --years 2019 2020 2025

The pipeline will automatically regenerate all datasets.

---

## Individual Pipeline Components (Optional)

### 1) Download and aggregate TLC data

python download_tlc_2025.py --year 2025 --modes yellow --aggregate

---

### 2) Build POI zone features

python build_poi_zone_features.py \
  --poi-csv data/raw/poi/CommonPlace_20260408.csv \
  --zones-file data/raw/taxi_zones/taxi_zones.shp \
  --output-file data/processed/poi/poi_zone_features.parquet \
  --point-col the_geom

---

### 3) Build weather data

python build_weather_2025.py \
  --station USW00094728 \
  --start-date 2025-01-01 \
  --end-date 2025-12-31 \
  --output-file data/processed/weather/weather_2025.csv

---

### 4) Build integrated dataset

python build_zone_date_master.py \
  --tlc-files data/processed/tlc/yellow_zone_date_2025.parquet \
  --poi-file data/processed/poi/poi_zone_features.parquet \
  --weather-file data/processed/weather/weather_2025.csv \
  --output-file data/processed/final/zone_date_master_2025.parquet

---

## Hypothesis 1: Taxi Demand Drivers Analysis

### Objective

Evaluate how weather conditions and POI density impact taxi trip volume.

---

### Variables

Dependent Variable:
- trips_pickup_yellow

Independent Variables:
- Weather: rainy_day, heavy_rain_day, temp_avg, pleasant_temp_day
- Spatial: poi_count_total

---

### Analytical Approach

- Zone × Date (base level)
- Zone-level aggregation (spatial patterns)
- City-level aggregation (temporal trends)

---

### Key Findings

- Taxi demand increases on rainy and heavy rain days  
- Temperature has negligible impact  
- POI density shows weak correlation  
- Demand exhibits strong temporal variation  

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
jupyter notebook

---

