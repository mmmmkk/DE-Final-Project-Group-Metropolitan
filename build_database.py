#!/usr/bin/env python3
"""Build the DuckDB database from processed pipeline outputs.

Creates views over the processed parquet/csv files and populates
permanent tables (holidays, analytical views) used for downstream
analysis and the queries_database notebook.

Accepts multiple TLC and weather files (one per year) — the views
union all years so the database covers the full requested time span.

Usage
-----
python build_database.py
python build_database.py --years 2019 2020 2025
python build_database.py --db-path db/nyc.duckdb --tlc-files f1.parquet f2.parquet --weather-files f1.csv f2.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build DuckDB database from pipeline outputs.")
    p.add_argument("--db-path",        default="db/nyc.duckdb")
    p.add_argument("--tlc-files",      nargs="+",
                   default=["data/processed/tlc/yellow_zone_date_2025.parquet"])
    p.add_argument("--poi-file",       default="data/processed/poi/poi_zone_features.parquet")
    p.add_argument("--weather-files",  nargs="+",
                   default=["data/processed/weather/weather_2025.csv"])
    return p.parse_args()


def _parquet_union_sql(files: list[str]) -> str:
    if len(files) == 1:
        return f"SELECT * FROM '{files[0]}'"
    file_list = ", ".join(f"'{f}'" for f in files)
    return f"SELECT * FROM read_parquet([{file_list}])"


def _csv_union_sql(files: list[str]) -> str:
    if len(files) == 1:
        return f"SELECT * FROM read_csv_auto('{files[0]}')"
    file_list = ", ".join(f"'{f}'" for f in files)
    return f"SELECT * FROM read_csv_auto([{file_list}])"


# US federal holidays by year (plus Christmas Eve which was in the original data)
_HOLIDAYS: dict[int, list[tuple[str, str]]] = {
    2019: [
        ("2019-01-01", "New Year's Day"),
        ("2019-01-21", "Martin Luther King Jr. Day"),
        ("2019-02-18", "Presidents' Day"),
        ("2019-05-27", "Memorial Day"),
        ("2019-07-04", "Independence Day"),
        ("2019-09-02", "Labor Day"),
        ("2019-10-14", "Columbus Day"),
        ("2019-11-11", "Veterans Day"),
        ("2019-11-28", "Thanksgiving Day"),
        ("2019-12-24", "Christmas Eve"),
        ("2019-12-25", "Christmas Day"),
    ],
    2020: [
        ("2020-01-01", "New Year's Day"),
        ("2020-01-20", "Martin Luther King Jr. Day"),
        ("2020-02-17", "Presidents' Day"),
        ("2020-05-25", "Memorial Day"),
        ("2020-07-03", "Independence Day"),
        ("2020-09-07", "Labor Day"),
        ("2020-10-12", "Columbus Day"),
        ("2020-11-11", "Veterans Day"),
        ("2020-11-26", "Thanksgiving Day"),
        ("2020-12-24", "Christmas Eve"),
        ("2020-12-25", "Christmas Day"),
    ],
    2025: [
        ("2025-01-01", "New Year's Day"),
        ("2025-01-20", "Martin Luther King Jr. Day"),
        ("2025-02-17", "Presidents' Day"),
        ("2025-05-26", "Memorial Day"),
        ("2025-06-19", "Juneteenth"),
        ("2025-07-04", "Independence Day"),
        ("2025-09-01", "Labor Day"),
        ("2025-10-13", "Columbus Day"),
        ("2025-11-11", "Veterans Day"),
        ("2025-11-27", "Thanksgiving Day"),
        ("2025-12-24", "Christmas Eve"),
        ("2025-12-25", "Christmas Day"),
    ],
}


def _build_holidays_sql(tlc_files: list[str]) -> str:
    """Build the VALUES clause covering all years present in the TLC file list."""
    years_in_data: set[int] = set()
    for f in tlc_files:
        for year in _HOLIDAYS:
            if str(year) in f:
                years_in_data.add(year)

    rows: list[str] = []
    for year in sorted(years_in_data):
        for date, name in _HOLIDAYS.get(year, []):
            safe_name = name.replace("'", "''")
            rows.append(f"        (DATE '{date}', '{safe_name}')")

    if not rows:
        # Fallback: include all known holidays
        for year in sorted(_HOLIDAYS):
            for date, name in _HOLIDAYS[year]:
                safe_name = name.replace("'", "''")
                rows.append(f"        (DATE '{date}', '{safe_name}')")

    values = ",\n".join(rows)
    return f"""
        CREATE OR REPLACE TABLE holidays AS
        SELECT * FROM (VALUES
{values}
        ) AS t(date, holiday_name)
    """


def build_database(
    db_path: str,
    tlc_files: list[str],
    poi_file: str,
    weather_files: list[str],
) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path)
    print(f"[INFO] Connected to {db_path}")
    print(f"[INFO] TLC files    : {tlc_files}")
    print(f"[INFO] Weather files: {weather_files}")

    # ── Base views over processed files ───────────────────────────────────────
    con.execute(f"CREATE OR REPLACE VIEW tlc     AS {_parquet_union_sql(tlc_files)}")
    con.execute(f"CREATE OR REPLACE VIEW poi     AS SELECT * FROM '{poi_file}'")
    con.execute(f"CREATE OR REPLACE VIEW weather AS {_csv_union_sql(weather_files)}")
    print("[OK] Created views: tlc, poi, weather")

    # ── Holidays table (covers all years present in the TLC files) ─────────────
    con.execute(_build_holidays_sql(tlc_files))
    print("[OK] Created table: holidays")

    # ── Analytical views ───────────────────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE VIEW tlc_with_day_tag AS
        SELECT
            t.*,
            CASE
                WHEN h.date IS NOT NULL THEN 'holiday'
                WHEN EXTRACT('dow' FROM CAST(t.date AS DATE)) IN (0, 6) THEN 'weekend'
                ELSE 'weekday'
            END AS day_tag,
            h.holiday_name
        FROM tlc t
        LEFT JOIN holidays h ON CAST(t.date AS DATE) = h.date
    """)
    print("[OK] Created view: tlc_with_day_tag")

    con.execute("""
        CREATE OR REPLACE VIEW zone_tag_pickup_counts AS
        SELECT
            taxi_zone_id,
            day_tag,
            SUM(trips_pickup)                  AS total_pickups,
            AVG(trips_pickup)                  AS mean_daily_pickups,
            COUNT(DISTINCT CAST(date AS DATE)) AS num_days
        FROM tlc_with_day_tag
        GROUP BY taxi_zone_id, day_tag
    """)
    print("[OK] Created view: zone_tag_pickup_counts")

    con.execute("""
        CREATE OR REPLACE VIEW zone_tag_pickup_wide AS
        SELECT
            taxi_zone_id,
            MAX(CASE WHEN day_tag = 'weekday' THEN mean_daily_pickups END) AS mean_pickups_weekday,
            MAX(CASE WHEN day_tag = 'weekend' THEN mean_daily_pickups END) AS mean_pickups_weekend,
            MAX(CASE WHEN day_tag = 'holiday' THEN mean_daily_pickups END) AS mean_pickups_holiday
        FROM zone_tag_pickup_counts
        GROUP BY taxi_zone_id
    """)
    print("[OK] Created view: zone_tag_pickup_wide")

    con.execute("""
        CREATE OR REPLACE VIEW zone_weekend_holiday_lift AS
        SELECT
            taxi_zone_id,
            mean_pickups_weekday,
            mean_pickups_weekend,
            mean_pickups_holiday,
            mean_pickups_weekend / NULLIF(mean_pickups_weekday, 0) AS weekend_lift,
            mean_pickups_holiday / NULLIF(mean_pickups_weekday, 0) AS holiday_lift
        FROM zone_tag_pickup_wide
    """)
    print("[OK] Created view: zone_weekend_holiday_lift")

    con.execute("""
        CREATE OR REPLACE VIEW zone_tag_pickup_stats AS
        SELECT
            taxi_zone_id,
            day_tag,
            SUM(trips_pickup)                  AS total_pickups,
            AVG(trips_pickup)                  AS mean_daily_pickups,
            MIN(trips_pickup)                  AS min_daily_pickups,
            MAX(trips_pickup)                  AS max_daily_pickups,
            COUNT(DISTINCT CAST(date AS DATE)) AS num_days
        FROM tlc_with_day_tag
        GROUP BY taxi_zone_id, day_tag
    """)
    print("[OK] Created view: zone_tag_pickup_stats")

    con.close()
    print(f"\n[OK] Database ready at {db_path}")


def main() -> int:
    args = parse_args()
    build_database(args.db_path, args.tlc_files, args.poi_file, args.weather_files)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
