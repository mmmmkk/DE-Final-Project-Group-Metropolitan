#!/usr/bin/env python3
"""Build an interactive year-switching pickup map with clickable OD flows.

The output is a standalone Leaflet HTML map inspired by the interactive maps in
queries_database.ipynb, extended to compare 2019, 2020, and 2025 without
changing the map position or choropleth color domain.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd


DEFAULT_YEARS = [2019, 2020, 2025]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build interactive yearly pickup + OD flow map.")
    parser.add_argument("--years", nargs="+", type=int, default=DEFAULT_YEARS)
    parser.add_argument(
        "--day-tag",
        choices=["all", "weekday", "weekend", "holiday"],
        default="all",
        help="Use all days by default; optionally filter to one day type.",
    )
    parser.add_argument("--db-path", default="db/nyc.duckdb")
    parser.add_argument("--raw-dir", default="data/raw/tlc")
    parser.add_argument("--zones-file", default="data/raw/taxi_zones/taxi_zones.shp")
    parser.add_argument("--poi-file", default="data/processed/poi/poi_zone_features.parquet")
    parser.add_argument("--output-html", default=None)
    parser.add_argument("--flow-cache", default=None)
    parser.add_argument("--rebuild-flow-cache", action="store_true")
    return parser.parse_args()


def sql_list(values: list[str]) -> str:
    return "[" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + "]"


def load_zone_stats(db_path: str, years: list[int], day_tag: str) -> pd.DataFrame:
    year_list = ", ".join(str(y) for y in years)
    table = "tlc_with_day_tag" if day_tag != "all" else "tlc"
    where_day_tag = "AND day_tag = ?" if day_tag != "all" else ""
    params = [day_tag] if day_tag != "all" else []
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(
            f"""
            SELECT
                EXTRACT(year FROM CAST(date AS DATE))::INTEGER AS year,
                taxi_zone_id::INTEGER AS taxi_zone_id,
                ? AS day_tag,
                SUM(trips_pickup)::DOUBLE AS total_pickups,
                AVG(trips_pickup)::DOUBLE AS mean_daily_pickups,
                MIN(trips_pickup)::DOUBLE AS min_daily_pickups,
                MAX(trips_pickup)::DOUBLE AS max_daily_pickups,
                COUNT(DISTINCT CAST(date AS DATE))::INTEGER AS num_days
            FROM {table}
            WHERE TRUE
              {where_day_tag}
              AND EXTRACT(year FROM CAST(date AS DATE)) IN ({year_list})
            GROUP BY 1, 2, 3
            ORDER BY 1, 2
            """,
            [day_tag, *params],
        ).fetchdf()
    finally:
        con.close()


def load_zones(zones_file: str) -> tuple[gpd.GeoDataFrame, dict[int, dict[str, float]]]:
    zones = gpd.read_file(zones_file)
    zones = zones.rename(columns={"LocationID": "taxi_zone_id"})
    zones["taxi_zone_id"] = zones["taxi_zone_id"].astype(int)

    centroids_projected = zones.geometry.centroid
    centroids = gpd.GeoDataFrame(
        zones[["taxi_zone_id"]].copy(),
        geometry=centroids_projected,
        crs=zones.crs,
    ).to_crs("EPSG:4326")
    centroid_lookup = {
        int(row.taxi_zone_id): {"lat": float(row.geometry.y), "lng": float(row.geometry.x)}
        for row in centroids.itertuples()
    }

    zones = zones.to_crs("EPSG:4326")
    zones["geometry"] = zones.geometry.simplify(0.00018, preserve_topology=True)
    return zones, centroid_lookup


def load_poi(poi_file: str) -> pd.DataFrame:
    poi = pd.read_parquet(poi_file)
    poi["taxi_zone_id"] = poi["taxi_zone_id"].astype(int)
    return poi


def make_features(
    zones: gpd.GeoDataFrame,
    poi: pd.DataFrame,
    stats: pd.DataFrame,
    years: list[int],
) -> tuple[dict, float]:
    poi_cols = [
        "taxi_zone_id",
        "poi_count_total",
        "poi_count_commercial",
        "poi_count_cultural",
        "poi_count_recreational",
        "poi_count_transportation",
        "poi_count_education",
        "poi_name_examples",
    ]
    poi_cols = [c for c in poi_cols if c in poi.columns]
    gdf = zones.merge(poi[poi_cols], on="taxi_zone_id", how="left")

    stats_lookup: dict[tuple[int, int], dict] = {}
    for row in stats.itertuples(index=False):
        stats_lookup[(int(row.taxi_zone_id), int(row.year))] = {
            "day_tag": row.day_tag,
            "total_pickups": float(row.total_pickups or 0),
            "mean_daily_pickups": float(row.mean_daily_pickups or 0),
            "min_daily_pickups": float(row.min_daily_pickups or 0),
            "max_daily_pickups": float(row.max_daily_pickups or 0),
            "num_days": int(row.num_days or 0),
        }

    max_metric = float(stats["total_pickups"].max()) if len(stats) else 0.0

    def int_or_zero(value) -> int:
        if pd.isna(value):
            return 0
        return int(value or 0)

    features = []
    for row in gdf.itertuples():
        props = {
            "taxi_zone_id": int(row.taxi_zone_id),
            "zone": getattr(row, "zone"),
            "borough": getattr(row, "borough"),
            "poi_count_total": int_or_zero(getattr(row, "poi_count_total", 0)),
            "poi_count_commercial": int_or_zero(getattr(row, "poi_count_commercial", 0)),
            "poi_count_cultural": int_or_zero(getattr(row, "poi_count_cultural", 0)),
            "poi_count_recreational": int_or_zero(getattr(row, "poi_count_recreational", 0)),
            "poi_count_transportation": int_or_zero(getattr(row, "poi_count_transportation", 0)),
            "poi_count_education": int_or_zero(getattr(row, "poi_count_education", 0)),
            "poi_name_examples": "" if pd.isna(getattr(row, "poi_name_examples", "")) else getattr(row, "poi_name_examples", ""),
            "metrics": {},
        }
        for year in years:
            props["metrics"][str(year)] = stats_lookup.get(
                (props["taxi_zone_id"], year),
                {
                    "day_tag": None,
                    "total_pickups": 0,
                    "mean_daily_pickups": 0,
                    "min_daily_pickups": 0,
                    "max_daily_pickups": 0,
                    "num_days": 0,
                },
            )
        features.append(
            {
                "type": "Feature",
                "geometry": row.geometry.__geo_interface__,
                "properties": props,
            }
        )

    return {"type": "FeatureCollection", "features": features}, max_metric


def raw_files_for_year(raw_dir: str, year: int) -> list[str]:
    return [str(p) for p in sorted(Path(raw_dir).glob(f"yellow_tripdata_{year}-*.parquet"))]


def build_flow_counts(
    db_path: str,
    raw_dir: str,
    years: list[int],
    day_tag: str,
    cache_path: Path,
    rebuild: bool,
) -> pd.DataFrame:
    if cache_path.exists() and not rebuild:
        return pd.read_parquet(cache_path)

    frames: list[pd.DataFrame] = []
    con = duckdb.connect(db_path, read_only=True)
    try:
        for year in years:
            files = raw_files_for_year(raw_dir, year)
            if not files:
                print(f"[WARN] No raw yellow TLC files found for {year}; skipping flows.")
                continue
            file_expr = sql_list(files)
            print(f"[INFO] Aggregating top OD candidates for {year} {day_tag} trips from {len(files)} files")
            day_tag_select = (
                """
                            CASE
                                WHEN h.date IS NOT NULL THEN 'holiday'
                                WHEN EXTRACT('dow' FROM pickup_date) IN (0, 6) THEN 'weekend'
                                ELSE 'weekday'
                            END AS day_tag
                """
                if day_tag != "all"
                else "'all' AS day_tag"
            )
            holidays_join = "LEFT JOIN holidays h ON r.pickup_date = h.date" if day_tag != "all" else ""
            day_tag_where = "WHERE day_tag = ?" if day_tag != "all" else ""
            params = [day_tag] if day_tag != "all" else []
            frames.append(
                con.execute(
                    f"""
                    WITH raw_trips AS (
                        SELECT
                            CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
                            PULocationID::INTEGER AS pickup_zone_id,
                            DOLocationID::INTEGER AS dropoff_zone_id
                        FROM read_parquet({file_expr}, union_by_name=true)
                        WHERE PULocationID BETWEEN 1 AND 263
                          AND DOLocationID BETWEEN 1 AND 263
                          AND EXTRACT(year FROM CAST(tpep_pickup_datetime AS DATE)) = {year}
                    ),
                    tagged AS (
                        SELECT
                            {year}::INTEGER AS year,
                            pickup_zone_id,
                            dropoff_zone_id,
{day_tag_select}
                        FROM raw_trips r
                        {holidays_join}
                    )
                    SELECT
                        year,
                        pickup_zone_id,
                        dropoff_zone_id,
                        COUNT(*)::BIGINT AS trip_count
                    FROM tagged
                    {day_tag_where}
                    GROUP BY 1, 2, 3
                    """,
                    params,
                ).fetchdf()
            )
    finally:
        con.close()

    flow_counts = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=["year", "pickup_zone_id", "dropoff_zone_id", "trip_count"]
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    flow_counts.to_parquet(cache_path, index=False)
    print(f"[OK] Wrote flow cache: {cache_path}")
    return flow_counts


def select_top_flows(flow_counts: pd.DataFrame) -> dict[str, dict[str, list[dict]]]:
    out: dict[str, dict[str, list[dict]]] = {}
    if flow_counts.empty:
        return out

    for (year, pickup_zone), group in flow_counts.groupby(["year", "pickup_zone_id"]):
        ranked = group.sort_values("trip_count", ascending=False)
        top = ranked.head(5).copy()
        self_flow = ranked[ranked["dropoff_zone_id"] == pickup_zone]
        if not self_flow.empty and pickup_zone not in set(top["dropoff_zone_id"]):
            top = pd.concat([top.head(4), self_flow.head(1)], ignore_index=True)
            top = top.sort_values("trip_count", ascending=False)

        out.setdefault(str(int(year)), {})[str(int(pickup_zone))] = [
            {
                "dropoff_zone_id": int(row.dropoff_zone_id),
                "trip_count": int(row.trip_count),
            }
            for row in top.itertuples(index=False)
        ]
    return out


def render_html(
    feature_collection: dict,
    centroids: dict[int, dict[str, float]],
    flows: dict,
    years: list[int],
    day_tag: str,
    max_metric: float,
    output_html: Path,
) -> None:
    template = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>NYC Taxi Pickup Map with Year Slider and Dropoff Flows</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    html, body, #map { height: 100%; margin: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #172033; }
    #map { background: #f5f7fa; }
    .title-card {
      position: absolute; top: 18px; left: 18px; z-index: 900;
      width: 330px; padding: 14px 16px; background: rgba(255,255,255,0.94);
      border: 1px solid #d9dee7; border-radius: 8px; box-shadow: 0 8px 24px rgba(24,37,56,0.16);
    }
    .title-card h1 { font-size: 17px; line-height: 1.2; margin: 0 0 8px; letter-spacing: 0; }
    .title-card p { font-size: 12px; line-height: 1.45; margin: 0; color: #4c596f; }
    .year-panel {
      position: absolute; right: 22px; bottom: 22px; z-index: 910;
      width: 300px; padding: 14px 16px; background: rgba(255,255,255,0.96);
      border: 1px solid #d9dee7; border-radius: 8px; box-shadow: 0 8px 24px rgba(24,37,56,0.18);
    }
    .year-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .year-label { font-weight: 700; font-size: 24px; color: #172033; }
    .phase-label { font-size: 12px; color: #4c596f; text-align: right; }
    input[type="range"] { width: 100%; margin-top: 12px; accent-color: #2f5597; }
    .tick-row { display: flex; justify-content: space-between; color: #657086; font-size: 11px; margin-top: 4px; }
    .legend {
      position: absolute; right: 22px; bottom: 154px; z-index: 905;
      width: 300px; padding: 12px 16px; background: rgba(255,255,255,0.94);
      border: 1px solid #d9dee7; border-radius: 8px; box-shadow: 0 8px 24px rgba(24,37,56,0.14);
      font-size: 12px;
    }
    .gradient { height: 12px; margin: 8px 0 5px; border-radius: 999px; background: linear-gradient(90deg, #f7fbff, #c6dbef, #6baed6, #2171b5, #08306b); }
    .legend-scale { display: flex; justify-content: space-between; color: #657086; }
    .flow-panel {
      position: absolute; left: 18px; bottom: 22px; z-index: 910;
      width: 350px; max-height: 38vh; overflow: auto; padding: 14px 16px;
      background: rgba(255,255,255,0.96); border: 1px solid #d9dee7; border-radius: 8px;
      box-shadow: 0 8px 24px rgba(24,37,56,0.18); display: none;
    }
    .flow-panel h2 { font-size: 15px; margin: 0 0 8px; letter-spacing: 0; }
    .flow-panel table { width: 100%; border-collapse: collapse; font-size: 12px; }
    .flow-panel th, .flow-panel td { text-align: left; padding: 6px 4px; border-bottom: 1px solid #e8ecf2; }
    .flow-panel button {
      margin-top: 10px; border: 1px solid #b8c1d1; background: #fff; border-radius: 6px;
      padding: 7px 10px; cursor: pointer; font-size: 12px;
    }
    .zone-tooltip {
      min-width: 260px; color: #172033; font-size: 12px; line-height: 1.35;
    }
    .zone-tooltip .name { font-weight: 700; font-size: 13px; margin-bottom: 4px; }
    .zone-tooltip table { border-collapse: collapse; width: 100%; }
    .zone-tooltip td { padding: 2px 4px 2px 0; vertical-align: top; }
    .zone-tooltip td:first-child { color: #5c687c; white-space: nowrap; }
    .flow-label {
      padding: 2px 5px; background: rgba(255,255,255,0.88); border: 1px solid #b8c1d1;
      border-radius: 4px; color: #172033; font-size: 11px; font-weight: 700; white-space: nowrap;
      box-shadow: 0 2px 8px rgba(24,37,56,0.16);
    }
    .arrow-icon {
      width: 0; height: 0; border-top: 7px solid transparent; border-bottom: 7px solid transparent;
      border-left: 15px solid #1f4e79; transform-origin: 50% 50%; filter: drop-shadow(0 1px 2px rgba(0,0,0,0.25));
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <div class="title-card">
    <h1>NYC Yellow Taxi __DAY_SCOPE_TITLE__ Pickup Mobility</h1>
    <p>Slide between years for a fixed-scale choropleth based on annual total pickups. Hover for pickup and POI metrics. Click a zone to show its top five dropoff flows for the selected year.</p>
  </div>
  <div class="legend">
    <strong>__LEGEND_TITLE__</strong>
    <div class="gradient"></div>
    <div class="legend-scale"><span>0</span><span id="legendMax"></span></div>
  </div>
  <div class="year-panel">
    <div class="year-row">
      <div class="year-label" id="yearLabel"></div>
      <div class="phase-label" id="phaseLabel"></div>
    </div>
    <input id="yearSlider" type="range" min="0" max="__SLIDER_MAX__" step="1" value="__SLIDER_START__" />
    <div class="tick-row">__YEAR_TICKS__</div>
  </div>
  <div class="flow-panel" id="flowPanel">
    <h2 id="flowTitle"></h2>
    <div id="flowSummary"></div>
    <button id="clearFlows">Return to main map</button>
  </div>
  <script>
    const zoneData = __ZONE_DATA__;
    const centroids = __CENTROIDS__;
    const flowsByYear = __FLOWS__;
    const years = __YEARS__;
    const phases = {{
      2019: "Pre-COVID baseline",
      2020: "COVID disruption",
      2025: "Post-COVID comparison"
    }};
    const maxMetric = __MAX_METRIC__;
    let currentYear = years[__SLIDER_START__];
    let selectedZone = null;
    let selectedLayer = null;

    const map = L.map("map", {{
      center: [40.7128, -74.0060],
      zoom: 10,
      preferCanvas: true
    }});
    L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png", {{
      attribution: "&copy; OpenStreetMap contributors &copy; CARTO",
      subdomains: "abcd",
      maxZoom: 19
    }}).addTo(map);

    const flowLayer = L.layerGroup().addTo(map);
    const zoneById = new Map(zoneData.features.map(f => [String(f.properties.taxi_zone_id), f.properties]));

    function fmtNumber(value, digits = 0) {{
      return Number(value || 0).toLocaleString(undefined, {{ maximumFractionDigits: digits }});
    }}

    function colorFor(value) {{
      const denom = Math.log1p(maxMetric || 1);
      const t = Math.max(0, Math.min(1, Math.log1p(value || 0) / denom));
      if (t < 0.2) return "#f7fbff";
      if (t < 0.4) return "#c6dbef";
      if (t < 0.6) return "#6baed6";
      if (t < 0.8) return "#2171b5";
      return "#08306b";
    }}

    function zoneStyle(feature) {{
      const metric = feature.properties.metrics[String(currentYear)] || {{}};
      const value = metric.total_pickups || 0;
      return {{
        fillColor: colorFor(value),
        fillOpacity: value > 0 ? 0.78 : 0.08,
        color: selectedZone === String(feature.properties.taxi_zone_id) ? "#111827" : "#48566a",
        weight: selectedZone === String(feature.properties.taxi_zone_id) ? 2.2 : 0.45,
        opacity: 0.75
      }};
    }}

    function tooltipHtml(props) {{
      const metric = props.metrics[String(currentYear)] || {{}};
      return `
        <div class="zone-tooltip">
          <div class="name">${{props.zone}} <span style="color:#657086;">(${{props.taxi_zone_id}})</span></div>
          <table>
            <tr><td>Borough</td><td>${{props.borough || ""}}</td></tr>
            <tr><td>Year</td><td>${{currentYear}}</td></tr>
            <tr><td>Scope</td><td>${{metric.day_tag || "__DAY_TAG__"}}</td></tr>
            <tr><td>Total pickups</td><td>${{fmtNumber(metric.total_pickups)}}</td></tr>
            <tr><td>Mean daily pickups</td><td>${{fmtNumber(metric.mean_daily_pickups, 1)}}</td></tr>
            <tr><td>Min / max daily</td><td>${{fmtNumber(metric.min_daily_pickups)}} / ${{fmtNumber(metric.max_daily_pickups)}}</td></tr>
            <tr><td>Observed days</td><td>${{fmtNumber(metric.num_days)}}</td></tr>
            <tr><td>Total POIs</td><td>${{fmtNumber(props.poi_count_total)}}</td></tr>
            <tr><td>Commercial / cultural</td><td>${{fmtNumber(props.poi_count_commercial)}} / ${{fmtNumber(props.poi_count_cultural)}}</td></tr>
            <tr><td>Recreation / transit</td><td>${{fmtNumber(props.poi_count_recreational)}} / ${{fmtNumber(props.poi_count_transportation)}}</td></tr>
          </table>
        </div>`;
    }}

    function onEachZone(feature, layer) {{
      layer.on({{
        mouseover: e => {{
          e.target.setStyle({{ weight: selectedZone === String(feature.properties.taxi_zone_id) ? 2.4 : 1.4, color: "#111827" }});
          e.target.bindTooltip(tooltipHtml(feature.properties), {{ sticky: true, direction: "auto" }}).openTooltip();
        }},
        mouseout: e => {{
          zoneLayer.resetStyle(e.target);
          if (selectedLayer) selectedLayer.setStyle(zoneStyle(selectedLayer.feature));
        }},
        click: e => {{
          selectedZone = String(feature.properties.taxi_zone_id);
          selectedLayer = e.target;
          zoneLayer.setStyle(zoneStyle);
          drawFlows(selectedZone);
        }}
      }});
    }}

    const zoneLayer = L.geoJSON(zoneData, {{ style: zoneStyle, onEachFeature: onEachZone }}).addTo(map);

    function screenAngleDegrees(a, b) {{
      const start = map.latLngToLayerPoint([a.lat, a.lng]);
      const end = map.latLngToLayerPoint([b.lat, b.lng]);
      return Math.atan2(end.y - start.y, end.x - start.x) * 180 / Math.PI;
    }}

    function interpolate(a, b, t) {{
      return [a.lat + (b.lat - a.lat) * t, a.lng + (b.lng - a.lng) * t];
    }}

    function drawFlows(originId) {{
      flowLayer.clearLayers();
      const origin = centroids[originId];
      const flows = ((flowsByYear[String(currentYear)] || {{}})[originId] || []);
      const originProps = zoneById.get(originId);
      if (!origin || !originProps) return;

      const maxFlow = flows.reduce((m, f) => Math.max(m, f.trip_count || 0), 1);
      const rows = [];

      flows.forEach((flow, idx) => {{
        const destId = String(flow.dropoff_zone_id);
        const dest = centroids[destId];
        const destProps = zoneById.get(destId);
        const count = flow.trip_count || 0;
        const width = 2 + 8 * Math.sqrt(count / maxFlow);
        const color = idx === 0 ? "#12355b" : "#1f77b4";
        rows.push(`<tr><td>${{idx + 1}}</td><td>${{destProps ? destProps.zone : destId}}</td><td>${{fmtNumber(count)}}</td></tr>`);

        if (!dest) return;

        if (destId === originId) {{
          L.circleMarker([origin.lat, origin.lng], {{
            radius: 14 + width,
            color,
            weight: Math.max(2, width / 2),
            fillColor: "#ffffff",
            fillOpacity: 0.15,
            opacity: 0.9
          }}).addTo(flowLayer);
          L.marker([origin.lat, origin.lng], {{
            interactive: false,
            icon: L.divIcon({{ className: "flow-label", html: `${{fmtNumber(count)}} self`, iconSize: null }})
          }}).addTo(flowLayer);
          return;
        }}

        const line = L.polyline([[origin.lat, origin.lng], [dest.lat, dest.lng]], {{
          color,
          weight: width,
          opacity: 0.72,
          lineCap: "round"
        }}).addTo(flowLayer);
        line.bindTooltip(`${{originProps.zone}} to ${{destProps ? destProps.zone : destId}}: ${{fmtNumber(count)}} trips`);

        const arrowPoint = interpolate(origin, dest, 0.86);
        const angle = screenAngleDegrees(origin, dest);
        L.marker(arrowPoint, {{
          interactive: false,
          icon: L.divIcon({{
            className: "",
            html: `<div class="arrow-icon" style="transform: rotate(${{angle}}deg);"></div>`,
            iconSize: [20, 20],
            iconAnchor: [10, 10]
          }})
        }}).addTo(flowLayer);

        const labelPoint = interpolate(origin, dest, 0.52);
        L.marker(labelPoint, {{
          interactive: false,
          icon: L.divIcon({{ className: "flow-label", html: fmtNumber(count), iconSize: null }})
        }}).addTo(flowLayer);
      }});

      document.getElementById("flowTitle").textContent = `${{originProps.zone}} dropoff flows, ${{currentYear}}`;
      document.getElementById("flowSummary").innerHTML = flows.length
        ? `<table><thead><tr><th>#</th><th>Dropoff zone</th><th>Trips</th></tr></thead><tbody>${{rows.join("")}}</tbody></table>`
        : `<p style="font-size:12px; color:#657086;">No dropoff flows found for this zone and year.</p>`;
      document.getElementById("flowPanel").style.display = "block";
    }}

    function clearFlows() {{
      selectedZone = null;
      selectedLayer = null;
      flowLayer.clearLayers();
      document.getElementById("flowPanel").style.display = "none";
      zoneLayer.setStyle(zoneStyle);
    }}

    function updateYear(index) {{
      currentYear = years[Number(index)];
      document.getElementById("yearLabel").textContent = currentYear;
      document.getElementById("phaseLabel").textContent = phases[currentYear] || "";
      zoneLayer.setStyle(zoneStyle);
      zoneLayer.eachLayer(layer => {{
        if (layer.getTooltip()) layer.setTooltipContent(tooltipHtml(layer.feature.properties));
      }});
      if (selectedZone) drawFlows(selectedZone);
    }}

    document.getElementById("legendMax").textContent = fmtNumber(maxMetric, 1);
    document.getElementById("yearSlider").addEventListener("input", e => updateYear(e.target.value));
    document.getElementById("clearFlows").addEventListener("click", clearFlows);
    updateYear(__SLIDER_START__);
  </script>
</body>
</html>
"""

    slider_start = years.index(2025) if 2025 in years else len(years) - 1
    scope_title = "All Trips" if day_tag == "all" else f"{day_tag.title()} Trips"
    legend_title = "Annual total pickups" if day_tag == "all" else f"{day_tag.title()} total pickups"
    replacements = {
        "__ZONE_DATA__": json.dumps(feature_collection),
        "__CENTROIDS__": json.dumps({str(k): v for k, v in centroids.items()}),
        "__FLOWS__": json.dumps(flows),
        "__YEARS__": json.dumps(years),
        "__SLIDER_MAX__": str(len(years) - 1),
        "__SLIDER_START__": str(slider_start),
        "__YEAR_TICKS__": "".join(f"<span>{year}</span>" for year in years),
        "__DAY_TAG__": day_tag,
        "__DAY_SCOPE_TITLE__": scope_title,
        "__LEGEND_TITLE__": legend_title,
        "__MAX_METRIC__": json.dumps(max_metric),
    }
    html = template.replace("{{", "{").replace("}}", "}")
    for token, value in replacements.items():
        html = html.replace(token, value)
    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_html.write_text(html, encoding="utf-8")


def main() -> int:
    args = parse_args()
    years = sorted(args.years)
    output_html = Path(args.output_html or f"outputs/interactive_pickups_{args.day_tag}_year_flow.html")
    flow_cache = Path(
        args.flow_cache
        or f"data/processed/tlc/yellow_od_counts_{args.day_tag}_{'_'.join(map(str, years))}.parquet"
    )

    print("[INFO] Loading zone stats, POI features, and taxi-zone geometries")
    stats = load_zone_stats(args.db_path, years, args.day_tag)
    zones, centroids = load_zones(args.zones_file)
    poi = load_poi(args.poi_file)
    feature_collection, max_metric = make_features(zones, poi, stats, years)

    flow_counts = build_flow_counts(
        args.db_path,
        args.raw_dir,
        years,
        args.day_tag,
        flow_cache,
        args.rebuild_flow_cache,
    )
    flows = select_top_flows(flow_counts)

    render_html(feature_collection, centroids, flows, years, args.day_tag, max_metric, output_html)
    print(f"[OK] Wrote interactive map: {output_html}")
    print(f"[INFO] Fixed color scale max total pickups: {max_metric:,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
