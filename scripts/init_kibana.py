#!/usr/bin/env python3
"""
Creates Kibana index patterns, visualizations, and a dashboard via the
Kibana Saved Objects REST API.

Run once after the pipeline has indexed data into Elasticsearch:
    python scripts/init_kibana.py
or from inside the airflow-scheduler container:
    docker compose exec airflow-scheduler python /opt/airflow/dags/../scripts/init_kibana.py
"""

from __future__ import annotations

import json
import sys
import time

import requests

KIBANA_URL = "http://localhost:5601"
HEADERS = {"kbn-xsrf": "true", "Content-Type": "application/json"}

# ── Helpers ────────────────────────────────────────────────────────────────────

def wait_for_kibana(retries: int = 30, delay: int = 5) -> None:
    print("Waiting for Kibana to be ready…")
    for _ in range(retries):
        try:
            r = requests.get(f"{KIBANA_URL}/api/status", timeout=5)
            status = r.json().get("status", {}).get("overall", {}).get("level", "")
            if status == "available":
                print("Kibana is ready.")
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(delay)
    sys.exit("Kibana did not become ready — aborting.")


def save_object(obj_type: str, obj_id: str, attributes: dict, references: list | None = None) -> dict:
    body: dict = {"attributes": attributes}
    if references:
        body["references"] = references
    r = requests.post(
        f"{KIBANA_URL}/api/saved_objects/{obj_type}/{obj_id}?overwrite=true",
        headers=HEADERS,
        json=body,
        timeout=15,
    )
    label = attributes.get("title", obj_id)
    print(f"  [{r.status_code}] {obj_type}: {label}")
    if r.status_code not in (200, 201):
        print(f"         Response: {r.text[:200]}")
    return r.json()


def index_ref(pattern_id: str) -> list:
    return [{"id": pattern_id, "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}]


def search_source(pattern_id: str) -> str:
    return json.dumps({
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
        "query": {"language": "kuery", "query": ""},
        "filter": [],
    })


# ── Index Patterns ─────────────────────────────────────────────────────────────

def create_index_patterns() -> None:
    print("\n── Index Patterns ──")
    patterns = [
        ("salary-by-skill-ip",    "salary_by_skill",    None),
        ("skill-demand-ip",       "skill_demand",        "month"),
        ("forecasts-ip",          "forecasts",           "forecast_date"),
        ("salary-comparison-ip",  "salary_comparison",   None),
        ("salary-predictions-ip", "salary_predictions",  None),
    ]
    for pid, title, time_field in patterns:
        attrs: dict = {"title": title, "fields": "[]"}
        if time_field:
            attrs["timeFieldName"] = time_field
        save_object("index-pattern", pid, attrs)


# ── Visualizations ─────────────────────────────────────────────────────────────

def _vis_attrs(title: str, vis_type: str, aggs: list, params: dict, pattern_id: str) -> dict:
    vis_state = json.dumps({
        "title": title,
        "type": vis_type,
        "aggs": aggs,
        "params": params,
    })
    return {
        "title": title,
        "visState": vis_state,
        "uiStateJSON": "{}",
        "description": "",
        "kibanaSavedObjectMeta": {"searchSourceJSON": search_source(pattern_id)},
    }


def create_visualizations() -> None:
    print("\n── Visualizations ──")

    # 1. Average salary by skill (horizontal bar)
    save_object(
        "visualization", "salary-bar-viz",
        _vis_attrs(
            "Average Salary by Skill (GBP)", "horizontal_bar",
            aggs=[
                {"id": "1", "enabled": True, "type": "avg",
                 "params": {"field": "avg_salary_gbp"}, "schema": "metric"},
                {"id": "2", "enabled": True, "type": "terms",
                 "params": {"field": "skill", "size": 12, "order": "desc", "orderBy": "1"},
                 "schema": "segment"},
            ],
            params={
                "type": "histogram",
                "grid": {"categoryLines": False},
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "left", "show": True,
                                  "labels": {"show": True, "truncate": 200}}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
                               "position": "bottom", "show": True,
                               "title": {"text": "Average Salary (£)"}}],
                "seriesParams": [{"show": True, "type": "bar", "mode": "stacked",
                                  "data": {"label": "Avg Salary", "id": "1"},
                                  "valueAxis": "ValueAxis-1"}],
                "addTooltip": True, "addLegend": True, "legendPosition": "right",
            },
            pattern_id="salary-by-skill-ip",
        ),
        references=index_ref("salary-by-skill-ip"),
    )

    # 2. Job count by skill (horizontal bar)
    save_object(
        "visualization", "job-count-viz",
        _vis_attrs(
            "Job Count by Skill", "horizontal_bar",
            aggs=[
                {"id": "1", "enabled": True, "type": "sum",
                 "params": {"field": "job_count"}, "schema": "metric"},
                {"id": "2", "enabled": True, "type": "terms",
                 "params": {"field": "skill", "size": 12, "order": "desc", "orderBy": "1"},
                 "schema": "segment"},
            ],
            params={
                "type": "histogram",
                "grid": {"categoryLines": False},
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "left", "show": True,
                                  "labels": {"show": True, "truncate": 200}}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
                               "position": "bottom", "show": True,
                               "title": {"text": "Number of Jobs"}}],
                "seriesParams": [{"show": True, "type": "bar", "mode": "stacked",
                                  "data": {"label": "Job Count", "id": "1"},
                                  "valueAxis": "ValueAxis-1"}],
                "addTooltip": True, "addLegend": True, "legendPosition": "right",
            },
            pattern_id="salary-by-skill-ip",
        ),
        references=index_ref("salary-by-skill-ip"),
    )

    # 3. Skill demand over time (multi-line)
    save_object(
        "visualization", "demand-line-viz",
        _vis_attrs(
            "Skill Demand Over Time", "line",
            aggs=[
                {"id": "1", "enabled": True, "type": "sum",
                 "params": {"field": "job_count"}, "schema": "metric"},
                {"id": "2", "enabled": True, "type": "date_histogram",
                 "params": {"field": "month", "interval": "auto", "min_doc_count": 1},
                 "schema": "segment"},
                {"id": "3", "enabled": True, "type": "terms",
                 "params": {"field": "skill", "size": 8, "order": "desc", "orderBy": "1"},
                 "schema": "group"},
            ],
            params={
                "type": "line",
                "grid": {"categoryLines": False},
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "bottom", "show": True}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
                               "position": "left", "show": True,
                               "title": {"text": "Job Postings"}}],
                "seriesParams": [{"show": True, "type": "line",
                                  "drawLinesBetweenPoints": True, "showCircles": True,
                                  "data": {"id": "1", "label": "Postings"},
                                  "valueAxis": "ValueAxis-1"}],
                "addTooltip": True, "addLegend": True, "legendPosition": "right",
            },
            pattern_id="skill-demand-ip",
        ),
        references=index_ref("skill-demand-ip"),
    )

    # 4. 6-month demand forecast (line)
    save_object(
        "visualization", "forecast-line-viz",
        _vis_attrs(
            "6-Month Demand Forecast (Prophet)", "line",
            aggs=[
                {"id": "1", "enabled": True, "type": "avg",
                 "params": {"field": "predicted_demand"}, "schema": "metric"},
                {"id": "2", "enabled": True, "type": "date_histogram",
                 "params": {"field": "forecast_date", "interval": "auto", "min_doc_count": 1},
                 "schema": "segment"},
                {"id": "3", "enabled": True, "type": "terms",
                 "params": {"field": "skill", "size": 8, "order": "desc", "orderBy": "1"},
                 "schema": "group"},
            ],
            params={
                "type": "line",
                "grid": {"categoryLines": False},
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "bottom", "show": True}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
                               "position": "left", "show": True,
                               "title": {"text": "Predicted Demand"}}],
                "seriesParams": [{"show": True, "type": "line",
                                  "drawLinesBetweenPoints": True, "showCircles": True,
                                  "data": {"id": "1", "label": "Predicted"},
                                  "valueAxis": "ValueAxis-1"}],
                "addTooltip": True, "addLegend": True, "legendPosition": "right",
            },
            pattern_id="forecasts-ip",
        ),
        references=index_ref("forecasts-ip"),
    )

    # 5. UK vs Global salary gap (horizontal bar)
    save_object(
        "visualization", "salary-gap-viz",
        _vis_attrs(
            "UK vs Global Salary Gap by Skill (GBP)", "horizontal_bar",
            aggs=[
                {"id": "1", "enabled": True, "type": "avg",
                 "params": {"field": "uk_vs_global_gap_gbp"}, "schema": "metric"},
                {"id": "2", "enabled": True, "type": "terms",
                 "params": {"field": "skill", "size": 12, "order": "desc", "orderBy": "1"},
                 "schema": "segment"},
            ],
            params={
                "type": "histogram",
                "grid": {"categoryLines": False},
                "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                  "position": "left", "show": True,
                                  "labels": {"show": True, "truncate": 200}}],
                "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
                               "position": "bottom", "show": True,
                               "title": {"text": "UK minus Global (£)"}}],
                "seriesParams": [{"show": True, "type": "bar", "mode": "stacked",
                                  "data": {"label": "Gap", "id": "1"},
                                  "valueAxis": "ValueAxis-1"}],
                "addTooltip": True, "addLegend": True, "legendPosition": "right",
            },
            pattern_id="salary-comparison-ip",
        ),
        references=index_ref("salary-comparison-ip"),
    )


# ── Dashboard ──────────────────────────────────────────────────────────────────

def create_dashboard() -> None:
    print("\n── Dashboard ──")
    panels = [
        {"id": "salary-bar-viz",   "x": 0,  "y": 0,  "w": 24, "h": 15, "i": "0", "ref": "panel_0"},
        {"id": "job-count-viz",    "x": 24, "y": 0,  "w": 24, "h": 15, "i": "1", "ref": "panel_1"},
        {"id": "demand-line-viz",  "x": 0,  "y": 15, "w": 24, "h": 15, "i": "2", "ref": "panel_2"},
        {"id": "forecast-line-viz","x": 24, "y": 15, "w": 24, "h": 15, "i": "3", "ref": "panel_3"},
        {"id": "salary-gap-viz",   "x": 0,  "y": 30, "w": 48, "h": 15, "i": "4", "ref": "panel_4"},
    ]

    panels_json = json.dumps([
        {
            "version": "8.13.0",
            "type": "visualization",
            "gridData": {"x": p["x"], "y": p["y"], "w": p["w"], "h": p["h"], "i": p["i"]},
            "panelIndex": p["i"],
            "embeddableConfig": {},
            "panelRefName": p["ref"],
        }
        for p in panels
    ])

    references = [
        {"id": p["id"], "name": p["ref"], "type": "visualization"}
        for p in panels
    ]

    attrs = {
        "title": "Developer Job Market",
        "description": "UK developer job market: salaries, skill demand, forecasts, and comparison with global SO survey benchmarks.",
        "panelsJSON": panels_json,
        "optionsJSON": json.dumps({"useMargins": True, "syncColors": False, "hidePanelTitles": False}),
        "version": 1,
        "timeRestore": False,
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({"query": {"language": "kuery", "query": ""}, "filter": []})
        },
    }
    save_object("dashboard", "dev-job-market-dashboard", attrs, references)


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    wait_for_kibana()
    create_index_patterns()
    create_visualizations()
    create_dashboard()
    print("\nDone. Open Kibana → Dashboards → 'Developer Job Market'")
    print(f"URL: {KIBANA_URL}/app/dashboards")
