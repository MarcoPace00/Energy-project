"""LOCAL MODE: the same pipeline, orchestrated by Airflow, with the lake in MinIO.

The DAG is deliberately thin: each task calls one function from energy_lab.pipeline,
the same functions GitHub Actions calls. Airflow adds scheduling, retries per step,
a UI to see which step failed, and logs per run.

The two downloads don't depend on each other, so they run in parallel; the transform
waits for both:

    fetch_open_meteo ─┐
                      ├─► transform_duckdb ─► export_dashboard
    fetch_entsoe ─────┘

Unlike the course weather DAG, no values are passed between tasks with XCom: every step
reads what it needs from the lake. Each step can therefore be re-run on its own.
"""

import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from energy_lab import pipeline

with DAG(
    dag_id="energy_lab_daily",
    description="Open-Meteo + ENTSO-E -> MinIO raw/ -> DuckDB processed/ -> dashboard export",
    schedule="30 13 * * *",  # same time as the GitHub Actions cron (UTC), after the day-ahead auction
    start_date=datetime.datetime(2026, 11, 1),
    catchup=False,  # don't create runs for the days before today: fetch is incremental anyway
    max_active_runs=1,  # two runs writing the same lake at once could clash
    default_args={
        "owner": "energy_lab",
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=10),
    },
    tags=["energy_lab"],
) as dag:
    fetch_weather = PythonOperator(task_id="fetch_open_meteo", python_callable=pipeline.step_fetch_open_meteo)
    fetch_power = PythonOperator(
        task_id="fetch_entsoe",
        python_callable=pipeline.step_fetch_entsoe,
        execution_timeout=datetime.timedelta(hours=2),  # the first run backfills since 2023
    )
    transform = PythonOperator(task_id="transform_duckdb", python_callable=pipeline.step_transform)
    export = PythonOperator(task_id="export_dashboard", python_callable=pipeline.step_export)

    [fetch_weather, fetch_power] >> transform >> export
