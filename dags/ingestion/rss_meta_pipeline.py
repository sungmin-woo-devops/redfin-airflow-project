# -*- coding: utf-8 -*-
from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

try:
    from dags.datasets import RSS_META
except Exception:
    from airflow.datasets import Dataset
    RSS_META = Dataset("mongo://redfin/rss_meta")

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="rss_meta_collect",
    description="Scrapy 기반 RSS 메타/아이템 수집(최소형)",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule="*/30 * * * *",
    catchup=False,
    tags=["ingestion", "rss", "scrapy"],
) as dag:

    SCRAPY_PROJECT_DIR = "/opt/airflow/app/redfin_scraper"
    FEEDS_CFG = "/opt/airflow/config/feeds.yaml"
    OUT_DIR = "/opt/airflow/data/rss/meta/{{ ds_nodash }}"
    OUT_FILE = "rss_meta__{{ ds_nodash }}__{{ ts_nodash }}.jsonl"

    run_rss_spider = BashOperator(
        task_id="run_rss_feed_spider",
        bash_command=(
            "set -euo pipefail; "
            "mkdir -p {{ params.out_dir }} && "
            "cd {{ params.project_dir }} && "
            "scrapy crawl rss_feed -O {{ params.out_dir }}/{{ params.out_file }}"
        ),
        params={
            "project_dir": SCRAPY_PROJECT_DIR,
            "out_dir": OUT_DIR,
            "out_file": OUT_FILE,
        },
        env={
            "PYTHONPATH": "/opt/airflow/app",
            "FEEDS_CONFIG_PATH": FEEDS_CFG,
        },
        outlets=[RSS_META],
    )
