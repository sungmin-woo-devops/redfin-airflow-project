# -*- coding: utf-8 -*-
from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset

try:
    from dags.datasets import RSS_META, RSS_EXTRACTED
except Exception:
    RSS_META = Dataset("mongo://redfin/rss_meta")
    RSS_EXTRACTED = Dataset("mongo://redfin/rss_extracted")

KST = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="rss_extract_pipeline",
    description="메타 산출물 기반 Scrapy extractor 실행(최소형)",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_META],   # 상류 Dataset 트리거
    catchup=False,
    tags=["ingestion", "rss", "extract", "scrapy"],
) as dag:

    SCRAPY_PROJECT_DIR = "/opt/airflow/app/redfin_scraper"

    # 입력(메타) 루트 및 출력(컨벤션)
    META_ROOT = "/opt/airflow/data/rss/meta"
    OUT_DIR = "/opt/airflow/data/rss/extracted/{{ ds_nodash }}"
    OUT_FILE = "rss_extracted__{{ ds_nodash }}__{{ ts_nodash }}.jsonl"

    # 최신 메타 파일 선택 (컨벤션 패턴: rss_meta__*.jsonl)
    find_latest_meta = BashOperator(
        task_id="find_latest_meta",
        bash_command=(
            "set -euo pipefail; "
            "latest=$(find {{ params.meta_root }} -type f -name 'rss_meta__*.jsonl' "
            "-printf '%T@ %p\n' | sort -nr | awk 'NR==1{print $2}'); "
            'if [ -z "$latest" ]; then echo "NO_META_FOUND" >&2; exit 1; fi; '
            'echo "$latest"'
        ),
        params={"meta_root": META_ROOT},
        do_xcom_push=True,
    )

    run_extractor = BashOperator(
        task_id="run_extractor",
        bash_command=(
            "set -euo pipefail; "
            'INPUT="{{ ti.xcom_pull(task_ids=\'find_latest_meta\') }}"; '
            "mkdir -p {{ params.out_dir }}; "
            "cd {{ params.project_dir }}; "
            "scrapy crawl extractor -a input_file=\"$INPUT\" -O {{ params.out_dir }}/{{ params.out_file }}"
        ),
        params={
            "project_dir": SCRAPY_PROJECT_DIR,
            "out_dir": OUT_DIR,
            "out_file": OUT_FILE,
        },
        env={"PYTHONPATH": "/opt/airflow/app"},
        outlets=[RSS_EXTRACTED],
    )

    find_latest_meta >> run_extractor
