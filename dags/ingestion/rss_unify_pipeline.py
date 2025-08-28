from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict, List

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

try:
    from dags.datasets import RSS_RAW_DATA, RSS_UNIFIED_DATA
except Exception:
    RSS_RAW_DATA = Dataset("file:///opt/airflow/data/rss/raw")
    RSS_UNIFIED_DATA = Dataset("mongo://redfin/rss_unified")

# Plugins imports
from plugins.utils.rss_unifier import process_rss_files, create_batch_envelope

# ---- 설정(Variable 우선) ----
KST = pendulum.timezone("Asia/Seoul")
DEFAULT_INPUT_DIR = "/opt/airflow/data/rss/raw"   # docker-compose 기준
INPUT_DIR = Variable.get("RSS_INPUT_DIR", default_var=DEFAULT_INPUT_DIR)
SCHEMA_VERSION = Variable.get("RSS_SCHEMA_VERSION", default_var="1.0.0")

# ---- 태스크 구현 ----
def unify_rss_data(**context):
    """RSS 데이터 파일들을 통합 스키마로 변환"""
    # ulid는 rss_unifier에서 처리됨

    # 입력/출력 디렉토리 설정
    ds_nodash = context['ds_nodash']
    ts_nodash = context['ts_nodash']
    
    input_dir = f"{INPUT_DIR}/{ds_nodash}"
    output_dir = f"/opt/airflow/data/rss/unified/{ds_nodash}"
    
    print(f"Processing RSS feeds from: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    # 입력 디렉토리에서 RSS JSON 파일들 처리
    unified_docs = process_rss_files(input_dir, output_dir)
    
    if not unified_docs:
        print("No RSS files found to process")
        return {"processed_count": 0, "unified_docs": [], "output_dir": output_dir}

    # 배치 봉투 생성
    batch_doc = create_batch_envelope(unified_docs, batch_type="rss_collection")
    
    # 배치 문서 저장
    batch_file = f"{output_dir}/batch_envelope_{ts_nodash}.json"
    import json
    with open(batch_file, 'w', encoding='utf-8') as f:
        json.dump(batch_doc, f, ensure_ascii=False, indent=2)
    
    print(f"Processed {len(unified_docs)} RSS feeds")
    print(f"Batch envelope saved to: {batch_file}")
    
    # XCom을 통해 다음 태스크로 데이터 전달
    return {
        "processed_count": len(unified_docs),
        "unified_docs": unified_docs,
        "batch_doc": batch_doc,
        "output_dir": output_dir,
        "batch_file": batch_file
    }

# ---- DAG ----
with DAG(
    dag_id="04_rss_unify_pipeline",
    description="RSS 피드를 통합 스키마로 변환",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_RAW_DATA],  # RSS_RAW_DATA Dataset 트리거
    catchup=False,
    tags=["ingestion", "rss", "unify", "schema"],
    default_args={"owner": "airflow", "retries": 2},
) as dag:

    # RSS 데이터 통합
    unify_task = PythonOperator(
        task_id="unify_rss_data",
        python_callable=unify_rss_data,
        provide_context=True,
        retries=2,  # 데이터 처리는 재시도 가능
        outlets=[RSS_UNIFIED_DATA],
    )
