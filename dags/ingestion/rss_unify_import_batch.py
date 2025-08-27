from __future__ import annotations
import os
from pathlib import Path
from typing import Any, Dict, List

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Plugins imports
from plugins.utils.common_utils import iso_now_utc, ymd, sha1, first_nonempty
from plugins.utils.rss_processor import create_unified_rss_item
from plugins.utils.file_handler import load_files, parse_any_file, save_json_file

# ---- 설정(Variable 우선) ----
KST = pendulum.timezone("Asia/Seoul")
DEFAULT_INPUT_DIR = "/opt/airflow/data/collect"   # docker-compose 기준
INPUT_DIR = Variable.get("RSS_INPUT_DIR", default_var=DEFAULT_INPUT_DIR)
SCHEMA_VERSION = Variable.get("RSS_SCHEMA_VERSION", default_var="1.0.0")
MONGO_URI = Variable.get(
    "MONGO_URI",
    default_var="mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin"
)
MONGO_DB = Variable.get("MONGO_DB", default_var="redfin")
MONGO_COLL = Variable.get("MONGO_COLL", default_var="rss_meta")

# ---- 이전 유틸리티 함수들은 plugins/utils로 이동됨 ----

# ---- 태스크 구현 ----
def unify_rss_data(**context):
    """RSS 데이터 파일들을 통합 스키마로 변환"""
    try:
        import ulid
    except ImportError:
        import sys, subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "ulid-py"])
        import ulid

    files = load_files(INPUT_DIR)
    items: List[Dict[str, Any]] = []

    for f in files:
        for obj in parse_any_file(f):
            items.append(create_unified_rss_item(obj))

    now = pendulum.now("UTC")
    y, m, d = ymd(now)
    batch_doc = {
        "_id": str(ulid.new()),
        "schema_version": SCHEMA_VERSION,
        "status": "meta",
        "run": {"run_id": str(ulid.new()), "exec_ts": iso_now_utc()},
        "source": {  # 배치 단위 대표 소스(옵션) — 조회 편의를 위해 첫 항목 기준
            "feed_id": items and items[0]["source"]["feed_id"] or "",
            "feed_url": items and items[0]["source"]["feed_url"] or "",
            "feed_name": items and items[0]["source"]["feed_name"] or "",
        },
        "partition": {"year": y, "month": m, "day": d},
        "audit": {"created_at": iso_now_utc()},
        "rss": { "feed": {"title": "Batch Envelope"}, "entries": [] },  # 상위 스키마 호환 필드(빈 페이로드)
        "items": items
    }

    # 산출 파일 저장(옵션)
    out_path = Path(INPUT_DIR) / "batch_enveloped.json"
    save_json_file(batch_doc, out_path)
    
    # XCom을 통해 다음 태스크로 데이터 전달
    return batch_doc


def insert_to_mongo(**context):
    """통합된 RSS 데이터를 MongoDB에 삽입"""
    try:
        import pymongo
        from pymongo import MongoClient
    except ImportError:
        import sys, subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "pymongo"])
        from pymongo import MongoClient

    # 이전 태스크에서 XCom으로 데이터 받기
    task_instance = context['task_instance']
    batch_doc = task_instance.xcom_pull(task_ids='unify_rss_data')
    
    if not batch_doc:
        raise ValueError("이전 태스크에서 데이터를 받지 못했습니다")

    # MongoDB 삽입
    client = MongoClient(MONGO_URI)
    try:
        coll = client[MONGO_DB][MONGO_COLL]
        result = coll.insert_one(batch_doc)
        print(f"문서 삽입 성공: {result.inserted_id}")
        return {"inserted_id": str(result.inserted_id), "status": "success"}
    except Exception as e:
        print(f"MongoDB 삽입 실패: {e}")
        raise
    finally:
        client.close()

# ---- DAG ----
with DAG(
    dag_id="rss_unify_import_batch",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=None,               # 필요 시 "0 * * * *" 등으로 변경
    catchup=False,
    tags=["rss","unify","mongo"],
    default_args={"owner": "airflow", "retries": 0},
) as dag:

    # 1단계: RSS 데이터 통합
    unify_task = PythonOperator(
        task_id="unify_rss_data",
        python_callable=unify_rss_data,
        provide_context=True,
        retries=2,  # 데이터 처리는 재시도 가능
    )

    # 2단계: MongoDB 삽입
    insert_task = PythonOperator(
        task_id="insert_to_mongo",
        python_callable=insert_to_mongo,
        provide_context=True,
        retries=1,  # DB 삽입은 신중하게 재시도
    )

    # 태스크 의존성 설정
    unify_task >> insert_task
