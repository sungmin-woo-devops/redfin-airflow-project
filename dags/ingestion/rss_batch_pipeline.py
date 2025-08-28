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
    from dags.datasets import RSS_UNIFIED_DATA, RSS_BATCH_DATA
except Exception:
    RSS_UNIFIED_DATA = Dataset("mongo://redfin/rss_unified")
    RSS_BATCH_DATA = Dataset("mongo://redfin/rss_batch")

from plugins.utils.rss_unifier import save_to_mongodb

# ----- 설정 -----
DAG_ID = "05_rss_batch_pipeline"
KST = pendulum.timezone("Asia/Seoul")
MONGO_URI = Variable.get(
    "MONGO_URI",
    default_var="mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin"
)
MONGO_DB = Variable.get("MONGO_DB", default_var="redfin")
MONGO_COLL_UNIFIED = Variable.get("MONGO_COLL_UNIFIED", default_var="rss_unified")
MONGO_COLL_BATCH = Variable.get("MONGO_COLL_BATCH", default_var="rss_batch")


# ---- 태스크 구현 ----
def save_unified_to_mongodb(**context):
    """통합된 RSS 데이터를 MongoDB에 저장"""
    # 이전 태스크에서 XCom으로 데이터 받기
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='unify_rss_data', dag_id='rss_unify_pipeline')
    
    if not result or not result.get('unified_docs'):
        print("No unified documents to save")
        return {"saved_count": 0}

    unified_docs = result['unified_docs']
    
    print(f"Saving {len(unified_docs)} unified documents to MongoDB")
    
    # MongoDB에 저장
    save_result = save_to_mongodb(
        docs=unified_docs,
        mongo_uri=MONGO_URI,
        db_name=MONGO_DB,
        collection_name=MONGO_COLL_UNIFIED
    )
    
    if save_result['success']:
        print(f"Successfully saved {save_result['inserted_count']} documents to MongoDB")
        print(f"Inserted IDs: {save_result['inserted_ids'][:5]}...")  # 처음 5개만 표시
    else:
        print(f"Failed to save to MongoDB: {save_result['error']}")
        raise Exception(f"MongoDB save failed: {save_result['error']}")
    
    return save_result


def save_batch_to_mongodb(**context):
    """배치 봉투를 MongoDB에 저장"""
    # 이전 태스크에서 XCom으로 데이터 받기
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='unify_rss_data', dag_id='rss_unify_pipeline')
    
    if not result or not result.get('batch_doc'):
        print("No batch document to save")
        return {"saved_count": 0}

    batch_doc = result['batch_doc']
    
    print(f"Saving batch envelope to MongoDB")
    print(f"Batch ID: {batch_doc['_id']}")
    print(f"Feed count: {batch_doc['source']['feed_count']}")
    print(f"Total entries: {batch_doc['source']['total_entries']}")
    
    # MongoDB에 배치 문서 저장
    save_result = save_to_mongodb(
        docs=[batch_doc],
        mongo_uri=MONGO_URI,
        db_name=MONGO_DB,
        collection_name=MONGO_COLL_BATCH
    )
    
    if save_result['success']:
        print(f"Successfully saved batch envelope to MongoDB")
        print(f"Batch ID: {save_result['inserted_ids'][0]}")
    else:
        print(f"Failed to save batch to MongoDB: {save_result['error']}")
        raise Exception(f"MongoDB batch save failed: {save_result['error']}")
    
    return save_result


def update_batch_statistics(**context):
    """배치 통계 정보 업데이트"""
    # 이전 태스크에서 XCom으로 데이터 받기
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='unify_rss_data', dag_id='rss_unify_pipeline')
    
    if not result or not result.get('batch_doc'):
        print("No batch document to update")
        return {"updated": False}

    batch_doc = result['batch_doc']
    
    # 처리 시간 계산 (실제로는 시작 시간을 별도로 저장해야 함)
    import time
    processing_time = time.time()  # 임시로 현재 시간 사용
    
    # 통계 정보 업데이트
    batch_doc['statistics']['processing_time_seconds'] = round(processing_time, 2)
    batch_doc['audit']['updated_at'] = pendulum.now('UTC').to_iso8601_string()
    
    print(f"Updated batch statistics:")
    print(f"  - Processing time: {batch_doc['statistics']['processing_time_seconds']} seconds")
    print(f"  - Updated at: {batch_doc['audit']['updated_at']}")
    
    # 업데이트된 배치 문서를 MongoDB에 저장
    save_result = save_to_mongodb(
        docs=[batch_doc],
        mongo_uri=MONGO_URI,
        db_name=MONGO_DB,
        collection_name=MONGO_COLL_BATCH
    )
    
    if save_result['success']:
        print("Successfully updated batch statistics in MongoDB")
        return {"updated": True, "batch_id": batch_doc['_id']}
    else:
        print(f"Failed to update batch statistics: {save_result['error']}")
        raise Exception(f"Batch statistics update failed: {save_result['error']}")


# ---- DAG ----
with DAG(
    dag_id=DAG_ID,
    description="배치 봉투를 MongoDB에 저장하고 통계 업데이트",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_UNIFIED_DATA],  # RSS_UNIFIED_DATA Dataset 트리거
    catchup=False,
    tags=["ingestion", "rss", "batch", "mongodb"],
    default_args={"owner": "airflow", "retries": 1},
) as dag:

    # 1단계: 개별 통합 문서 MongoDB 저장
    save_unified_task = PythonOperator(
        task_id="save_unified_to_mongodb",
        python_callable=save_unified_to_mongodb,
        provide_context=True,
        retries=1,  # DB 삽입은 신중하게 재시도
    )

    # 2단계: 배치 봉투 MongoDB 저장
    save_batch_task = PythonOperator(
        task_id="save_batch_to_mongodb",
        python_callable=save_batch_to_mongodb,
        provide_context=True,
        retries=1,  # DB 삽입은 신중하게 재시도
    )

    # 3단계: 배치 통계 업데이트
    update_stats_task = PythonOperator(
        task_id="update_batch_statistics",
        python_callable=update_batch_statistics,
        provide_context=True,
        retries=1,
        outlets=[RSS_BATCH_DATA],
    )

    # 태스크 의존성 설정
    save_unified_task >> save_batch_task >> update_stats_task
