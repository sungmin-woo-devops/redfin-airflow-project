# -*- coding: utf-8 -*-
from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.datasets import Dataset
from plugins.utils.rss_unifier import save_to_mongodb

try:
    from dags.datasets import RSS_RAW_DATA, RSS_UNIFIED_DATA
except Exception:
    from airflow.datasets import Dataset
    RSS_RAW_DATA = Dataset("file:///opt/airflow/data/rss/raw")
    RSS_UNIFIED_DATA = Dataset("mongo://redfin/rss_unified")

KST = pendulum.timezone("Asia/Seoul")

# MongoDB 설정
MONGO_URI = Variable.get(
    "MONGO_URI",
    default_var="mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin"
)
MONGO_DB = Variable.get("MONGO_DB", default_var="redfin")
MONGO_COLL = Variable.get("MONGO_COLL_UNIFIED", default_var="rss_unified")


def unify_rss_feeds(**context):
    """RSS 피드들을 통합 스키마로 변환"""
    from plugins.utils.rss_unifier import process_rss_files, create_batch_envelope
    
    # 입력/출력 디렉토리 설정
    ds_nodash = context['ds_nodash']
    ts_nodash = context['ts_nodash']
    
    input_dir = f"/opt/airflow/data/rss/raw/{ds_nodash}"
    output_dir = f"/opt/airflow/data/rss/unified/{ds_nodash}"
    
    print(f"Processing RSS feeds from: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    # RSS 파일들을 통합 스키마로 변환
    unified_docs = process_rss_files(input_dir, output_dir)
    
    if not unified_docs:
        print("No RSS feeds found to process")
        return {"processed_count": 0, "unified_docs": []}
    
    # 배치 봉투 생성
    batch_doc = create_batch_envelope(unified_docs)
    
    # 배치 문서 저장
    batch_file = f"{output_dir}/batch_envelope_{ts_nodash}.json"
    import json
    with open(batch_file, 'w', encoding='utf-8') as f:
        json.dump(batch_doc, f, ensure_ascii=False, indent=2)
    
    print(f"Processed {len(unified_docs)} RSS feeds")
    print(f"Batch envelope saved to: {batch_file}")
    
    return {
        "processed_count": len(unified_docs),
        "unified_docs": unified_docs,
        "batch_doc": batch_doc,
        "batch_file": batch_file
    }


def save_unified_to_mongodb(**context):
    """통합된 RSS 데이터를 MongoDB에 저장"""
    from plugins.utils.rss_unifier import save_to_mongodb
    
    # 이전 태스크에서 결과 가져오기
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='unify_rss_feeds')
    
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
        collection_name=MONGO_COLL
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
    from plugins.utils.rss_unifier import save_to_mongodb
    
    # 이전 태스크에서 결과 가져오기
    task_instance = context['task_instance']
    result = task_instance.xcom_pull(task_ids='unify_rss_feeds')
    
    if not result or not result.get('batch_doc'):
        print("No batch document to save")
        return {"saved_count": 0}
    
    batch_doc = result['batch_doc']
    
    print(f"Saving batch envelope to MongoDB")
    
    # MongoDB에 배치 문서 저장
    save_result = save_to_mongodb(
        docs=[batch_doc],
        mongo_uri=MONGO_URI,
        db_name=MONGO_DB,
        collection_name=f"{MONGO_COLL}_batch"
    )
    
    if save_result['success']:
        print(f"Successfully saved batch envelope to MongoDB")
        print(f"Batch ID: {save_result['inserted_ids'][0]}")
    else:
        print(f"Failed to save batch to MongoDB: {save_result['error']}")
        raise Exception(f"MongoDB batch save failed: {save_result['error']}")
    
    return save_result


with DAG(
    dag_id="04_rss_unified_schema_pipeline",
    description="RSS 피드를 통합 스키마로 변환하고 MongoDB에 저장",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_RAW_DATA],  # RSS_RAW_DATA Dataset 트리거
    catchup=False,
    tags=["ingestion", "rss", "unified", "schema", "mongodb"],
) as dag:

    # 1단계: RSS 피드를 통합 스키마로 변환
    unify_task = PythonOperator(
        task_id="unify_rss_feeds",
        python_callable=unify_rss_feeds,
        provide_context=True,
    )

    # 2단계: 개별 통합 문서들을 MongoDB에 저장
    save_individual_task = PythonOperator(
        task_id="save_unified_to_mongodb",
        python_callable=save_unified_to_mongodb,
        provide_context=True,
    )

    # 3단계: 배치 봉투를 MongoDB에 저장
    save_batch_task = PythonOperator(
        task_id="save_batch_to_mongodb",
        python_callable=save_batch_to_mongodb,
        provide_context=True,
        outlets=[RSS_UNIFIED_DATA],
    )

    # 태스크 의존성 설정
    unify_task >> [save_individual_task, save_batch_task]
