from __future__ import annotations
import json, pendulum, os
from pymongo import MongoClient, UpdateOne

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.models import Variable

try:
    from datasets import RSS_FEEDS, RSS_ARTICLES_EXTRACTED
except ImportError:
    RSS_FEEDS = Dataset("mongo://redfin/rss_feeds")
    RSS_ARTICLES_EXTRACTED = Dataset("mongo://redfin/rss_articles_extracted")

# ----- 설정 -----
DAG_ID = "03_to_rag_pipeline"
KST = pendulum.timezone("Asia/Seoul")
START = pendulum.datetime(2025, 1, 1, tz=KST)

# ----- 함수 -----
from utils.mongo import test_mongo_connection,get_mongo_client



def task_preprocess(ti, **_):
    raw = ti.xcom_pull(task_ids="scrape")
    clean = [to_clean_record(r) for r in raw if r.get("html")]
    # 빈 텍스트 제거
    clean = [d for d in clean if d.get("text")]
    return clean

# Upsert란? 데이터베이스에서 특정 조건에 맞는 문서가 존재하면 업데이트하고, 존재하지 않으면 새로 삽입하는 작업
def task_upsert(ti, **_):
    clean = ti.xcom_pull(task_ids="preprocess")
    if not clean:
        return {"upserted": 0, "modified": 0}
    col = _mongo_client()[os.getenv("MONGO_DB","redfin_rag_db")][os.getenv("MONGO_COL","articles")]
    ops = [UpdateOne({"doc_id": d["doc_id"]}, {"$set": d}, upsert=True) for d in clean]
    res = col.bulk_write(ops, ordered=False)
    return {"upserted": len(res.upserted_ids), "modified": res.modified_count}

with DAG(
    dag_id=DAG_ID,
    start_date=START,
    schedule="0 * * * *",     # 매시 정각
    catchup=False,
    tags=["redfin","ingest"],
) as dag:

    scrape = PythonOperator(task_id="scrape", python_callable=task_scrape)

    preprocess = PythonOperator(task_id="preprocess", python_callable=task_preprocess)

    upsert = PythonOperator(
        task_id="upsert",
        python_callable=task_upsert,
        # upsert가 끝나면 Dataset 업데이트로 표시
        outlets=[ARTICLES_DATASET],
    )

    scrape >> preprocess >> upsert

# Point
# XCom으로 단계 간 데이터 전달(소량 기준). 대용량이면 파일/DB 경유
# outlets=[ARTICLES_DATASET]로 “정제 컬렉션 갱신”을 신호화 → 다음 DAG 트리거에 사용.
