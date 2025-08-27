from __future__ import annotations
import json, pendulum, os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.models import Variable
from pymongo import MongoClient, UpdateOne

# ---- Dataset: "정제된 문서가 갱신됨" 신호
ARTICLES_DATASET = Dataset("mongo://redfin/articles_clean")

KST = pendulum.timezone("Asia/Seoul")
START = pendulum.datetime(2025, 1, 1, tz=KST)

# ---- 외부 모듈(프로젝트 코드) import (임시 스텁)
def scrape_run(limit: int = 5):
    """임시 스텁: 스크래핑 실행"""
    print(f"scrape_run called with limit={limit}")
    return [{"doc_id": f"temp_{i}", "html": f"<html>임시 HTML {i}</html>", "url": f"https://example{i}.com"} for i in range(limit)]

def to_clean_record(raw_record):
    """임시 스텁: 원본 → 정제 레코드"""
    return {
        "doc_id": raw_record.get("doc_id", "unknown"),
        "text": f"임시 정제 텍스트: {raw_record.get('doc_id', 'unknown')}",
        "url": raw_record.get("url", ""),
        "title": f"임시 제목: {raw_record.get('doc_id', 'unknown')}"
    }


def _mongo_client():
    # Admin → Connections에 등록해두는 편이 표준이지만,
    # 최소 예시는 환경변수/Variable에서 읽도록 함.
    mongo_uri = os.getenv("MONGO_URI") or Variable.get("MONGO_URI")
    return MongoClient(mongo_uri)

def task_scrape(**_):
    # 필요하면 Variable로 입력 소스 제어 (SEED_URL 등)
    raw = scrape_run(limit=int(Variable.get("SCRAPE_LIMIT", default_var="5")))
    # XCom 반환
    return raw

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
    dag_id="ingestion_pipeline",
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
