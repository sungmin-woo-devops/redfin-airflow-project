# -*- coding: utf-8 -*-
# C: RSS_EXTRACTED (선택 P: INDEX_READY)

from __future__ import annotations
import json, os, glob, pathlib, pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------- 로깅 ----------------- [REMOVED]

# ----------------- Dataset Import -----------------
try:
    from dags.datasets import RSS_LABELED, RSS_RAG_READY
except ImportError:
    from airflow.datasets import Dataset
    RSS_LABELED = Dataset("mongo://redfin/rss_labeled")
    RSS_RAG_READY = Dataset("file:///opt/airflow/data/indexes/faiss/.ready")

# ----------------- 기본 설정 -----------------
KST = pendulum.timezone("Asia/Seoul")
DATA_DIR = pathlib.Path(os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data"))

# ----------------- 함수 정의 -----------------
def build_embedding_min(**context) -> dict:
    """라벨링된 RSS 데이터를 기반으로 RAG 인덱스(스텁) 생성."""
    ds = context.get("ds_nodash")

    # 최신 라벨링 파일 선택
    labeled_files = sorted(glob.glob(f"{DATA_DIR}/rss/labeled/*/rss_labeled__*.jsonl"))
    if not labeled_files:
        raise FileNotFoundError("No labeled JSONL files found")

    latest_file = max(labeled_files, key=os.path.getmtime)
    # Selected latest labeled file: %s [REMOVED]

    # 출력 디렉토리 준비
    out_dir = DATA_DIR / "indexes" / "faiss" / ds
    out_dir.mkdir(parents=True, exist_ok=True)
    # Created output directory: %s [REMOVED]

    # TODO: sentence-transformers + FAISS 로 인덱스 구축
    # 현재는 스텁 → 최소 마커(.ready) + 메타파일(meta.json)만 생성
    (out_dir / ".ready").write_text(latest_file, encoding="utf-8")

    meta = {"source": latest_file}
    with open(out_dir / "meta.json", "w", encoding="utf-8") as wf:
        json.dump(meta, wf, ensure_ascii=False, indent=2)
    # Meta info saved: %s [REMOVED]

    return {"index_dir": str(out_dir), "source": latest_file}

# ----------------- DAG 정의 -----------------
with DAG(
    dag_id="rss_rag_pipeline",
    description="Pipeline to build embeddings/FAISS index for RAG (stub version)",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_LABELED],
    catchup=False,
    tags=["enrichment", "rag", "indexing"],
) as dag:

    build_index_task = PythonOperator(
        task_id="build_rag_min",
        python_callable=build_embedding_min,
        outlets=[RSS_RAG_READY],
    )