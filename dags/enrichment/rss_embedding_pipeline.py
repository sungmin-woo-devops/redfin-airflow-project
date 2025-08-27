# -*- coding: utf-8 -*-
# C: RSS_EXTRACTED (선택 P: INDEX_READY)

from __future__ import annotations
import json, os, glob, pathlib, pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from dags.datasets import RSS_LABELED, RSS_RAG_READY
except Exception:
    from airflow.datasets import Dataset
    RSS_LABELED = Dataset("mongo://redfin/rss_labeled")
    RSS_RAG_READY = Dataset("file:///opt/airflow/data/indexes/faiss/.ready")

KST = pendulum.timezone("Asia/Seoul")

def build_embedding_min(**_):
    ds = _.get("ds_nodash")
    labeled = sorted(glob.glob(f"/opt/airflow/data/rss/labeled/*/rss_labeled__*.jsonl"))
    if not labeled:
        raise FileNotFoundError("no labeled jsonl")
    latest = max(labeled, key=os.path.getmtime)

    out_dir = pathlib.Path("/opt/airflow/data/indexes/faiss") / ds
    out_dir.mkdir(parents=True, exist_ok=True)

    # TODO: sentence-transformers + faiss 로 임베딩/인덱스 구축
    # 여기서는 최소 마커만 생성
    (out_dir / ".ready").write_text(latest, encoding="utf-8")

    # 참고용 메타
    with open(out_dir / "meta.json", "w", encoding="utf-8") as wf:
        json.dump({"source": latest}, wf, ensure_ascii=False)

    return {"index_dir": str(out_dir), "source": latest}

with DAG(
    dag_id="rss_rag_pipeline",
    description="RAG용 임베딩/인덱스 빌드(스텁)",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_LABELED],
    catchup=False,
    tags=["enrichment","rag","indexing"],
) as dag:

    task = PythonOperator(
        task_id="build_rag_min",
        python_callable=build_embedding_min,
        outlets=[RSS_RAG_READY],
    )