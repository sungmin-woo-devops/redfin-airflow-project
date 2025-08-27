# -*- coding: utf-8 -*-
from __future__ import annotations
import json, pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from dags.datasets import RSS_PREPROCESSED, RSS_LABELED
except Exception:
    from airflow.datasets import Dataset
    RSS_PREPROCESSED = Dataset("mongo://redfin/rss_preprocessed")
    RSS_LABELED = Dataset("mongo://redfin/rss_labeled")

KST = pendulum.timezone("Asia/Seoul")

def label_min(**_):
    import os, glob
    ds = _.get("ds_nodash"); ts = _.get("ts_nodash")
    in_files = sorted(glob.glob(f"/opt/airflow/data/rss/preprocessed/*/rss_preprocessed__*.jsonl"))
    if not in_files:
        raise FileNotFoundError("no preprocessed jsonl")
    latest = max(in_files, key=os.path.getmtime)

    out_dir = f"/opt/airflow/data/rss/labeled/{ds}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/rss_labeled__{ds}__{ts}.jsonl"

    # 최소: 더미 라벨
    with open(latest, "r", encoding="utf-8") as rf, open(out_file, "w", encoding="utf-8") as wf:
        for line in rf:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            obj.setdefault("keywords", [])     # TODO: YAKE 결과로 대체
            obj.setdefault("tags", [])         # TODO: Gemma 3 4B 태깅 결과로 대체
            obj.setdefault("category", "NA")   # TODO: Gemma 3 4B 카테고라이징 결과로 대체
            wf.write(json.dumps(obj, ensure_ascii=False) + "\n")
    return {"input": latest, "output": out_file}

with DAG(
    dag_id="rss_labeling_pipeline",
    description="키워드/태깅/카테고리 라벨링(스텁)",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_PREPROCESSED],
    catchup=False,
    tags=["enrichment","labeling"],
) as dag:

    task = PythonOperator(
        task_id="label_min",
        python_callable=label_min,
        outlets=[RSS_LABELED],
    )
