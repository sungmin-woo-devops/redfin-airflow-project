# -*- coding: utf-8 -*-
from __future__ import annotations
import json, pendulum, urllib.parse
from airflow import DAG
from airflow.operators.python import PythonOperator

try:
    from dags.datasets import RSS_EXTRACTED, RSS_PREPROCESSED
except Exception:
    from airflow.datasets import Dataset
    RSS_EXTRACTED = Dataset("mongo://redfin/rss_extracted")
    RSS_PREPROCESSED = Dataset("mongo://redfin/rss_preprocessed")

KST = pendulum.timezone("Asia/Seoul")

def _normalize_url(u: str) -> str:
    try:
        p = urllib.parse.urlsplit(u.strip())
        p = p._replace(scheme=p.scheme.lower(), netloc=p.netloc.lower(), fragment="")
        return urllib.parse.urlunsplit(p)
    except Exception:
        return u

def _iso8601(dt: str) -> str:
    try:
        return pendulum.parse(dt).to_iso8601_string()
    except Exception:
        return dt

def preprocess(**_):
    import os, glob
    ds = _.get("ds_nodash"); ts = _.get("ts_nodash")
    in_files = sorted(glob.glob(f"/opt/airflow/data/rss/extracted/*/rss_extracted__*.jsonl"))
    if not in_files:
        raise FileNotFoundError("no extracted jsonl")
    latest = max(in_files, key=os.path.getmtime)

    out_dir = f"/opt/airflow/data/rss/preprocessed/{ds}"
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/rss_preprocessed__{ds}__{ts}.jsonl"

    with open(latest, "r", encoding="utf-8") as rf, open(out_file, "w", encoding="utf-8") as wf:
        for line in rf:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if "url" in obj:
                obj["url"] = _normalize_url(obj["url"])
            for k in ("pub_date","updated","published_at"):
                if k in obj and obj[k]:
                    obj[k] = _iso8601(obj[k])
            wf.write(json.dumps(obj, ensure_ascii=False) + "\n")
    return {"input": latest, "output": out_file}

with DAG(
    dag_id="03_rss_preprocess_pipeline",
    description="ISO8601 시간 통일, URL 정규화 등 전처리",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_EXTRACTED],
    catchup=False,
    tags=["ingestion","preprocess"],
) as dag:

    task = PythonOperator(
        task_id="preprocess",
        python_callable=preprocess,
        outlets=[RSS_PREPROCESSED],
    )
