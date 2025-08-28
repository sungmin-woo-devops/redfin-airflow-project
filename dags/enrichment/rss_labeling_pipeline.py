# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import os
import glob
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


# ----------------- Dataset Import -----------------
try:
    from dags.datasets import RSS_PREPROCESSED, RSS_LABELED, RSS_RAG_READY
except ImportError:
    from airflow.datasets import Dataset
    import os

    # 추가된 부분: 환경 변수로 데이터 디렉토리 설정 (유연성 ↑)
    DATA_DIR = os.getenv("AIRFLOW_DATA_DIR", "/opt/airflow/data")

    # Dataset 정의
    RSS_PREPROCESSED = Dataset("mongo://redfin/rss_preprocessed")
    RSS_LABELED = Dataset("mongo://redfin/rss_labeled")
    RSS_RAG_READY = Dataset(f"file://{DATA_DIR}/indexes/faiss/.ready")

KST = pendulum.timezone("Asia/Seoul")

# 추가된 부분: 입력 파일 검증 함수 분리. 모듈화로 가독성과 유지보수성 향상.
def validate_input_files(**kwargs) -> str:
    """Validate and select the latest preprocessed RSS file."""
    ds = kwargs.get("ds_nodash")
    # 추가된 부분: ds_nodash 유효성 검사. 누락 시 에러로 조기 실패 방지.
    if not ds:
        raise ValueError("Execution date (ds_nodash) is required")

    input_pattern = f"{DATA_DIR}/rss/preprocessed/*/rss_preprocessed__*.jsonl"
    in_files = sorted(glob.glob(input_pattern))
    if not in_files:
        raise FileNotFoundError("No preprocessed JSONL files found")

    latest = max(in_files, key=os.path.getmtime)
    # 추가된 부분: 로그 추가로 선택된 파일 추적. 실행 가시성 향상. [REMOVED]
    return latest

def label_data(**kwargs) -> dict:
    """Add keywords, tags, and categories to preprocessed RSS data (stub)."""
    ds = kwargs.get("ds_nodash")
    ts = kwargs.get("ts_nodash")
    # 추가된 부분: TaskInstance로 XCom에서 이전 태스크 결과 가져오기. 데이터 흐름 명확화.
    ti = kwargs.get("ti")
    latest_file = ti.xcom_pull(task_ids="validate_input_files")

    # 추가된 부분: 입력 및 ts_nodash 유효성 검사. 안정성 강화.
    if not latest_file:
        raise ValueError("No input file provided")
    if not ts:
        raise ValueError("Timestamp (ts_nodash) is required")

    out_dir = os.path.join(DATA_DIR, "rss", "labeled", ds)
    os.makedirs(out_dir, exist_ok=True)
    out_file = f"{out_dir}/rss_labeled__{ds}__{ts}.jsonl"
    # 추가된 부분: 로그 추가로 출력 경로 추적. [REMOVED]

    # Process input file and add dummy labels
    invalid_lines = 0
    with open(latest_file, "r", encoding="utf-8") as rf, open(out_file, "w", encoding="utf-8") as wf:
        for line in rf:
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                # 추가된 부분: 잘못된 JSON 라인 로그 기록. 문제 추적 가능. [REMOVED]
                invalid_lines += 1
                continue
            obj.setdefault("keywords", [])     # TODO: YAKE로 키워드 추출
            obj.setdefault("tags", [])         # TODO: Gemma 3 4B로 태그 생성
            obj.setdefault("category", "NA")   # TODO: Gemma 3 4B로 카테고리 분류
            wf.write(json.dumps(obj, ensure_ascii=False) + "\n")
    
    
    return {"input": latest_file, "output": out_file}

with DAG(
    dag_id="rss_labeling_pipeline",
    description="Pipeline to label RSS data with keywords, tags, and categories",  # 추가된 부분: 설명을 영어로 명확히 수정. 국제적 이해 용이.
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule=[RSS_PREPROCESSED],
    catchup=False,
    tags=["enrichment", "labeling"],
) as dag:
    # 추가된 부분: 입력 검증 태스크 추가. 파이프라인 모듈화.
    validate_task = PythonOperator(
        task_id="validate_input_files",
        python_callable=validate_input_files,
        provide_context=True,  # 추가된 부분: 컨텍스트 제공으로 kwargs 접근.
    )

    label_task = PythonOperator(
        task_id="label_data",
        python_callable=label_data,
        provide_context=True,  # 추가된 부분: 컨텍스트 제공으로 kwargs 접근.
        outlets=[RSS_LABELED],
    )

    # 추가된 부분: 태스크 간 의존성 정의. 순차 실행 보장.
    validate_task >> label_task