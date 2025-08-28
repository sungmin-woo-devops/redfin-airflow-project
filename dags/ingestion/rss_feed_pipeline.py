# -*- coding: utf-8 -*-
from __future__ import annotations
import pendulum
import os
import json
import yaml
import feedparser
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

try:
    from dags.datasets import RSS_FEEDS
except Exception:
    from airflow.datasets import Dataset
    RSS_FEEDS = Dataset("mongo://redfin/rss_feeds")

KST = pendulum.timezone("Asia/Seoul")

# MongoDB 설정
MONGO_URI = Variable.get(
    "MONGO_URI",
    default_var="mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin"
)
MONGO_DB = Variable.get("MONGO_DB", default_var="redfin")
MONGO_COLL = Variable.get("MONGO_COLL_META", default_var="rss_feeds")

def normalize_feed_name(feed_name: str) -> str:
    """피드명을 파일명으로 사용할 수 있도록 정규화"""
    normalized = feed_name.lower()
    normalized = normalized.replace(" ", "_")
    normalized = normalized.replace("/", "_")
    normalized = normalized.replace(".", "_")
    normalized = normalized.replace("-", "_")
    normalized = normalized.replace("(", "")
    normalized = normalized.replace(")", "")
    normalized = normalized.replace("__", "_")
    return normalized.strip("_")

def collect_rss_feeds(**context):
    """RSS 피드 전체를 JSON으로 수집하는 함수"""
    execution_date = context['execution_date']
    ds_nodash = execution_date.strftime('%Y%m%d')
    ts_nodash = execution_date.strftime('%Y%m%d_%H%M%S')
    
    feeds_cfg = "/opt/airflow/config/feeds.yaml"
    out_dir = f"/opt/airflow/data/rss/meta/{ds_nodash}"
    
    # 디렉토리 생성
    os.makedirs(out_dir, exist_ok=True)
    
    # feeds.yaml 파일 읽기
    try:
        with open(feeds_cfg, 'r', encoding='utf-8') as f:
            feeds_config = yaml.safe_load(f)
    except Exception as e:
        print(f"feeds.yaml 파일 읽기 실패: {e}")
        raise
    
    collected_feeds = []
    
    # 각 피드에 대해 전체 RSS 데이터 수집
    for feed_info in feeds_config.get('feeds', []):
        feed_name = feed_info.get('name')
        feed_url = feed_info.get('url')
        
        if not feed_url:
            continue
            
        print(f"피드 수집 중: {feed_name} - {feed_url}")
        
        try:
            # feedparser로 RSS 피드 파싱
            feed = feedparser.parse(feed_url)
            
            # feedparser의 FeedParserDict를 dict로 변환
            feed_as_dict = {
                "feed": dict(feed.feed),
                "entries": [dict(entry) for entry in feed.entries]
            }
            
            # 개별 피드를 JSON 파일로 저장
            normalized_name = normalize_feed_name(feed_name)
            feed_filename = f"rss_feed_{normalized_name}_{ds_nodash}_{ts_nodash}.json"
            feed_filepath = os.path.join(out_dir, feed_filename)
            
            with open(feed_filepath, 'w', encoding='utf-8') as f:
                json.dump(feed_as_dict, f, ensure_ascii=False, indent=2)
            
            collected_feeds.append(feed_as_dict)
            
            print(f"{feed_name}: {len(feed.entries)}개 엔트리 수집 완료")
            
        except Exception as e:
            print(f"{feed_name} 수집 실패: {e}")
    
    # 통합 JSONL 파일 생성
    jsonl_file = f"rss_feeds__{ds_nodash}__{ts_nodash}.jsonl"
    jsonl_path = os.path.join(out_dir, jsonl_file)
    
    with open(jsonl_path, 'w', encoding='utf-8') as f:
        for feed_data in collected_feeds:
            f.write(json.dumps(feed_data, ensure_ascii=False) + '\n')
    
    print(f"수집 완료: {len(collected_feeds)}개 피드")
    
    # XCom에 결과 저장
    return {
        'jsonl_file': jsonl_path,
        'collected_feeds': collected_feeds
    }

def save_feeds_to_mongodb(**context):
    """수집된 RSS 피드 데이터를 MongoDB에 저장"""
    from plugins.utils.rss_unifier import save_to_mongodb
    
    # 이전 태스크에서 결과 가져오기
    task_result = context['task_instance'].xcom_pull(task_ids='collect_rss_feeds')
    collected_feeds = task_result['collected_feeds']
    
    print(f"MongoDB 저장 시작: {len(collected_feeds)}개 피드")
    
    try:
        # MongoDB에 저장
        save_result = save_to_mongodb(
            docs=collected_feeds,
            mongo_uri=MONGO_URI,
            db_name=MONGO_DB,
            collection_name=MONGO_COLL
        )
        
        if save_result['success']:
            print(f"MongoDB 저장 성공: {save_result['inserted_count']}개 문서")
            return save_result
        else:
            print(f"MongoDB 저장 실패: {save_result.get('error', 'Unknown error')}")
            raise Exception(f"MongoDB save failed: {save_result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"MongoDB 연결/저장 오류: {e}")
        raise

with DAG(
    dag_id="01_rss_feed_pipeline",
    description="RSS 피드 전체 데이터 수집 및 MongoDB 저장 파이프라인",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule="0 */12 * * *",  # 12시간마다 실행 (0시, 12시)
    catchup=False,
    tags=["ingestion", "rss", "feedparser", "mongodb"],
) as dag:

    # 1단계: RSS 피드 전체 수집
    collect_rss_feeds_task = PythonOperator(
        task_id="collect_rss_feeds",
        python_callable=collect_rss_feeds,
    )

    # 2단계: MongoDB에 저장
    save_to_mongodb_task = PythonOperator(
        task_id="save_feeds_to_mongodb",
        python_callable=save_feeds_to_mongodb,
        outlets=[RSS_FEEDS],
    )

    # 태스크 의존성
    collect_rss_feeds_task >> save_to_mongodb_task
