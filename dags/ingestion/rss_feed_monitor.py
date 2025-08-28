# -*- coding: utf-8 -*-
from __future__ import annotations
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset

# RSS Feed Parser Operator import (새로운 YAML 기반 피드 소스 사용)
from plugins.operators.feedparser_operator import RSSFeedParserOperator
from plugins.operators.feed_sources import (
    RSS_FEED_SOURCES, 
    ML_FEEDS, 
    AI_FEEDS,
    FRONTIER_LAB_FEEDS,
    ACADEMIA_FEEDS
)

try:
    from dags.datasets import RSS_QUALITY_REPORT
except Exception:
    RSS_QUALITY_REPORT = Dataset("file:///opt/airflow/data/rss/quality")

KST = pendulum.timezone("Asia/Seoul")

def analyze_feed_quality(**context):
    """RSS 피드 품질 분석 및 리포트 생성"""
    import json
    import os
    from datetime import datetime
    
    # XCom에서 결과 가져오기
    ti = context['ti']
    results = ti.xcom_pull(task_ids='fetch_feeds_for_monitoring')
    
    # 품질 분석
    quality_report = {
        "analysis_date": datetime.now().isoformat(),
        "total_feeds": results['processed_feeds'],
        "successful_feeds": results['successful_feeds'],
        "failed_feeds": len(results['failed_feeds']),
        "empty_feeds": len(results['empty_feeds']),
        "success_rate": results['successful_feeds'] / results['processed_feeds'] * 100,
        "failed_feed_details": results['failed_feeds'],
        "empty_feed_details": results['empty_feeds'],
        "recommendations": []
    }
    
    # 권장사항 생성
    if quality_report['success_rate'] < 80:
        quality_report['recommendations'].append("성공률이 80% 미만입니다. 실패한 피드들을 확인해주세요.")
    
    if len(results['empty_feeds']) > 5:
        quality_report['recommendations'].append("빈 피드가 5개 이상입니다. 피드 URL을 확인해주세요.")
    
    # 리포트 저장
    report_dir = "/opt/airflow/data/rss/quality"
    os.makedirs(report_dir, exist_ok=True)
    
    report_file = f"{report_dir}/quality_report_{context['ds_nodash']}.json"
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(quality_report, f, ensure_ascii=False, indent=2)
    
    print(f"Quality report saved to: {report_file}")
    print(f"Success rate: {quality_report['success_rate']:.1f}%")
    print(f"Failed feeds: {quality_report['failed_feeds']}")
    print(f"Empty feeds: {quality_report['empty_feeds']}")
    
    return quality_report

with DAG(
    dag_id="99_rss_feed_monitor",
    description="RSS 피드 품질 모니터링 및 분석",
    start_date=pendulum.datetime(2025, 8, 1, tz=KST),
    schedule="0 2 * * *",  # 매일 새벽 2시에 실행
    catchup=False,
    tags=["ingestion", "rss", "monitor", "quality"],
) as dag:

    RSS_MONITOR_DIR = "/opt/airflow/data/rss/monitor"

    # 모니터링용 피드 수집 (최소 엔트리 수 5개로 설정)
    fetch_feeds_for_monitoring = RSSFeedParserOperator(
        task_id="fetch_feeds_for_monitoring",
        feed_sources=RSS_FEED_SOURCES,
        output_dir=f"{RSS_MONITOR_DIR}/{{ ds_nodash }}",
        save_schema=True,
        min_entries=5,  # 최소 5개 엔트리가 있어야 유효한 피드로 간주
    )

    # 품질 분석
    analyze_quality = PythonOperator(
        task_id="analyze_quality",
        python_callable=analyze_feed_quality,
        outlets=[RSS_QUALITY_REPORT],
    )

    # 의존성 설정
    fetch_feeds_for_monitoring >> analyze_quality
