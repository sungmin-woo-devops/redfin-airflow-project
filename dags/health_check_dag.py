"""
시스템 헬스체크 DAG
MongoDB, Elasticsearch 등 인프라 상태 확인
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# 커스텀 유틸리티 임포트 (조건부)
try:
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
    from utils.mongo_connection import mongo_health_check
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False
    # 더미 함수 정의
    def mongo_health_check(**context):
        raise Exception("MongoDB 연결 테스트 모듈을 로드할 수 없습니다. pymongo를 설치하세요.")

# DAG 기본 설정
default_args = {
    'owner': 'redfin',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'health_check',
    default_args=default_args,
    description='시스템 헬스체크 DAG',
    schedule_interval='*/30 * * * *',  # 30분마다 실행
    catchup=False,
    tags=['health', 'monitoring', 'mongodb'],
)

# MongoDB 헬스체크 Task
mongo_check = PythonOperator(
    task_id='mongodb_health_check',
    python_callable=mongo_health_check,
    dag=dag,
)

# Docker 컨테이너 상태 확인
docker_check = BashOperator(
    task_id='docker_containers_check',
    bash_command='''
    echo "🐳 Docker 컨테이너 상태 확인..."
    docker ps --filter "name=mongodb" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    
    if docker ps | grep -q mongodb; then
        echo "✅ MongoDB 컨테이너 실행 중"
    else
        echo "❌ MongoDB 컨테이너 실행되지 않음"
        exit 1
    fi
    ''',
    dag=dag,
)

# Tailscale 연결 확인 (선택사항)
tailscale_check = BashOperator(
    task_id='tailscale_check',
    bash_command='''
    echo "🌐 Tailscale 상태 확인..."
    if command -v tailscale > /dev/null; then
        tailscale status | head -5
        echo "✅ Tailscale 연결됨"
    else
        echo "⚠️ Tailscale 설치되지 않음"
    fi
    ''',
    dag=dag,
)

# Task 의존성 설정
docker_check >> mongo_check >> tailscale_check
