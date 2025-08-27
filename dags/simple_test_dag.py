"""
Airflow 기본 테스트 DAG
의존성 없이 동작하는 간단한 테스트용 DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def test_python_task(**context):
    """간단한 Python 태스크"""
    print("✅ Python 태스크 실행 성공!")
    print(f"📅 실행 시간: {context['execution_date']}")
    print("🎯 Airflow가 정상적으로 작동합니다!")
    return "success"

# DAG 정의
dag = DAG(
    'simple_test',
    default_args={
        'owner': 'redfin',
        'depends_on_past': False,
        'start_date': datetime(2025, 8, 27),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='의존성 없는 간단한 테스트 DAG',
    schedule_interval=timedelta(hours=1),  # 1시간마다
    catchup=False,
    tags=['test', 'simple'],
)

# Bash 태스크
bash_task = BashOperator(
    task_id='bash_test',
    bash_command='echo "✅ Bash 태스크 실행 성공! 현재 시간: $(date)"',
    dag=dag,
)

# Python 태스크
python_task = PythonOperator(
    task_id='python_test',
    python_callable=test_python_task,
    dag=dag,
)

# 태스크 의존성
bash_task >> python_task
