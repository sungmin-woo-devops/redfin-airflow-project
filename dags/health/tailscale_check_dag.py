from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="tailscale_check",
    start_date=datetime(2025, 8, 1),
    schedule_interval="*/30 * * * *",  # 30분마다
    catchup=False,
    tags=["health", "network", "tailscale"],
) as dag:

    tailscale_check = BashOperator(
        task_id="tailscale_check",
        bash_command=r'''
        echo "🌐 Tailscale 상태 확인..."
        if command -v tailscale > /dev/null; then
            tailscale status | head -5 || true
            echo "✅ Tailscale 연결 확인"
        else
            echo "⚠️ Tailscale 미설치"
        fi
        '''
    )
