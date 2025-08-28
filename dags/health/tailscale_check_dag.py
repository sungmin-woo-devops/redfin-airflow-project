import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.utils.tailscale import tailscale_status

# ----- 설정 -----
DAG_ID = "A003_tailscale_check"
MIN_PEERS = 0

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2025, 8, 1),
    schedule_interval="*/30 * * * *",  # 30분마다
    catchup=False,
    tags=["health", "network", "tailscale"],
) as dag:

    # ----- 태스크 -----

    # 1. Tailscale 상태 확인
    check = PythonOperator(
        task_id="check",
        python_callable=tailscale_status,
        op_kwargs={"min_peers": MIN_PEERS},
    )
