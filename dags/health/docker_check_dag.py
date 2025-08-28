import pendulum

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# ----- 설정 -----
DAG_ID = "A001_docker_check"

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2025, 8, 1),
    schedule_interval="*/30 * * * *",  # 30분마다
    catchup=False,
    tags=["health", "docker"],
) as dag:

    # ----- 태스크 -----

    # 1. Docker 연결 테스트
    docker_check = DockerOperator(
        task_id="docker_check",
        image="alpine:3.20",
        api_version="auto",
        auto_remove=True,
        command='sh -lc "echo Health && true"',
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
    )
