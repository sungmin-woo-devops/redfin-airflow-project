import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from utils.mongo import test_mongo_connection

# ----- 설정 -----
DAG_ID = "A002_mongo_conn_test"
DEFAULT_URI = "mongodb://redfin:Redfin7620!@172.17.0.1:27017/redfin?authSource=redfin"
# DEFAULT_URI: "mongodb://redfin:Redfin7620!@host.docker.internal:27017/redfin?authSource=redfin"
MONGO_URI = Variable.get("MONGO_URI", default_var=DEFAULT_URI) # 우선순위: Airflow Variable(MONGO_URI) > 기본값

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2025, 8, 1),
    schedule_interval=None, # 수동 실행
    catchup=False,
    tags=["healthcheck", "mongo"],
) as dag:

    # ----- 태스크 -----

    # 1. MongoDB 연결 테스트
    ping_mongo = PythonOperator(
        task_id="ping_mongodb",
        python_callable=test_mongo_connection,
    )
