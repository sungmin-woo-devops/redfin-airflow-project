from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from utils.mongo_ping import test_connection

# 우선순위: Airflow Variable(MONGO_URI) > 기본값
DEFAULT_URI = "mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin"
MONGO_URI = Variable.get("MONGO_URI", default_var=DEFAULT_URI)

with DAG(
    dag_id="mongo_conn_test",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None, # 수동 실행
    catchup=False,
    tags=["healthcheck", "mongo"],
) as dag:

    ping_mongo = PythonOperator(
        task_id="ping_mongodb",
        python_callable=test_connection,
        op_kwargs={"mongo_uri": MONGO_URI, "timeout_ms": 5000},
    )
