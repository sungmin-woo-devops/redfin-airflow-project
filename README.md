# Redfin Airflow 프로젝트

Redfin 부동산 데이터 수집 및 처리를 위한 Apache Airflow 환경입니다.

## 🏗️ 프로젝트 구조

```
redfin_airflow/
├── .env                    # 환경 변수 설정
├── docker-compose.yaml     # Docker Compose 설정
├── config/
│   └── airflow.cfg        # Airflow 설정 파일
├── dags/                  # DAG 파일들
├── plugins/               # 커스텀 플러그인
└── logs/                  # Airflow 로그
```

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 프로젝트 디렉토리로 이동
cd redfin_airflow

# 권한 설정 (Linux/Mac)
sudo chown -R 50000:0 .
```

### 2. Airflow 시작

```bash
# Airflow 컨테이너 시작
docker-compose up -d

# 로그 확인
docker-compose logs -f
```

### 3. 웹 인터페이스 접속

- **URL**: http://localhost:18080
- **사용자명**: airflow
- **비밀번호**: airflow

## 🔧 환경 변수 설정

### `_PIP_ADDITIONAL_REQUIREMENTS` 사용법

이 환경변수는 Airflow 컨테이너에 추가 Python 패키지를 설치할 때 사용됩니다.

#### 사용 시나리오:

1. **웹 스크래핑 작업**
   ```bash
   _PIP_ADDITIONAL_REQUIREMENTS=requests==2.31.0 beautifulsoup4==4.12.2 selenium==4.15.2
   ```

2. **데이터 처리 작업**
   ```bash
   _PIP_ADDITIONAL_REQUIREMENTS=pandas==2.1.4 numpy==1.24.3 openpyxl==3.1.2
   ```

3. **데이터베이스 연결**
   ```bash
   _PIP_ADDITIONAL_REQUIREMENTS=psycopg2-binary==2.9.7 sqlalchemy==2.0.23
   ```

4. **API 통신**
   ```bash
   _PIP_ADDITIONAL_REQUIREMENTS=requests==2.31.0 aiohttp==3.8.6
   ```

#### ⚠️ 주의사항:

- **빠른 테스트용으로만 사용**: 컨테이너 시작 시마다 패키지가 재설치됩니다
- **프로덕션 환경**: 커스텀 Docker 이미지를 빌드하는 것을 권장합니다
- **패키지 버전**: 명시적으로 버전을 지정하여 재현 가능한 환경을 만드세요

#### 설정 방법:

`.env` 파일에서 다음과 같이 설정:

```bash
# 예시: Redfin 스크래핑을 위한 패키지
_PIP_ADDITIONAL_REQUIREMENTS=requests==2.31.0 beautifulsoup4==4.12.2 selenium==4.15.2 pandas==2.1.4
```

## 📊 서비스 구성

| 서비스 | 포트 | 설명 |
|--------|------|------|
| Airflow Webserver | 18080 | 웹 인터페이스 |
| Airflow API Server | 8080 | REST API |
| PostgreSQL | 5432 | 메타데이터 데이터베이스 |

## 🛠️ 개발 가이드

### DAG 작성 예시

```python
# dags/redfin_scraper.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def scrape_redfin_data():
    import requests
    from bs4 import BeautifulSoup
    
    # Redfin 스크래핑 로직
    pass

default_args = {
    'owner': 'redfin-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redfin_data_pipeline',
    default_args=default_args,
    description='Redfin 부동산 데이터 수집 파이프라인',
    schedule_interval=timedelta(hours=6),
    catchup=False
)

scrape_task = PythonOperator(
    task_id='scrape_redfin_data',
    python_callable=scrape_redfin_data,
    dag=dag
)
```

### 커스텀 플러그인 작성

```python
# plugins/redfin_plugin.py
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook

class RedfinHook(BaseHook):
    def __init__(self, conn_id='redfin_default'):
        super().__init__()
        self.conn_id = conn_id

class RedfinPlugin(AirflowPlugin):
    name = 'redfin_plugin'
    hooks = [RedfinHook]
```

## �� 모니터링 및 로그

```bash
# 실시간 로그 확인
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# 특정 DAG 로그 확인
docker-compose exec airflow-webserver airflow tasks logs <dag_id> <task_id>
```

## 🧹 정리

```bash
# 컨테이너 중지
docker-compose down

# 볼륨까지 삭제 (데이터 초기화)
docker-compose down -v

# 이미지 재빌드 (패키지 변경 시)
docker-compose build --no-cache
```

## 📝 참고 자료

- [Apache Airflow 공식 문서](https://airflow.apache.org/docs/)
- [Docker Compose 가이드](https://docs.docker.com/compose/)
- [Airflow Docker 이미지](https://hub.docker.com/r/apache/airflow)
