# 커스텀 Airflow 이미지
FROM apache/airflow:2.9.2

# 루트 사용자로 전환하여 시스템 패키지 설치
USER root

# 시스템 의존성 설치 (필요한 경우)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# airflow 사용자로 다시 전환
USER airflow

# 추가 Python 패키지 설치
COPY config/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 작업 디렉토리 설정
WORKDIR /opt/airflow
