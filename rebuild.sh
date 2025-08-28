#!/bin/bash
set -euo pipefail

# Scrapy 의존성 문제 해결을 위한 컨테이너 재빌드 스크립트

echo "=== Airflow 컨테이너 재빌드 (Scrapy 의존성 포함) ==="

# 현재 디렉토리 확인
if [[ ! -f "docker-compose.yaml" ]]; then
    echo "Error: docker-compose.yaml not found. Run this from the redfin_airflow directory."
    exit 1
fi

echo "1. 기존 컨테이너 중지..."
docker-compose down

echo "2. 이미지 재빌드 (no-cache)..."
docker-compose build --no-cache

echo "3. 컨테이너 재시작..."
docker-compose up -d

echo "4. Airflow 초기화 대기..."
sleep 30

echo "5. 컨테이너 상태 확인..."
docker-compose ps

echo "6. Scrapy 설치 확인..."
docker-compose exec airflow-scheduler python -c "import scrapy; print(f'Scrapy version: {scrapy.__version__}')" || echo "Scrapy not available in scheduler"

echo "=== 재빌드 완료 ==="
echo "웹 UI: http://localhost:8080"
echo "로그인: admin / admin"
