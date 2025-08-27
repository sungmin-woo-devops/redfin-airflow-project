#!/bin/bash

# MongoDB 시작 및 초기화 스크립트
set -euo pipefail

# 스크립트 디렉토리로 이동
cd "$(dirname "$0")"

# 환경 변수 로드
if [ -f "env.mongo" ]; then
    export $(cat env.mongo | grep -v '^#' | xargs)
fi

echo "🚀 MongoDB 시작 중..."

# 기존 컨테이너 정리
docker-compose -f docker-compose.mongo.yaml down -v 2>/dev/null || true

# MongoDB 컨테이너 시작
docker-compose -f docker-compose.mongo.yaml up -d

echo "⏳ MongoDB 초기화 대기 중..."
sleep 10

# MongoDB 연결 테스트
echo "🔍 MongoDB 연결 테스트 중..."
docker exec mongodb mongosh --username "$MONGO_INITDB_ROOT_USERNAME" --password "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin --eval "
  db = db.getSiblingDB('redfin');
  print('✅ redfin 데이터베이스 연결 성공');
  print('📊 컬렉션 목록:');
  db.getCollectionNames().forEach(function(collection) {
    print('  - ' + collection);
  });
"

echo "✅ MongoDB 설정 완료!"
echo "📊 데이터베이스: redfin"
echo "👤 일반 사용자: redfin/Redfin7620!"
echo "🌐 Mongo Express: http://localhost:8081"
echo "🔗 MongoDB: mongodb://localhost:27017"
