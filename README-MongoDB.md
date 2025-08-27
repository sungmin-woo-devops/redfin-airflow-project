# MongoDB 설정 가이드

## 개요
Redfin 프로젝트를 위한 MongoDB 데이터베이스 설정입니다.

## 데이터베이스 구조
- **데이터베이스**: `redfin`
- **컬렉션**:
  - `rss_meta`: RSS 메타데이터
  - `rss_extracted`: 추출된 RSS 데이터
  - `rss_analyzed`: 분석된 RSS 데이터
  - `rss_public`: 공개 RSS 데이터

## 사용자 계정
- **관리자**: `admin/admin123`
- **일반 사용자**: `redfin/Redfin7620!`

## 시작 방법

### 1. 자동 시작 (권장)
```bash
./start-mongo.sh
```

### 2. 수동 시작
```bash
# 환경 변수 설정
source env.mongo

# 컨테이너 시작
docker-compose -f docker-compose.mongo.yaml up -d
```

## 접속 정보

### MongoDB 직접 접속
```bash
# 관리자로 접속
docker exec -it mongodb mongosh --username admin --password admin123 --authenticationDatabase admin

# 일반 사용자로 접속
docker exec -it mongodb mongosh --username redfin --password Redfin7620! --authenticationDatabase redfin
```

### Mongo Express (웹 UI)
- URL: http://localhost:8081
- 관리자 계정으로 로그인

### 연결 문자열
```
mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin
```

## 컬렉션 사용 예시

### Python에서 사용
```python
from pymongo import MongoClient

# 연결
client = MongoClient("mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin")
db = client.redfin

# 컬렉션 사용
rss_meta = db.rss_meta
rss_extracted = db.rss_extracted
rss_analyzed = db.rss_analyzed
rss_public = db.rss_public
```

### 데이터 삽입 예시
```python
# RSS 메타데이터 삽입
rss_meta.insert_one({
    "url": "https://example.com/rss",
    "title": "Example RSS Feed",
    "description": "Example description",
    "last_updated": "2024-01-01T00:00:00Z"
})
```

## 정리
```bash
# 컨테이너 중지 및 데이터 삭제
docker-compose -f docker-compose.mongo.yaml down -v
```

## 문제 해결

### 포트 충돌
포트 27017이나 8081이 이미 사용 중인 경우:
```bash
# 사용 중인 프로세스 확인
lsof -i :27017
lsof -i :8081

# 프로세스 종료 후 재시작
./start-mongo.sh
```

### 권한 문제
```bash
# 스크립트 실행 권한 부여
chmod +x start-mongo.sh
```
