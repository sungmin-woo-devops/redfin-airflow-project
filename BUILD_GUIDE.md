# 🚀 Docker Buildx 고속 빌드 가이드

## 기본 vs Buildx 빌드 비교

### 기존 방식 (느림)
```bash
# 캐시 없이 빌드 (매번 처음부터)
docker-compose build --no-cache

# 일반 빌드
docker-compose build
```

### 🔥 Buildx 고속 빌드 (권장)

#### 1. 기본 고속 빌드
```bash
# 스크립트 실행 (추천)
./scripts/fast-build.sh

# 또는 직접 명령어
docker buildx build --cache-from type=local,src=/tmp/buildx-cache --cache-to type=local,dest=/tmp/buildx-cache,mode=max --load --tag redfin_airflow:latest .
```

#### 2. 초고속 병렬 빌드
```bash
# 병렬 처리 + 고급 캐시
./scripts/parallel-build.sh

# 또는 원라이너
docker buildx build --builder redfin_builder --cache-from type=local,src=/tmp/buildx-cache --cache-to type=local,dest=/tmp/buildx-cache-new,mode=max --load --tag redfin_airflow:latest --progress=plain -f Dockerfile.buildx .
```

#### 3. 원라이너 명령어들

**빠른 빌드 (캐시 활용)**
```bash
docker buildx build --cache-from type=local,src=/tmp/buildx-cache --cache-to type=local,dest=/tmp/buildx-cache --load -t redfin_airflow:latest .
```

**초고속 빌드 (멀티 캐시)**
```bash
docker buildx build --cache-from type=local,src=/tmp/buildx-cache --cache-from type=registry,ref=redfin/cache:latest --cache-to type=local,dest=/tmp/buildx-cache,mode=max --load -t redfin_airflow:latest .
```

**메타데이터 포함 빌드**
```bash
docker buildx build --load --metadata-file /tmp/metadata.json --tag redfin_airflow:$(date +%Y%m%d) --cache-from type=local,src=/tmp/buildx-cache .
```

## 🎯 성능 최적화 팁

### 1. 캐시 전략
- **로컬 캐시**: `/tmp/buildx-cache` 사용
- **레지스트리 캐시**: Docker Hub 또는 GitHub Container Registry
- **인라인 캐시**: `--build-arg BUILDKIT_INLINE_CACHE=1`

### 2. 빌드 시간 단축 요소
```bash
# 1. BuildKit 멀티스테이지 캐시
--cache-from type=local,src=/tmp/buildx-cache

# 2. 병렬 다운로드
--build-arg BUILDKIT_INLINE_CACHE=1

# 3. 레이어 최적화
-f Dockerfile.buildx  # 최적화된 Dockerfile 사용

# 4. 플랫폼 특정 빌드
--platform linux/arm64
```

### 3. 개발 워크플로우

**첫 번째 빌드** (캐시 생성)
```bash
./scripts/fast-build.sh
# 시간: ~5-10분 (캐시 생성)
```

**두 번째 빌드부터** (캐시 활용)
```bash
./scripts/fast-build.sh
# 시간: ~1-3분 (대폭 단축)
```

**코드 변경 후 빌드**
```bash
# requirements.txt 변경 없는 경우
docker buildx build --cache-from type=local,src=/tmp/buildx-cache --load -t redfin_airflow:latest .
# 시간: ~30초-1분
```

## 🔧 고급 설정

### Builder 인스턴스 관리
```bash
# 새 builder 생성
docker buildx create --name mybuilder --use

# builder 목록 확인
docker buildx ls

# builder 제거
docker buildx rm mybuilder
```

### 캐시 관리
```bash
# 캐시 크기 확인
du -sh /tmp/buildx-cache

# 캐시 정리
rm -rf /tmp/buildx-cache

# 캐시 사용량 최적화
docker buildx prune
```

## 📊 성능 비교

| 빌드 방식 | 첫 번째 빌드 | 두 번째 빌드 | 캐시 활용 |
|-----------|-------------|-------------|----------|
| `docker-compose build --no-cache` | 10-15분 | 10-15분 | ❌ |
| `docker-compose build` | 8-12분 | 3-5분 | ⚠️ 부분적 |
| `docker buildx` (기본) | 6-10분 | 1-3분 | ✅ 우수 |
| `docker buildx` (최적화) | 5-8분 | 30초-1분 | 🚀 최고 |

## 🚀 추천 사용법

### 일반 개발
```bash
# 첫 설정
./scripts/fast-build.sh

# 이후 변경사항 반영
docker buildx build --cache-from type=local,src=/tmp/buildx-cache --load -t redfin_airflow:latest .
```

### 프로덕션 배포
```bash
# 최고 성능으로 빌드
./scripts/parallel-build.sh

# 태그와 함께 빌드
docker buildx build --cache-from type=local,src=/tmp/buildx-cache --load -t redfin_airflow:prod-$(date +%Y%m%d) .
```

### CI/CD 환경
```bash
# GitHub Actions, GitLab CI 등에서
docker buildx build --cache-from type=gha --cache-to type=gha,mode=max --load -t redfin_airflow:latest .
```
