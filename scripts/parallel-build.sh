#!/bin/bash
# 병렬 빌드를 활용한 초고속 빌드 스크립트

set -e

echo "🚀 병렬 Docker Buildx 빌드 시작"

# 색상 코드
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

# 빌드 설정
BUILDER_NAME="redfin_parallel_builder"
IMAGE_NAME="redfin_airflow"

# 1. 고성능 Builder 생성 (멀티 노드, 병렬 처리)
log "고성능 Parallel Builder 설정..."
if ! docker buildx inspect $BUILDER_NAME > /dev/null 2>&1; then
    docker buildx create \
        --name $BUILDER_NAME \
        --driver docker-container \
        --driver-opt network=host \
        --buildkitd-flags '--allow-insecure-entitlement=security.insecure --allow-insecure-entitlement=network.host' \
        --use
    docker buildx inspect --bootstrap
else
    docker buildx use $BUILDER_NAME
fi

# 2. 병렬 빌드 실행
log "병렬 빌드 시작 (최대 성능 모드)..."

# 병렬 처리를 위한 환경변수
export BUILDKIT_STEP_LOG_MAX_SIZE=50000000
export BUILDKIT_STEP_LOG_MAX_SPEED=100000000

# 고급 캐시 전략과 병렬 처리
docker buildx build \
    --builder $BUILDER_NAME \
    --platform linux/arm64 \
    --cache-from type=registry,ref=redfin/cache:buildcache \
    --cache-to type=registry,ref=redfin/cache:buildcache,mode=max \
    --cache-from type=local,src=/tmp/buildx-cache \
    --cache-to type=local,dest=/tmp/buildx-cache-new,mode=max \
    --load \
    --tag $IMAGE_NAME:latest \
    --tag $IMAGE_NAME:fast-$(date +%Y%m%d-%H%M%S) \
    --build-arg BUILDKIT_INLINE_CACHE=1 \
    --build-arg BUILDKIT_MULTI_PLATFORM=1 \
    --progress=plain \
    --metadata-file /tmp/build-metadata.json \
    -f Dockerfile.buildx \
    .

# 3. 빌드 결과 분석
log "빌드 결과 분석..."
if [ -f "/tmp/build-metadata.json" ]; then
    echo "📊 빌드 메타데이터:"
    cat /tmp/build-metadata.json | jq '.buildinfo' 2>/dev/null || cat /tmp/build-metadata.json
fi

# 4. 캐시 최적화
if [ -d "/tmp/buildx-cache-new" ]; then
    log "캐시 최적화..."
    rm -rf /tmp/buildx-cache
    mv /tmp/buildx-cache-new /tmp/buildx-cache
fi

success "🎉 병렬 빌드 완료!"

# 5. 성능 정보 출력
log "빌드 성능 정보:"
docker images | grep $IMAGE_NAME | head -3
echo ""
echo "💡 병렬 빌드의 장점:"
echo "   - 의존성 설치가 병렬로 처리됨"
echo "   - 레이어 캐싱이 더 효율적"
echo "   - 네트워크 최적화로 다운로드 속도 향상"
echo ""
echo "🔧 추가 최적화:"
echo "   - 레지스트리 캐시 사용 (Docker Hub, GitHub Container Registry)"
echo "   - 로컬 캐시와 원격 캐시 동시 활용"
echo "   - 멀티 플랫폼 빌드 지원"
