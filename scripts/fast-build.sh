#!/bin/bash
# Docker Buildx를 사용한 고속 빌드 스크립트

set -e

echo "🚀 Docker Buildx 고속 빌드 시작"

# 색상 코드
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 빌드 설정
BUILDER_NAME="redfin_builder"
IMAGE_NAME="redfin_airflow"
CACHE_FROM="type=local,src=/tmp/buildx-cache"
CACHE_TO="type=local,dest=/tmp/buildx-cache-new,mode=max"

# 함수: 로그 출력
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# 1. Buildx Builder 설정
log "Buildx Builder 설정 확인..."
if ! docker buildx inspect $BUILDER_NAME > /dev/null 2>&1; then
    log "새로운 Builder 생성: $BUILDER_NAME"
    docker buildx create --name $BUILDER_NAME --driver docker-container --use
    docker buildx inspect --bootstrap
else
    log "기존 Builder 사용: $BUILDER_NAME"
    docker buildx use $BUILDER_NAME
fi

# 2. 캐시 디렉토리 생성
log "캐시 디렉토리 설정..."
mkdir -p /tmp/buildx-cache

# 3. Buildx를 사용한 고속 빌드 (캐시 활용)
log "Docker Buildx 빌드 시작 (캐시 활용)..."

# BuildKit 기능 활성화 옵션들
export BUILDKIT_PROGRESS=plain
export DOCKER_BUILDKIT=1

# 고속 빌드 실행
docker buildx build \
    --builder $BUILDER_NAME \
    --platform linux/arm64 \
    --cache-from $CACHE_FROM \
    --cache-to $CACHE_TO \
    --load \
    --tag $IMAGE_NAME:latest \
    --tag $IMAGE_NAME:$(date +%Y%m%d-%H%M%S) \
    --build-arg BUILDKIT_INLINE_CACHE=1 \
    --progress=plain \
    .

# 4. 캐시 최적화 (새 캐시로 교체)
if [ -d "/tmp/buildx-cache-new" ]; then
    log "빌드 캐시 최적화..."
    rm -rf /tmp/buildx-cache
    mv /tmp/buildx-cache-new /tmp/buildx-cache
fi

success "빌드 완료!"

# 5. 이미지 정보 출력
log "빌드된 이미지 정보:"
docker images | grep $IMAGE_NAME | head -5

# 6. Docker Compose에서 사용할 수 있도록 태그 확인
log "Docker Compose 서비스 업데이트..."
if docker-compose ps | grep -q "Up"; then
    warning "현재 서비스가 실행 중입니다. 다음 명령으로 재시작하세요:"
    echo "  docker-compose down && docker-compose up -d"
else
    log "서비스가 중지된 상태입니다."
fi

success "🎉 Docker Buildx 고속 빌드 완료!"
echo ""
echo "💡 빌드 시간 단축 팁:"
echo "   - 첫 번째 빌드: 캐시 생성으로 조금 더 걸림"
echo "   - 두 번째 빌드부터: 캐시 활용으로 월등히 빠름"
echo "   - 캐시 위치: /tmp/buildx-cache"
echo ""
echo "🔧 추가 최적화 옵션:"
echo "   ./scripts/fast-build.sh --no-cache    # 캐시 없이 빌드"
echo "   ./scripts/fast-build.sh --parallel    # 병렬 빌드 (실험적)"
