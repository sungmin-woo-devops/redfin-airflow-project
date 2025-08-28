# RSS Feed Parser Operator

RSS 피드를 가져와서 JSON 파일로 저장하는 Airflow Operator입니다.

## 주요 기능

- **RSS 피드 파싱**: `feedparser` 라이브러리를 사용하여 RSS/Atom 피드 파싱
- **JSON 저장**: 파싱된 데이터를 JSON 형식으로 저장
- **스키마 생성**: 자동으로 데이터 스키마 파일 생성
- **품질 관리**: `min_entries` 파라미터로 최소 엔트리 수 검증
- **Jinja 템플릿 지원**: Airflow context를 사용한 동적 경로 생성
- **통합 스키마 변환**: RSS 데이터를 MongoDB 통합 스키마로 변환
- **배치 봉투 관리**: 스크래핑 단위별 배치 봉투 생성 및 관리

## Operator 종류

### 1. RSSFeedParserOperator

피드 소스 딕셔너리를 받아서 RSS 피드를 파싱하고 저장합니다.

```python
from plugins.operators.feedparser_operator import RSSFeedParserOperator
from plugins.operators.feed_sources import RSS_FEED_SOURCES

fetch_feeds = RSSFeedParserOperator(
    task_id="fetch_rss_feeds",
    feed_sources=RSS_FEED_SOURCES,
    output_dir="/opt/airflow/data/rss/raw/{{ ds_nodash }}",
    save_schema=True,
    min_entries=3,
)
```

### 2. RSSFeedFetcherOperator

피드 URL 목록을 받아서 RSS 피드를 가져옵니다.

```python
from plugins.operators.feedparser_operator import RSSFeedFetcherOperator

fetch_urls = RSSFeedFetcherOperator(
    task_id="fetch_specific_feeds",
    feed_urls=["https://example.com/feed.xml", "https://blog.com/rss"],
    output_dir="/opt/airflow/data/rss/raw/{{ ds_nodash }}",
    min_entries=1,
)
```

## 피드 소스 관리 (feed_sources.py)

YAML 기반의 피드 소스 관리 시스템을 제공합니다.

### 주요 기능

- **YAML 설정**: `config/feeds.yaml`에서 피드 소스 관리
- **그룹별 분류**: 카테고리별 피드 그룹화
- **동적 필터링**: 그룹, 키워드 기반 피드 검색
- **하위 호환성**: 기존 상수들 유지

### 사용 예시

```python
from plugins.operators.feed_sources import feed_manager

# 모든 피드 가져오기
all_feeds = feed_manager.get_all_feeds()

# 특정 그룹의 피드만 가져오기
ml_feeds = feed_manager.get_feeds_by_group('legacy_ml')

# 여러 그룹의 피드 가져오기
ai_feeds = feed_manager.get_feeds_by_groups(['legacy_ai', 'frontier_lab'])

# URL만 가져오기
urls = feed_manager.get_feed_urls_by_group('arxiv')
```

## 통합 스키마 변환 (rss_unifier.py)

RSS 피드 데이터를 MongoDB 통합 스키마로 변환하는 유틸리티입니다. 자세한 스키마는 `config/unified_rss_schema.json` 파일을 참조하세요.

### 주요 기능

- **스키마 변환**: RSS 데이터를 `unified_rss_schema.json` 형식으로 변환
- **배치 처리**: 여러 피드를 배치 봉투로 감싸기
- **MongoDB 저장**: 변환된 데이터를 MongoDB에 저장
- **ULID 생성**: 고유 식별자 자동 생성

### 사용 예시

```python
from plugins.utils.rss_unifier import process_rss_files, save_to_mongodb

# RSS 파일들을 통합 스키마로 변환
unified_docs = process_rss_files(
    input_dir="/opt/airflow/data/rss/raw/20250828",
    output_dir="/opt/airflow/data/rss/unified/20250828"
)

# MongoDB에 저장
result = save_to_mongodb(
    docs=unified_docs,
    mongo_uri="mongodb://localhost:27017/redfin",
    db_name="redfin",
    collection_name="rss_unified"
)
```

## 배치 봉투 스키마 (batch_envelope_schema.json)

스크래핑 단위별 배치 관리를 위한 스키마입니다.

### 주요 특징

- **배치 단위 관리**: 각 스크래핑 실행을 배치로 관리
- **통계 정보**: 피드 수, 엔트리 수, 처리 시간 등 통계
- **에러 추적**: 실패한 피드 정보 수집
- **상태 관리**: 배치 처리 상태 추적

### 스키마 구조

자세한 스키마는 `config/batch_envelope_schema.json` 파일을 참조하세요.

```json
{
  "_id": "ulid",
  "schema_version": "1.0.0",
  "status": "meta|extracted|processed|labeled|embedded",
  "run": {
    "run_id": "ulid",
    "exec_ts": "2025-08-28T00:00:00Z"
  },
  "source": {
    "batch_type": "rss_collection",
    "batch_name": "daily_rss_collection_20250828",
    "feed_count": 47,
    "total_entries": 1250
  },
  "partition": {
    "year": "2025",
    "month": "08",
    "day": "28"
  },
  "audit": {
    "created_at": "2025-08-28T00:00:00Z",
    "updated_at": "2025-08-28T00:00:00Z",
    "processed_at": null,
    "labeled_at": null,
    "embedded_at": null
  },
  "items": [/* 통합 스키마 문서들 */],
  "statistics": {
    "total_feeds": 47,
    "successful_feeds": 42,
    "failed_feeds": 3,
    "empty_feeds": 2,
    "total_entries": 1250,
    "avg_entries_per_feed": 26.6,
    "processing_time_seconds": 45.2
  },
  "errors": [/* 에러 정보 */]
}
```

## DAG 구조

### 1. 분리된 DAG 구조 (권장)

#### A. RSS 통합 스키마 변환 (`rss_unify_pipeline.py`)
- **목적**: RSS 피드를 통합 스키마로 변환
- **트리거**: `RSS_RAW_DATA` Dataset
- **출력**: `RSS_UNIFIED_DATA` Dataset

#### B. 배치 봉투 처리 (`rss_batch_pipeline.py`)
- **목적**: 배치 봉투 생성 및 MongoDB 저장
- **트리거**: `RSS_UNIFIED_DATA` Dataset
- **출력**: `RSS_BATCH_DATA` Dataset

### 2. 통합 DAG 구조 (레거시)

#### RSS 통합 및 배치 처리 (`rss_unify_import_batch.py`)
- **목적**: 통합 스키마 변환과 배치 처리를 하나의 DAG에서 수행
- **스케줄**: 수동 실행 또는 설정된 스케줄

### 파이프라인 흐름

```
RSS 피드 수집 → 메타 데이터 생성 → 콘텐츠 추출 → 통합 스키마 변환 → 배치 봉투 처리 → MongoDB 저장
     ↓              ↓              ↓              ↓              ↓              ↓
rss_feed_collector → rss_meta_pipeline → rss_extract_pipeline → rss_unify_pipeline → rss_batch_pipeline → MongoDB
```

## 품질 모니터링 DAG

RSS 피드 품질을 모니터링하는 전용 DAG를 제공합니다.

### 기능

- **품질 기준**: `min_entries` 파라미터로 최소 엔트리 수 검증
- **상태 추적**: 성공, 실패, 빈 피드 분류
- **상세 로깅**: 각 피드별 처리 결과 기록
- **품질 보고서**: JSON 형식의 품질 분석 결과

### 사용 예시

```python
# 품질 모니터링 DAG에서 사용
monitor_feeds = RSSFeedParserOperator(
    task_id="monitor_feed_quality",
    feed_sources=RSS_FEED_SOURCES,
    output_dir="/opt/airflow/data/rss/monitor/{{ ds_nodash }}",
    min_entries=5,  # 품질 기준 설정
)
```

## 파라미터 설명

### RSSFeedParserOperator

| 파라미터 | 타입 | 기본값 | 설명 |
|---------|------|--------|------|
| `feed_sources` | `Dict[str, str]` | 필수 | 피드 이름과 URL 매핑 |
| `output_dir` | `str` | 필수 | 출력 디렉토리 (Jinja 템플릿 지원) |
| `save_schema` | `bool` | `True` | 스키마 파일 저장 여부 |
| `min_entries` | `int` | `1` | 최소 엔트리 수 (품질 기준) |

### RSSFeedFetcherOperator

| 파라미터 | 타입 | 기본값 | 설명 |
|---------|------|--------|------|
| `feed_urls` | `List[str]` | 필수 | RSS 피드 URL 목록 |
| `output_dir` | `str` | 필수 | 출력 디렉토리 (Jinja 템플릿 지원) |
| `min_entries` | `int` | `1` | 최소 엔트리 수 (품질 기준) |

## 반환 결과

모든 Operator는 다음과 같은 구조의 결과를 반환합니다:

```python
{
    "processed_feeds": 10,        # 처리된 피드 수
    "successful_feeds": 8,        # 성공한 피드 수
    "failed_feeds": [             # 실패한 피드 목록
        {
            "feed_name": "Failed Feed",
            "feed_url": "https://failed.com/feed",
            "error": "Connection timeout"
        }
    ],
    "empty_feeds": [              # 빈 피드 목록 (min_entries 미달)
        {
            "feed_name": "Empty Feed",
            "feed_url": "https://empty.com/feed",
            "entry_count": 2
        }
    ],
    "saved_files": [              # 저장된 파일 경로 목록
        "/opt/airflow/data/rss/raw/20250828/feed1.json",
        "/opt/airflow/data/rss/raw/20250828/feed1_schema.json"
    ]
}
```

## 에러 처리 및 품질 관리

### 에러 처리

- **네트워크 오류**: 연결 실패 시 자동 재시도
- **파싱 오류**: `bozo` 플래그로 파싱 문제 감지
- **파일 시스템 오류**: 디렉토리 생성 실패 시 예외 발생

### 품질 기준

- **최소 엔트리 수**: `min_entries` 파라미터로 설정
- **파싱 상태**: `feed_status` 필드에 상세 정보 저장
- **빈 피드 감지**: 엔트리가 없는 피드 별도 분류

### 품질 모니터링

```python
# 품질 보고서 예시
{
    "total_feeds": 15,
    "successful_feeds": 12,
    "failed_feeds": 2,
    "empty_feeds": 1,
    "quality_score": 0.8,
    "timestamp": "2025-08-28T00:00:00Z"
}
```

## 파일 구조

```
redfin_airflow/
├── plugins/
│   ├── operators/
│   │   ├── feedparser_operator.py    # RSS 피드 파서 Operator
│   │   ├── feed_sources.py           # 피드 소스 관리
│   │   └── README.md                 # 이 문서
│   └── utils/
│       ├── rss_unifier.py            # 통합 스키마 변환 유틸리티
│       ├── test_unified_schema.py    # 통합 스키마 테스트
│       └── test_batch_envelope.py    # 배치 봉투 테스트
├── config/
│   ├── feeds.yaml                    # 피드 소스 설정
│   ├── unified_rss_schema.json       # MongoDB 통합 스키마
│   └── batch_envelope_schema.json    # 배치 봉투 스키마
└── dags/
    └── ingestion/
        ├── rss_feed_collector.py     # RSS 피드 수집 DAG
        ├── rss_feed_monitor.py       # 품질 모니터링 DAG
        ├── rss_unify_pipeline.py     # 통합 스키마 변환 DAG
        ├── rss_batch_pipeline.py     # 배치 봉투 처리 DAG
        └── rss_unify_import_batch.py # 레거시 통합 DAG
```

## 의존성

- `feedparser`: RSS/Atom 피드 파싱
- `ulid-py`: 고유 식별자 생성
- `pendulum`: 날짜/시간 처리
- `pymongo`: MongoDB 연결 (선택적)
- `PyYAML`: YAML 파일 처리

## 설치

```bash
pip install feedparser ulid-py pendulum pymongo PyYAML
```

## 테스트

### 통합 스키마 테스트
```bash
cd redfin_airflow/plugins/utils
python test_unified_schema.py
```

### 배치 봉투 테스트
```bash
cd redfin_airflow/plugins/utils
python test_batch_envelope.py
```
