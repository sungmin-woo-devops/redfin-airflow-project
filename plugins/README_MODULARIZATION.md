# RSS 학습 데이터 추출 모듈화

## 개요

기존 `rss_extract_learning_data.py` DAG의 기능을 `plugins/` 디렉토리 내부로 모듈화하여 재사용 가능하고 유지보수하기 쉬운 구조로 개선했습니다.

## 모듈화 구조

### 1. 키워드 추출 모듈
**파일**: `utils/keyword_extractor.py`

```python
from plugins.utils.keyword_extractor import KeywordExtractor, extract_keywords

# 사용 예시
extractor = KeywordExtractor(min_length=3, max_keywords=10)
keywords = extractor.extract_from_title_and_summary(title, summary)
```

**주요 기능**:
- 제목과 요약에서 키워드 추출
- 정규표현식을 사용한 한글/영어/숫자 패턴 매칭
- 중복 제거 및 길이 제한

### 2. 카테고리 분류 모듈
**파일**: `utils/category_classifier.py`

```python
from plugins.utils.category_classifier import CategoryClassifier, extract_and_classify

# 사용 예시
classifier = CategoryClassifier()
category, tags = classifier.classify_entry(entry)
```

**주요 기능**:
- 태그 기반 카테고리 분류
- 7개 주요 카테고리 지원 (technology, business, politics, sports, entertainment, health, education)
- 키워드 매핑 규칙 기반 분류

### 3. RSS 학습 데이터 처리 모듈
**파일**: `utils/rss_learning_processor.py`

```python
from plugins.utils.rss_learning_processor import RSSLearningProcessor, process_rss_for_learning

# 사용 예시
processor = RSSLearningProcessor()
result = processor.process_collection(collection, output_file)
```

**주요 기능**:
- RSS 엔트리에서 학습 데이터 추출
- 뉴스 ID, 제목, 요약, 키워드, 카테고리, 태그 처리
- JSONL 파일 저장

### 4. 학습 데이터 저장 오퍼레이터
**파일**: `operators/learning_data_operator.py`

```python
from plugins.operators.learning_data_operator import LearningDataSaveOperator, LearningDataValidationOperator

# 사용 예시
save_op = LearningDataSaveOperator(
    task_id="save_learning_data",
    jsonl_file_path="/path/to/file.jsonl",
    collection_name="rss_extracted"
)
```

**주요 기능**:
- JSONL 파일을 MongoDB에 배치 저장
- 배치 메타데이터 관리
- 저장 결과 검증

## 모듈화된 DAG

**파일**: `dags/ingestion/rss_extract_learning_data_modular.py`

기존 DAG와 동일한 기능을 제공하지만 모듈화된 구조를 사용합니다:

```python
# 모듈화된 추출 함수
def extract_learning_data_modular(**context):
    processor = RSSLearningProcessor()
    result = processor.process_collection(rss_collection, output_file)
    return result

# 커스텀 오퍼레이터 사용
save_task = LearningDataSaveOperator(
    task_id="save_to_rss_extracted_modular",
    jsonl_file_path="...",
    collection_name="rss_extracted"
)
```

## 모듈화의 장점

### 1. 재사용성
- 각 모듈은 독립적으로 사용 가능
- 다른 DAG에서도 동일한 기능 활용 가능

### 2. 유지보수성
- 기능별로 분리되어 수정이 용이
- 테스트 작성이 쉬움

### 3. 확장성
- 새로운 카테고리나 키워드 규칙 추가가 쉬움
- 다른 데이터 소스에도 적용 가능

### 4. 가독성
- 각 모듈의 책임이 명확
- 코드 이해가 쉬움

## 사용 방법

### 1. 개별 모듈 사용
```python
# 키워드 추출만 사용
from plugins.utils.keyword_extractor import extract_keywords
keywords = extract_keywords("제목", "요약")

# 카테고리 분류만 사용
from plugins.utils.category_classifier import extract_and_classify
category, tags = extract_and_classify(entry)
```

### 2. 전체 파이프라인 사용
```python
# 모듈화된 DAG 사용
# dags/ingestion/rss_extract_learning_data_modular.py
```

### 3. 커스텀 오퍼레이터 사용
```python
# 다른 DAG에서 커스텀 오퍼레이터 사용
from plugins.operators.learning_data_operator import LearningDataSaveOperator

save_task = LearningDataSaveOperator(
    task_id="custom_save",
    jsonl_file_path="/custom/path.jsonl",
    collection_name="custom_collection"
)
```

## 테스트

각 모듈은 독립적으로 테스트 가능합니다:

```python
# 키워드 추출 테스트
extractor = KeywordExtractor()
keywords = extractor.extract_from_text("AI 기술 발전에 대한 뉴스")
assert "AI" in keywords

# 카테고리 분류 테스트
classifier = CategoryClassifier()
category, tags = classifier.classify_entry({"tags": [{"term": "AI"}]})
assert category == "technology"
```

## 향후 개선 방향

1. **설정 파일화**: 카테고리 매핑 규칙을 YAML/JSON 파일로 분리
2. **성능 최적화**: 배치 처리 크기 조정 및 병렬 처리
3. **모니터링**: 각 모듈별 성능 메트릭 추가
4. **에러 처리**: 더 세밀한 예외 처리 및 복구 로직
