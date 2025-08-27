from airflow.datasets import Dataset

# Data SSOT (Shared State of Truth)

# Dataset
# - "데이터 자산"의 상태 변경 이벤트를 표준화해 DAG 간 의존/트리거를 연결하는 객체
# - Producer 태스크가 outlets[Dataset(...)]로 "업데이트됨"을 기록하면,
# - Consumer DAG가 schedule=[Dataset(...)] 또는 Dataset Expression(A & B, A | B)로 조건을 만족할 때 실행

# 핵심
# - Dataset은 파일이 아니라 식별자(URI 문자열+경로)
# - “업데이트”는 Producer 태스크 완료 시점에 발생

# ============================
# 1. Ingestion Stage
# ============================

# 1. RSS 메타데이터 (피드 원본/요약)
# Producer: 피드 수집/정규화 태스크
# Consumer: 기사 본문 추출 DAG(메타가 갱신되면 실행)
RSS_META      = Dataset("mongo://redfin/articles")        # RSS 피드 메타 계층

# 2. RSS 추출 (기사 메타데이터를 근거로 추가 정보 추출)
# Producer: 본문 추출·정제·업서트 태스크
# Consumer: 임베딩/인덱싱 DAG, 품질검사 DAG
RSS_EXTRACTED = Dataset("mongo://redfin/articles")   # RSS 피드 추가 정보 계층

# 3. RSS 전처리(URL 정규화, 문자열 데이터 정규화, 날짜 형식 정규화, 결측값 제거, 등)
# Producer: 전처리 태스크
# Consumer: 임베딩/인덱싱 DAG, 품질검사 DAG
RSS_PROCESSED  = Dataset("mongo://redfin/articles")    # RSS 피드 전처리 계층

# ============================
# 2. Enrichment Stage
# ============================

# 1. RSS 라벨링 데이터
# Producer: 기사별 키워드, 카테고리, 태그 추출 (RSS_PROCESSED에 컬럼 3개 추가)
# Consumer: 외부 제공용 덤프/S3 업로드/캐시 무효화 DAG
RSS_LABELED    = Dataset("mongo://redfin/rss_labeld")    # RSS 피드 라벨링 계층

# 2. RSS 임베딩 데이터
# Question: 임베딩 vs 인덱싱 ?
# Producer: 임베딩 태스크
# Consumer: 인덱싱 DAG
RSS_EMBEDDED   = Dataset("mongo://redfin/rss_embedded")
