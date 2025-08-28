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
RSS_FEEDS      = Dataset("mongo://redfin/rss_feeds")        # RSS 피드 메타 계층

# 2. RSS 추천 시스템 학습 데이터 추출
# Producer: 본문 추출·정제·업서트 태스크
# Consumer: 추천 시스템 학습을 위한 키워드, 태그, 카테고리 (라벨링) 추출 DAG
RSS_EXTRACTED = Dataset("mongo://redfin/rss_extracted") # 추천 시스템 학습 데이터 추출

# 3. RSS RAG 데이터 추출
# Producer: 기사 본문 추출·정제·업서트 태스크
# Consumer: RAG 데이터 추출 DAG
RSS_ARTICLES_EXTRACTED  = Dataset("mongo://redfin/rss_articles_extracted") # RAG 데이터 추출

# ============================
# 2. Enrichment Stage
# ============================

# 1. RSS 라벨링 데이터
# Producer: 기사별 키워드, 카테고리, 태그 추출 (RSS_PROCESSED에 컬럼 3개 추가)
# Consumer: 외부 제공용 덤프/S3 업로드/캐시 무효화 DAG
RSS_LABELED    = Dataset("mongo://redfin/rss_labeld")    # RSS 피드 라벨링 계층

# 2. RSS 임베딩 데이터
# Question: 임베딩 vs 인덱싱 ?
# Producer: 임베딩 태스크 (기사별 청킹, 일간/주간/월간 트렌드 분석을 위한 RAPTOR용 청킹 + 인덱싱)
# Consumer: 인덱싱 DAG
RSS_EMBEDDED   = Dataset("mongo://redfin/rss_embedded")
