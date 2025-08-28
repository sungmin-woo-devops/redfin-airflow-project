"""
Airflow 유틸리티 모듈
"""

# 절대 임포트로 변경 (상대 임포트 오류 방지)
try:
    from plugins.utils.mongo_connection import test_mongo_connection, MongoConnectionTester
    from plugins.utils.keyword_extractor import KeywordExtractor, extract_keywords, extract_keywords_from_entry
    from plugins.utils.category_classifier import CategoryClassifier, determine_category, extract_and_classify
    from plugins.utils.rss_learning_processor import RSSLearningProcessor, process_rss_for_learning, process_single_entry
    
    __all__ = [
        'test_mongo_connection', 
        'MongoConnectionTester',
        'KeywordExtractor',
        'extract_keywords',
        'extract_keywords_from_entry',
        'CategoryClassifier',
        'determine_category',
        'extract_and_classify',
        'RSSLearningProcessor',
        'process_rss_for_learning',
        'process_single_entry'
    ]
except ImportError:
    # pymongo가 없는 경우 더미 함수 제공
    def test_mongo_connection(*args, **kwargs):
        raise ImportError("pymongo가 설치되지 않았습니다. pip install pymongo")
    
    class MongoConnectionTester:
        def __init__(self, *args, **kwargs):
            raise ImportError("pymongo가 설치되지 않았습니다. pip install pymongo")
    
    class KeywordExtractor:
        def __init__(self, *args, **kwargs):
            raise ImportError("필요한 의존성이 설치되지 않았습니다")
    
    class CategoryClassifier:
        def __init__(self, *args, **kwargs):
            raise ImportError("필요한 의존성이 설치되지 않았습니다")
    
    class RSSLearningProcessor:
        def __init__(self, *args, **kwargs):
            raise ImportError("필요한 의존성이 설치되지 않았습니다")
    
    __all__ = [
        'test_mongo_connection', 
        'MongoConnectionTester',
        'KeywordExtractor',
        'CategoryClassifier',
        'RSSLearningProcessor'
    ]
