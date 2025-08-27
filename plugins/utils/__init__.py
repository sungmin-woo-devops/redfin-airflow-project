"""
Airflow 유틸리티 모듈
"""

# 절대 임포트로 변경 (상대 임포트 오류 방지)
try:
    from plugins.utils.mongo_connection import test_mongo_connection, MongoConnectionTester
    __all__ = [
        'test_mongo_connection', 
        'MongoConnectionTester'
    ]
except ImportError:
    # pymongo가 없는 경우 더미 함수 제공
    def test_mongo_connection(*args, **kwargs):
        raise ImportError("pymongo가 설치되지 않았습니다. pip install pymongo")
    
    class MongoConnectionTester:
        def __init__(self, *args, **kwargs):
            raise ImportError("pymongo가 설치되지 않았습니다. pip install pymongo")
    
    __all__ = ['test_mongo_connection', 'MongoConnectionTester']
