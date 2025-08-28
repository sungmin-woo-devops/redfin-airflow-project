"""
MongoDB 연결 테스트 유틸리티
Airflow DAG에서 사용할 수 있는 MongoDB 연결 확인 함수들
"""

import os
from typing import Dict, Any, Optional

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    # 더미 클래스들 정의
    class MongoClient:
        def __init__(self, *args, **kwargs):
            raise ImportError("pymongo가 설치되지 않았습니다")
    
    class ConnectionFailure(Exception):
        pass
    
    class ServerSelectionTimeoutError(Exception):
        pass

try:
    from airflow.utils.log.logging_mixin import LoggingMixin
except ImportError:
    # Airflow가 없는 환경에서는 기본 로깅 사용
    import logging
    class LoggingMixin:
        @property
        def log(self):
            return logging.getLogger(__name__)


class MongoConnectionTester(LoggingMixin):
    """MongoDB 연결 테스터 클래스"""
    
    def __init__(self, mongo_uri: Optional[str] = None):
        """
        Args:
            mongo_uri: MongoDB 연결 URI. None이면 환경변수에서 읽음
        """
        if not PYMONGO_AVAILABLE:
            raise ImportError("pymongo가 설치되지 않았습니다. Docker 컨테이너에서 실행해주세요.")
        
        self.mongo_uri = mongo_uri or os.getenv(
            'MONGO_CONNECTION_STRING', 
            'mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin'
        )
    
    def test_connection(self, timeout_ms: int = 5000) -> Dict[str, Any]:
        """
        MongoDB 연결 테스트
        
        Args:
            timeout_ms: 연결 타임아웃 (밀리초)
            
        Returns:
            Dict: 연결 결과 정보
        """
        result = {
            'success': False,
            'error': None,
            'server_info': None,
            'collections': [],
            'db_name': None
        }
        
        try:
            self.log.info("MongoDB 연결 테스트 시작...")
            
            # 클라이언트 생성
            client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=timeout_ms)
            
            # 연결 테스트
            client.admin.command('ping')
            
            # 서버 정보 수집
            server_info = client.server_info()
            result['server_info'] = {
                'version': server_info.get('version'),
                'build_info': server_info.get('buildEnvironment', {}).get('target_arch')
            }
            
            # 데이터베이스 정보
            db_name = self.mongo_uri.split('/')[-1].split('?')[0]
            result['db_name'] = db_name
            
            db = client[db_name]
            result['collections'] = db.list_collection_names()
            
            result['success'] = True
            self.log.info("✅ MongoDB 연결 성공!")
            self.log.info(f"🗄️  MongoDB 버전: {server_info.get('version')}")
            self.log.info(f"📋 컬렉션: {result['collections']}")
            
            client.close()
            
        except ConnectionFailure as e:
            result['error'] = f"연결 실패: {str(e)}"
            self.log.error(f"❌ {result['error']}")
            
        except ServerSelectionTimeoutError as e:
            result['error'] = f"서버 선택 타임아웃: {str(e)}"
            self.log.error(f"❌ {result['error']}")
            
        except Exception as e:
            result['error'] = f"예기치 않은 오류: {str(e)}"
            self.log.error(f"❌ {result['error']}")
        
        return result
    
    def _get_client(self):
        """
        MongoDB 클라이언트 반환
        
        Returns:
            MongoClient: MongoDB 클라이언트
        """
        return MongoClient(self.mongo_uri, serverSelectionTimeoutMS=10000)


def test_mongo_connection(mongo_uri: Optional[str] = None, timeout_ms: int = 5000) -> bool:
    """
    간단한 MongoDB 연결 테스트 함수
    
    Args:
        mongo_uri: MongoDB 연결 URI
        timeout_ms: 타임아웃 (밀리초)
        
    Returns:
        bool: 연결 성공 여부
    """
    tester = MongoConnectionTester(mongo_uri)
    result = tester.test_connection(timeout_ms)
    return result['success']


# Airflow Task에서 사용할 수 있는 함수
def mongo_health_check(**context) -> str:
    """
    Airflow Task에서 사용할 MongoDB 헬스체크 함수
    
    Returns:
        str: 결과 메시지
    """
    tester = MongoConnectionTester()
    result = tester.test_connection()
    
    if result['success']:
        return f"MongoDB 연결 성공 - 버전: {result['server_info']['version']}, 컬렉션: {len(result['collections'])}개"
    else:
        raise Exception(f"MongoDB 연결 실패: {result['error']}")


if __name__ == "__main__":
    # 직접 실행 시 테스트
    tester = MongoConnectionTester()
    result = tester.test_connection()
    
    if result['success']:
        print("✅ 연결 테스트 성공!")
    else:
        print(f"❌ 연결 테스트 실패: {result['error']}")
        exit(1)
