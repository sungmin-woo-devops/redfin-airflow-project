#!/usr/bin/env python3
"""
최소한의 MongoDB 연결 테스트 코드
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# MongoDB 연결 정보
MONGO_URI = "mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin"

def test_connection():
    """MongoDB 연결 테스트"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ MongoDB 연결 성공!")
        
        # 데이터베이스 정보
        db = client.redfin
        collections = db.list_collection_names()
        print(f"📋 컬렉션: {collections}")
        
        # 서버 정보
        server_info = client.server_info()
        print(f"🗄️  MongoDB 버전: {server_info['version']}")
        
        client.close()
        return True
        
    except ConnectionFailure:
        print("❌ MongoDB 서버에 연결할 수 없습니다")
        return False
    except ServerSelectionTimeoutError:
        print("❌ MongoDB 서버 선택 타임아웃")
        return False
    except Exception as e:
        print(f"❌ 예기치 않은 오류: {e}")
        return False

if __name__ == "__main__":
    test_connection()
