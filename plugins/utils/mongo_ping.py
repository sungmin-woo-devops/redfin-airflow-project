from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

def test_connection(mongo_uri: str, timeout_ms: int = 5000) -> bool:
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=timeout_ms)
        client.admin.command("ping")
        db = client.get_default_database() or client["redfin"]
        collections = db.list_collection_names()
        server_info = client.server_info()
        print("✅ MongoDB 연결 성공!")
        print(f"📋 컬렉션: {collections}")
        print(f"🗄️  MongoDB 버전: {server_info.get('version')}")
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
