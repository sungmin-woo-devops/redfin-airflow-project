#!/usr/bin/env python3
"""
MongoDB 연결 테스트 스크립트
rss_meta_pipeline.py에서 사용할 MongoDB 연결을 테스트합니다.
"""

import os
import sys
from dotenv import load_dotenv
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = "redfin"
MONGO_COLL = "rss_meta"

# Airflow 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_mongodb_connection():
    # ----- 설정 -----
    print("=== MongoDB 연결 테스트 ===")
    try:
        from plugins.utils.mongo import test_mongo_connection
        print(f"MongoDB URI: {MONGO_URI}")
        print(f"데이터베이스: {MONGO_DB}")
        print(f"컬렉션: {MONGO_COLL}")
        
        success = test_mongo_connection(MONGO_URI)
        
        if success:
            print("\n=== 추가 테스트: rss_meta 컬렉션 확인 ===")
            try:
                from pymongo import MongoClient
                client = MongoClient(MONGO_URI)
                db = client[MONGO_DB]
                collection = db[MONGO_COLL]
                
                # 컬렉션 문서 수 확인
                count = collection.count_documents({})
                print(f"[ INFO ] {MONGO_COLL} 컬렉션 문서 수: {count}")
                
                # 최근 문서 1개 확인
                latest_doc = collection.find_one(sort=[("collection_timestamp", -1)])
                if latest_doc:
                    print(f"[ INFO ] 최신 문서 예시:")
                    print(f"   feed_name: {latest_doc.get('feed_name', 'N/A')}")
                    print(f"   data_type: {latest_doc.get('data_type', 'N/A')}")
                    print(f"   timestamp: {latest_doc.get('collection_timestamp', 'N/A')}")
                else:
                    print("[ INFO ] 컬렉션에 문서가 없습니다")
                
                client.close()
                
            except Exception as e:
                print(f"[ FAIL ] 컬렉션 확인 실패: {e}")
        
        return success
        
    except Exception as e:
        print(f"[ FAIL ] 테스트 실행 실패: {e}")
        return False

def test_save_function():
    print("\n=== save_to_mongodb 함수 테스트 ===")
    
    try:
        from plugins.utils.rss_unifier import save_to_mongodb
        
        # ----- 테스트 데이터 -----
        test_docs = [
            {
                'feed_name': 'MongoDB Test Feed',
                'data_type': 'test',
                'test_timestamp': '2025-08-28T10:00:00',
                'message': 'MongoDB 연결 테스트용 문서'
            }
        ]
        
        print(f"테스트 문서 {len(test_docs)}개를 {MONGO_COLL} 컬렉션에 저장 시도...")
        result = save_to_mongodb(
            docs=test_docs,
            mongo_uri=MONGO_URI,
            db_name=MONGO_DB,
            collection_name=MONGO_COLL
        )
        
        if result['success']:
            print(f"[ INFO ] 테스트 저장 성공: {result['inserted_count']}개 문서")
            print(f"   inserted_ids: {result['inserted_ids']}")
            
            # 저장된 문서 확인
            from pymongo import MongoClient
            client = MongoClient(MONGO_URI)
            db = client[MONGO_DB]
            collection = db[MONGO_COLL]
            
            saved_doc = collection.find_one({'data_type': 'test'})
            if saved_doc:
                print(f"[ INFO ] 저장 확인: {saved_doc['message']}")
            
            # 테스트 문서 정리
            collection.delete_many({'data_type': 'test'})
            print("[ INFO ] 테스트 문서 정리 완료")
            
            client.close()
            return True
        else:
            print(f"[ FAIL ] 테스트 저장 실패: {result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"[ FAIL ] save_to_mongodb 테스트 실패: {e}")
        return False

def main():
    print("MongoDB 연결 및 저장 기능 테스트")
    print("=" * 50)
    
    # ----- 테스트 -----
    connection_ok = test_mongodb_connection()
    if connection_ok:
        save_ok = test_save_function()
        
        print("\n" + "=" * 50)
        print("[ INFO ] 테스트 결과 요약:")
        print(f"   MongoDB 연결: {'[SUCCESS] 성공' if connection_ok else '[FAIL] 실패'}")
        print(f"   데이터 저장: {'[SUCCESS] 성공' if save_ok else '[FAIL] 실패'}")
        
        if connection_ok and save_ok:
            print("\n[ INFO ] rss_meta_pipeline.py가 MongoDB에 정상적으로 저장할 수 있습니다!")
        else:
            print("\n[ INFO ] 일부 기능에 문제가 있습니다. 설정을 확인해주세요.")
            
    else:
        print("\n[ FAIL ] MongoDB 연결 실패로 인해 저장 테스트를 건너뜁니다.")
        print("   MongoDB 서버가 실행 중인지 확인해주세요.")

if __name__ == "__main__":
    main()
