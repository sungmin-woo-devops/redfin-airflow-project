#!/usr/bin/env python3
"""
가장 간단한 MongoDB 연결 테스트
"""

from pymongo import MongoClient

# 연결 및 테스트
try:
    client = MongoClient("mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin", 
                        serverSelectionTimeoutMS=3000)
    client.admin.command('ping')
    print("✅ MongoDB 연결 성공!")
    print(f"📋 컬렉션: {client.redfin.list_collection_names()}")
except Exception as e:
    print(f"❌ 연결 실패: {e}")
finally:
    client.close()
