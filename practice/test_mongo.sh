#!/bin/bash
# Bash MongoDB 연결 테스트

MONGO_URI='mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin'

echo "🔌 MongoDB 연결 테스트..."

# mongosh로 연결 테스트
if mongosh "$MONGO_URI" --eval 'db.adminCommand("ping")' --quiet > /dev/null 2>&1; then
    echo "✅ MongoDB 연결 성공!"
    
    # 컬렉션 목록 출력
    echo "📋 컬렉션 목록:"
    mongosh "$MONGO_URI" --eval 'db.listCollectionNames().forEach(name => print("  - " + name))' --quiet
    
    # 데이터 개수 확인 (예: news 컬렉션)
    if mongosh "$MONGO_URI" --eval 'db.news.countDocuments({})' --quiet > /dev/null 2>&1; then
        COUNT=$(mongosh "$MONGO_URI" --eval 'print(db.news.countDocuments({}))' --quiet 2>/dev/null)
        echo "📊 news 컬렉션 문서 수: $COUNT"
    fi
else
    echo "❌ MongoDB 연결 실패"
    exit 1
fi
