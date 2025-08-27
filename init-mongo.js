// MongoDB 초기화 스크립트
// redfin 데이터베이스 및 컬렉션 생성
// 일반 사용자 추가

// redfin 데이터베이스 생성 및 사용
db = db.getSiblingDB('redfin');

// 컬렉션들 생성
db.createCollection('rss_meta');
db.createCollection('rss_extracted');
db.createCollection('rss_analyzed');
db.createCollection('rss_public');

// 일반 사용자 생성 (redfin/Redfin7620!)
db.createUser({
  user: 'redfin',
  pwd: 'Redfin7620!',
  roles: [
    {
      role: 'readWrite',
      db: 'redfin'
    }
  ]
});

// 컬렉션에 인덱스 추가 (선택사항)
db.rss_meta.createIndex({ "url": 1 }, { unique: true });
db.rss_extracted.createIndex({ "url": 1 });
db.rss_analyzed.createIndex({ "url": 1 });
db.rss_public.createIndex({ "url": 1 });

print('MongoDB 초기화 완료:');
print('- redfin 데이터베이스 생성됨');
print('- 컬렉션 생성됨: rss_meta, rss_extracted, rss_analyzed, rss_public');
print('- 일반 사용자 생성됨: redfin/Redfin7620!');
