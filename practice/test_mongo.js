// JavaScript (Node.js) MongoDB 연결 테스트

const { MongoClient } = require('mongodb');

const uri = 'mongodb://redfin:Redfin7620!@localhost:27017/redfin?authSource=redfin';

async function testConnection() {
    const client = new MongoClient(uri, { 
        serverSelectionTimeoutMS: 3000 
    });
    
    try {
        console.log('🔌 MongoDB 연결 시도 중...');
        
        await client.connect();
        await client.db('admin').command({ ping: 1 });
        
        console.log('✅ MongoDB 연결 성공!');
        
        const db = client.db('redfin');
        const collections = await db.listCollections().toArray();
        console.log('📋 컬렉션:', collections.map(c => c.name));
        
    } catch (error) {
        console.error('❌ 연결 실패:', error.message);
    } finally {
        await client.close();
    }
}

testConnection();
