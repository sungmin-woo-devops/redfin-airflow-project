#!/usr/bin/env python3
"""
배치 봉투 스키마 테스트 스크립트
"""

import os
import sys
import json
from pathlib import Path

# 현재 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def test_batch_envelope_schema():
    """배치 봉투 스키마 테스트"""
    print("=== 배치 봉투 스키마 테스트 ===")
    
    try:
        from rss_unifier import create_batch_envelope
        
        # 테스트용 통합 문서들 생성
        test_unified_docs = [
            {
                "_id": "test_id_1",
                "schema_version": "1.0.0",
                "status": "meta",
                "source": {
                    "feed_id": "hash1",
                    "feed_url": "https://example1.com/feed",
                    "feed_name": "Test Feed 1"
                },
                "rss": {
                    "feed": {"title": "Test Feed 1"},
                    "entries": [{"title": "Entry 1"}, {"title": "Entry 2"}]
                }
            },
            {
                "_id": "test_id_2",
                "schema_version": "1.0.0",
                "status": "meta",
                "source": {
                    "feed_id": "hash2",
                    "feed_url": "https://example2.com/feed",
                    "feed_name": "Test Feed 2"
                },
                "rss": {
                    "feed": {"title": "Test Feed 2"},
                    "entries": [{"title": "Entry 3"}]
                }
            }
        ]
        
        # 배치 봉투 생성
        batch_doc = create_batch_envelope(test_unified_docs, batch_type="rss_collection")
        
        print("배치 봉투 생성 결과:")
        print(f"  - _id: {batch_doc['_id']}")
        print(f"  - schema_version: {batch_doc['schema_version']}")
        print(f"  - status: {batch_doc['status']}")
        print(f"  - source.batch_type: {batch_doc['source']['batch_type']}")
        print(f"  - source.batch_name: {batch_doc['source']['batch_name']}")
        print(f"  - source.feed_count: {batch_doc['source']['feed_count']}")
        print(f"  - source.total_entries: {batch_doc['source']['total_entries']}")
        print(f"  - items count: {len(batch_doc['items'])}")
        print(f"  - statistics.total_feeds: {batch_doc['statistics']['total_feeds']}")
        print(f"  - statistics.avg_entries_per_feed: {batch_doc['statistics']['avg_entries_per_feed']}")
        
        # 필수 필드 확인
        required_fields = [
            '_id', 'schema_version', 'status', 'run', 'source', 
            'partition', 'audit', 'items', 'statistics', 'errors'
        ]
        
        missing_fields = []
        for field in required_fields:
            if field not in batch_doc:
                missing_fields.append(field)
        
        if missing_fields:
            print(f"누락된 필드: {missing_fields}")
            return False
        else:
            print("모든 필수 필드가 존재합니다")
        
        # 통계 검증
        expected_total_entries = 3  # Entry 1, Entry 2, Entry 3
        actual_total_entries = batch_doc['source']['total_entries']
        
        if actual_total_entries == expected_total_entries:
            print(f"통계 검증 성공: 총 엔트리 수 {actual_total_entries}")
        else:
            print(f"통계 검증 실패: 예상 {expected_total_entries}, 실제 {actual_total_entries}")
            return False
        
        print("배치 봉투 스키마 테스트 성공!")
        return True
        
    except Exception as e:
        print(f"배치 봉투 스키마 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_batch_envelope_file():
    """배치 봉투 파일 스키마 검증"""
    print("\n=== 배치 봉투 파일 스키마 검증 ===")
    
    try:
        # 배치 봉투 스키마 파일 로드
        schema_path = Path(__file__).parent.parent.parent / "config" / "batch_envelope_schema.json"
        
        if not schema_path.exists():
            print(f"배치 봉투 스키마 파일을 찾을 수 없습니다: {schema_path}")
            return False
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        
        print(f"배치 봉투 스키마 파일 로드 성공: {schema_path}")
        
        # 스키마 구조 확인
        required_schema_fields = [
            '_id', 'schema_version', 'status', 'run', 'source', 
            'partition', 'audit', 'items', 'statistics', 'errors'
        ]
        
        for field in required_schema_fields:
            if field in schema:
                print(f"  {field}: 존재함")
            else:
                print(f"  {field}: 누락됨")
                return False
        
        # source 필드 구조 확인
        source_fields = ['batch_type', 'batch_name', 'feed_count', 'total_entries']
        for field in source_fields:
            if field in schema.get('source', {}):
                print(f"    source.{field}: 존재함")
            else:
                print(f"    source.{field}: 누락됨")
                return False
        
        # statistics 필드 구조 확인
        stats_fields = ['total_feeds', 'successful_feeds', 'failed_feeds', 'empty_feeds', 'total_entries', 'avg_entries_per_feed', 'processing_time_seconds']
        for field in stats_fields:
            if field in schema.get('statistics', {}):
                print(f"    statistics.{field}: 존재함")
            else:
                print(f"    statistics.{field}: 누락됨")
                return False
        
        print("배치 봉투 파일 스키마 검증 성공!")
        return True
        
    except Exception as e:
        print(f"배치 봉투 파일 스키마 검증 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_unified_schema_compatibility():
    """통합 스키마와 배치 봉투 호환성 테스트"""
    print("\n=== 통합 스키마와 배치 봉투 호환성 테스트 ===")
    
    try:
        from rss_unifier import parse_feed_data, create_batch_envelope
        
        # 테스트용 RSS 피드 데이터
        test_feed_data = {
            "feed": {
                "title": "Test RSS Feed",
                "link": "https://example.com/feed.xml",
                "subtitle": "Test feed description",
                "updated": "2025-08-28T00:00:00Z",
                "language": "en"
            },
            "entries": [
                {
                    "title": "Test Article 1",
                    "link": "https://example.com/article1",
                    "published": "2025-08-28T00:00:00Z",
                    "summary": "This is a test article",
                    "author": "Test Author"
                },
                {
                    "title": "Test Article 2",
                    "link": "https://example.com/article2",
                    "published": "2025-08-28T01:00:00Z",
                    "summary": "This is another test article",
                    "author": "Test Author 2"
                }
            ],
            "parsed_at": "2025-08-28T02:00:00Z"
        }
        
        # 통합 스키마로 변환
        unified_doc = parse_feed_data(test_feed_data)
        
        # 배치 봉투 생성
        batch_doc = create_batch_envelope([unified_doc], batch_type="rss_collection")
        
        print("호환성 테스트 결과:")
        print(f"  - 통합 문서 _id: {unified_doc['_id']}")
        print(f"  - 배치 봉투 _id: {batch_doc['_id']}")
        print(f"  - 배치 아이템 수: {len(batch_doc['items'])}")
        print(f"  - 배치 아이템 _id: {batch_doc['items'][0]['_id']}")
        
        # 호환성 검증
        if unified_doc['_id'] == batch_doc['items'][0]['_id']:
            print("  ✅ 통합 문서와 배치 아이템 ID 일치")
        else:
            print("  ❌ 통합 문서와 배치 아이템 ID 불일치")
            return False
        
        if len(batch_doc['items']) == 1:
            print("  ✅ 배치 아이템 수 정확")
        else:
            print("  ❌ 배치 아이템 수 부정확")
            return False
        
        print("통합 스키마와 배치 봉투 호환성 테스트 성공!")
        return True
        
    except Exception as e:
        print(f"통합 스키마와 배치 봉투 호환성 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """메인 테스트 함수"""
    print("배치 봉투 스키마 테스트 시작")
    
    # 배치 봉투 스키마 테스트
    schema_success = test_batch_envelope_schema()
    
    # 배치 봉투 파일 스키마 검증
    file_success = test_batch_envelope_file()
    
    # 호환성 테스트
    compatibility_success = test_unified_schema_compatibility()
    
    # 결과 요약
    print("\n" + "="*50)
    print("테스트 결과 요약:")
    print(f"배치 봉투 스키마: {'성공' if schema_success else '실패'}")
    print(f"배치 봉투 파일 스키마: {'성공' if file_success else '실패'}")
    print(f"호환성 테스트: {'성공' if compatibility_success else '실패'}")
    
    if schema_success and file_success and compatibility_success:
        print("\n모든 테스트가 성공했습니다!")
        return 0
    else:
        print("\n일부 테스트가 실패했습니다.")
        return 1

if __name__ == "__main__":
    exit(main())
