#!/usr/bin/env python3
"""
통합 스키마 변환 테스트 스크립트
"""

import os
import sys
import json
from pathlib import Path

# 현재 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def test_unified_schema_conversion():
    """통합 스키마 변환 테스트"""
    print("=== 통합 스키마 변환 테스트 ===")
    
    try:
        from rss_unifier import parse_feed_data, process_rss_files, create_batch_envelope
        
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
        
        print("통합 스키마 변환 결과:")
        print(f"  - _id: {unified_doc['_id']}")
        print(f"  - schema_version: {unified_doc['schema_version']}")
        print(f"  - status: {unified_doc['status']}")
        print(f"  - source.feed_name: {unified_doc['source']['feed_name']}")
        print(f"  - source.feed_url: {unified_doc['source']['feed_url']}")
        print(f"  - entries count: {len(unified_doc['rss']['entries'])}")
        
        # 필수 필드 확인
        required_fields = [
            '_id', 'schema_version', 'status', 'run', 'source', 
            'partition', 'audit', 'rss'
        ]
        
        missing_fields = []
        for field in required_fields:
            if field not in unified_doc:
                missing_fields.append(field)
        
        if missing_fields:
            print(f"누락된 필드: {missing_fields}")
            return False
        else:
            print("모든 필수 필드가 존재합니다")
        
        # 배치 봉투 생성 테스트
        batch_doc = create_batch_envelope([unified_doc])
        print(f"  - 배치 봉투 생성: {batch_doc['_id']}")
        print(f"  - 배치 아이템 수: {len(batch_doc['items'])}")
        
        print("통합 스키마 변환 테스트 성공!")
        return True
        
    except Exception as e:
        print(f"통합 스키마 변환 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_file_processing():
    """파일 처리 테스트"""
    print("\n=== 파일 처리 테스트 ===")
    
    try:
        from rss_unifier import process_rss_files
        
        # 테스트용 임시 디렉토리 생성
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            test_input_dir = os.path.join(temp_dir, "input")
            test_output_dir = os.path.join(temp_dir, "output")
            
            os.makedirs(test_input_dir, exist_ok=True)
            os.makedirs(test_output_dir, exist_ok=True)
            
            # 테스트용 JSON 파일 생성
            test_feed_data = {
                "feed": {
                    "title": "Test Feed",
                    "link": "https://test.com/feed.xml"
                },
                "entries": [
                    {
                        "title": "Test Entry",
                        "link": "https://test.com/entry1"
                    }
                ],
                "parsed_at": "2025-08-28T00:00:00Z"
            }
            
            test_file_path = os.path.join(test_input_dir, "test_feed.json")
            with open(test_file_path, 'w', encoding='utf-8') as f:
                json.dump(test_feed_data, f, ensure_ascii=False, indent=2)
            
            # 파일 처리 테스트
            unified_docs = process_rss_files(test_input_dir, test_output_dir)
            
            print(f"  - 처리된 파일 수: {len(unified_docs)}")
            print(f"  - 출력 디렉토리: {test_output_dir}")
            
            # 출력 파일 확인
            output_files = list(Path(test_output_dir).glob("*.json"))
            print(f"  - 생성된 출력 파일 수: {len(output_files)}")
            
            if unified_docs and output_files:
                print("파일 처리 테스트 성공!")
                return True
            else:
                print("파일 처리 테스트 실패: 파일이 생성되지 않았습니다")
                return False
                
    except Exception as e:
        print(f"파일 처리 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_schema_validation():
    """스키마 검증 테스트"""
    print("\n=== 스키마 검증 테스트 ===")
    
    try:
        # 통합 스키마 파일 로드
        schema_path = Path(__file__).parent.parent.parent / "config" / "unified_rss_schema.json"
        
        if not schema_path.exists():
            print(f"통합 스키마 파일을 찾을 수 없습니다: {schema_path}")
            return False
        
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)
        
        print(f"통합 스키마 파일 로드 성공: {schema_path}")
        
        # 스키마 구조 확인
        required_schema_fields = [
            '_id', 'schema_version', 'status', 'run', 'source', 
            'partition', 'audit', 'rss'
        ]
        
        for field in required_schema_fields:
            if field in schema:
                print(f"  {field}: 존재함")
            else:
                print(f"  {field}: 누락됨")
                return False
        
        print("스키마 검증 테스트 성공!")
        return True
        
    except Exception as e:
        print(f"스키마 검증 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """메인 테스트 함수"""
    print("통합 스키마 변환 테스트 시작")
    
    # 통합 스키마 변환 테스트
    conversion_success = test_unified_schema_conversion()
    
    # 파일 처리 테스트
    file_success = test_file_processing()
    
    # 스키마 검증 테스트
    schema_success = test_schema_validation()
    
    # 결과 요약
    print("\n" + "="*50)
    print("테스트 결과 요약:")
    print(f"통합 스키마 변환: {'성공' if conversion_success else '실패'}")
    print(f"파일 처리: {'성공' if file_success else '실패'}")
    print(f"스키마 검증: {'성공' if schema_success else '실패'}")
    
    if conversion_success and file_success and schema_success:
        print("\n모든 테스트가 성공했습니다!")
        return 0
    else:
        print("\n일부 테스트가 실패했습니다.")
        return 1

if __name__ == "__main__":
    exit(main())
