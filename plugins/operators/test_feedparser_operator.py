#!/usr/bin/env python3
"""
RSS Feed Parser Operator 테스트 스크립트
기존 get_rss_feeds.py와 동일한 기능을 Operator로 테스트
"""

import os
import sys
from datetime import datetime

# 현재 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    from feedparser_operator import RSSFeedParserOperator, RSSFeedFetcherOperator
    from feed_sources import (
        RSS_FEED_SOURCES, 
        DEFAULT_FEED_URLS, 
        ML_FEEDS,
        FRONTIER_LAB_FEEDS,
        ACADEMIA_FEEDS
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Trying alternative import path...")
    
    # Airflow 프로젝트 루트 기준으로 상대 경로 설정
    airflow_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    sys.path.insert(0, airflow_root)
    
    from plugins.operators.feedparser_operator import RSSFeedParserOperator, RSSFeedFetcherOperator
    from plugins.operators.feed_sources import (
        RSS_FEED_SOURCES, 
        DEFAULT_FEED_URLS, 
        ML_FEEDS,
        FRONTIER_LAB_FEEDS,
        ACADEMIA_FEEDS
    )

def test_rss_feed_parser_operator():
    """RSSFeedParserOperator 테스트"""
    print("=== RSSFeedParserOperator 테스트 ===")
    
    # 테스트용 출력 디렉토리
    test_output_dir = "/tmp/rss_test_output"
    os.makedirs(test_output_dir, exist_ok=True)
    
    # Operator 인스턴스 생성 (실제 Airflow context 없이)
    operator = RSSFeedParserOperator(
        task_id="test_fetch_feeds",
        feed_sources=ML_FEEDS,  # ML 피드만 테스트
        output_dir=test_output_dir,
        save_schema=True,
        min_entries=1,  # 테스트용으로 낮은 기준 설정
    )
    
    # execute 메서드 직접 호출
    try:
        results = operator.execute(context={})
        print(f"테스트 성공!")
        print(f"처리된 피드: {results['processed_feeds']}")
        print(f"성공한 피드: {results['successful_feeds']}")
        print(f"실패한 피드: {len(results['failed_feeds'])}")
        print(f"빈 피드: {len(results['empty_feeds'])}")
        print(f"저장된 파일: {len(results['saved_files'])}")
        
        if results['failed_feeds']:
            print("\n실패한 피드들:")
            for failed in results['failed_feeds']:
                print(f"  - {failed['feed_name']}: {failed['error']}")
        
        if results['empty_feeds']:
            print("\n빈 피드들:")
            for empty in results['empty_feeds']:
                print(f"  - {empty['feed_name']}: {empty['entry_count']} entries")
        
        print(f"\n저장된 파일들:")
        for filepath in results['saved_files']:
            print(f"  - {filepath}")
            
    except Exception as e:
        print(f"테스트 실패: {e}")
        return False
    
    return True

def test_rss_feed_fetcher_operator():
    """RSSFeedFetcherOperator 테스트"""
    print("\n=== RSSFeedFetcherOperator 테스트 ===")
    
    # 테스트용 출력 디렉토리
    test_output_dir = "/tmp/rss_test_fetcher_output"
    os.makedirs(test_output_dir, exist_ok=True)
    
    # 테스트용 URL 목록 (일부만)
    test_urls = DEFAULT_FEED_URLS[:3]  # 처음 3개만 테스트
    
    # Operator 인스턴스 생성
    operator = RSSFeedFetcherOperator(
        task_id="test_fetch_urls",
        feed_urls=test_urls,
        output_dir=test_output_dir,
        min_entries=1,  # 테스트용으로 낮은 기준 설정
    )
    
    # execute 메서드 직접 호출
    try:
        results = operator.execute(context={})
        print(f"테스트 성공!")
        print(f"처리된 피드: {results['processed_feeds']}")
        print(f"성공한 피드: {results['successful_feeds']}")
        print(f"실패한 피드: {len(results['failed_feeds'])}")
        print(f"빈 피드: {len(results['empty_feeds'])}")
        print(f"저장된 파일: {len(results['saved_files'])}")
        
        if results['failed_feeds']:
            print("\n실패한 피드들:")
            for failed in results['failed_feeds']:
                print(f"  - {failed['feed_url']}: {failed['error']}")
        
        if results['empty_feeds']:
            print("\n빈 피드들:")
            for empty in results['empty_feeds']:
                print(f"  - {empty['feed_url']}: {empty['entry_count']} entries")
        
        print(f"\n저장된 파일들:")
        for filepath in results['saved_files']:
            print(f"  - {filepath}")
            
    except Exception as e:
        print(f"테스트 실패: {e}")
        return False
    
    return True

def test_feed_sources():
    """피드 소스 정의 테스트"""
    print("\n=== 피드 소스 정의 테스트 ===")
    
    print(f"전체 피드 수: {len(RSS_FEED_SOURCES)}")
    print(f"ML 피드 수: {len(ML_FEEDS)}")
    print(f"Frontier Lab 피드 수: {len(FRONTIER_LAB_FEEDS)}")
    print(f"Academia 피드 수: {len(ACADEMIA_FEEDS)}")
    print(f"기본 피드 URL 수: {len(DEFAULT_FEED_URLS)}")
    
    print("\nML 피드 목록:")
    for name, url in ML_FEEDS.items():
        print(f"  - {name}: {url}")
    
    print("\nFrontier Lab 피드 목록:")
    for name, url in FRONTIER_LAB_FEEDS.items():
        print(f"  - {name}: {url}")
    
    print("\n기본 피드 URL 목록:")
    for i, url in enumerate(DEFAULT_FEED_URLS[:5]):  # 처음 5개만
        print(f"  {i+1}. {url}")

def test_yaml_loading():
    """YAML 파일 로딩 테스트"""
    print("\n=== YAML 파일 로딩 테스트 ===")
    
    try:
        from feed_sources import feed_manager
        
        print(f"YAML 파일 경로: {feed_manager.yaml_path}")
        print(f"사용 가능한 그룹들: {feed_manager.get_available_groups()}")
        
        # 특정 그룹 테스트
        frontier_feeds = feed_manager.get_feeds_by_group('frontier_lab')
        print(f"Frontier Lab 피드 수: {len(frontier_feeds)}")
        
        # 여러 그룹 테스트
        academic_feeds = feed_manager.get_feeds_by_groups(['academia', 'academia_korea'])
        print(f"Academic 피드 수: {len(academic_feeds)}")
        
        print("YAML 로딩 테스트 성공!")
        return True
        
    except Exception as e:
        print(f"YAML 로딩 테스트 실패: {e}")
        return False

def main():
    """메인 테스트 함수"""
    print("RSS Feed Parser Operator 테스트 시작")
    print(f"테스트 시간: {datetime.now()}")
    
    # 피드 소스 테스트
    test_feed_sources()
    
    # YAML 로딩 테스트
    yaml_success = test_yaml_loading()
    
    # Operator 테스트
    success1 = test_rss_feed_parser_operator()
    success2 = test_rss_feed_fetcher_operator()
    
    # 결과 요약
    print("\n" + "="*50)
    print("테스트 결과 요약:")
    print(f"YAML 로딩: {'성공' if yaml_success else '실패'}")
    print(f"RSSFeedParserOperator: {'성공' if success1 else '실패'}")
    print(f"RSSFeedFetcherOperator: {'성공' if success2 else '실패'}")
    
    if yaml_success and success1 and success2:
        print("\n모든 테스트가 성공했습니다!")
        return 0
    else:
        print("\n일부 테스트가 실패했습니다.")
        return 1

if __name__ == "__main__":
    exit(main())
