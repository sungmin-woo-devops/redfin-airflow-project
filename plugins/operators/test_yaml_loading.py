#!/usr/bin/env python3
"""
YAML 기반 피드 소스 로딩 테스트
"""

import os
import sys
import yaml
from pathlib import Path

def test_yaml_loading():
    """YAML 파일 로딩 테스트"""
    print("=== YAML 파일 로딩 테스트 ===")
    
    try:
        # YAML 파일 경로 설정
        current_dir = Path(__file__).parent
        yaml_path = current_dir.parent.parent / "config" / "feeds.yaml"
        
        print(f"YAML 파일 경로: {yaml_path}")
        
        if not yaml_path.exists():
            print(f"YAML 파일이 존재하지 않습니다: {yaml_path}")
            return False
        
        # YAML 파일 로드
        with open(yaml_path, 'r', encoding='utf-8') as f:
            feeds_data = yaml.safe_load(f)
        
        print(f"YAML 파일 로드 성공!")
        
        # 피드 데이터 분석
        feeds = feeds_data.get('feeds', [])
        print(f"총 피드 수: {len(feeds)}")
        
        # 그룹별 분류
        groups = {}
        for feed in feeds:
            group = feed.get('group', 'ungrouped')
            if group not in groups:
                groups[group] = []
            groups[group].append(feed['name'])
        
        print(f"\n사용 가능한 그룹들:")
        for group, feed_names in groups.items():
            print(f"  - {group}: {len(feed_names)}개 피드")
            for name in feed_names[:3]:  # 처음 3개만 표시
                print(f"    * {name}")
            if len(feed_names) > 3:
                print(f"    ... (총 {len(feed_names)}개)")
        
        # 특정 그룹 테스트
        print(f"\n특정 그룹 테스트:")
        frontier_feeds = [f for f in feeds if f.get('group') == 'frontier_lab']
        print(f"  - frontier_lab: {len(frontier_feeds)}개")
        
        legacy_ml_feeds = [f for f in feeds if f.get('group') == 'legacy_ml']
        print(f"  - legacy_ml: {len(legacy_ml_feeds)}개")
        
        # 그룹이 없는 피드들
        ungrouped_feeds = [f for f in feeds if 'group' not in f]
        if ungrouped_feeds:
            print(f"  - ungrouped: {len(ungrouped_feeds)}개")
            for feed in ungrouped_feeds:
                print(f"    * {feed['name']}")
        
        print("\nYAML 로딩 테스트 완료!")
        return True
        
    except Exception as e:
        print(f"YAML 로딩 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_feed_manager_simulation():
    """FeedSourceManager 시뮬레이션 테스트"""
    print("\n=== FeedSourceManager 시뮬레이션 테스트 ===")
    
    try:
        # YAML 파일 경로 설정
        current_dir = Path(__file__).parent
        yaml_path = current_dir.parent.parent / "config" / "feeds.yaml"
        
        # YAML 파일 로드
        with open(yaml_path, 'r', encoding='utf-8') as f:
            feeds_data = yaml.safe_load(f)
        
        feeds = feeds_data.get('feeds', [])
        
        # FeedSourceManager 메서드들 시뮬레이션
        def get_all_feeds():
            return {feed['name']: feed['url'] for feed in feeds}
        
        def get_feeds_by_group(group):
            return {feed['name']: feed['url'] for feed in feeds if feed.get('group') == group}
        
        def get_feeds_by_groups(groups):
            return {feed['name']: feed['url'] for feed in feeds if feed.get('group') in groups}
        
        def get_available_groups():
            return sorted(list(set(feed.get('group', 'ungrouped') for feed in feeds)))
        
        # 테스트 실행
        all_feeds = get_all_feeds()
        print(f"전체 피드 수: {len(all_feeds)}")
        
        frontier_feeds = get_feeds_by_group('frontier_lab')
        print(f"Frontier Lab 피드 수: {len(frontier_feeds)}")
        
        academic_feeds = get_feeds_by_groups(['academia', 'academia_korea'])
        print(f"Academic 피드 수: {len(academic_feeds)}")
        
        available_groups = get_available_groups()
        print(f"사용 가능한 그룹들: {available_groups}")
        
        print("FeedSourceManager 시뮬레이션 테스트 완료!")
        return True
        
    except Exception as e:
        print(f"FeedSourceManager 시뮬레이션 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """메인 테스트 함수"""
    print("YAML 기반 피드 소스 테스트 시작")
    
    # YAML 로딩 테스트
    yaml_success = test_yaml_loading()
    
    # FeedSourceManager 시뮬레이션 테스트
    manager_success = test_feed_manager_simulation()
    
    # 결과 요약
    print("\n" + "="*50)
    print("테스트 결과 요약:")
    print(f"YAML 로딩: {'성공' if yaml_success else '실패'}")
    print(f"FeedSourceManager 시뮬레이션: {'성공' if manager_success else '실패'}")
    
    if yaml_success and manager_success:
        print("\n모든 테스트가 성공했습니다!")
        return 0
    else:
        print("\n일부 테스트가 실패했습니다.")
        return 1

if __name__ == "__main__":
    exit(main())
