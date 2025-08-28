#!/usr/bin/env python3
"""
Jinja 템플릿 렌더링 테스트
"""

import os
import sys
from datetime import datetime

# 현재 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def test_jinja_template_rendering():
    """Jinja 템플릿 렌더링 테스트"""
    print("=== Jinja 템플릿 렌더링 테스트 ===")
    
    try:
        from airflow.utils.template import render_template
        
        # 테스트용 context 생성
        context = {
            'ds_nodash': '20250828',
            'ts_nodash': '20250828T000000',
            'ds': '2025-08-28',
            'ts': '2025-08-28T00:00:00'
        }
        
        # 테스트할 템플릿들
        templates = [
            "/opt/airflow/data/rss/raw/{{ ds_nodash }}/all",
            "/opt/airflow/data/rss/raw/{{ ds_nodash }}/ml",
            "/opt/airflow/data/rss/raw/{{ ds_nodash }}/ai",
            "rss_meta__{{ ds_nodash }}__{{ ts_nodash }}.jsonl"
        ]
        
        print("템플릿 렌더링 결과:")
        for template in templates:
            rendered = render_template(template, context)
            print(f"  {template} -> {rendered}")
        
        print("Jinja 템플릿 렌더링 테스트 성공!")
        return True
        
    except Exception as e:
        print(f"Jinja 템플릿 렌더링 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_operator_template_rendering():
    """Operator 내부 템플릿 렌더링 시뮬레이션"""
    print("\n=== Operator 템플릿 렌더링 시뮬레이션 ===")
    
    try:
        from airflow.utils.template import render_template
        
        # RSSFeedParserOperator의 execute 메서드 시뮬레이션
        def simulate_operator_execute(output_dir_template, context):
            # Jinja 템플릿 렌더링
            rendered_output_dir = render_template(output_dir_template, context)
            print(f"  원본: {output_dir_template}")
            print(f"  렌더링: {rendered_output_dir}")
            return rendered_output_dir
        
        # 테스트용 context
        context = {
            'ds_nodash': '20250828',
            'ts_nodash': '20250828T000000',
            'ds': '2025-08-28',
            'ts': '2025-08-28T00:00:00'
        }
        
        # 테스트할 output_dir 템플릿들
        output_dirs = [
            "/opt/airflow/data/rss/raw/{{ ds_nodash }}/all",
            "/opt/airflow/data/rss/raw/{{ ds_nodash }}/ml",
            "/opt/airflow/data/rss/raw/{{ ds_nodash }}/ai",
            "/opt/airflow/data/rss/raw/{{ ds_nodash }}/frontier"
        ]
        
        print("Operator 템플릿 렌더링 시뮬레이션:")
        for output_dir in output_dirs:
            rendered = simulate_operator_execute(output_dir, context)
            
            # 디렉토리 생성 테스트
            try:
                os.makedirs(rendered, exist_ok=True)
                print(f"  디렉토리 생성 성공: {rendered}")
            except Exception as e:
                print(f"  디렉토리 생성 실패: {e}")
        
        print("Operator 템플릿 렌더링 시뮬레이션 성공!")
        return True
        
    except Exception as e:
        print(f"Operator 템플릿 렌더링 시뮬레이션 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """메인 테스트 함수"""
    print("Jinja 템플릿 렌더링 테스트 시작")
    
    # Jinja 템플릿 렌더링 테스트
    jinja_success = test_jinja_template_rendering()
    
    # Operator 템플릿 렌더링 시뮬레이션
    operator_success = test_operator_template_rendering()
    
    # 결과 요약
    print("\n" + "="*50)
    print("테스트 결과 요약:")
    print(f"Jinja 템플릿 렌더링: {'성공' if jinja_success else '실패'}")
    print(f"Operator 템플릿 렌더링: {'성공' if operator_success else '실패'}")
    
    if jinja_success and operator_success:
        print("\n모든 테스트가 성공했습니다!")
        return 0
    else:
        print("\n일부 테스트가 실패했습니다.")
        return 1

if __name__ == "__main__":
    exit(main())
