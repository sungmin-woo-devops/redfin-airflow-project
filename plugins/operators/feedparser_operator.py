# -*- coding: utf-8 -*-
from __future__ import annotations
import feedparser
import json
import os
from typing import Any, List, Dict, Optional
from datetime import datetime
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.context import Context


class RSSFeedParserOperator(BaseOperator):
    """
    RSS 피드를 가져와서 JSON 파일로 저장하는 Airflow Operator
    
    Args:
        feed_sources (Dict[str, str]): RSS 피드 소스 딕셔너리 (이름: URL)
        output_dir (str): 출력 디렉토리 경로 (Jinja 템플릿 지원)
        save_schema (bool): 스키마 파일도 함께 저장할지 여부
        min_entries (int): 최소 엔트리 수 (이보다 적으면 경고)
        **kwargs: BaseOperator의 기본 파라미터들
    """
    
    # Jinja 템플릿을 지원하는 필드들 명시
    template_fields = ["output_dir"]
    
    @apply_defaults
    def __init__(
        self,
        feed_sources: Dict[str, str],
        output_dir: str,
        save_schema: bool = True,
        min_entries: int = 1,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.feed_sources = feed_sources
        self.output_dir = output_dir
        self.save_schema = save_schema
        self.min_entries = min_entries

    def execute(self, context: Context) -> Dict[str, Any]:
        """Operator 실행 메서드"""
        self.log.info(f"Starting RSS feed parsing for {len(self.feed_sources)} sources")
        
        # Jinja 템플릿이 이미 Airflow에 의해 렌더링됨 (template_fields 덕분)
        rendered_output_dir = self.output_dir
        self.log.info(f"Using output directory: {rendered_output_dir}")
        
        # 출력 디렉토리 생성
        os.makedirs(rendered_output_dir, exist_ok=True)
        
        results = {
            "processed_feeds": 0,
            "successful_feeds": 0,
            "failed_feeds": [],
            "empty_feeds": [],
            "saved_files": []
        }
        
        # 각 피드 소스에 대해 처리
        for feed_name, feed_url in self.feed_sources.items():
            try:
                self.log.info(f"Processing feed: {feed_name} ({feed_url})")
                
                # RSS 피드 가져오기
                feed = feedparser.parse(feed_url)
                
                # 피드 상태 확인
                if feed.bozo:
                    self.log.warning(f"Feed {feed_name} has parsing issues: {feed.bozo_exception}")
                
                # 엔트리 수 확인
                entry_count = len(feed.entries)
                if entry_count < self.min_entries:
                    self.log.warning(f"Feed {feed_name} has only {entry_count} entries (min: {self.min_entries})")
                    results["empty_feeds"].append({
                        "feed_name": feed_name,
                        "feed_url": feed_url,
                        "entry_count": entry_count
                    })
                
                # 파일명 생성
                filename = self._generate_filename(feed_name)
                filepath = os.path.join(rendered_output_dir, filename)
                
                # 피드 데이터를 딕셔너리로 변환
                feed_dict = self._convert_feed_to_dict(feed)
                
                # JSON 파일로 저장
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(feed_dict, f, ensure_ascii=False, indent=2)
                
                results["saved_files"].append(filepath)
                results["successful_feeds"] += 1
                
                # 스키마 파일 저장 (옵션)
                if self.save_schema:
                    schema_filepath = filepath.replace('.json', '_schema.json')
                    schema = self._generate_schema(feed_dict)
                    with open(schema_filepath, "w", encoding="utf-8") as f:
                        json.dump(schema, f, ensure_ascii=False, indent=2)
                    results["saved_files"].append(schema_filepath)
                
                self.log.info(f"Successfully saved {feed_name} to {filepath} ({entry_count} entries)")
                
            except Exception as e:
                error_msg = f"Failed to process feed {feed_name}: {str(e)}"
                self.log.error(error_msg)
                results["failed_feeds"].append({
                    "feed_name": feed_name,
                    "feed_url": feed_url,
                    "error": str(e)
                })
            
            results["processed_feeds"] += 1
        
        # 결과 로깅
        self.log.info(f"RSS feed parsing completed. "
                     f"Processed: {results['processed_feeds']}, "
                     f"Successful: {results['successful_feeds']}, "
                     f"Failed: {len(results['failed_feeds'])}, "
                     f"Empty: {len(results['empty_feeds'])}")
        
        return results

    def _generate_filename(self, feed_name: str) -> str:
        """피드 이름을 기반으로 파일명 생성"""
        normalized = feed_name.lower()
        normalized = normalized.replace(" ", "_")
        normalized = normalized.replace("/", "_")
        normalized = normalized.replace(".", "_")
        normalized = normalized.replace("-", "_")
        normalized = normalized.replace("__", "_")
        return f"{normalized}.json"

    def _convert_feed_to_dict(self, feed: Any) -> Dict[str, Any]:
        """feedparser의 FeedParserDict를 일반 딕셔너리로 변환"""
        return {
            "feed": dict(feed.feed),
            "entries": [dict(entry) for entry in feed.entries],
            "parsed_at": datetime.now().isoformat(),
            "total_entries": len(feed.entries),
            "feed_status": {
                "bozo": feed.bozo,
                "bozo_exception": str(feed.bozo_exception) if feed.bozo_exception else None,
                "version": feed.version,
                "encoding": feed.encoding
            }
        }

    def _generate_schema(self, data: Any, max_depth: int = 3) -> Dict[str, Any]:
        """데이터 구조를 분석하여 스키마 생성"""
        def _analyze_structure(obj: Any, depth: int = 0) -> Dict[str, Any]:
            if depth >= max_depth:
                return {"type": "truncated", "depth": depth}
            
            if isinstance(obj, dict):
                schema = {"type": "object", "properties": {}}
                for key, value in obj.items():
                    schema["properties"][key] = _analyze_structure(value, depth + 1)
                return schema
            elif isinstance(obj, list):
                if obj:
                    schema = {"type": "array", "items": _analyze_structure(obj[0], depth + 1)}
                else:
                    schema = {"type": "array", "items": {"type": "unknown"}}
                return schema
            else:
                return {"type": type(obj).__name__}
        
        return _analyze_structure(data)


class RSSFeedFetcherOperator(BaseOperator):
    """
    특정 RSS 피드 목록을 가져오는 간단한 Operator
    
    Args:
        feed_urls (List[str]): RSS 피드 URL 목록
        output_dir (str): 출력 디렉토리 경로 (Jinja 템플릿 지원)
        min_entries (int): 최소 엔트리 수 (이보다 적으면 경고)
        **kwargs: BaseOperator의 기본 파라미터들
    """
    
    # Jinja 템플릿을 지원하는 필드들 명시
    template_fields = ["output_dir"]
    
    @apply_defaults
    def __init__(
        self,
        feed_urls: List[str],
        output_dir: str,
        min_entries: int = 1,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.feed_urls = feed_urls
        self.output_dir = output_dir
        self.min_entries = min_entries

    def execute(self, context: Context) -> Dict[str, Any]:
        """Operator 실행 메서드"""
        self.log.info(f"Fetching {len(self.feed_urls)} RSS feeds")
        
        # Jinja 템플릿이 이미 Airflow에 의해 렌더링됨 (template_fields 덕분)
        rendered_output_dir = self.output_dir
        self.log.info(f"Using output directory: {rendered_output_dir}")
        
        # 출력 디렉토리 생성
        os.makedirs(rendered_output_dir, exist_ok=True)
        
        results = {
            "processed_feeds": 0,
            "successful_feeds": 0,
            "failed_feeds": [],
            "empty_feeds": [],
            "saved_files": []
        }
        
        for i, url in enumerate(self.feed_urls):
            try:
                self.log.info(f"Fetching feed {i+1}/{len(self.feed_urls)}: {url}")
                
                # RSS 피드 가져오기
                feed = feedparser.parse(url)
                
                # 엔트리 수 확인
                entry_count = len(feed.entries)
                if entry_count < self.min_entries:
                    self.log.warning(f"Feed {url} has only {entry_count} entries (min: {self.min_entries})")
                    results["empty_feeds"].append({
                        "feed_url": url,
                        "entry_count": entry_count
                    })
                
                # 파일명 생성 (URL 기반)
                filename = self._generate_filename_from_url(url)
                filepath = os.path.join(rendered_output_dir, filename)
                
                # 피드 데이터를 딕셔너리로 변환
                feed_dict = self._convert_feed_to_dict(feed)
                
                # JSON 파일로 저장
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(feed_dict, f, ensure_ascii=False, indent=2)
                
                results["saved_files"].append(filepath)
                results["successful_feeds"] += 1
                
                self.log.info(f"Successfully saved feed to {filepath} ({entry_count} entries)")
                
            except Exception as e:
                error_msg = f"Failed to fetch feed {url}: {str(e)}"
                self.log.error(error_msg)
                results["failed_feeds"].append({
                    "feed_url": url,
                    "error": str(e)
                })
            
            results["processed_feeds"] += 1
        
        self.log.info(f"RSS feed fetching completed. "
                     f"Processed: {results['processed_feeds']}, "
                     f"Successful: {results['successful_feeds']}, "
                     f"Failed: {len(results['failed_feeds'])}, "
                     f"Empty: {len(results['empty_feeds'])}")
        
        return results

    def _generate_filename_from_url(self, url: str) -> str:
        """URL을 기반으로 파일명 생성"""
        filename = url.replace("https://", "").replace("http://", "")
        filename = filename.replace("/", "_").replace(".", "_")
        filename = filename.replace(":", "_")
        return f"{filename}.json"

    def _convert_feed_to_dict(self, feed: Any) -> Dict[str, Any]:
        """feedparser의 FeedParserDict를 일반 딕셔너리로 변환"""
        return {
            "feed": dict(feed.feed),
            "entries": [dict(entry) for entry in feed.entries],
            "parsed_at": datetime.now().isoformat(),
            "total_entries": len(feed.entries),
            "feed_status": {
                "bozo": feed.bozo,
                "bozo_exception": str(feed.bozo_exception) if feed.bozo_exception else None,
                "version": feed.version,
                "encoding": feed.encoding
            }
        }

