"""
RSS 학습 데이터 처리 유틸리티
RSS 피드에서 추천 알고리즘용 학습 데이터를 추출하고 처리하는 기능
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging

from .keyword_extractor import KeywordExtractor
from .category_classifier import CategoryClassifier

logger = logging.getLogger(__name__)


class RSSLearningProcessor:
    """RSS 학습 데이터 처리 클래스"""
    
    def __init__(self):
        """처리기 초기화"""
        self.keyword_extractor = KeywordExtractor()
        self.category_classifier = CategoryClassifier()
    
    def extract_news_id(self, entry: Dict[str, Any], fallback_id: str) -> str:
        """
        뉴스 ID 추출
        
        Args:
            entry: RSS 엔트리 딕셔너리
            fallback_id: 대체 ID
            
        Returns:
            str: 추출된 뉴스 ID
        """
        news_id = (entry.get('id') or 
                  entry.get('guid') or 
                  entry.get('link') or 
                  fallback_id)
        
        return str(news_id)[:100]  # 길이 제한
    
    def extract_title(self, entry: Dict[str, Any]) -> str:
        """
        제목 추출
        
        Args:
            entry: RSS 엔트리 딕셔너리
            
        Returns:
            str: 추출된 제목
        """
        title = entry.get('title', '')
        if isinstance(title, dict):
            title = title.get('value', '')
        
        return str(title)[:300]  # 길이 제한
    
    def extract_abstract(self, entry: Dict[str, Any]) -> str:
        """
        요약 추출
        
        Args:
            entry: RSS 엔트리 딕셔너리
            
        Returns:
            str: 추출된 요약
        """
        candidates = [
            entry.get('summary'),
            entry.get('summary_detail', {}).get('value') if isinstance(entry.get('summary_detail'), dict) else None,
            entry.get('description'),
            entry.get('content', [{}])[0].get('value') if entry.get('content') else None
        ]
        
        for candidate in candidates:
            if candidate and isinstance(candidate, str) and len(candidate.strip()) > 10:
                return candidate.strip()[:800]  # 길이 제한
        
        return ""
    
    def process_entry(self, entry: Dict[str, Any], source_name: str, entry_index: int) -> Optional[Dict[str, Any]]:
        """
        RSS 엔트리 처리
        
        Args:
            entry: RSS 엔트리 딕셔너리
            source_name: 소스 이름
            entry_index: 엔트리 인덱스
            
        Returns:
            Optional[Dict[str, Any]]: 처리된 학습 데이터 또는 None
        """
        try:
            # 뉴스 ID 추출
            news_id = self.extract_news_id(entry, f"entry_{entry_index}")
            
            # 제목 추출
            title = self.extract_title(entry)
            
            # 기본 검증
            if not news_id or not title or len(title.strip()) < 5:
                logger.warning(f"유효하지 않은 엔트리: news_id={news_id}, title={title}")
                return None
            
            # 요약 추출
            abstract = self.extract_abstract(entry)
            
            # 키워드 추출
            keywords = self.keyword_extractor.extract_from_entry(entry)
            
            # 카테고리 및 태그 분류
            category, tags = self.category_classifier.classify_entry(entry)
            
            # 학습 데이터 구조 생성
            learning_record = {
                "News ID": news_id,
                "Title": title,
                "Abstract": abstract,
                "keywords": json.dumps(keywords, ensure_ascii=False),
                "category": category,
                "tags": json.dumps(tags, ensure_ascii=False)
            }
            
            # 메타데이터 추가
            learning_record["_metadata"] = {
                "source": source_name,
                "published": entry.get('published', ''),
                "link": entry.get('link', ''),
                "processed_at": datetime.now().isoformat()
            }
            
            return learning_record
            
        except Exception as e:
            logger.error(f"엔트리 처리 오류: {e}")
            return None
    
    def process_feed_document(self, doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        RSS 피드 문서 처리
        
        Args:
            doc: RSS 피드 문서
            
        Returns:
            List[Dict[str, Any]]: 처리된 학습 데이터 리스트
        """
        entries = doc.get('entries', [])
        if not entries:
            return []
        
        feed_info = doc.get('feed', {})
        source_name = feed_info.get('title', 'unknown')
        
        processed_records = []
        
        for i, entry in enumerate(entries):
            processed_record = self.process_entry(entry, source_name, i)
            if processed_record:
                processed_records.append(processed_record)
        
        return processed_records
    
    def save_to_jsonl(self, records: List[Dict[str, Any]], output_file: str) -> int:
        """
        학습 데이터를 JSONL 파일로 저장
        
        Args:
            records: 학습 데이터 리스트
            output_file: 출력 파일 경로
            
        Returns:
            int: 저장된 레코드 수
        """
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        saved_count = 0
        with open(output_file, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
                saved_count += 1
        
        logger.info(f"JSONL 파일 저장 완료: {saved_count}개 레코드 -> {output_file}")
        return saved_count
    
    def process_collection(self, collection, output_file: str) -> Dict[str, Any]:
        """
        MongoDB 컬렉션 전체 처리
        
        Args:
            collection: MongoDB 컬렉션
            output_file: 출력 파일 경로
            
        Returns:
            Dict[str, Any]: 처리 결과 통계
        """
        total_docs = collection.count_documents({})
        logger.info(f"처리할 문서 수: {total_docs}")
        
        all_records = []
        processed_docs = 0
        error_count = 0
        
        cursor = collection.find()
        for doc in cursor:
            try:
                records = self.process_feed_document(doc)
                all_records.extend(records)
                processed_docs += 1
                
                if processed_docs % 10 == 0:
                    logger.info(f"문서 처리 진행: {processed_docs}/{total_docs}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"문서 처리 오류: {e}")
                continue
        
        # JSONL 파일로 저장
        saved_count = self.save_to_jsonl(all_records, output_file)
        
        return {
            "total_docs": total_docs,
            "processed_docs": processed_docs,
            "error_count": error_count,
            "extracted_records": len(all_records),
            "saved_records": saved_count,
            "output_file": output_file
        }


# 편의 함수들
def process_rss_for_learning(collection, output_file: str) -> Dict[str, Any]:
    """
    RSS 컬렉션을 학습 데이터로 처리 (편의 함수)
    
    Args:
        collection: MongoDB 컬렉션
        output_file: 출력 파일 경로
        
    Returns:
        Dict[str, Any]: 처리 결과 통계
    """
    processor = RSSLearningProcessor()
    return processor.process_collection(collection, output_file)


def process_single_entry(entry: Dict[str, Any], source_name: str = "unknown") -> Optional[Dict[str, Any]]:
    """
    단일 RSS 엔트리 처리 (편의 함수)
    
    Args:
        entry: RSS 엔트리 딕셔너리
        source_name: 소스 이름
        
    Returns:
        Optional[Dict[str, Any]]: 처리된 학습 데이터 또는 None
    """
    processor = RSSLearningProcessor()
    return processor.process_entry(entry, source_name, 0)
