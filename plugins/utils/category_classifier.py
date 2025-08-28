"""
카테고리 분류 유틸리티
RSS 피드의 태그와 카테고리 정보를 기반으로 카테고리를 분류하는 기능
"""

from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class CategoryClassifier:
    """카테고리 분류 클래스"""
    
    def __init__(self):
        """카테고리 분류기 초기화"""
        self._init_category_mappings()
    
    def _init_category_mappings(self):
        """카테고리 매핑 규칙 초기화"""
        self.category_keywords = {
            'technology': [
                '기술', 'ai', 'tech', 'software', '개발', '프로그래밍', '코딩',
                '인공지능', '머신러닝', '딥러닝', '알고리즘', '데이터', '클라우드',
                '블록체인', '암호화폐', '비트코인', '이더리움', 'web3', '메타버스',
                'vr', 'ar', 'iot', '5g', '6g', '자율주행', '로봇', '드론'
            ],
            'business': [
                '경제', 'business', '금융', '투자', '기업', '주식', '증권',
                '은행', '보험', '부동산', '재테크', '창업', '스타트업', '벤처',
                'ipo', 'm&a', '합병', '인수', '상장', '투자유치', '펀드',
                '채권', '파생상품', '외환', '원화', '달러', '엔화', '유로'
            ],
            'politics': [
                '정치', 'politics', '정부', '국회', '선거', '대통령', '국무총리',
                '장관', '의원', '여당', '야당', '정책', '법안', '예산',
                '외교', '국방', '통일', '북한', '미국', '중국', '일본',
                'eu', 'un', 'who', 'wto', '국제기구', '협정', '조약'
            ],
            'sports': [
                '스포츠', 'sports', '축구', '야구', '농구', '배구', '골프',
                '테니스', '올림픽', '월드컵', '아시안게임', 'k리그', 'kbo',
                'nba', 'mlb', 'nfl', 'nhl', '선수', '감독', '팀', '리그',
                '경기', '시즌', '포스트시즌', '플레이오프', '챔피언십'
            ],
            'entertainment': [
                '엔터테인먼트', 'entertainment', '영화', '드라마', '음악', '가수',
                '배우', '연예인', '아이돌', 'k-pop', '방송', 'tv', '라디오',
                '유튜브', '넷플릭스', '디즈니플러스', '웨이브', '티빙',
                '콘서트', '공연', '뮤지컬', '연극', '전시', '갤러리'
            ],
            'health': [
                '건강', 'health', '의료', '병원', '의사', '약', '치료',
                '질병', '감염', '백신', '코로나', 'covid', '플루', '독감',
                '암', '당뇨', '고혈압', '심장병', '뇌졸중', '치매',
                '운동', '다이어트', '영양', '비타민', '보충제'
            ],
            'education': [
                '교육', 'education', '학교', '대학', '학생', '교사', '교수',
                '수업', '강의', '온라인', '원격', 'e러닝', 'mooc', 'edx',
                '커리큘럼', '교과서', '시험', '입시', '수능', '토익', '토플',
                '유학', '어학연수', '자격증', '공무원', '고시'
            ]
        }
    
    def classify_from_tags(self, tags: List[str]) -> str:
        """
        태그 리스트에서 카테고리 분류
        
        Args:
            tags: 태그 리스트
            
        Returns:
            str: 분류된 카테고리
        """
        if not tags:
            return 'general'
        
        # 첫 번째 태그를 우선적으로 사용
        first_tag = tags[0].lower()
        
        # 키워드 매칭
        for category, keywords in self.category_keywords.items():
            if any(kw in first_tag for kw in keywords):
                return category
        
        # 매칭되지 않으면 태그 자체를 카테고리로 사용 (길이 제한)
        return first_tag[:20]
    
    def classify_from_entry(self, entry: Dict[str, Any], tags: List[str]) -> str:
        """
        RSS 엔트리에서 카테고리 분류
        
        Args:
            entry: RSS 엔트리 딕셔너리
            tags: 태그 리스트
            
        Returns:
            str: 분류된 카테고리
        """
        # 태그 기반 우선 분류
        if tags:
            return self.classify_from_tags(tags)
        
        # 카테고리 필드 사용
        category = entry.get('category', [])
        if isinstance(category, list) and category:
            return str(category[0])[:20]
        elif isinstance(category, str) and category:
            return category[:20]
        
        return 'general'
    
    def extract_tags_from_entry(self, entry: Dict[str, Any]) -> List[str]:
        """
        RSS 엔트리에서 태그 추출
        
        Args:
            entry: RSS 엔트리 딕셔너리
            
        Returns:
            List[str]: 추출된 태그 리스트
        """
        tags = []
        
        if entry.get('tags') and isinstance(entry['tags'], list):
            for tag in entry['tags']:
                if isinstance(tag, dict) and tag.get('term'):
                    tags.append(tag['term'])
                elif isinstance(tag, str):
                    tags.append(tag)
        
        # 중복 제거, 공백 제거, 최대 15개 태그
        cleaned_tags = []
        for tag in tags:
            cleaned_tag = tag.strip()
            if cleaned_tag and cleaned_tag not in cleaned_tags:
                cleaned_tags.append(cleaned_tag)
        
        return cleaned_tags[:15]
    
    def classify_entry(self, entry: Dict[str, Any]) -> tuple[str, List[str]]:
        """
        RSS 엔트리에서 태그 추출 및 카테고리 분류
        
        Args:
            entry: RSS 엔트리 딕셔너리
            
        Returns:
            tuple[str, List[str]]: (카테고리, 태그리스트)
        """
        tags = self.extract_tags_from_entry(entry)
        category = self.classify_from_entry(entry, tags)
        return category, tags


# 편의 함수들
def determine_category(entry: Dict[str, Any], tags: List[str]) -> str:
    """
    카테고리 결정 (편의 함수)
    
    Args:
        entry: RSS 엔트리 딕셔너리
        tags: 태그 리스트
        
    Returns:
        str: 분류된 카테고리
    """
    classifier = CategoryClassifier()
    return classifier.classify_from_entry(entry, tags)


def extract_and_classify(entry: Dict[str, Any]) -> tuple[str, List[str]]:
    """
    태그 추출 및 카테고리 분류 (편의 함수)
    
    Args:
        entry: RSS 엔트리 딕셔너리
        
    Returns:
        tuple[str, List[str]]: (카테고리, 태그리스트)
    """
    classifier = CategoryClassifier()
    return classifier.classify_entry(entry)
