# 기본적인 시간 윈도우 필터링
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pymongo import MongoClient
from pymongo.collection import Collection

class TimeWindowFilter:
    """시간 윈도우 기반 기사 필터링"""
    
    TIME_WINDOWS = {
        '12h': timedelta(hours=12),
        '1d': timedelta(days=1), 
        '1w': timedelta(weeks=1),
        '1mo': timedelta(days=30)
    }
    
    def __init__(self, collection: Collection):
        self.collection = collection
    
    def filter_recent_articles(
        self, 
        window_type: str = '1w',
        stage: str = 'embedded',
        additional_filters: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        최근 기간 내 기사 필터링
        
        Args:
            window_type: 시간 윈도우 ('12h', '1d', '1w', '1mo')
            stage: 처리 단계
            additional_filters: 추가 필터 조건
        """
        if window_type not in self.TIME_WINDOWS:
            raise ValueError(f"지원하지 않는 시간 윈도우: {window_type}")
        
        # 시간 계산
        now = datetime.utcnow()
        from_time = now - self.TIME_WINDOWS[window_type]
        
        # 기본 매치 조건
        match_condition = {
            'published_at': {
                '$gte': from_time,
                '$lt': now
            },
            'stage': stage
        }
        
        # 추가 필터 병합
        if additional_filters:
            match_condition.update(additional_filters)
        
        # 집계 파이프라인
        pipeline = [{'$match': match_condition}]
        
        return list(self.collection.aggregate(pipeline))

    def filter_with_prefilter(
        self,
        window_type: str = '1w',
    ) -> List[Dict[str, Any]]:
        """year_month 프리필터 저굥ㅇ된 최적화 버전"""
        
        now = datetime.utcnow()
        from_time = now - self.TIME_WINDOWS[window_type]

        # year_month 범위 계산 (프리필터용)
        from_year_month = from_time.strftime('%Y-%m')
        to_year_month = now.strftime('%Y-%m')
    
        pipeline = [
            # 1단계: year_month 프리필터
            {
                '$match': {
                    'year_month': {
                        '$gte': from_year_month,
                        '$lte': to_year_month
                    }
                }
            },
            # 2단계: 정밀한 시간 필터
            {
                '$match': {
                    'published_at': {
                        '$gte': from_time,
                        '$lt': now
                    },
                    'stage': 'embedded'
                }
            }
        ]
        
        return list(self.collection.aggregate(pipeline))

    def get_time_range_info(self, window_type: str) -> Dict[str, datetime]:
        """시간 범위 정보 반환"""
        now = datetime.utcnow()
        from_time = now - self.TIME_WINDOWS[window_type]
        
        return {
            'from': from_time,
            'to': now,
            'duration': self.TIME_WINDOWS[window_type]
        }


# 사용 예시
def main():
    client = MongoClient('mongodb://localhost:27017/')
    db = client.redfin
    
    filter_service = TimeWindowFilter(db.articles)
    
    # 지난 7일간 기사 조회
    recent_articles = filter_service.filter_recent_articles('1w')
    print(f"지난 7일간 기사 수: {len(recent_articles)}")
    
    # 시간 범위 정보
    time_info = filter_service.get_time_range_info('1w')
    print(f"조회 범위: {time_info['from']} ~ {time_info['to']}")