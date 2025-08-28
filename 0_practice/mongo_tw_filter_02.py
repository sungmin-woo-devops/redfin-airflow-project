# Pendulum 기반 MongoDB 시간 윈도우 필터링
import pendulum
from typing import List, Dict, Any, Optional
from pymongo.collection import Collection

class PendulumTimeWindowFilter:
    """PendulumTimeWindowFilter 필터링"""
    
    TIME_WINDOWS = {
        '12h': {'hours': 12},
        '1d': {'days': 1},
        '1w': {'weeks': 1}, 
        '1mo': {'months': 1}  # pendulum은 정확한 월 계산 지원
    }

    def __init__(self, collection: Collection):
        self.collection = collection

    def filter_recent_articles(
        self,
        window_type: str = '1w',
        stage: str = 'embedded',
        timezone: str = 'UTC',
    ) -> List[Dict[str, Any]]:
        """
        Pendulum 기반 시간 윈도우 필터링

        Args:
            window_type: 시간 윈도우
            stage: 처리 단계
            timezone: 시간대 (기본: UTC)
        """
        if window_type not in self.TIME_WINDOWS:
            raise ValueError(f"지원하지 않는 시간 윈도우: {window_type}")
        
        # Pendulum으로 시간 계산
        now = pendulum.now(timezone)
        from_time = now.subtract(**self.TIME_WINDOWS[window_type])
        
        # MongoDB용 datetime 변환
        match_condition = {
            'published_at': {
                '$gte': from_time.to_datetime_string(),
                '$lt': now.to_datetime_string()
            },
            'stage': stage
        }
        
        pipeline = [{'$match': match_condition}]
        
        return list(self.collection.aggregate(pipeline))
