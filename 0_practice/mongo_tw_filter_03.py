# Pendulum 기반 고급 시간 윈도우 필터링
import pendulum
from typing import List, Dict, Any, Optional
from pymongo.collection import Collection

class AdvancedPendulumFilter:
    """고급 Pendulum 시간 필터"""
    
    def __init__(self, collection: Collection):
        self.collection = collection
    
    def filter_by_window(
        self,
        window_type: str,
        count: int = 1,
        timezone: str = 'UTC',
        reference_time: Optional[pendulum.DateTime] = None
    ) -> List[Dict[str, Any]]:
        """
        유연한 시간 윈도우 필터링
        
        Args:
            window_type: 'hours', 'days', 'weeks', 'months'
            count: 기간 수 (예: 3일이면 count=3, window_type='days')
            timezone: 시간대
            reference_time: 기준 시간 (None이면 현재 시간)
        """
        # 기준 시간 설정
        ref_time = reference_time or pendulum.now(timezone)
        
        # 동적 시간 계산
        from_time = ref_time.subtract(**{window_type: count})
        
        return self._execute_time_filter(from_time, ref_time)
    
    def filter_business_hours(
        self,
        days_back: int = 7,
        start_hour: int = 9,
        end_hour: int = 18,
        timezone: str = 'Asia/Seoul'
    ) -> List[Dict[str, Any]]:
        """업무시간 내 기사만 필터링"""
        
        now = pendulum.now(timezone)
        from_date = now.subtract(days=days_back)
        
        # 업무시간 조건 생성
        business_condition = {
            'published_at': {
                '$gte': from_date.to_datetime_string(),
                '$lt': now.to_datetime_string()
            },
            '$expr': {
                '$and': [
                    {'$gte': [{'$hour': '$published_at'}, start_hour]},
                    {'$lt': [{'$hour': '$published_at'}, end_hour]},
                    {'$not': {'$in': [{'$dayOfWeek': '$published_at'}, [1, 7]]}}  # 주말 제외
                ]
            },
            'stage': 'embedded'
        }
        
        return list(self.collection.aggregate([{'$match': business_condition}]))
    
    def filter_by_period(
        self,
        period_type: str,
        timezone: str = 'UTC'
    ) -> List[Dict[str, Any]]:
        """
        특정 기간 단위로 필터링
        
        Args:
            period_type: 'today', 'yesterday', 'this_week', 'last_week', 'this_month'
        """
        now = pendulum.now(timezone)
        
        periods = {
            'today': (now.start_of('day'), now.end_of('day')),
            'yesterday': (
                now.subtract(days=1).start_of('day'),
                now.subtract(days=1).end_of('day')
            ),
            'this_week': (now.start_of('week'), now.end_of('week')),
            'last_week': (
                now.subtract(weeks=1).start_of('week'),
                now.subtract(weeks=1).end_of('week')
            ),
            'this_month': (now.start_of('month'), now.end_of('month'))
        }
        
        if period_type not in periods:
            raise ValueError(f"지원하지 않는 기간: {period_type}")
        
        from_time, to_time = periods[period_type]
        return self._execute_time_filter(from_time, to_time)
    
    def _execute_time_filter(
        self,
        from_time: pendulum.DateTime,
        to_time: pendulum.DateTime
    ) -> List[Dict[str, Any]]:
        """시간 필터 실행"""
        
        match_condition = {
            'published_at': {
                '$gte': from_time.to_datetime_string(),
                '$lt': to_time.to_datetime_string()
            },
            'stage': 'embedded'
        }
        
        return list(self.collection.aggregate([{'$match': match_condition}]))
    
    def get_time_info(
        self,
        from_time: pendulum.DateTime,
        to_time: pendulum.DateTime
    ) -> Dict[str, str]:
        """시간 범위 정보 반환"""
        
        return {
            'from': from_time.to_iso8601_string(),
            'to': to_time.to_iso8601_string(),
            'duration': to_time.diff(from_time).in_words(),
            'from_local': from_time.format('YYYY-MM-DD HH:mm:ss zz'),
            'to_local': to_time.format('YYYY-MM-DD HH:mm:ss zz')
        }

# 사용 예시
if __name__ == "__main__":
    # 기본 사용
    filter_service = AdvancedPendulumFilter(db.articles)

    # 지난 3일간 기사
    articles_3d = filter_service.filter_by_window('days', 3)

    # 어제 기사만
    yesterday_articles = filter_service.filter_by_period('yesterday')

    # 지난 주 업무시간 기사 (한국 시간)
    business_articles = filter_service.filter_business_hours(
        days_back=7,
        timezone='Asia/Seoul'
    )

    # 시간 정보 확인
    now = pendulum.now('Asia/Seoul')
    week_ago = now.subtract(weeks=1)
    time_info = filter_service.get_time_info(week_ago, now)
    print(f"기간: {time_info['duration']}")  # "1주일"