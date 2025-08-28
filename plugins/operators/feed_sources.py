"""
YAML 기반 RSS 피드 소스 관리 모듈
feeds.yaml 파일을 읽어서 피드 소스를 제공
"""

import yaml
import os
from typing import Dict, List, Any, Optional
from pathlib import Path


class FeedSourceManager:
    """YAML 파일에서 피드 소스를 관리하는 클래스"""
    
    def __init__(self, yaml_path: str = None):
        """
        Args:
            yaml_path: feeds.yaml 파일 경로 (기본값: config/feeds.yaml)
        """
        if yaml_path is None:
            # Airflow 프로젝트 루트 기준으로 상대 경로 설정
            airflow_root = Path(__file__).parent.parent.parent
            yaml_path = airflow_root / "config" / "feeds.yaml"
        
        self.yaml_path = Path(yaml_path)
        self._feeds_data = None
        self._load_feeds()
    
    def _load_feeds(self) -> None:
        """YAML 파일에서 피드 데이터 로드"""
        if not self.yaml_path.exists():
            raise FileNotFoundError(f"Feeds YAML file not found: {self.yaml_path}")
        
        with open(self.yaml_path, 'r', encoding='utf-8') as f:
            self._feeds_data = yaml.safe_load(f)
    
    def get_all_feeds(self) -> Dict[str, str]:
        """모든 피드를 딕셔너리 형태로 반환 (이름: URL)"""
        feeds_dict = {}
        for feed in self._feeds_data.get('feeds', []):
            feeds_dict[feed['name']] = feed['url']
        return feeds_dict
    
    def get_feeds_by_group(self, group: str) -> Dict[str, str]:
        """특정 그룹의 피드들을 딕셔너리 형태로 반환"""
        feeds_dict = {}
        for feed in self._feeds_data.get('feeds', []):
            if 'group' in feed and feed['group'] == group:
                feeds_dict[feed['name']] = feed['url']
        return feeds_dict
    
    def get_feeds_by_groups(self, groups: List[str]) -> Dict[str, str]:
        """여러 그룹의 피드들을 딕셔너리 형태로 반환"""
        feeds_dict = {}
        for feed in self._feeds_data.get('feeds', []):
            if 'group' in feed and feed['group'] in groups:
                feeds_dict[feed['name']] = feed['url']
        return feeds_dict
    
    def get_feed_urls_by_group(self, group: str) -> List[str]:
        """특정 그룹의 피드 URL 목록 반환"""
        urls = []
        for feed in self._feeds_data.get('feeds', []):
            if 'group' in feed and feed['group'] == group:
                urls.append(feed['url'])
        return urls
    
    def get_feed_urls_by_groups(self, groups: List[str]) -> List[str]:
        """여러 그룹의 피드 URL 목록 반환"""
        urls = []
        for feed in self._feeds_data.get('feeds', []):
            if 'group' in feed and feed['group'] in groups:
                urls.append(feed['url'])
        return urls
    
    def get_available_groups(self) -> List[str]:
        """사용 가능한 모든 그룹 목록 반환"""
        groups = set()
        for feed in self._feeds_data.get('feeds', []):
            if 'group' in feed:
                groups.add(feed['group'])
        return sorted(list(groups))
    
    def get_feed_info(self, feed_name: str) -> Optional[Dict[str, Any]]:
        """특정 피드의 상세 정보 반환"""
        for feed in self._feeds_data.get('feeds', []):
            if feed['name'] == feed_name:
                return feed
        return None
    
    def get_feeds_without_group(self) -> Dict[str, str]:
        """그룹이 지정되지 않은 피드들을 반환"""
        feeds_dict = {}
        for feed in self._feeds_data.get('feeds', []):
            if 'group' not in feed:
                feeds_dict[feed['name']] = feed['url']
        return feeds_dict


# 전역 인스턴스 생성
feed_manager = FeedSourceManager()

# 기존 코드와의 호환성을 위한 별칭들
def get_all_feeds() -> Dict[str, str]:
    """모든 피드 반환 (기존 RSS_FEED_SOURCES와 동일한 형태)"""
    return feed_manager.get_all_feeds()

def get_feeds_by_group(group: str) -> Dict[str, str]:
    """특정 그룹의 피드 반환"""
    return feed_manager.get_feeds_by_group(group)

def get_feeds_by_groups(groups: List[str]) -> Dict[str, str]:
    """여러 그룹의 피드 반환"""
    return feed_manager.get_feeds_by_groups(groups)

def get_feed_urls_by_group(group: str) -> List[str]:
    """특정 그룹의 피드 URL 목록 반환"""
    return feed_manager.get_feed_urls_by_group(group)

def get_feed_urls_by_groups(groups: List[str]) -> List[str]:
    """여러 그룹의 피드 URL 목록 반환"""
    return feed_manager.get_feed_urls_by_groups(groups)

# 기존 코드와의 호환성을 위한 상수들
RSS_FEED_SOURCES = get_all_feeds()

# 카테고리별 피드 그룹 (기존과 동일한 형태 유지)
ML_FEEDS = get_feeds_by_groups(['legacy_ml', 'arxiv'])
AI_FEEDS = get_feeds_by_groups(['legacy_ai', 'frontier_lab'])
RL_FEEDS = get_feeds_by_group('legacy_rl')
DS_FEEDS = get_feeds_by_group('legacy_ds')

# 새로운 그룹들
FRONTIER_LAB_FEEDS = get_feeds_by_group('frontier_lab')
FRONTIER_LAB_KOREA_FEEDS = get_feeds_by_group('frontier_lab_korea')
ACADEMIA_FEEDS = get_feeds_by_groups(['academia', 'academia_korea'])
CLOUD_AI_FEEDS = get_feeds_by_group('cloud_ai')
ARXIV_FEEDS = get_feeds_by_group('arxiv')
MEDIA_FEEDS = get_feeds_by_group('media')
INDUSTRY_FEEDS = get_feeds_by_group('industry')
ECOSYSTEM_FEEDS = get_feeds_by_group('ecosystem')
OPEN_RESEARCH_FEEDS = get_feeds_by_group('open_research')

# 기본 피드 목록 (기존과 동일한 형태 유지)
DEFAULT_FEED_URLS = [
    # Frontier Labs
    "https://openai.com/blog/rss.xml",
    "https://deepmind.google/blog/rss.xml",
    "https://www.microsoft.com/en-us/research/feed/",
    "https://ai.googleblog.com/feeds/posts/default",
    
    # Legacy ML
    "https://blog.statsbot.co/rss",
    "https://machinelearningmastery.com/feed/",
    "https://eng.uber.com/feed/",
    "https://aws.amazon.com/blogs/machine-learning/feed/",
    "https://export.arxiv.org/rss/cs.LG",
    "https://export.arxiv.org/rss/stat.ML",
    "https://www.reddit.com/r/MachineLearning/.rss",
    "https://mlinproduction.com/feed/",
    "http://jalammar.github.io/feed.xml",
    "http://www.jmlr.org/jmlr.xml",
    "https://distill.pub/rss.xml",
    "https://www.inference.vc/feed/",
    
    # Legacy AI
    "https://danieltakeshi.github.io/feed.xml",
    "http://www.fast.ai/feeds/blog.xml",
    "https://www.aitrends.com/feed/",
    "https://aiweirdness.com/feed",
    "https://bair.berkeley.edu/blog/feed.xml",
    "https://becominghuman.ai/feed",
    "http://news.mit.edu/rss/topic/artificial-intelligence2",
    "https://blogs.nvidia.com/feed/",
]
