import feedparser
import json
from typing import Any, List
import pandas as pd

feed_sources = {
    # Machine Learning
    "Cube_dev": "https://blog.statsbot.co/rss",
    "Machine_Learning_Mastery": "https://machinelearningmastery.com/feed/",
    "ML_Uber_Engineering": "https://eng.uber.com/feed/",
    "AWS_Machine_Learning": "https://aws.amazon.com/blogs/machine-learning/feed/",
    "arXiv_cs.ML": "https://export.arxiv.org/rss/cs.LG",
    "arXiv_stat.ML": "https://export.arxiv.org/rss/stat.ML",
    "ML_Reddit": "https://www.reddit.com/r/MachineLearning/.rss",
    "ML_in_production": "https://mlinproduction.com/feed/",
    "Jay_Alammar_blog": "http://jalammar.github.io/feed.xml",
    "JMLR_recent_papers": "http://www.jmlr.org/jmlr.xml",
    "Distill_blog": "https://distill.pub/rss.xml",
    "inFERENCe_blog": "https://www.inference.vc/feed/",
    
    # Artifical Intelligence
    "AI_Trends": "https://www.aitrends.com/feed/",
    "AI_Weirdness": "https://aiweirdness.com/feed",
    "BAIR_blog": "https://bair.berkeley.edu/blog/feed.xml",
    "Becoming_Human": "https://becominghuman.ai/feed",
    "MIT_AI_News": "http://news.mit.edu/rss/topic/artificial-intelligence2",
    "NVIDIA_AI_Blog": "https://blogs.nvidia.com/feed/",
    "David_Stutz_AI_Paper_Review": "https://davidstutz.de/feed/",
    "AI_Reddit": "https://www.reddit.com/r/artificial/.rss",
    "Reddit_NN_DL": "https://www.reddit.com/r/neuralnetworks/.rss",
    "ScienceDaily_AI": "https://www.sciencedaily.com/rss/computers_ai.xml",
    "Daniel_Takeshi_Blog": "https://danieltakeshi.github.io/feed.xml",
    "DeepMind_blog": "https://deepmind.google/blog/rss.xml",
    "Vitalab_LitReview": "https://vitalab.github.io/feed.xml",
    "Andrej_Karpathy": "https://karpathy.medium.com/feed",
    "OpenAI_Blog": "https://openai.com/blog/rss.xml",
    "Microsoft_Research": "https://www.microsoft.com/en-us/research/feed/",
    "Google_AI_Blog": "https://ai.googleblog.com/feeds/posts/default",
    "FastAI_Blog": "http://www.fast.ai/feeds/blog.xml",

    # Reinforcement Learning
    "RL_Reddit": "https://www.reddit.com/r/reinforcementlearning/.rss",
    "RL_Weekly": "https://www.getrevue.co/profile/seungjaeryanlee?format=rss",
    "RL_Paper_Review": "https://dtransposed.github.io/feed.xml",

    # Data Science
    "Data_Science_Central": "https://www.datasciencecentral.com/feed/",
    "John_Cook_Blog": "https://www.johndcook.com/blog/feed/",
}

def fetch_rss_feeds(feedlist: List[str]) -> List[Any]:
    parsed_feeds = []
    for url in feedlist:
        feed = feedparser.parse(url)
        parsed_feeds.append(feed)
    return parsed_feeds

def save_feed_to_file(feed: Any, filename: str) -> None:
    # feedparser의 FeedParserDict는 바로 직렬화가 안 되므로 dict로 변환
    feed_as_dict = {
        "feed": dict(feed.feed),
        "entries": [dict(entry) for entry in feed.entries]
    }
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(feed_as_dict, f, ensure_ascii=False, indent=2)

def name_json(url: str) -> str:
    # feed_sources의 key값을 lowercase 및 정규화하여 파일명 생성
    for key, value in feed_sources.items():
        if value == url:
            normalized = key.lower()
            normalized = normalized.replace(" ", "_")
            normalized = normalized.replace("/", "_")
            normalized = normalized.replace(".", "_")
            normalized = normalized.replace("-", "_")
            normalized = normalized.replace("__", "_")
            return f"{normalized}.json"
    # fallback: URL 기반 파일명
    return url.replace("https://", "").replace("/", "_").replace(".", "_") + ".json"

def get_schema(data: Any, indent: int = 0):
    prefix = "  " * indent
    if isinstance(data, dict):
        print(f"{prefix}object {{")
        for key, value in data.items():
            print(f"{prefix}  '{key}': ", end="")
            get_schema(value, indent + 1)
        print(f"{prefix}}}")
    elif isinstance(data, list):
        print(f"{prefix}array [")
        if data:
            get_schema(data[0], indent + 1)  # 샘플 첫 번째 요소로 구조 표시
        print(f"{prefix}]")
    else:
        print(f"{type(data).__name__}")


if __name__ == "__main__":
    # feedlist = ["https://feeds.devpods.dev/devdiscuss_podcast.xml"]
    # feedlist = [
    #     "https://openai.com/blog/rss.xml",
    #     "https://blog.statsbot.co/feed",
    # ]
    # feed_sources를 사용하도록 변경
    feedlist = [
        feed_sources["DeepMind_blog"],
        feed_sources["Cube_dev"],
        feed_sources["Machine_Learning_Mastery"],
        feed_sources["ML_Uber_Engineering"],
        feed_sources["AWS_Machine_Learning"],
        feed_sources["arXiv_cs.ML"],
        feed_sources["arXiv_stat.ML"],
        feed_sources["ML_Reddit"],
        feed_sources["ML_in_production"],
        feed_sources["Jay_Alammar_blog"],
        feed_sources["JMLR_recent_papers"],
        feed_sources["Distill_blog"],
        feed_sources["inFERENCe_blog"],
        feed_sources["Daniel_Takeshi_Blog"],
        feed_sources["OpenAI_Blog"],
        feed_sources["Microsoft_Research"],
        feed_sources["Google_AI_Blog"],
        feed_sources["FastAI_Blog"],

        feed_sources["AI_Trends"],
        feed_sources["AI_Weirdness"],
        feed_sources["BAIR_blog"],
        feed_sources["Becoming_Human"],
        feed_sources["MIT_AI_News"],
        feed_sources["NVIDIA_AI_Blog"],
    ]
    feeds = fetch_rss_feeds(feedlist)
    print(f"Fetched {len(feeds)} feeds")
    
    # feed 별로 파일 저장
    for i, feed in enumerate(feeds):
        # 파일명 생성
        filename = name_json(feedlist[i])
        
        # 파일 저장
        print(f"Saving feed {i+1} to {filename}")
        save_feed_to_file(feed, filename)

        # 파일 읽어서 스키마 확인
        with open(filename, "r", encoding="utf-8") as f:
            json_data = json.load(f)
        schema = get_schema(json_data)

        # 스키마 파일 생성 및 저장
        with open(f"{filename.replace('.json', '_schema.json')}", "w", encoding="utf-8") as f:
            json.dump(schema, f, ensure_ascii=False, indent=2)
        
        print(f"Successfully saved {filename}")
        
        