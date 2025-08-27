# app/ingest/preprocess.py
from __future__ import annotations
import os, hashlib, time, feedparser, pendulum
from typing import List, Dict, Any
from urllib.parse import urlparse
from app.ingest.scraper import fetch_html

try:
    import trafilatura
except Exception as e:
    raise RuntimeError("trafilatura 설치 필요: pip install trafilatura") from e

# 핵심: RSS 메타(DB) → 엔트리 파싱 → 기사 본문 수집/정제 → 문서 리스트 반환
from pymongo import MongoClient

def _mongo_col(uri: str, db: str, col: str):
    return MongoClient(uri)[db][col]

def _doc_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]

def _extract_text(html: str) -> str:
    return trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        include_images=False,
        favor_recall=True,
    ) or ""

def _now_iso() -> str:
    return pendulum.now("UTC").to_iso8601_string()

def extract_articles(
    limit: int = 100,
    mongo_uri: str | None = None,
    db: str = "redfin_rag_db",
    meta_col: str = "rss_meta",
    user_agent: str = "RedfinBot/0.1",
) -> List[Dict[str, Any]]:
    """
    - rss_meta 컬렉션에서 feed_url 목록을 읽는다.
    - 각 피드의 엔트리를 순회하며 기사 본문을 수집/정제한다.
    - 결과: [{doc_id, url, title, source, published_ts, text, fetched_at}, ...]
    """
    mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:changeme@mongodb:27017/?authSource=admin")
    feeds = list(_mongo_col(mongo_uri, db, meta_col).find({}, {"_id": 0, "feed_url": 1, "name": 1}))
    if not feeds:
        return []

    docs: List[Dict[str, Any]] = []
    for feed in feeds:
        if len(docs) >= limit:
            break
        fp = feedparser.parse(feed["feed_url"])
        entries = fp.get("entries") or []
        for e in entries:
            if len(docs) >= limit:
                break
            url = e.get("link") or e.get("id")
            if not url:
                continue
            try:
                html = fetch_html(url, user_agent=user_agent)
                text = _extract_text(html).strip()
                if not text:
                    continue
                docs.append({
                    "doc_id": _doc_id(url),
                    "url": url,
                    "title": (e.get("title") or "").strip(),
                    "source": urlparse(url).netloc,
                    "published_ts": int(time.mktime(e.get("published_parsed") or e.get("updated_parsed") or time.gmtime())),
                    "text": text,
                    "fetched_at": _now_iso(),
                    "feed_name": feed.get("name"),
                })
            except Exception:
                # 네트워크/파서 오류는 건너뜀(최소 예제)
                continue
    return docs

# 공개 데이터셋 생성(라이선스/도메인 기준 최소 필터)
def build_public_dataset(
    mongo_uri: str | None = None,
    db: str = "redfin_rag_db",
    src_col: str = "articles",
    dst_col: str = "public_articles",
    allow_domains_env: str = "PUBLIC_ALLOW_DOMAINS",  # "example.com,another.com"
) -> dict:
    """
    - 허용 도메인만 본문 포함하여 복제, 그 외는 메타만 남김(최소 정책).
    - 운영에서는 별도 라이선스 테이블/플래그로 대체 권장.
    """
    mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:changeme@mongodb:27017/?authSource=admin")
    allow = set([d.strip().lower() for d in os.getenv(allow_domains_env, "").split(",") if d.strip()])

    src = _mongo_col(mongo_uri, db, src_col)
    dst = _mongo_col(mongo_uri, db, dst_col)

    copied, redacted = 0, 0
    for d in src.find({}, {"_id": 0}):
        domain = (d.get("source") or "").lower()
        pub = {
            "doc_id": d["doc_id"],
            "url": d["url"],
            "title": d.get("title"),
            "source": domain,
            "published_ts": d.get("published_ts"),
            "fetched_at": d.get("fetched_at"),
            "license": "public" if domain in allow else "restricted",
        }
        if domain in allow:
            pub["text"] = d.get("text", "")
            copied += 1
        else:
            # 본문 제거(메타만 공개)
            pub["text"] = ""
            redacted += 1
        dst.update_one({"doc_id": pub["doc_id"]}, {"$set": pub}, upsert=True)

    return {"copied": copied, "redacted": redacted}
