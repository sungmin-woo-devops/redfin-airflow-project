"""
RSS 데이터 정규화 및 처리 로직
"""
from __future__ import annotations
from typing import Any, Dict

from .common_utils import sha1, first_nonempty


def _norm_feed(feed: Dict[str, Any]) -> Dict[str, Any]:
    """RSS 피드 정보를 정규화"""
    title = first_nonempty(feed.get("title"), feed.get("feed_title"))
    link = feed.get("link")
    
    if not link:
        for lk in (feed.get("links") or []):
            if isinstance(lk, dict) and lk.get("rel") == "self" and lk.get("href"):
                link = lk["href"]
                break
        if not link and (feed.get("links") or []):
            link = feed["links"][0].get("href")
    
    out = {}
    if title: 
        out["title"] = title
    if link:  
        out["link"] = link
    
    for k in (
        "title_detail", "links", "subtitle", "subtitle_detail", "updated", "updated_parsed",
        "language", "sy_updateperiod", "sy_updatefrequency", "image", "generator",
        "generator_detail", "publisher", "publisher_detail", "docs", "ttl",
        "authors", "author", "author_detail", "day", "skipdays",
    ):
        if k in feed: 
            out[k] = feed[k]
    
    return out


def _entry_fp(e: Dict[str, Any]) -> str:
    """엔트리 고유 식별을 위한 fingerprint 생성"""
    return f"{e.get('title','')}\n{e.get('link','')}\n{e.get('published') or e.get('updated') or ''}"


def _norm_entry(e: Dict[str, Any]) -> Dict[str, Any]:
    """RSS 엔트리를 정규화"""
    title = first_nonempty(e.get("title"))
    link = first_nonempty(e.get("link"))
    
    out: Dict[str, Any] = {}
    if title: 
        out["title"] = title
    if link:  
        out["link"] = link
    
    for k in (
        "title_detail", "links", "authors", "author", "author_detail",
        "published", "published_parsed", "updated", "updated_parsed", "tags",
        "id", "guidislink", "summary", "summary_detail", "content",
        "footnotes", "media_content", "media_thumbnail",
        "arxiv_announce_type", "rights", "rights_detail",
    ):
        if k in e: 
            out[k] = e[k]
    
    entry_id = e.get("id") or ("e_" + sha1(_entry_fp(e)))
    out.setdefault("id", entry_id)
    
    return out


def _to_unified(obj: Dict[str, Any]) -> Dict[str, Any]:
    """RSS 객체를 통합 스키마로 변환"""
    feed = obj.get("feed") or {}
    entries = obj.get("entries") or []
    
    return {
        "feed": _norm_feed(feed), 
        "entries": [_norm_entry(x) for x in entries if isinstance(x, dict)]
    }


def _feed_url(unified_feed: Dict[str, Any]) -> str:
    """통합된 피드에서 URL 추출"""
    link = unified_feed.get("link")
    if link: 
        return link
    
    links = unified_feed.get("links") or []
    for lk in links:
        if isinstance(lk, dict) and lk.get("rel") == "self" and lk.get("href"):
            return lk["href"]
    
    if links and links[0].get("href"):
        return links[0]["href"]
    
    return "about:blank"


def create_unified_rss_item(obj: Dict[str, Any]) -> Dict[str, Any]:
    """RSS 객체를 통합 스키마 아이템으로 변환"""
    unified = _to_unified(obj)
    feed = unified.get("feed", {})
    feed_url = _feed_url(feed)
    feed_name = first_nonempty(feed.get("title"), "Unknown Feed")
    
    return {
        "source": {
            "feed_id": sha1(feed_url),
            "feed_url": feed_url,
            "feed_name": feed_name,
            "group": None
        },
        "rss": unified
    }
