# UNIFIED_RSS_SCHEMA 정규화 로직
from __future__ import annotations
import hashlib
from typing import Any, Dict
import pendulum

# 현재 시간(UTC) 반환
def iso_now_utc() -> str:
    return pendulum.now("UTC").to_iso8601_string().replace("+00:00", "Z")

# 날짜 조회
def ymd(dt: pendulum.DateTime | None = None) -> tuple[str, str, str]:
    dt = dt or pendulum.now("UTC")
   
# 해시 생성
def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

# 첫 번째 비어 있지 않은 값 반환
def first_nonempty(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v
        if v not in (None, "", []):
            return v
    return None

# 피드 정규화
def _norm_feed(feed: Dict[str, Any]) -> Dict[str, Any]:
    title = first_nonempty(feed.get("title"), feed.get("feed_title"))
    link = feed.get("link")
    if not link:
        for lk in (feed.get("links") or []):
            if isinstance(lk, dict) and lk.get("rel") == "self" and lk.get("href"):
                link = lk["href"]; break
        if not link and (feed.get("links") or []):
            link = feed["links"][0].get("href")
    out = {}
    if title: out["title"] = title
    if link:  out["link"]  = link
    for k in (
        "title_detail","links","subtitle","subtitle_detail","updated","updated_parsed",
        "language","sy_updateperiod","sy_updatefrequency","image","generator",
        "generator_detail","publisher","publisher_detail","docs","ttl",
        "authors","author","author_detail","day","skipdays",
    ):
        if k in feed: out[k] = feed[k]
    return out

# 엔트리 키 생성
def _entry_fp(e: Dict[str,Any]) -> str:
    return f"{e.get('title','')}\n{e.get('link','')}\n{e.get('published') or e.get('updated') or ''}"

# 엔트리 정규화
def _norm_entry(e: Dict[str, Any]) -> Dict[str, Any]:
    title = first_nonempty(e.get("title"))
    link  = first_nonempty(e.get("link"))
    out: Dict[str, Any] = {}
    if title: out["title"] = title
    if link:  out["link"]  = link
    for k in (
        "title_detail","links","authors","author","author_detail",
        "published","published_parsed","updated","updated_parsed","tags",
        "id","guidislink","summary","summary_detail","content",
        "footnotes","media_content","media_thumbnail",
        "arxiv_announce_type","rights","rights_detail",
    ):
        if k in e: out[k] = e[k]
    entry_id = e.get("id") or ("e_" + sha1(_entry_fp(e)))
    out.setdefault("id", entry_id)
    return out

# 메타 정규화
def to_unified(obj: Dict[str, Any]) -> Dict[str, Any]:
    feed = obj.get("feed") or {}
    entries = obj.get("entries") or []
    return {"feed": _norm_feed(feed), "entries": [_norm_entry(x) for x in entries if isinstance(x, dict)]}

# 소스 파일 정규화
def feed_url_from_unified(feed: Dict[str,Any]) -> str:
    link = feed.get("link")
    if link: return link
    links = feed.get("links") or []
    for lk in links:
        if isinstance(lk, dict) and lk.get("rel") == "self" and lk.get("href"):
            return lk["href"]
    if links and links[0].get("href"):
        return links[0]["href"]
    return "about:blank"
