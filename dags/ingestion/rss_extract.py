from __future__ import annotations
import os, re, json, logging
import requests
import pendulum
from typing import List, Dict, Any, Optional, Iterable

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from pymongo import UpdateOne

from utils.mongo import get_mongo_client

LOG = logging.getLogger(__name__)

# ---------- HTTP ----------
def _create_session(
    timeout: int = 20,
    total_retries: int = 3,
    backoff: float = 0.5
) -> requests.Session:
    # 세션 객체 생성
    session = requests.Session()
    # 재시도 정책 설정
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    # 어댑터에 재시도 정책 적용
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    # 기본 헤더 설정
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; rss-extractor/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    # 모든 요청에 타임아웃 적용
    session.request = _with_timeout(session.request, timeout)  # type: ignore
    return session

def _with_timeout(fn, timeout: int):
    """
    주어진 함수(fn)에 기본 timeout 값을 적용하는 래퍼를 반환합니다.
    모든 HTTP 요청에 timeout 파라미터가 없으면 기본값을 자동으로 추가합니다.
    """
    def _wrapped(method, url, **kw):
        kw.setdefault("timeout", timeout)
        return fn(method, url, **kw)
    return _wrapped

# ---------- HTML -> TEXT ----------

# 기본 선택자 목록
_SELECTORS = [
    "article",
    ".article-content", ".article-body", ".post-content", ".entry-content",
    "main", ".main-content", "#content", ".content",
    ".story-body", ".news-content",
]

# 기사 텍스트 추출
def extract_article_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")

    for sel in ["script", "style", "nav", "header", "footer", ".advertisement", ".ad", ".sidebar", ".menu", ".navigation"]:
        for el in soup.select(sel):
            el.decompose()

    text = ""
    for sel in _SELECTORS:
        elems = soup.select(sel)
        if not elems:
            continue
        parts: List[str] = []
        for el in elems:
            for p in el.find_all(["p", "div"], recursive=True):
                t = (p.get_text() or "").strip()
                if len(t) > 20:
                    parts.append(re.sub(r"\s+", " ", t))
        if parts:
            text = "\n".join(parts)
            break

    if not text:
        parts = [re.sub(r"\s+", " ", (p.get_text() or "").strip())
                 for p in soup.find_all("p")]
        parts = [t for t in parts if len(t) > 20]
        text = "\n".join(parts) if parts else ""

    return text.strip()

def _detect_language(text: str) -> str:
    """
    텍스트 내 한글과 영문자의 개수를 비교하여 주요 언어를 판별합니다.
    - 한글이 더 많으면 'KOREAN', 그렇지 않으면 'ENGLISH' 반환
    """
    korean_count: int = sum(1 for ch in text if "\uac00" <= ch <= "\ud7af")
    english_count: int = sum(1 for ch in text if ch.isalpha() and ord(ch) < 128)
    return "KOREAN" if korean_count > english_count else "ENGLISH"

# ---------- RSS entries ----------

def _normalize_entries(entries: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    norm: List[Dict[str, Any]] = []
    for e in entries or []:
        try:
            # 1. 제목 추출 및 정규화
            title = e.get("title", "")
            if isinstance(title, dict):
                title = title.get("value", "") or ""
            # 2. 요약 추출 및 정규화
            summary = e.get("summary", "")
            if isinstance(summary, dict):
                summary = summary.get("value", "") or ""
            # 3. 태그 추출 및 정규화
            tags: List[str] = []
            for tag in (e.get("tags") or []):
                if isinstance(tag, dict) and tag.get("term"):
                    tags.append(tag["term"])
                elif isinstance(tag, str):
                    tags.append(tag)
            # 4. 링크 추출 (필수)
            link = e.get("link") or ""
            if not link:
                continue
            # 5. 정규화된 엔트리 딕셔너리 생성
            norm.append({
                "guid": e.get("id") or e.get("guid") or link,
                "title": title,
                "link": link,
                "summary": summary,
                "tags": tags,
                "published": e.get("published", ""),
                "author": e.get("author", ""),
                "category": e.get("category", []),
            })
        except Exception as ex:
            # 6. 예외 발생 시 경고 로그
            LOG.warning("entry normalize failed: %s", ex)
    return norm



# ---------- High-level ops (Airflow task callables) ----------
# High-level ops ? 