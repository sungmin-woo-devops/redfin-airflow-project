"""
공통 유틸리티 함수들
"""
from __future__ import annotations
import hashlib
from typing import Any

import pendulum


def iso_now_utc() -> str:
    """현재 시간을 UTC ISO 8601 형식으로 반환"""
    return pendulum.now("UTC").to_iso8601_string().replace("+00:00", "Z")


def ymd(dt: pendulum.DateTime | None = None) -> tuple[str, str, str]:
    """DateTime을 (year, month, day) 문자열 튜플로 변환"""
    dt = dt or pendulum.now("UTC")
    return f"{dt.year:04d}", f"{dt.month:02d}", f"{dt.day:02d}"


def sha1(s: str) -> str:
    """문자열의 SHA1 해시값 반환"""
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()


def first_nonempty(*vals: Any) -> Any:
    """비어있지 않은 첫 번째 값 반환"""
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v
        if v not in (None, "", []):
            return v
    return None
