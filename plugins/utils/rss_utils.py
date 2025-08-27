# I/O, 파일명 정규화 등
from __future__ import annotations
import json, re
from pathlib import Path
from typing import Any, Dict

def normalize_filename(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return f"{s}.json"

def save_feed_to_file(feed: Any, filepath: Path) -> None:
    obj = {"feed": dict(feed.feed), "entries": [dict(e) for e in feed.entries]}
    filepath.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
