from __future__ import annotations
import requests

DEFAULT_UA = "RedfinBot/0.1"

def fetch_html(url: str, timeout: int = 30, user_agent: str = DEFAULT_UA) -> str:
    r = requests.get(url, headers={"User-Agent": user_agent}, timeout=timeout)
    r.raise_for_status()
    return r.text
