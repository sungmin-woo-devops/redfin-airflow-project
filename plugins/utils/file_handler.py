"""
파일 처리 관련 유틸리티 함수들
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any, Dict, List


def load_files(input_dir: str) -> List[Path]:
    """입력 디렉토리에서 JSON/JSONL 파일들을 로드"""
    paths: List[Path] = []
    for pat in ("*.json", "*.jsonl"):
        paths.extend(Path(input_dir).glob(pat))
    return sorted(p for p in paths if p.is_file())


def parse_any_file(path: Path) -> List[Dict[str, Any]]:
    """JSON 또는 JSONL 파일을 파싱하여 딕셔너리 리스트 반환"""
    txt = path.read_text(encoding="utf-8", errors="ignore")
    out: List[Dict[str, Any]] = []
    
    if path.suffix.lower() == ".jsonl":
        for line in txt.splitlines():
            line = line.strip()
            if not line: 
                continue
            try: 
                out.append(json.loads(line))
            except Exception:
                pass
    else:
        try:
            obj = json.loads(txt)
            if isinstance(obj, list): 
                out.extend([x for x in obj if isinstance(x, dict)])
            elif isinstance(obj, dict): 
                out.append(obj)
        except Exception:
            pass
    
    return out


def save_json_file(data: Any, file_path: Path) -> None:
    """데이터를 JSON 파일로 저장"""
    file_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), 
        encoding="utf-8"
    )
