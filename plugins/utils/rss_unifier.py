"""
RSS 피드를 통합 스키마로 변환하는 유틸리티 모듈
"""

import json
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

# pendulum import (필요할 때만)
try:
    import pendulum
except ImportError:
    # pendulum이 없으면 datetime을 사용
    pendulum = None


def generate_feed_id(feed_url: str) -> str:
    """피드 URL을 기반으로 고유 ID 생성"""
    return hashlib.sha256(feed_url.encode()).hexdigest()[:16]


def parse_feed_data(feed_data: Dict[str, Any]) -> Dict[str, Any]:
    """RSS 피드 데이터를 통합 스키마로 변환"""
    
    # 기본 피드 정보 추출
    feed_info = feed_data.get('feed', {})
    entries = feed_data.get('entries', [])
    parsed_at = feed_data.get('parsed_at', datetime.now().isoformat())
    
    # 현재 시간 정보
    if pendulum:
        now = pendulum.now('UTC')
        year, month, day = str(now.year), f"{now.month:02d}", f"{now.day:02d}"
    else:
        now = datetime.now()
        year, month, day = str(now.year), f"{now.month:02d}", f"{now.day:02d}"
    
    # 피드 URL 추출 (첫 번째 엔트리의 링크에서 추정)
    feed_url = feed_info.get('link', '')
    if not feed_url and entries:
        # 첫 번째 엔트리의 링크에서 도메인 추출
        first_entry = entries[0] if entries else {}
        entry_link = first_entry.get('link', '')
        if entry_link:
            from urllib.parse import urlparse
            parsed_url = urlparse(entry_link)
            feed_url = f"{parsed_url.scheme}://{parsed_url.netloc}/"
    
    # 피드 ID 생성
    feed_id = generate_feed_id(feed_url)
    
    # ULID import (필요할 때만)
    try:
        import ulid
    except ImportError:
        # ulid가 없으면 uuid를 사용
        import uuid
        class UlidMock:
            @staticmethod
            def new():
                return str(uuid.uuid4())
        ulid = UlidMock()
    
    # 통합 스키마 구조 생성
    unified_doc = {
        "_id": str(ulid.new()),
        "schema_version": "1.0.0",
        "status": "meta",
        "run": {
            "run_id": str(ulid.new()),
            "exec_ts": now.isoformat() if hasattr(now, 'isoformat') else now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        },
        "source": {
            "feed_id": feed_id,
            "feed_url": feed_url,
            "feed_name": feed_info.get('title', 'Unknown Feed')
        },
        "partition": {
            "year": year,
            "month": month,
            "day": day
        },
        "audit": {
            "created_at": now.isoformat() if hasattr(now, 'isoformat') else now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "extracted_at": parsed_at,
            "processed_at": None,
            "labeled_at": None,
            "embedded_at": None
        },
        "rss": {
            "feed": {
                "title": feed_info.get('title', ''),
                "title_detail": feed_info.get('title_detail', {}),
                "links": feed_info.get('links', []),
                "link": feed_info.get('link', ''),
                "subtitle": feed_info.get('subtitle', ''),
                "subtitle_detail": feed_info.get('subtitle_detail', {}),
                "updated": feed_info.get('updated', ''),
                "updated_parsed": feed_info.get('updated_parsed', []),
                "language": feed_info.get('language', ''),
                "sy_updateperiod": feed_info.get('sy_updateperiod', ''),
                "sy_updatefrequency": feed_info.get('sy_updatefrequency', ''),
                "image": feed_info.get('image', {}),
                "generator_detail": feed_info.get('generator_detail', {}),
                "generator": feed_info.get('generator', ''),
                "publisher": feed_info.get('publisher', ''),
                "publisher_detail": feed_info.get('publisher_detail', {})
            },
            "entries": []
        }
    }
    
    # 엔트리 데이터 변환
    for entry in entries:
        unified_entry = {
            "title": entry.get('title', ''),
            "title_detail": entry.get('title_detail', {}),
            "links": entry.get('links', []),
            "link": entry.get('link', ''),
            "authors": entry.get('authors', []),
            "author": entry.get('author', ''),
            "author_detail": entry.get('author_detail', {}),
            "published": entry.get('published', ''),
            "published_parsed": entry.get('published_parsed', []),
            "updated": entry.get('updated', ''),
            "updated_parsed": entry.get('updated_parsed', []),
            "tags": entry.get('tags', []),
            "id": entry.get('id', ''),
            "guidislink": entry.get('guidislink', False),
            "summary": entry.get('summary', ''),
            "summary_detail": entry.get('summary_detail', {}),
            "content": entry.get('content', []),
            "footnotes": entry.get('footnotes', []),
            "media_content": entry.get('media_content', []),
            "media_thumbnail": entry.get('media_thumbnail', [])
        }
        unified_doc["rss"]["entries"].append(unified_entry)
    
    return unified_doc


def process_rss_files(input_dir: str, output_dir: str) -> List[Dict[str, Any]]:
    """RSS JSON 파일들을 읽어서 통합 스키마로 변환"""
    
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    unified_docs = []
    
    # JSON 파일들 찾기
    json_files = list(input_path.glob("**/*.json"))
    json_files = [f for f in json_files if not f.name.endswith('_schema.json')]
    
    for json_file in json_files:
        try:
            # JSON 파일 읽기
            with open(json_file, 'r', encoding='utf-8') as f:
                feed_data = json.load(f)
            
            # 통합 스키마로 변환
            unified_doc = parse_feed_data(feed_data)
            
            # 피드 이름을 파일명으로 사용
            feed_name = unified_doc['source']['feed_name']
            safe_feed_name = "".join(c for c in feed_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_feed_name = safe_feed_name.replace(' ', '_')
            
            # 출력 파일 저장
            output_file = output_path / f"{safe_feed_name}_unified.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(unified_doc, f, ensure_ascii=False, indent=2)
            
            unified_docs.append(unified_doc)
            
            print(f"Processed: {json_file.name} -> {output_file.name}")
            
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
            continue
    
    return unified_docs


def create_batch_envelope(unified_docs: List[Dict[str, Any]], batch_id: str = None, batch_type: str = "rss_collection") -> Dict[str, Any]:
    """여러 통합 문서를 배치 봉투로 감싸기"""
    
    # ULID import (필요할 때만)
    try:
        import ulid
    except ImportError:
        # ulid가 없으면 uuid를 사용
        import uuid
        class UlidMock:
            @staticmethod
            def new():
                return str(uuid.uuid4())
        ulid = UlidMock()
    
    if not unified_docs:
        return None
    
    if batch_id is None:
        batch_id = str(ulid.new())
    
    if pendulum:
        now = pendulum.now('UTC')
        year, month, day = str(now.year), f"{now.month:02d}", f"{now.day:02d}"
    else:
        now = datetime.now()
        year, month, day = str(now.year), f"{now.month:02d}", f"{now.day:02d}"
    
    # 통계 계산
    total_entries = sum(len(doc.get('rss', {}).get('entries', [])) for doc in unified_docs)
    avg_entries = total_entries / len(unified_docs) if unified_docs else 0
    
    # 배치 봉투 생성
    batch_doc = {
        "_id": batch_id,
        "schema_version": "1.0.0",
        "status": "meta",
        "run": {
            "run_id": str(ulid.new()),
            "exec_ts": now.isoformat() if hasattr(now, 'isoformat') else now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        },
        "source": {
            "batch_type": batch_type,
            "batch_name": f"daily_{batch_type}_{year}{month}{day}",
            "feed_count": len(unified_docs),
            "total_entries": total_entries
        },
        "partition": {
            "year": year,
            "month": month,
            "day": day
        },
        "audit": {
            "created_at": now.isoformat() if hasattr(now, 'isoformat') else now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "updated_at": now.isoformat() if hasattr(now, 'isoformat') else now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            "processed_at": None,
            "labeled_at": None,
            "embedded_at": None
        },
        "items": unified_docs,
        "statistics": {
            "total_feeds": len(unified_docs),
            "successful_feeds": len(unified_docs),  # 통합된 문서는 모두 성공한 것으로 간주
            "failed_feeds": 0,
            "empty_feeds": 0,
            "total_entries": total_entries,
            "avg_entries_per_feed": round(avg_entries, 1),
            "processing_time_seconds": 0.0  # 실제 처리 시간은 별도 계산 필요
        },
        "errors": []  # 에러 정보는 별도로 수집 필요
    }
    
    return batch_doc


def save_to_mongodb(docs: List[Dict[str, Any]], mongo_uri: str, db_name: str, collection_name: str) -> Dict[str, Any]:
    """통합 문서들을 MongoDB에 저장"""
    
    try:
        import pymongo
        from pymongo import MongoClient
    except ImportError:
        raise ImportError("pymongo is required. Install with: pip install pymongo")
    
    client = MongoClient(mongo_uri)
    try:
        db = client[db_name]
        collection = db[collection_name]
        
        # 배치 삽입
        if len(docs) == 1:
            result = collection.insert_one(docs[0])
            inserted_ids = [str(result.inserted_id)]
        else:
            result = collection.insert_many(docs)
            inserted_ids = [str(id) for id in result.inserted_ids]
        
        return {
            "success": True,
            "inserted_count": len(inserted_ids),
            "inserted_ids": inserted_ids
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
    finally:
        client.close()
