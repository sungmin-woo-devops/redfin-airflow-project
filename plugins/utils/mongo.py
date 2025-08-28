import os, logging
from functools import lru_cache
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, response
from pymongo.errors import PyMongoError

# Airflow에 비의존적으로 동작: 존재 시에만 import
try:
    from airflow.models import Variable
    from airflow.hooks.base import BaseHook
except ImportError:
    Variable = None
    BaseHook = None

# ----- 설정 -----
LOG = logging.getLogger(__name__)

def resolve_mongo_uri(default_uri: Optional[str] = None) -> str:
    """
    우선순위: ENV(MONGO_URI) > Airflow Connection(mongo_redfin) > Airflow Variable(MONGO_URI) > default_uri
    """
    # 1) ENV
    uri = os.getenv("MONGO_URI")
    if uri:
        return uri
    
    # 2) Airflow Connection
    if BaseHook is not None:
        try:
            conn = BaseHook.get_connection("mongo_redfin")
            if conn:
                return conn.get_uri()
        except Exception:
            pass
    
    # 3) Airflow Variable
    if Variable is not None:
        try:
            return Variable.get("MONGO_URI")
        except Exception:
            pass
    
    # 4) DEFAULT
    if default_uri:
        return default_uri
    raise ValueError("Mongo URI not found (ENV/Conn/Variable/default 모두 없음)")

@lru_cache(maxsize=1)
def get_mongo_client(uri: str, timeout_ms: int) -> MongoClient:
    # 필요 시 옵션 추가: directConnection=True, retryWrites=False 등
    return MongoClient(uri, timeout_ms)

def get_mongo_client(default_uri: Optional[str] = None, timeout_ms: int = 3000) -> MongoClient:
    uri = resolve_mongo_uri(default_uri)
    LOG.info(f"Using MongoDB URI: {uri}")
    return MongoClient(uri, timeout_ms)

def asssert_mongo_up(default_uri: Optional[str] = None, timeout_ms: int = 3000) -> None:
    try:
        client = get_mongo_client(default_uri, timeout_ms)
        pong = client.admin.command("ping")
        return {"ok": pong.get("ok", False)}
    except PyMongoError as e:
        # Airflow 환경에서는 AirflowException을 던져도 되고, 여기선 일반 예외로 충분
        from airflow.exceptions import AirflowException if 'airflow' in globals() else Exception  # type: ignore
        raise AirflowException(f"Mongo ping 실패: {e}")  # Airflow 없으면 일반 Exception로 동작

# --- CRUD 유틸은 인터페이스 유지, 내부에서 get_mongo_client 재사용 ---
def fetch_documents(db_name: str, collection_name: str,
                    query: Optional[Dict[str, Any]] = None,
                    projection: Optional[Dict[str, int]] = None,
                    limit: Optional[int] = None) -> List[Dict[str, Any]]:
    