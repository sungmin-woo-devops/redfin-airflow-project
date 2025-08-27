# app/storage/mongo_loader.py
from __future__ import annotations
import os
from typing import List, Dict
from pymongo import MongoClient, UpdateOne

def _col(uri: str, db: str, col: str):
    return MongoClient(uri)[db][col]

def upsert_articles(
    docs: List[Dict],
    mongo_uri: str | None = None,
    db: str = "redfin_rag_db",
    col: str = "articles",
) -> dict:
    """
    - doc_id 기준 upsert (중복 안전)
    - text 없는 문서는 스킵
    """
    if not docs:
        return {"upserted": 0, "modified": 0, "requested": 0}
    mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:changeme@mongodb:27017/?authSource=admin")
    c = _col(mongo_uri, db, col)
    ops = [UpdateOne({"doc_id": d["doc_id"]}, {"$set": d}, upsert=True) for d in docs if d.get("text")]
    if not ops:
        return {"upserted": 0, "modified": 0, "requested": len(docs)}
    res = c.bulk_write(ops, ordered=False)
    return {"upserted": len(res.upserted_ids), "modified": res.modified_count, "requested": len(docs)}
