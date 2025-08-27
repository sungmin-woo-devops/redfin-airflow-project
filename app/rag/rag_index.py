# app/rag/rag_index.py
from __future__ import annotations
import os
from typing import List
from pymongo import MongoClient
from langchain.docstore.document import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain.vectorstores.faiss import FAISS

def _col(uri: str, db: str, col: str):
    return MongoClient(uri)[db][col]

def _load_docs(mongo_uri: str, db: str, col: str, limit: int) -> List[Document]:
    cur = _col(mongo_uri, db, col).find({"text": {"$ne": ""}}, {"_id": 0}).limit(limit)
    return [Document(page_content=d["text"], metadata={"url": d["url"], "doc_id": d["doc_id"]}) for d in cur]

def build_faiss_and_save(
    index_dir: str = "/opt/airflow/data/indexes/faiss",
    limit: int = 200,
    mongo_uri: str | None = None,
    db: str = "redfin_rag_db",
    col: str = "public_articles",  # 기본: 공개본 대상으로 인덱스(권장)
) -> str:
    """
    - 공개본(public_articles)에서 문서를 읽어와 로컬 FAISS 인덱스 생성/저장.
    - OPENAI_API_KEY 필요.
    """
    mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://root:changeme@mongodb:27017/?authSource=admin")
    os.makedirs(index_dir, exist_ok=True)

    docs = _load_docs(mongo_uri, db, col, limit)
    if not docs:
        return "no_docs"

    splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=150)
    chunks = splitter.split_documents(docs)

    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY env 필요")

    vs = FAISS.from_documents(chunks, OpenAIEmbeddings())
    vs.save_local(index_dir)
    return f"saved:{index_dir}"
