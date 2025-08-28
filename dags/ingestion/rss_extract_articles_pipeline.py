# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import pendulum
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from airflow.models import Variable
from pymongo import MongoClient, UpdateOne
import requests
from bs4 import BeautifulSoup
import logging

# Dataset 정의
try:
    from dags.datasets import RSS_FEEDS, RSS_EXTRACTED
except Exception:
    RSS_FEEDS = Dataset("mongo://redfin/rss_feeds")
    RSS_EXTRACTED = Dataset("mongo://redfin/rss_extracted")

KST = pendulum.timezone("Asia/Seoul")

def _mongo_client():
    """MongoDB 클라이언트 생성"""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://172.17.0.1:27017/")
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        client.server_info()  # 연결 테스트
        return client
    except Exception as e:
        logging.error(f"MongoDB 연결 실패: {e}")
        raise

def _extract_article_text(html_content: str) -> str:
    """기사 본문 텍스트 추출"""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 불필요한 요소 제거
        for unwanted in soup.select('script, style, nav, header, footer, .advertisement, .sidebar, .ad, .menu, .navigation'):
            unwanted.decompose()
        
        # 기사 본문 선택자들 (우선순위)
        selectors = [
            'article',
            '.article-content', '.article-body', '.post-content', '.entry-content',
            'main', '.main-content', '#content', '.content',
            '.story-body', '.news-content'
        ]
        
        article_text = ""
        
        # 선택자로 본문 찾기
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                text_parts = []
                for element in elements:
                    # 텍스트 노드만 추출
                    paragraphs = element.find_all(['p', 'div'], recursive=True)
                    for p in paragraphs:
                        text = p.get_text().strip()
                        if len(text) > 20:  # 의미 있는 텍스트만
                            text_parts.append(text)
                
                if text_parts:
                    article_text = '\n'.join(text_parts)
                    break
        
        # 본문을 찾지 못한 경우 p 태그 사용
        if not article_text:
            paragraphs = soup.find_all('p')
            if paragraphs:
                valid_texts = [p.get_text().strip() for p in paragraphs 
                             if len(p.get_text().strip()) > 20]
                if valid_texts:
                    article_text = '\n'.join(valid_texts)
        
        # 텍스트 정리
        if article_text:
            article_text = re.sub(r'\s+', ' ', article_text).strip()
        
        return article_text
        
    except Exception as e:
        logging.warning(f"HTML 파싱 오류: {e}")
        return ""

def _fetch_article_content(url: str) -> Optional[str]:
    """기사 URL에서 HTML 콘텐츠 가져오기"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        response.raise_for_status()
        response.encoding = response.apparent_encoding  # 인코딩 자동 감지
        
        return response.text
        
    except Exception as e:
        logging.warning(f"URL 가져오기 실패 ({url}): {e}")
        return None

def _process_rss_entries(entries: List[Dict]) -> List[Dict]:
    """RSS entries 배열 처리"""
    processed = []
    
    for entry in entries:
        try:
            # 기본 정보 추출
            link = entry.get('link', '')
            if not link:
                continue
            
            # 제목 추출 (중첩 구조 처리)
            title = entry.get('title', '')
            if isinstance(title, dict):
                title = title.get('value', '')
            
            # 요약 추출
            summary = entry.get('summary', '')
            if isinstance(summary, dict):
                summary = summary.get('value', '')
            
            # 태그 추출
            tags = []
            if entry.get('tags') and isinstance(entry['tags'], list):
                for tag in entry['tags']:
                    if isinstance(tag, dict) and tag.get('term'):
                        tags.append(tag['term'])
                    elif isinstance(tag, str):
                        tags.append(tag)
            
            processed_entry = {
                'guid': entry.get('id') or entry.get('guid') or link,
                'title': title,
                'link': link,
                'summary': summary,
                'tags': tags,
                'published': entry.get('published', ''),
                'author': entry.get('author', ''),
                'category': entry.get('category', [])
            }
            
            processed.append(processed_entry)
            
        except Exception as e:
            logging.warning(f"Entry 처리 오류: {e}")
            continue
    
    return processed

def extract_articles(**context):
    """RSS 피드에서 기사 본문 추출"""
    client = _mongo_client()
    
    try:
        db = client[os.getenv("MONGO_DB", "redfin")]
        rss_collection = db["rss_feeds"]
        
        # 처리 제한
        limit = int(Variable.get("EXTRACT_LIMIT", default_var="50"))
        
        # RSS 피드 문서 가져오기
        rss_docs = list(rss_collection.find().limit(limit))
        logging.info(f"처리할 RSS 문서 수: {len(rss_docs)}")
        
        extracted_articles = []
        
        for rss_doc in rss_docs:
            entries = rss_doc.get('entries', [])
            if not entries:
                continue
            
            # entries 처리
            processed_entries = _process_rss_entries(entries)
            
            for entry in processed_entries:
                try:
                    # 기사 본문 추출
                    html_content = _fetch_article_content(entry['link'])
                    if not html_content:
                        continue
                    
                    article_text = _extract_article_text(html_content)
                    if not article_text or len(article_text) < 100:  # 최소 길이 체크
                        continue
                    
                    # 언어 감지
                    korean_chars = sum(1 for char in article_text if '\uac00' <= char <= '\ud7af')
                    english_chars = sum(1 for char in article_text if char.isalpha() and ord(char) < 128)
                    language = 'KOREAN' if korean_chars > english_chars else 'ENGLISH'
                    
                    # 최종 기사 데이터
                    article = {
                        'guid': entry['guid'],
                        'title': entry['title'],
                        'link': entry['link'],
                        'article_text': article_text,
                        'summary': entry['summary'] or article_text[:300] + "...",
                        'tags': entry['tags'],
                        'language': language,
                        'text_length': len(article_text),
                        'published': entry['published'],
                        'author': entry['author'],
                        'category': entry['category'],
                        'processed_at': datetime.now().isoformat(),
                        'extraction_status': 'success'
                    }
                    
                    extracted_articles.append(article)
                    logging.info(f"기사 추출 성공: {entry['title'][:50]}...")
                    
                except Exception as e:
                    logging.error(f"기사 처리 실패 ({entry.get('link', 'unknown')}): {e}")
                    continue
        
        logging.info(f"총 {len(extracted_articles)}개 기사 추출 완료")
        return extracted_articles
        
    finally:
        client.close()

def save_articles(ti, **context):
    """추출된 기사들을 rss_articles 컬렉션에 저장"""
    extracted_articles = ti.xcom_pull(task_ids="extract_articles")
    
    if not extracted_articles:
        logging.warning("저장할 기사가 없습니다")
        return {"saved": 0, "skipped": 0}
    
    client = _mongo_client()
    
    try:
        db = client[os.getenv("MONGO_DB", "redfin")]
        collection = db["rss_articles"]  # 올바른 컬렉션명
        
        # guid 기준 upsert 작업
        operations = []
        for article in extracted_articles:
            operations.append(
                UpdateOne(
                    {"guid": article["guid"]},
                    {
                        "$set": article,
                        "$setOnInsert": {"created_at": datetime.now().isoformat()}
                    },
                    upsert=True
                )
            )
        
        # 배치 실행
        if operations:
            result = collection.bulk_write(operations, ordered=False)
            
            logging.info(f"저장 완료 - Upserted: {len(result.upserted_ids)}, Modified: {result.modified_count}")
            
            return {
                "saved": len(result.upserted_ids),
                "modified": result.modified_count,
                "total_processed": len(extracted_articles)
            }
        else:
            return {"saved": 0, "modified": 0}
            
    except Exception as e:
        logging.error(f"MongoDB 저장 오류: {e}")
        raise
    finally:
        client.close()

with DAG(
    dag_id="02_rss_extract_articles_pipeline",
    description="RSS 피드에서 기사 본문을 추출하여 rss_articles 컬렉션에 저장",
    start_date=pendulum.datetime(2025, 1, 1, tz=KST),
    schedule=[RSS_FEEDS],
    catchup=False,
    tags=["extraction", "rss", "articles"],
) as dag:
    
    # 의존성 확인
    check_dependencies = BashOperator(
        task_id="check_dependencies",
        bash_command=(
            "python -c 'import requests, bs4, pymongo; print(\"Dependencies OK\")' && "
            "echo 'MongoDB URI: ${MONGO_URI:-mongodb://localhost:27017/}'"
        ),
    )
    
    # 기사 본문 추출
    extract_task = PythonOperator(
        task_id="extract_articles",
        python_callable=extract_articles,
        provide_context=True,
    )
    
    # MongoDB에 저장
    save_task = PythonOperator(
        task_id="save_articles",
        python_callable=save_articles,
        provide_context=True,
        outlets=[RSS_EXTRACTED],
    )
    
    # 결과 검증
    validate_results = BashOperator(
        task_id="validate_results",
        bash_command=(
            "echo 'Extraction pipeline completed successfully'; "
            "echo 'Check rss_articles collection for extracted articles'"
        ),
    )
    
    # 의존성
    check_dependencies >> extract_task >> save_task >> validate_results
