# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import pendulum
import re, os, requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from pymongo import MongoClient, UpdateOne

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from airflow.models import Variable


DAG_ID = "02_rss_extract_pipeline"


# ---- Dataset 정의
RSS_FEEDS = Dataset("mongo://redfin/rss_feeds")  # 소스 데이터
RSS_EXTRACTED = Dataset("mongo://redfin/rss_extracted")  # 추출 완료 신호

KST = pendulum.timezone("Asia/Seoul")
START = pendulum.datetime(2025, 1, 1, tz=KST)

def _mongo_client():
    """MongoDB 클라이언트 생성"""
    mongo_uri = os.getenv("MONGO_URI") or Variable.get("MONGO_URI")
    return MongoClient(mongo_uri)

def _extract_article_text(html_content: str) -> str:
    """기사 본문 텍스트 추출"""
    if not html_content:
        return ""
    
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 일반적인 기사 본문 선택자들
        selectors = [
            'article',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.content',
            'main',
            '.main-content',
            '#content',
            '.story-body',
            '.article-body'
        ]
        
        article_text = ""
        
        # 선택자로 본문 찾기
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                text_parts = []
                for element in elements:
                    # 불필요한 요소 제거
                    for unwanted in element.select('script, style, nav, header, footer, .advertisement, .sidebar, .ad'):
                        unwanted.decompose()
                    
                    # 텍스트 추출
                    text_nodes = element.find_all(['p', 'div', 'span'])
                    clean_texts = [node.get_text().strip() for node in text_nodes if node.get_text().strip()]
                    
                    if clean_texts:
                        text_parts.extend(clean_texts)
                
                if text_parts:
                    article_text = ' '.join(text_parts)
                    break
        
        # 본문을 찾지 못한 경우 p 태그들의 텍스트를 수집
        if not article_text:
            paragraphs = soup.find_all('p')
            if paragraphs:
                article_text = ' '.join([p.get_text().strip() for p in paragraphs if p.get_text().strip()])
        
        # 텍스트 정리
        if article_text:
            article_text = ' '.join(article_text.split())
        
        return article_text
        
    except Exception as e:
        print(f"HTML 파싱 오류: {e}")
        return ""

def _generate_summary(article_text: str, rss_description: str) -> str:
    """요약 생성"""
    if rss_description and len(rss_description.strip()) > 0:
        return rss_description.strip()
    
    # 간단한 요약 생성 (첫 200자)
    if article_text:
        summary = article_text[:200].strip()
        if len(article_text) > 200:
            summary += "..."
        return summary
    
    return ""

def _extract_tags(rss_data: Dict[str, Any]) -> List[str]:
    """태그 추출"""
    tags = []
    
    # RSS 데이터에서 태그 가져오기
    if rss_data.get('category'):
        if isinstance(rss_data['category'], list):
            tags.extend(rss_data['category'])
        else:
            tags.append(rss_data['category'])
    
    if rss_data.get('dc_subject'):
        if isinstance(rss_data['dc_subject'], list):
            tags.extend(rss_data['dc_subject'])
        else:
            tags.append(rss_data['dc_subject'])
    
    # 중복 제거 및 정리
    tags = list(set([tag.strip() for tag in tags if tag.strip()]))
    
    return tags

def _classify_content_type(rss_data: Dict[str, Any], article_text: str) -> str:
    """콘텐츠 유형 분류"""
    title = rss_data.get('title', '').lower()
    description = rss_data.get('description', '').lower()
    
    # 키워드 기반 분류
    if any(word in title or word in description for word in ['interview', '인터뷰']):
        return 'INTERVIEW'
    elif any(word in title or word in description for word in ['opinion', '칼럼', '기고']):
        return 'OPINION'
    elif any(word in title or word in description for word in ['press release', '보도자료', '공지']):
        return 'PRESS_RELEASE'
    elif any(word in title or word in description for word in ['blog', '블로그']):
        return 'BLOG'
    else:
        return 'NEWS'

def _detect_language(article_text: str, rss_data: Dict[str, Any]) -> str:
    """언어 감지"""
    if not article_text:
        return 'UNKNOWN'
        
    korean_chars = sum(1 for char in article_text if '\uac00' <= char <= '\ud7af')
    english_chars = sum(1 for char in article_text if char.isalpha() and ord(char) < 128)
    
    if korean_chars > english_chars:
        return 'KOREAN'
    else:
        return 'ENGLISH'

def _calculate_readability(text: str) -> float:
    """가독성 점수 계산"""
    if not text:
        return 0.0
    
    # 문장 수 계산
    sentences = [s.strip() for s in text.split('.') if s.strip()]
    sentence_count = len(sentences)
    
    # 단어 수 계산
    words = text.split()
    word_count = len(words)
    
    # 평균 단어 길이
    if word_count > 0:
        avg_word_length = sum(len(word) for word in words) / word_count
    else:
        avg_word_length = 0
    
    # 간단한 가독성 점수 (0-100)
    if sentence_count > 0 and word_count > 0:
        score = max(0, 100 - (avg_word_length * 2) - (word_count / sentence_count * 0.5))
        return min(100, score)
    
    return 50.0

def _extract_entities(text: str) -> List[Dict[str, Any]]:
    """주요 엔티티 추출"""
    entities = []
    
    if not text:
        return entities
    
    # 이메일
    emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
    for email in emails:
        entities.append({
            'type': 'email',
            'value': email,
            'confidence': 0.9
        })
    
    # URL
    urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', text)
    for url in urls:
        entities.append({
            'type': 'url',
            'value': url,
            'confidence': 0.8
        })
    
    return entities

def _fetch_article_content(url: str) -> Optional[str]:
    """기사 URL에서 HTML 콘텐츠 가져오기"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        return response.text
        
    except Exception as e:
        print(f"URL 가져오기 실패 ({url}): {e}")
        return None

def task_extract_articles(**_):
    """RSS 피드에서 기사 본문 추출"""
    client = _mongo_client()
    db = client[os.getenv("MONGO_DB", "redfin")]
    
    # rss_feeds 컬렉션에서 데이터 읽기
    rss_collection = db["rss_feeds"]
    extract_collection = db["rss_extract_articles"]
    
    # 처리할 문서 수 제한 (테스트용)
    limit = int(Variable.get("EXTRACT_LIMIT", default_var="10"))
    
    # 최근 RSS 피드 데이터 가져오기
    rss_docs = list(rss_collection.find().limit(limit))
    
    extracted_articles = []
    
    for rss_doc in rss_docs:
        try:
            link = rss_doc.get('link')
            if not link:
                continue
            
            # 기사 HTML 콘텐츠 가져오기
            html_content = _fetch_article_content(link)
            if not html_content:
                continue
            
            # 본문 텍스트 추출
            article_text = _extract_article_text(html_content)
            if not article_text:
                continue
            
            # 요약 생성
            summary = _generate_summary(article_text, rss_doc.get('description', ''))
            
            # 태그 추출
            tags = _extract_tags(rss_doc)
            
            # 콘텐츠 유형 분류
            content_type = _classify_content_type(rss_doc, article_text)
            
            # 언어 감지
            language = _detect_language(article_text, rss_doc)
            
            # 추출된 기사 데이터 구성
            extracted_article = {
                'guid': rss_doc.get('guid', ''),
                'source': rss_doc.get('source', ''),
                'title': rss_doc.get('title', ''),
                'link': rss_doc.get('link', ''),
                'article_text': article_text,
                'summary': summary,
                'tags': tags,
                'content_type': content_type,
                'language': language,
                'readability_score': _calculate_readability(article_text),
                'key_entities': _extract_entities(article_text),
                'processed_at': datetime.now().isoformat(),
                'text_length': len(article_text),
                'pub_date': rss_doc.get('pub_date'),
                'author': rss_doc.get('author', []),
                'description': rss_doc.get('description', ''),
                'category': rss_doc.get('category', []),
                'dc_subject': rss_doc.get('dc_subject', []),
                'rss_metadata': {
                    'original_id': str(rss_doc.get('_id')),
                    'extraction_status': 'success',
                    'html_content_length': len(html_content)
                }
            }
            
            extracted_articles.append(extracted_article)
            
        except Exception as e:
            print(f"기사 추출 실패 ({rss_doc.get('link', 'unknown')}): {e}")
            # 실패한 경우에도 기본 정보만 저장
            failed_article = {
                'guid': rss_doc.get('guid', ''),
                'source': rss_doc.get('source', ''),
                'title': rss_doc.get('title', ''),
                'link': rss_doc.get('link', ''),
                'article_text': '',
                'summary': '',
                'tags': [],
                'content_type': 'UNKNOWN',
                'language': 'UNKNOWN',
                'readability_score': 0.0,
                'key_entities': [],
                'processed_at': datetime.now().isoformat(),
                'text_length': 0,
                'pub_date': rss_doc.get('pub_date'),
                'author': rss_doc.get('author', []),
                'description': rss_doc.get('description', ''),
                'category': rss_doc.get('category', []),
                'dc_subject': rss_doc.get('dc_subject', []),
                'rss_metadata': {
                    'original_id': str(rss_doc.get('_id')),
                    'extraction_status': 'failed',
                    'error_message': str(e)
                }
            }
            extracted_articles.append(failed_article)
    
    return extracted_articles

def task_upsert_extracted_articles(ti, **_):
    """추출된 기사들을 rss_extract_articles 컬렉션에 저장"""
    extracted_articles = ti.xcom_pull(task_ids="extract_articles")
    
    if not extracted_articles:
        return {"upserted": 0, "modified": 0}
    
    client = _mongo_client()
    db = client[os.getenv("MONGO_DB", "redfin")]
    collection = db["rss_extract_articles"]
    
    # guid를 기준으로 upsert 작업 수행
    ops = [
        UpdateOne(
            {"guid": article["guid"]}, 
            {"$set": article}, 
            upsert=True
        ) for article in extracted_articles
    ]
    
    result = collection.bulk_write(ops, ordered=False)
    
    return {
        "upserted": len(result.upserted_ids), 
        "modified": result.modified_count,
        "total_processed": len(extracted_articles)
    }

def task_save_to_rss_extracted(ti, **context):
    """추출된 데이터를 rss_extracted 컬렉션에 메타데이터와 함께 저장"""
    extracted_articles = ti.xcom_pull(task_ids="extract_articles")
    upsert_result = ti.xcom_pull(task_ids="upsert_extracted_articles")
    
    if not extracted_articles:
        return {"status": "no_data", "saved_count": 0}
    
    client = _mongo_client()
    db = client[os.getenv("MONGO_DB", "redfin")]
    rss_extracted_collection = db["rss_extracted"]
    
    # 실행 메타데이터 생성
    execution_metadata = {
        "dag_id": context.get('dag').dag_id,
        "task_id": context.get('task').task_id,
        "execution_date": context.get('execution_date').isoformat(),
        "run_id": context.get('run_id'),
        "processed_at": datetime.now().isoformat(),
        "total_articles_processed": len(extracted_articles),
        "successful_extractions": len([a for a in extracted_articles if a.get('rss_metadata', {}).get('extraction_status') == 'success']),
        "failed_extractions": len([a for a in extracted_articles if a.get('rss_metadata', {}).get('extraction_status') == 'failed']),
        "upsert_stats": upsert_result or {}
    }
    
    # 각 기사에 대해 rss_extracted 문서 생성
    rss_extracted_docs = []
    for article in extracted_articles:
        # RAG용 학습 데이터 형태로 변환
        learning_data = {
            "News ID": article.get('guid', article.get('link', '')),
            "Title": article.get('title', ''),
            "Abstract": article.get('summary', ''),
            "keywords": json.dumps(_extract_keywords_from_article(article)),
            "category": _determine_category(article),
            "tags": json.dumps(article.get('tags', []))
        }
        
        # rss_extracted 문서 구성
        rss_extracted_doc = {
            "guid": article.get('guid', ''),
            "source": article.get('source', ''),
            "original_rss_id": article.get('rss_metadata', {}).get('original_id', ''),
            "learning_data": learning_data,
            "extraction_metadata": {
                "content_type": article.get('content_type'),
                "language": article.get('language'),
                "readability_score": article.get('readability_score'),
                "text_length": article.get('text_length'),
                "extraction_status": article.get('rss_metadata', {}).get('extraction_status'),
                "key_entities_count": len(article.get('key_entities', [])),
                "html_content_length": article.get('rss_metadata', {}).get('html_content_length', 0)
            },
            "pipeline_metadata": execution_metadata,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        rss_extracted_docs.append(rss_extracted_doc)
    
    # MongoDB에 저장 (upsert)
    if rss_extracted_docs:
        ops = [
            UpdateOne(
                {"guid": doc["guid"]}, 
                {"$set": doc}, 
                upsert=True
            ) for doc in rss_extracted_docs
        ]
        
        result = rss_extracted_collection.bulk_write(ops, ordered=False)
        
        return {
            "status": "success",
            "saved_count": len(rss_extracted_docs),
            "upserted_count": len(result.upserted_ids),
            "modified_count": result.modified_count,
            "execution_metadata": execution_metadata
        }
    
    return {"status": "no_documents_to_save", "saved_count": 0}

def _extract_keywords_from_article(article: Dict[str, Any]) -> List[str]:
    """기사에서 키워드 추출"""
    keywords = []
    
    # 제목에서 키워드 추출
    title = article.get('title', '')
    if title:
        # 간단한 키워드 추출 (3글자 이상 단어)
        title_words = [w.strip() for w in title.split() if len(w.strip()) >= 3][:5]
        keywords.extend(title_words)
    
    # 태그에서 키워드 추출
    tags = article.get('tags', [])
    if tags:
        keywords.extend(tags[:3])  # 최대 3개 태그
    
    # 엔티티에서 키워드 추출
    entities = article.get('key_entities', [])
    if entities:
        entity_keywords = [e.get('value', '') for e in entities if e.get('confidence', 0) > 0.7][:3]
        keywords.extend(entity_keywords)
    
    # 중복 제거 및 정리
    keywords = list(set([k.strip() for k in keywords if k.strip()]))[:10]  # 최대 10개
    
    return keywords

def _determine_category(article: Dict[str, Any]) -> str:
    """기사 카테고리 결정"""
    # 콘텐츠 타입 기반
    content_type = article.get('content_type', 'NEWS')
    if content_type != 'NEWS':
        return content_type.lower()
    
    # 기존 카테고리 사용
    categories = article.get('category', [])
    if categories and isinstance(categories, list) and categories:
        return categories[0]
    elif categories and isinstance(categories, str):
        return categories
    
    # 태그 기반 카테고리 추정
    tags = article.get('tags', [])
    if tags:
        return tags[0]
    
    return "general"

with DAG(
    dag_id=DAG_ID,
    start_date=START,
    schedule="0 */2 * * *",  # 2시간마다 실행
    catchup=False,
    tags=["redfin", "extraction", "rag"],
    description="RSS 피드에서 기사 본문을 추출하여 RAG용 컬렉션에 저장"
) as dag:

    extract_articles = PythonOperator(
        task_id="extract_articles",
        python_callable=task_extract_articles,
        doc_md="RSS 피드 데이터에서 기사 본문을 추출하고 메타데이터를 추가합니다."
    )

    upsert_extracted = PythonOperator(
        task_id="upsert_extracted_articles",
        python_callable=task_upsert_extracted_articles,
        doc_md="추출된 기사들을 rss_extract_articles 컬렉션에 저장합니다."
    )

    save_to_rss_extracted = PythonOperator(
        task_id="save_to_rss_extracted",
        python_callable=task_save_to_rss_extracted,
        outlets=[RSS_EXTRACTED],  # Dataset 신호 발생
        doc_md="추출된 데이터를 rss_extracted 컬렉션에 메타데이터와 함께 저장합니다."
    )

    extract_articles >> upsert_extracted >> save_to_rss_extracted
