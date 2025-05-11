#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
월급쟁이부자들(tab=100143, subTab=5) 크롤러 v5
──────────────────────────────────────────────────────────────
0) 로그인 처리
1) REST API → 실패 시 2) Headless 렌더링(Fallback)
2) 첨부파일(pdf/pptx/docx) 있으면 파일만, 없으면 본문·이미지 OCR
3) JSONL 체크포인트 저장
──────────────────────────────────────────────────────────────
"""

from __future__ import annotations
import re, json, uuid, time, os, sys, logging
from pathlib import Path
from typing import List, Dict, Any, Tuple
from urllib.parse import urljoin
from datetime import datetime

import cloudscraper          # ↙︎ Cloudflare 우회용
scraper = cloudscraper.create_scraper()

from bs4 import BeautifulSoup
from tqdm import tqdm

from requests_html import HTMLSession
html_session = HTMLSession()

import cv2, numpy as np
from paddleocr import PaddleOCR
ocr = PaddleOCR(lang="korean", show_log=False)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# ─── 상수 ─────────────────────────────────────────────
BASE_URL   = "https://weolbu.com"
LIST_URL   = f"{BASE_URL}/community"
SPECIFIC_LIST_URL = f"{LIST_URL}?tab=100143&subTab=5"  # 특정 탭과 서브탭이 있는 URL
API_URL    = f"{BASE_URL}/api/v1/community/posts"   # ← devtools 로 확인한 공식 API
LOGIN_URL  = f"{BASE_URL}/api/v1/auth/login"        # 로그인 API URL
TAB, SUBTAB = 100143, 5
OUT_JSONL = Path("weolbu_posts.jsonl")
CHECKPOINT_FILE = Path("checkpoint.json")

# ─── 로그인 정보 ─────────────────────────────────────────
LOGIN_ID   = "hirvahapjh@naver.com"
LOGIN_PW   = "Wuss1256!@"

UA         = "Mozilla/5.0 (WeolbuCrawler/0.5)"

# 파일 확장자 패턴 - 원본 파일만 찾기
FILE_RE    = re.compile(r"\.(pdf|pptx|docx|xlsx|xls|doc|hwp)$", re.I)

# ─── 공용 함수 ────────────────────────────────────────
def save_jsonl(recs: List[Dict[str, Any]], fname=None) -> None:
    """
    레코드를 JSONL 파일로 저장
    같은 post_id를 가진 레코드는 하나로 합쳐서 저장
    """
    if fname is None:
        fname = OUT_JSONL
    
    # 현재 파일의 기존 레코드 불러오기 (중복 저장 방지)
    existing_records = {}
    if Path(fname).exists():
        with open(fname, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    post_id = record.get("post_id")
                    if post_id:
                        existing_records[post_id] = record
                except json.JSONDecodeError:
                    pass
    
    # 체크포인트 레코드는 제외하고 게시물 레코드만 처리
    post_records = []
    
    for rec in recs:
        if "_checkpoint_page" not in rec and rec.get("post_id"):
            post_records.append(rec)
    
    # post_id별로 레코드 그룹화
    posts_by_id = {}
    for rec in post_records:
        post_id = rec.get("post_id")
        if post_id not in posts_by_id:
            posts_by_id[post_id] = {
                "post_id": post_id,
                "_download_summary": rec.get("_download_summary", "[다운로드 없음] "),
                "src": rec.get("src", ""),
                "title": rec.get("title", "")
            }
        
        # 기본 필드를 제외한 나머지 관련 정보를 post 레코드에 추가
        for key, value in rec.items():
            if key not in ["post_id", "src", "title", "_download_summary"]:
                # 다운로드 관련 정보 추가
                if key in ["has_download", "file_formats", "download_links"]:
                    posts_by_id[post_id][key] = value
                # 타입별 정보 관리
                elif key == "type":
                    if value == "download_info" and "_download_summary" in rec:
                        posts_by_id[post_id]["_download_summary"] = rec["_download_summary"]
                    posts_by_id[post_id]["type"] = value
                # 나머지 정보는 그대로 추가
                else:
                    posts_by_id[post_id][key] = value
    
    # _download_summary는 이미 초기화되어 있으므로 추가 작업 필요 없음
    
    # 파일에 추가하기 (append)
    with open(fname, "a", encoding="utf-8") as f:
        # 1. 모든 게시물 레코드 저장
        for post_id, post in posts_by_id.items():
            if post_id not in existing_records:  # 현재 크롤링에서 새로 추가된 게시물만 저장
                f.write(json.dumps(post, ensure_ascii=False) + "\n")

def save_checkpoint(page, download_summary):
    # 체크포인트 정보를 별도 파일에 저장
    checkpoint_data = {
        "page": page,
        "download_summary": download_summary,
        "timestamp": datetime.now().isoformat()
    }
    
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint_data, f, ensure_ascii=False)

def checkpoint_page():
    # 기존 크롤링 진행 상황 확인 (이어서 진행)
    try:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
                checkpoint_data = json.load(f)
                return checkpoint_data["page"] + 1
        elif Path(OUT_JSONL).exists():
            # 기존 방식 호환성 유지 (한 번만 실행됨)
            with open(OUT_JSONL, "r", encoding="utf-8") as f:
                last_checkpoint = None
                for line in f:
                    rec = json.loads(line)
                    if "_checkpoint_page" in rec:
                        last_checkpoint = rec
                
                if last_checkpoint:
                    return last_checkpoint["_checkpoint_page"] + 1
    except Exception as e:
        logging.error(f"체크포인트 확인 실패: {e}")
        
    return 1

# ─── 0) 로그인 처리 ─────────────────────────────────────
def login(driver=None) -> Tuple[Dict[str, str], webdriver.Chrome]:
    """
    로그인 처리 후 인증 토큰을 포함한 헤더와 드라이버 반환
    """
    logging.info("로그인 시도 중...")
    
    # 1. API 로그인 시도
    try:
        login_data = {
            "email": LOGIN_ID,
            "password": LOGIN_PW
        }
        r = scraper.post(LOGIN_URL, json=login_data, headers={"User-Agent": UA}, timeout=20)
        if r.status_code == 200 and "application/json" in r.headers.get("content-type", ""):
            resp = r.json()
            if "accessToken" in resp:
                token = resp["accessToken"]
                logging.info("API 로그인 성공")
                return {"User-Agent": UA, "Authorization": f"Bearer {token}"}, driver
    except Exception as e:
        logging.error(f"API 로그인 실패: {e}")
    
    # 2. 브라우저 로그인 시도 (fallback)
    logging.info("브라우저 로그인 시도 중...")
    
    # 드라이버가 없으면 새로 생성
    if driver is None:
        options = Options()
        options.headless = False  # 디버깅을 위해 헤드리스 끄기
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f'user-agent={UA}')
        
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        # 특정 커뮤니티 페이지 접근 (탭과 서브탭 포함)
        driver.get(SPECIFIC_LIST_URL)
        time.sleep(3)  # 로딩 대기
        
        # 로그인 버튼 클릭 - 정확한 선택자 사용
        login_button = driver.find_element(By.CSS_SELECTOR, "button.inline-flex.items-center.justify-center.whitespace-nowrap.rounded-md.transition-colors.focus-visible\\:outline-none.focus-visible\\:ring-1.focus-visible\\:ring-ring.disabled\\:pointer-events-none.disabled\\:opacity-50.hover\\:bg-accent.hover\\:text-accent-foreground.h-10\\.5.w-10\\.5.cursor-pointer.border-none.bg-transparent.bg-none.bg-auto.p-1.px-0.font-semibold.text-\\[\\#222222\\].no-underline.text-sm")
        driver.execute_script("arguments[0].click();", login_button)  # JavaScript 클릭 사용
        time.sleep(3)  # 로그인 모달 로딩 대기
        
        # 로그인 폼 입력 - 정확한 선택자 사용
        email_input = driver.find_element(By.CSS_SELECTOR, "input[placeholder='이메일 또는 아이디']")
        email_input.clear()
        email_input.send_keys(LOGIN_ID)
        time.sleep(1)
        
        password_input = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        password_input.clear()
        password_input.send_keys(LOGIN_PW)
        time.sleep(1)
        
        # 로그인 제출 버튼 클릭
        submit_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        driver.execute_script("arguments[0].click();", submit_button)  # JavaScript 클릭 사용
        time.sleep(5)  # 로그인 처리 대기
        
        # 쿠키 획득
        cookies = driver.get_cookies()
        headers = {"User-Agent": UA}
        
        # 쿠키를 scraper에 적용
        for cookie in cookies:
            scraper.cookies.set(cookie['name'], cookie['value'])
            
        logging.info("브라우저 로그인 성공")
        return headers, driver
    except Exception as e:
        logging.error(f"브라우저 로그인 실패: {e}")
        raise RuntimeError("로그인 실패")

# ─── 1) REST API로 글 목록 받아오기 ───────────────────
def list_posts_api(page: int, auth_headers: Dict[str, str], driver=None, size: int = 30) -> List[Tuple[str, str]]:
    """
    공식 API:  /api/v1/community/posts?tab=100143&subTab=5&page=1&size=30
    응답 예: { content: [ {id, title, ...}, ... ] }
    """
    # 브라우저를 통해 접근하는 경우
    if driver is not None:
        try:
            url = f"{LIST_URL}?tab={TAB}&subTab={SUBTAB}&page={page}"
            driver.get(url)
            time.sleep(2)  # 로딩 대기
            
            # 자바스크립트로 페이지 내용 확인
            page_content = driver.execute_script("return document.body.innerText")
            if "로그인이 필요합니다" in page_content or "로그인" in page_content and "로그아웃" not in page_content:
                logging.warning("세션이 만료되었습니다. 다시 로그인합니다.")
                auth_headers, driver = login(driver)  # 다시 로그인
                driver.get(url)  # 페이지 다시 로드
                time.sleep(2)  # 로딩 대기
            
            # 글 목록 가져오기
            links = driver.find_elements(By.CSS_SELECTOR, "a[href^='/community/']")
            posts = []
            seen = set()
            
            for link in links:
                href = link.get_attribute('href')
                title = link.text.strip()
                if href and re.match(r"^https://weolbu.com/community/\d+$", href) and href not in seen:
                    posts.append((title, href))
                    seen.add(href)
            
            if posts:
                return posts
        except Exception as e:
            logging.error(f"Browser API 실패: {e}")
            # 실패하면 일반 API로 돌아감
    
    # 일반 API 사용
    params = dict(tab=TAB, subTab=SUBTAB, page=page, size=size)
    r = scraper.get(API_URL, params=params, headers=auth_headers, timeout=20)
    if r.status_code != 200 or "application/json" not in r.headers.get("content-type", ""):
        raise RuntimeError(f"API 실패: {r.status_code}")
    items = r.json().get("content", [])
    return [
        (item["title"], urljoin(BASE_URL, f"/community/{item['id']}"))
        for item in items
    ]

# ─── 2) JS 렌더링 Fallback ───────────────────────────
def list_posts_render(page: int, auth_headers: Dict[str, str]) -> List[Tuple[str, str]]:
    options = Options()
    options.headless = True
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'user-agent={UA}')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    url = f"{LIST_URL}?tab={TAB}&subTab={SUBTAB}&page={page}"
    driver.get(url)
    time.sleep(3)  # 로딩 대기

    links = driver.find_elements(By.CSS_SELECTOR, "a[href^='/community/']")
    posts = []
    seen = set()

    for link in links:
        href = link.get_attribute('href')
        title = link.text.strip()
        if href and re.match(r"^https://weolbu.com/community/\d+$", href) and href not in seen:
            posts.append((title, href))
            seen.add(href)

    driver.quit()
    return posts

def list_posts(page: int, auth_headers: Dict[str, str], driver=None) -> List[Tuple[str, str]]:
    """API → 실패 시 렌더링 fallback"""
    try:
        return list_posts_api(page, auth_headers, driver)
    except Exception as e:
        logging.debug(f"[list_posts] API 실패, 렌더링 fallback: {e}")
        return list_posts_render(page, auth_headers)

# ─── 3) 이미지 OCR ───────────────────────────────────
def ocr_image(url: str) -> str:
    try:
        img_bytes = scraper.get(url, timeout=30).content
        arr = np.frombuffer(img_bytes, np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        res = ocr.ocr(img, cls=True)
        return " ".join(x[1][0] for x in res[0]) if res else ""
    except Exception:
        return ""

# ─── 4) 첨부파일 파서 ─────────────────────
def parse_pdf(url: str, fname: str, pid: str) -> List[Dict[str, Any]]:
    """
    PDF 파일 텍스트 추출
    """
    # 실제 구현에서는 PDF 파일을 다운로드하고 텍스트를 추출하는 과정이 필요
    # 현재는 다운로드 링크만 제공
    return [{
        "post_id": pid,
        "type": "pdf_extract",
        "filename": fname,
        "content": f"PDF 파일 다운로드 링크: {url}\n파일명: {fname}"
    }]

def parse_pptx(url: str, fname: str, pid: str) -> List[Dict[str, Any]]:
    """
    PPTX 파일 텍스트 추출
    """
    # 실제 구현에서는 PPTX 파일을 다운로드하고 텍스트를 추출하는 과정이 필요
    # 현재는 다운로드 링크만 제공
    return [{
        "post_id": pid,
        "type": "pptx_extract",
        "filename": fname,
        "content": f"PowerPoint 파일 다운로드 링크: {url}\n파일명: {fname}"
    }]

def parse_docx(url: str, fname: str, pid: str) -> List[Dict[str, Any]]:
    """
    DOCX 파일 텍스트 추출
    """
    # 실제 구현에서는 DOCX 파일을 다운로드하고 텍스트를 추출하는 과정이 필요
    # 현재는 다운로드 링크만 제공
    return [{
        "post_id": pid,
        "type": "docx_extract",
        "filename": fname,
        "content": f"Word 파일 다운로드 링크: {url}\n파일명: {fname}"
    }]

def parse_hwp(url: str, fname: str, pid: str) -> List[Dict[str, Any]]:
    """
    HWP 파일 텍스트 추출
    """
    # 실제 구현에서는 HWP 파일을 다운로드하고 텍스트를 추출하는 과정이 필요
    # 현재는 다운로드 링크만 제공
    return [{
        "post_id": pid,
        "type": "hwp_extract",
        "filename": fname,
        "content": f"HWP 파일 다운로드 링크: {url}\n파일명: {fname}"
    }]

# ─── 4) 파일 추출 및 처리 ─────────────────────────────
def parse_file(url: str, pid: str, fname: str, auth_headers: Dict[str, str], driver=None) -> List[Dict[str, Any]]:
    """
    파일 추출 및 처리
    """
    file_ext = os.path.splitext(fname)[1].lower()
    if file_ext == ".pdf":
        return parse_pdf(url, fname, pid)
    elif file_ext == ".pptx":
        return parse_pptx(url, fname, pid)
    elif file_ext == ".docx":
        return parse_docx(url, fname, pid)
    elif file_ext == ".hwp":
        return parse_hwp(url, fname, pid)
    else:
        return []
    # 텍스트로 다운로드 버튼 찾기
    download_buttons = soup.find_all(string=re.compile(r'다운로드|download', re.IGNORECASE))
    
    # 특별히 span 태그와 같은 특정 클래스를 가진 다운로드 버튼 찾기
    span_buttons = soup.find_all('span', class_=re.compile(r'text-sm|font-semibold|download|btn'))
    for span in span_buttons:
        if '다운로드' in span.text or 'download' in span.text.lower():
            download_buttons.append(span)
    
    # 버튼 태그 찾기
    button_tags = soup.find_all('button')
    for button in button_tags:
        if '다운로드' in button.text or 'download' in button.text.lower():
            download_buttons.append(button)
    
    # 다운로드 버튼 처리
    for button in download_buttons:
        # 다운로드 버튼 발견
        result["has_download"] = True
        result["download_buttons"].append({
            "text": button.text.strip() if hasattr(button, 'text') else button.strip(),
            "element": button.name if hasattr(button, 'name') else button.parent.name
        })
        
        # 버튼의 부모가 링크인지 확인
        parent = button.parent
        if parent and parent.name == 'a' and parent.get('href'):
            href = parent.get('href')
            full_url = href if href.startswith('http') else urljoin(url, href)
            result["download_links"].append({
                "url": full_url,
                "text": button.text.strip() if hasattr(button, 'text') else button.strip()
            })
            
            # 파일 형식 추출
            ext_match = file_ext_pattern.search(href)
            if ext_match and ext_match.group(1).lower() not in result["file_formats"]:
                result["file_formats"].append(ext_match.group(1).lower())
        
        # 버튼 자체가 링크인지 확인
        if hasattr(button, 'name') and button.name == 'a' and button.get('href'):
            href = button.get('href')
            full_url = href if href.startswith('http') else urljoin(url, href)
            result["download_links"].append({
                "url": full_url,
                "text": button.text.strip()
            })
            
            # 파일 형식 추출
            ext_match = file_ext_pattern.search(href)
            if ext_match and ext_match.group(1).lower() not in result["file_formats"]:
                result["file_formats"].append(ext_match.group(1).lower())
    
    # 2. 첨부파일 검색
    attachment_elements = soup.find_all(string=re.compile(r'첨부파일|첨부|attachment', re.IGNORECASE))
    for element in attachment_elements:
        parent = element.parent
        if parent:
            result["has_download"] = True
            # 첨부파일 발견
            if parent.name == 'a' and parent.get('href'):
                href = parent.get('href')
                full_url = href if href.startswith('http') else urljoin(url, href)
                result["download_links"].append({
                    "url": full_url,
                    "text": element.strip()
                })
                
                # 파일 형식 추출
                ext_match = file_ext_pattern.search(href)
                if ext_match and ext_match.group(1).lower() not in result["file_formats"]:
                    result["file_formats"].append(ext_match.group(1).lower())
    
    # 3. 다운로드 가능한 파일 확장자를 가진 링크 검색
    for a_tag in soup.find_all('a', href=True):
        href = a_tag.get('href', '')
        ext_match = file_ext_pattern.search(href)
        if ext_match:
            file_ext = ext_match.group(1).lower()
            if file_ext not in result["file_formats"]:
                result["has_download"] = True
                result["file_formats"].append(file_ext)
                full_url = href if href.startswith('http') else urljoin(url, href)
                result["download_links"].append({
                    "url": full_url,
                    "text": a_tag.get_text(strip=True) or f"{file_ext} 파일"
                })
    
    # 4. 파일 형식을 추출했지만 링크가 없는 경우, 이미지 파일 제거
    if "jpg" in result["file_formats"]:
        result["file_formats"].remove("jpg")
    if "jpeg" in result["file_formats"]:
        result["file_formats"].remove("jpeg")
    if "png" in result["file_formats"]:
        result["file_formats"].remove("png")
    if "gif" in result["file_formats"]:
        result["file_formats"].remove("gif")
    
    # 5. 푸터에 있는 인증서 PDF 제거 (모든 페이지에 나타나는 공통 요소)
    for i in range(len(result["download_links"]) - 1, -1, -1):
        link = result["download_links"][i]
        if "certificate" in link["url"] and "원격평생교육원" in link["text"]:
            result["download_links"].pop(i)
            # PDF 파일 형식이 인증서만 있었다면 제거
            if "pdf" in result["file_formats"] and len(result["file_formats"]) == 1:
                result["file_formats"].remove("pdf")
    
    # 6. 실제 다운로드 버튼 확인 (중요)
    has_real_download_button = False
    for button in result["download_buttons"]:
        # 다운로드 버튼이 단독으로 존재하는지 확인
        if button["text"] == "다운로드" or button["text"] == "download":
            has_real_download_button = True
            break
    
    # 7. 다운로드 버튼이 있지만 파일 형식이 없는 경우
    if has_real_download_button and not result["file_formats"]:
        result["file_formats"] = ["pptx"]  # 대부분 PPT 파일이므로 기본값으로 설정
        result["has_download"] = True
    elif not has_real_download_button and not result["file_formats"]:
        # 실제 다운로드 버튼이 없고 파일 형식도 없으면 다운로드 없음
        result["has_download"] = False
        result["download_links"] = []
        result["download_buttons"] = []
    
    return result

def check_for_downloads(driver, url, pid) -> Dict[str, Any]:
    """
    브라우저를 통해 다운로드 버튼과 파일을 찾는 함수
    """
    result = {
        "has_download": False,
        "file_formats": [],
        "download_links": [],
        "download_buttons": []
    }
    
    try:
        # 1. 다운로드 버튼 찾기
        download_buttons = driver.find_elements(By.XPATH, "//span[contains(text(), '다운로드')]") + \
                          driver.find_elements(By.XPATH, "//a[contains(text(), '다운로드')]") + \
                          driver.find_elements(By.XPATH, "//button[contains(text(), '다운로드')]") + \
                          driver.find_elements(By.XPATH, "//div[contains(text(), '다운로드')]")
        
        # 2. 파일 확장자를 가진 링크 찾기
        file_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.pptx') or contains(@href, '.pdf') or contains(@href, '.docx') or contains(@href, '.hwp')]")
        
        # 3. 다운로드 버튼 처리
        for button in download_buttons:
            button_text = button.text.strip()
            if not button_text:
                continue
                
            result["has_download"] = True
            result["download_buttons"].append({
                "text": button_text,
                "element": button.tag_name
            })
            
            # 버튼이 링크인 경우
            if button.tag_name == "a":
                href = button.get_attribute("href")
                if href:
                    result["download_links"].append({
                        "url": href,
                        "text": button_text
                    })
                    
                    # 파일 형식 추출
                    file_ext = os.path.splitext(href.split("?")[0])[1].lower()
                    if file_ext and file_ext[1:] not in result["file_formats"] and file_ext[1:] not in ["jpg", "jpeg", "png", "gif"]:
                        result["file_formats"].append(file_ext[1:])
        
        # 4. 파일 링크 처리
        for link in file_links:
            href = link.get_attribute("href")
            if not href:
                continue
                
            # 인증서 PDF 제외
            if "certificate" in href and "원격평생교육원" in link.text:
                continue
                
            result["has_download"] = True
            result["download_links"].append({
                "url": href,
                "text": link.text.strip() or os.path.basename(href.split("?")[0])
            })
            
            # 파일 형식 추출
            file_ext = os.path.splitext(href.split("?")[0])[1].lower()
            if file_ext and file_ext[1:] not in result["file_formats"] and file_ext[1:] not in ["jpg", "jpeg", "png", "gif"]:
                result["file_formats"].append(file_ext[1:])
        
        # 5. 다운로드 버튼이 있지만 파일 형식이 없는 경우
        if result["download_buttons"] and not result["file_formats"]:
            result["file_formats"] = ["pptx"]  # 대부분 PPT 파일이므로 기본값으로 설정
        
        logging.info(f"[페이지 {pid}] 다운로드 검색 결과: {result['has_download']}, 파일 형식: {result['file_formats']}")
        
    except Exception as e:
        logging.error(f"[페이지 {pid}] 다운로드 검색 오류: {e}")
    
    return result

def check_for_downloads_api(soup, url, pid) -> Dict[str, Any]:
    """
    BeautifulSoup을 통해 다운로드 버튼과 파일을 찾는 함수
    """
    result = {
        "has_download": False,
        "file_formats": [],
        "download_links": [],
        "download_buttons": []
    }
    
    try:
        # 1. 다운로드 버튼 찾기
        download_buttons = soup.find_all(string=re.compile(r'다운로드|download', re.IGNORECASE))
        
        # 2. 파일 확장자를 가진 링크 찾기
        file_links = soup.find_all('a', href=re.compile(r'\.(pdf|pptx?|docx?|hwp)($|\?)', re.IGNORECASE))
        
        # 3. 다운로드 버튼 처리
        for button in download_buttons:
            parent = button.parent
            if not parent:
                continue
                
            result["has_download"] = True
            result["download_buttons"].append({
                "text": button.strip(),
                "element": parent.name
            })
            
            # 버튼이 링크인 경우
            if parent.name == "a" and parent.get('href'):
                href = parent.get('href')
                full_url = href if href.startswith('http') else urljoin(url, href)
                
                # 인증서 PDF 제외
                if "certificate" in full_url and "원격평생교육원" in button.strip():
                    continue
                    
                result["download_links"].append({
                    "url": full_url,
                    "text": button.strip()
                })
                
                # 파일 형식 추출
                file_ext = os.path.splitext(full_url.split("?")[0])[1].lower()
                if file_ext and file_ext[1:] not in result["file_formats"] and file_ext[1:] not in ["jpg", "jpeg", "png", "gif"]:
                    result["file_formats"].append(file_ext[1:])
        
        # 4. 파일 링크 처리
        for link in file_links:
            href = link.get('href')
            if not href:
                continue
                
            full_url = href if href.startswith('http') else urljoin(url, href)
            
            # 인증서 PDF 제외
            if "certificate" in full_url and "원격평생교육원" in link.get_text(strip=True):
                continue
                
            result["has_download"] = True
            result["download_links"].append({
                "url": full_url,
                "text": link.get_text(strip=True) or os.path.basename(full_url.split("?")[0])
            })
            
            # 파일 형식 추출
            file_ext = os.path.splitext(full_url.split("?")[0])[1].lower()
            if file_ext and file_ext[1:] not in result["file_formats"] and file_ext[1:] not in ["jpg", "jpeg", "png", "gif"]:
                result["file_formats"].append(file_ext[1:])
        
        # 5. 다운로드 버튼이 있지만 파일 형식이 없는 경우
        if result["download_buttons"] and not result["file_formats"]:
            result["file_formats"] = ["pptx"]  # 대부분 PPT 파일이므로 기본값으로 설정
        
        logging.info(f"[페이지 {pid}] API 다운로드 검색 결과: {result['has_download']}, 파일 형식: {result['file_formats']}")
        
    except Exception as e:
        logging.error(f"[페이지 {pid}] API 다운로드 검색 오류: {e}")
    
    return result

# ─── 5) 게시글 상세 파싱 ─────────────────────────────
def parse_post(url: str, title: str, pid: str, auth_headers: Dict[str, str], driver=None) -> List[Dict[str, Any]]:
    # 다운로드 요약 초기화
    download_summary = "[다운로드 없음] "
    
    # Basic post information addition
    recs = [{
        "_download_summary": download_summary,
        "post_id": pid,
        "src": url,
        "title": title,
        "type": "post_info"
    }]
    
    # 브라우저를 통해 게시물 내용 가져오기 (동적 콘텐츠 처리를 위해)
    if driver is not None:
        try:
            # 페이지 로드
            driver.get(url)
            time.sleep(3)  # 동적 콘텐츠가 로드될 시간 여유
            
            # 로그인 상태 확인
            page_content = driver.execute_script("return document.body.innerText")
            if "로그인이 필요합니다" in page_content or ("로그인" in page_content and "로그아웃" not in page_content):
                logging.warning(f"[페이지 {pid}] 세션이 만료되었습니다. 다시 로그인합니다.")
                auth_headers, driver = login(driver)  # 다시 로그인
                driver.get(url)  # 페이지 다시 로드
                time.sleep(3)  # 로딩 대기
            
            # 파일 다운로드 감지
            download_info = check_for_downloads(driver, url, pid)
            if download_info["has_download"]:
                formats_str = ", ".join(download_info["file_formats"]) if download_info["file_formats"] else "Unknown"
                logging.info(f"[페이지 {pid}] 다운로드 파일 발견: {formats_str}")
                
                # 다운로드 요약 업데이트
                download_summary = f"[다운로드 파일: {formats_str}] "
                recs[0]["_download_summary"] = download_summary
                
                # 다운로드 정보 추가
                download_info_rec = {
                    "post_id": pid,
                    "src": url,
                    "title": title,
                    "type": "download_info",
                    "_download_summary": download_summary,
                    "has_download": True,
                    "file_formats": download_info["file_formats"],
                    "download_links": download_info["download_links"]
                }
                recs.append(download_info_rec)
                
                # 파일 처리
                for link in download_info["download_links"]:
                    try:
                        download_url = link["url"]
                        
                        # 인증서 PDF 제외
                        if "certificate" in download_url:
                            continue
                            
                        filename = os.path.basename(download_url.split("?")[0])
                        if not filename:
                            filename = f"{link['text']}.pptx"
                            
                        file_ext = os.path.splitext(filename)[1].lower()
                        if file_ext == ".pdf":
                            file_recs = parse_pdf(download_url, filename, pid)
                            for rec in file_recs:
                                rec["_download_summary"] = download_summary
                            recs.extend(file_recs)
                        elif file_ext == ".pptx" or file_ext == ".ppt":
                            file_recs = parse_pptx(download_url, filename, pid)
                            for rec in file_recs:
                                rec["_download_summary"] = download_summary
                            recs.extend(file_recs)
                        elif file_ext == ".docx" or file_ext == ".doc":
                            file_recs = parse_docx(download_url, filename, pid)
                            for rec in file_recs:
                                rec["_download_summary"] = download_summary
                            recs.extend(file_recs)
                        elif file_ext == ".hwp":
                            file_recs = parse_hwp(download_url, filename, pid)
                            for rec in file_recs:
                                rec["_download_summary"] = download_summary
                            recs.extend(file_recs)
                    except Exception as e:
                        logging.error(f"[페이지 {pid}] 파일 처리 오류: {e}")
            else:
                # 다운로드 정보 추가
                download_info_rec = {
                    "post_id": pid,
                    "src": url,
                    "title": title,
                    "type": "download_info",
                    "_download_summary": download_summary,
                    "has_download": False
                }
                recs.append(download_info_rec)
            
            # 이미지 요소 찾기 (여러 가지 선택자 시도)
            try:
                # 이미지 처리
                images = driver.find_elements(By.CSS_SELECTOR, ".post-content img, .view-content img, .content img, article img, .fr-view img, .fr-element img")
                for idx, img in enumerate(images):
                    try:
                        src = img.get_attribute("src")
                        if src and not src.startswith("data:") and not src.endswith(".svg"):
                            img_url = src if src.startswith("http") else urljoin(BASE_URL, src)
                            logging.info(f"[페이지 {pid}] 이미지 발견: {img_url}")
                            recs.append({
                                "post_id": pid,
                                "src": url,
                                "title": title,
                                "type": "image",
                                "idx": idx,
                                "img_url": img_url
                            })
                    except Exception as img_err:
                        logging.error(f"[페이지 {pid}] 이미지 처리 오류: {img_err}")
            except Exception as img_section_err:
                logging.error(f"[페이지 {pid}] 이미지 섹션 처리 오류: {img_section_err}")
            
            # 텍스트 콘텐츠 처리
            try:
                # 여러 가지 선택자를 시도하여 본문 콘텐츠 찾기
                content_selectors = [
                    ".post-content", ".view-content", ".content", "article", ".fr-view", ".fr-element",
                    "#post-content", "#view-content", "#content", ".viewer_content", ".board-content"
                ]
                
                for selector in content_selectors:
                    content_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if content_elements:
                        for element in content_elements:
                            text = element.text.strip()
                            if text and len(text) > 50:  # 의미 있는 텍스트만 추출
                                logging.info(f"[페이지 {pid}] 본문 발견: {selector} ({len(text)} 글자)")
                                recs.append({
                                    "post_id": pid,
                                    "src": url,
                                    "title": title,
                                    "type": "text_content",
                                    "content": text,
                                    "selector": selector
                                })
                                break  # 텍스트를 찾았으므로 더 이상 찾지 않음
                        if any(r.get("type") == "text_content" for r in recs):
                            break  # 텍스트를 찾았으므로 더 이상 선택자를 시도하지 않음
                
                # 텍스트를 찾지 못한 경우 페이지 전체 텍스트를 추출
                if not any(r.get("type") == "text_content" for r in recs):
                    # 전체 페이지 텍스트 추출
                    body_text = driver.find_element(By.TAG_NAME, "body").text
                    if body_text and len(body_text) > 100:
                        logging.info(f"[페이지 {pid}] 본문 발견: body ({len(body_text)} 글자)")
                        recs.append({
                            "post_id": pid,
                            "src": url,
                            "title": title,
                            "type": "text_content",
                            "content": body_text,
                            "selector": "body"
                        })
            except Exception as text_err:
                logging.error(f"[페이지 {pid}] 텍스트 처리 오류: {text_err}")
            
            # 다운로드 버튼이 있는지 확인하고 파일 처리
            download_buttons = driver.find_elements(By.XPATH, "//span[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//a[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//button[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//div[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//a[contains(@href, '.pptx') or contains(@href, '.pdf') or contains(@href, '.docx') or contains(@href, '.hwp')]")                              
            
            if download_buttons:
                for button in download_buttons:
                    try:
                        # 버튼 텍스트 추출
                        button_text = button.text.strip()
                        
                        # 다운로드 URL 추출
                        download_url = ""
                        filename = ""
                        
                        # 버튼이 링크인 경우
                        if button.tag_name == "a":
                            download_url = button.get_attribute("href")
                            # 파일명 추출
                            filename = os.path.basename(download_url.split("?")[0])
                        # 버튼이 다른 요소인 경우 부모 요소 확인
                        else:
                            try:
                                parent = button.find_element(By.XPATH, "./ancestor::a")
                                if parent:
                                    download_url = parent.get_attribute("href")
                                    filename = os.path.basename(download_url.split("?")[0])
                            except Exception as e:
                                logging.debug(f"[페이지 {pid}] 부모 요소 찾기 오류: {e}")
                        
                        # 파일명이 없으면 버튼 텍스트를 파일명으로 사용
                        if not filename and button_text:
                            filename = f"{button_text}.pptx"  # 기본적으로 PPTX로 가정
                    except Exception as e:
                        logging.error(f"[페이지 {pid}] 버튼 처리 오류: {e}")
                
                # HTML 구조 분석
                logging.warning(f"[페이지 {pid}] 콘텐츠를 찾을 수 없습니다. HTML 구조를 분석합니다.")
                
                # BeautifulSoup 객체 생성
                try:
                    html_content = driver.page_source
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    # 페이지의 모든 div 요소 검색
                    for div in soup.find_all("div", class_=True):
                        class_name = div.get("class", [])
                        if class_name:
                            class_str = " ".join(class_name)
                            text = div.get_text(strip=True)
                            if text and len(text) > 100 and ("content" in class_str.lower() or "post" in class_str.lower() or "view" in class_str.lower()):
                                logging.info(f"[페이지 {pid}] 추가 분석으로 콘텐츠 발견: div.{class_str}")
                                recs.append({
                                    "post_id": pid,
                                    "src": url,
                                    "title": title,
                                    "type": "text_content",
                                    "content": text,
                                    "selector": f"div.{class_str}"
                                })
                                break
                except Exception as soup_err:
                    logging.error(f"[페이지 {pid}] BeautifulSoup 처리 오류: {soup_err}")
                    # 오류 기록 추가
                    recs.append({
                        "post_id": pid,
                        "src": url,
                        "title": title,
                        "type": "error",
                        "_download_summary": download_summary,
                        "message": f"BeautifulSoup 처리 오류: {soup_err}"
                    })
                
                # 여전히 콘텐츠를 찾지 못한 경우
                if not any(r.get("type") in ["text_content", "image"] for r in recs):
                    recs.append({
                        "post_id": pid,
                        "src": url,
                        "title": title,
                        "type": "error",
                        "message": "Content not found after extensive analysis"
                    })
        except Exception as e:
            logging.error(f"[페이지 {pid}] 브라우저 처리 오류: {e}")
            recs.append({
                "post_id": pid,
                "src": url,
                "title": title,
                "type": "error",
                "message": f"Browser processing error: {str(e)}"
            })
    else:
        # 브라우저가 없는 경우 API를 통해 시도
        try:
            html = scraper.get(url, headers=auth_headers, timeout=20).text
            
            # HTML 파싱
            soup = BeautifulSoup(html, "html.parser")
            
            # 파일 다운로드 감지
            download_info = check_for_downloads_api(soup, url, pid)
            if download_info["has_download"]:
                formats_str = ", ".join(download_info["file_formats"]) if download_info["file_formats"] else "Unknown"
                logging.info(f"[페이지 {pid}] 다운로드 파일 발견 (API): {formats_str}")
                
                # 다운로드 요약 업데이트
                download_summary = f"[다운로드 파일: {formats_str}] "
                recs[0]["_download_summary"] = download_summary
                
                # 다운로드 정보 추가
                download_info_rec = {
                    "post_id": pid,
                    "src": url,
                    "title": title,
                    "type": "download_info",
                    "_download_summary": download_summary,
                    "has_download": True,
                    "file_formats": download_info["file_formats"],
                    "download_links": download_info["download_links"]
                }
                recs.append(download_info_rec)
                
                # 파일 처리
                for link in download_info["download_links"]:
                    try:
                        download_url = link["url"]
                        
                        # 인증서 PDF 제외
                        if "certificate" in download_url:
                            continue
                            
                        filename = os.path.basename(download_url.split("?")[0])
                        if not filename:
                            filename = f"{link['text']}.pptx"
                            
                        file_ext = os.path.splitext(filename)[1].lower()
                        if file_ext == ".pdf":
                            file_recs = parse_pdf(download_url, filename, pid)
                            for rec in file_recs:
                                rec["_download_summary"] = download_summary
                            recs.extend(file_recs)
                        elif file_ext == ".pptx" or file_ext == ".ppt":
                            file_recs = parse_pptx(download_url, filename, pid)
                            for rec in file_recs:
                                rec["_download_summary"] = download_summary
                            recs.extend(file_recs)
                        elif file_ext == ".docx" or file_ext == ".doc":
                            file_recs = parse_docx(download_url, filename, pid)
                            for rec in file_recs:
                                rec["_download_summary"] = download_summary
                            recs.extend(file_recs)
                        elif file_ext == ".hwp":
                            file_recs = parse_hwp(download_url, filename, pid)
                            for rec in file_recs:
                                rec["_download_summary"] = download_summary
                            recs.extend(file_recs)
                    except Exception as e:
                        logging.error(f"[페이지 {pid}] 파일 처리 오류 (API): {e}")
            else:
                # 다운로드 정보 추가
                download_info_rec = {
                    "post_id": pid,
                    "src": url,
                    "title": title,
                    "type": "download_info",
                    "_download_summary": download_summary,
                    "has_download": False
                }
                recs.append(download_info_rec)
            
            soup = BeautifulSoup(html, "html.parser")
            
            # 다양한 선택자로 콘텐츠 찾기 시도
            content_selectors = [
                "div.post-content", "div.view-content", "div.content", "article.post", 
                "div.fr-view", "div.fr-element", "#post-content", "#view-content", 
                "#content", ".viewer_content", ".board-content"
            ]
            
            content_found = False
            for selector in content_selectors:
                content_div = soup.select_one(selector)
                if content_div:
                    # 본문 텍스트 추출
                    text_content = content_div.get_text(" ", strip=True)
                    if text_content and len(text_content) > 50:  # 의미 있는 텍스트만 추출
                        recs.append({
                            "post_id": pid,
                            "src": url,
                            "title": title,
                            "type": "text_content",
                            "content": text_content,
                            "selector": selector
                        })
                        content_found = True
                    
                    # 이미지 추출
                    images = content_div.find_all("img")
                    for idx, img in enumerate(images):
                        src = img.get("src", "")
                        if src and not src.startswith("data:") and not src.endswith(".svg"):
                            img_url = src if src.startswith("http") else urljoin(BASE_URL, src)
                            recs.append({
                                "post_id": pid,
                                "src": url,
                                "title": title,
                                "type": "image",
                                "idx": idx,
                                "img_url": img_url
                            })
                            content_found = True
                
                if content_found:
                    break
            
            # 다운로드 버튼 및 파일 검색
            download_links = []
            
            # 다운로드 버튼 찾기
            download_buttons = soup.find_all(string=re.compile(r'다운로드|download', re.IGNORECASE))
            for button in download_buttons:
                parent = button.parent
                if parent and parent.name == 'a' and parent.get('href'):
                    download_links.append((parent.get('href'), button.strip()))
            
            # 파일 확장자를 가진 링크 찾기
            file_links = soup.find_all('a', href=re.compile(r'\.(pdf|pptx?|docx?|hwp)($|\?)', re.IGNORECASE))
            for link in file_links:
                href = link.get('href')
                text = link.get_text(strip=True)
                download_links.append((href, text or os.path.basename(href)))
            
            # 파일 처리
            for href, text in download_links:
                try:
                    # 상대 URL을 절대 URL로 변환
                    download_url = href if href.startswith('http') else urljoin(url, href)
                    
                    # 파일명 추출
                    filename = os.path.basename(download_url.split('?')[0])
                    if not filename and text:
                        filename = f"{text}.pptx"  # 기본적으로 PPTX로 가정
                    
                    file_ext = os.path.splitext(filename)[1].lower()
                    if file_ext == ".pdf":
                        file_recs = parse_pdf(download_url, filename, pid)
                        for rec in file_recs:
                            rec["_download_summary"] = download_summary
                        recs.extend(file_recs)
                    elif file_ext == ".pptx" or file_ext == ".ppt":
                        file_recs = parse_pptx(download_url, filename, pid)
                        for rec in file_recs:
                            rec["_download_summary"] = download_summary
                        recs.extend(file_recs)
                    elif file_ext == ".docx" or file_ext == ".doc":
                        file_recs = parse_docx(download_url, filename, pid)
                        for rec in file_recs:
                            rec["_download_summary"] = download_summary
                        recs.extend(file_recs)
                    elif file_ext == ".hwp":
                        file_recs = parse_hwp(download_url, filename, pid)
                        for rec in file_recs:
                            rec["_download_summary"] = download_summary
                        recs.extend(file_recs)
                except Exception as e:
                    logging.error(f"[페이지 {pid}] 파일 처리 오류 (API): {e}")
            # 콘텐츠를 찾지 못한 경우
            recs.append({
                "post_id": pid,
                "src": url,
                "title": title,
                "type": "error",
                "message": "Content not found with API method"
            })
        except Exception as e:
            # 오류 발생 시 처리
            recs.append({
                "post_id": pid,
                "src": url,
                "title": title,
                "type": "error",
                "message": f"API error: {str(e)}"
            })
    
    return recs

# ─── 6) 메인 루프 ───────────────────────────────────
def crawl():
    # 브라우저 생성
    options = Options()
    options.headless = False  # 브라우저 창을 보이게 설정
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f'user-agent={UA}')
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        # 로그인 처리 및 인증 헤더 획득
        auth_headers, driver = login(driver)
        
        page = checkpoint_page()
        pbar = tqdm(desc="Page", initial=page-1)
        while True:
            posts = list_posts(page, auth_headers, driver)
            if not posts:
                break
            for title, link in tqdm(posts, desc=f"Posts p{page}", leave=False):
                pid = re.search(r"/community/(\d+)", link).group(1)
                recs = parse_post(link, title, pid, auth_headers, driver)
                # Get the download_summary from the first record if available
                download_summary = "[다운로드 없음] "
                
                # Try to find _download_summary in any record
                for rec in recs:
                    if "_download_summary" in rec:
                        download_summary = rec["_download_summary"]
                        break
                
                # 체크포인트 정보를 별도로 저장하고, JSONL에는 게시물 정보만 저장
                save_checkpoint(page, download_summary)
                save_jsonl(recs)
            page += 1
            pbar.update(1)
            time.sleep(1)  # polite delay
        pbar.close()
    finally:
        # 크롤링이 끝나면 브라우저 종료
        driver.quit()

# ─── 실행 ────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    crawl()
    print(f"✅ 완료 → {OUT_JSONL.resolve()}")
