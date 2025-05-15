#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main crawler class for real estate crawler
"""

import re
import os
import time
import logging
from typing import List, Dict, Any, Tuple

from bs4 import BeautifulSoup

import cloudscraper
from tqdm import tqdm
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from requests_html import HTMLSession

from src.config import Config
from src.crawler.auth import Authenticator
from src.parser.parser import ContentParser, ListParser
from src.models.models import Post
from src.storage.file_processor import FileProcessor, DownloadDetector
from src.storage.storage import JsonlStorage, CheckpointManager


# Patterns of words that indicate non-downloadable or irrelevant resources
# Centralised so that filtering rules stay consistent across the crawler.
EXCLUDE_PATTERNS = [
    "이미지", "사진", "갤러리", "썸네일", "미리보기", "광고", "배너", "로고",
    "certificate", "원격평생교육원", "인증서", "자격증", "수료증", "교육이수증",
    "학위증", "졸업증명서", "증명서", "인증", "logo", "banner", "thumbnail"
]


class Crawler:
    """Main crawler class that orchestrates the crawling process"""
    
    def __init__(self):
        """Initialize the crawler with all required components"""
        # Initialize core components
        self.scraper = cloudscraper.create_scraper()
        self.html_session = HTMLSession()
        
        # Create driver
        self.driver = self._create_driver()
        
        # Initialize component instances
        self.authenticator = Authenticator()
        self.content_parser = ContentParser(scraper=self.scraper, html_session=self.html_session)
        self.list_parser = ListParser(scraper=self.scraper)
        self.file_processor = FileProcessor(scraper=self.scraper)
        self.download_detector = DownloadDetector()
        self.storage = JsonlStorage()
        self.checkpoint_manager = CheckpointManager()
        
        # Authentication state
        self.auth_headers = None
    
    def crawl(self):
        """Main crawling method"""
        try:
            # Login and get authentication headers
            self.auth_headers, self.driver = self.authenticator.login()
            
            # Get last processed page from checkpoint
            page = self.checkpoint_manager.get_last_page()
            
            # Create progress bar
            pbar = tqdm(desc="Page", initial=page-1)
            
            # Process pages
            while True:
                # Get post list for current page
                try:
                    posts = self.list_parser.parse_list_api(page, self.auth_headers, self.driver)
                except Exception as e:
                    logging.error(f"Page {page} failed: {e}")
                    break
                
                # Break if no posts found
                if not posts:
                    logging.info(f"No posts found on page {page}, ending crawl")
                    break
                
                # Process each post
                for title, link in tqdm(posts, desc=f"Posts p{page}", leave=False):
                    # Extract post ID from URL
                    pid_match = re.search(r"/community/(\d+)", link)
                    if not pid_match:
                        logging.warning(f"Could not extract post ID from {link}")
                        continue
                        
                    pid = pid_match.group(1)
                    
                    # Parse post
                    post_records = self._parse_post(link, title, pid)
                    
                    # Get download summary
                    download_summary = "[다운로드 없음] "
                    for rec in post_records:
                        if "_download_summary" in rec:
                            download_summary = rec["_download_summary"]
                            break
                    
                    # Save checkpoint and posts
                    self.checkpoint_manager.save(page, download_summary)
                    self.storage.save_posts(post_records)
                
                # Go to next page
                page += 1
                pbar.update(1)
                time.sleep(Config.WAIT_BETWEEN_PAGES)  # Polite delay
            
            pbar.close()
            
        finally:
            # Clean up
            if self.driver:
                self.driver.quit()
    
    def _parse_post(self, url: str, title: str, pid: str) -> List[Dict[str, Any]]:
        """
        Parse a single post
        
        Args:
            url: Post URL
            title: Post title
            pid: Post ID
            
        Returns:
            List of post records for storage
        """
        # Initialize post object
        post = Post(post_id=pid, title=title, url=url)
        
        # Use driver if available
        if self.driver:
            try:
                # Load the page
                self.driver.get(url)
                time.sleep(Config.WAIT_PAGE_LOAD)
                
                # Check if login required
                page_content = self.driver.execute_script("return document.body.innerText")
                if "로그인이 필요합니다" in page_content or ("로그인" in page_content and "로그아웃" not in page_content):
                    logging.warning(f"[페이지 {pid}] 세션이 만료되었습니다. 다시 로그인합니다.")
                    self.auth_headers, self.driver = self.authenticator.login()
                    self.driver.get(url)
                    time.sleep(Config.WAIT_PAGE_LOAD)
                
                # Check for downloads
                download_info = self.download_detector.check_for_downloads_browser(self.driver, url, pid)
                
                # Parse content
                post = self.content_parser.parse_post_content(self.driver, post, download_info)
                
                # Process files if downloads found
                if download_info.has_download:
                    post = self._process_files(post, download_info)
                
                # Return as records
                return post.to_records()
                
            except Exception as e:
                logging.error(f"Browser processing error: {e}")
                post.error = f"Browser processing error: {str(e)}"
                return post.to_records()
        
        # Fallback to API if driver not available
        try:
            # Get page content
            html = self.scraper.get(url, headers=self.auth_headers, timeout=Config.REQUEST_TIMEOUT).text
            
            # Parse HTML
            soup = BeautifulSoup(html, "html.parser")
            
            # Check for downloads
            download_info = self.download_detector.check_for_downloads_soup(soup, url, pid)
            
            # Parse content
            post = self.content_parser.parse_post_content_api(html, url, post, download_info)
            
            # Process files if downloads found
            if download_info.has_download:
                post = self._process_files(post, download_info)
            
            # Return as records
            return post.to_records()
            
        except Exception as e:
            logging.error(f"API processing error: {e}")
            post.error = f"API error: {str(e)}"
            return post.to_records()
    
    def _process_files(self, post: Post, download_info: Dict[str, Any]) -> Post:
        """
        Process downloadable files
        
        Args:
            post: Post object
            download_info: Download information
            
        Returns:
            Updated Post object
        """
        # 불필요한 파일 필터링
        if download_info.download_links:
            # 제외할 파일 패턴
            exclude_files = ["BC.pdf", "BC.docx", "BC.xlsx"]
            # UUID 패턴 (예: 20b005cb-7b99-4143-9a4f-e0181f0af1e4.pptx)
            uuid_pattern = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.(pdf|pptx?|docx?|xlsx?|hwp)")
            
            # 필터링된 다운로드 링크만 유지
            filtered_links = []
            seen_urls = set()  # 중복 URL 체크용
            
            for link in download_info.download_links:
                url = link.get("url", "")
                filename = os.path.basename(url.split("?")[0])
                
                # 제외 파일 패턴에 해당하거나 UUID 패턴인 경우 건너뛰기
                if any(exclude in url for exclude in exclude_files) or uuid_pattern.search(url):
                    logging.info(f"[페이지 {post.post_id}] 불필요한 파일 제외: {url}")
                    continue
                    
                # 중복 URL 제거
                if url in seen_urls:
                    continue
                    
                seen_urls.add(url)
                filtered_links.append(link)
            
            # 필터링된 링크로 업데이트
            download_info.download_links = filtered_links
            
            # 파일 형식 재계산
            if filtered_links:
                file_formats = set()
                for link in filtered_links:
                    url = link.get("url", "")
                    ext = self.download_detector.extract_file_extension(url)
                    if ext:
                        file_formats.add(ext)
                download_info.file_formats = list(file_formats)
            else:
                download_info.file_formats = []
                download_info.has_download = False
        # Check if has_download is True but we need to correct the file_formats
        if download_info.has_download:
            # Enhanced PDF indicators including more Korean terms
            pdf_indicators = [
                "pdf", "문서", "보고서", "첨부파일", "다운로드", "download", 
                "리포트", "자료", "발표자료", "연구", "논문", "결과물", "레포트", 
                "결과보고서", "연구보고서", "분석", "정보"
            ]
            
            # URL patterns that strongly indicate PDF
            pdf_url_patterns = [".pdf", "/pdf/", "pdf=", "type=pdf", "format=pdf", "document", "report"]
            
            # If file_formats contains xlsx but evidence suggests PDF, replace with PDF
            if "xlsx" in download_info.file_formats:
                # First check URLs for PDF indicators
                for link in download_info.download_links:
                    url = link.get("url", "").lower()
                    # Check if any PDF URL pattern is in the URL
                    if any(pattern in url for pattern in pdf_url_patterns):
                        # Remove xlsx and add pdf if not already there
                        if "pdf" not in download_info.file_formats:
                            download_info.file_formats = ["pdf"]
                            logging.info(f"[페이지 {post.post_id}] 파일 형식 수정: xlsx -> pdf (URL 패턴 기반)")
                            break
                
                # Then check link text for PDF indicators if URL check didn't change format
                if "xlsx" in download_info.file_formats:
                    for link in download_info.download_links:
                        link_text = link.get("text", "").lower()
                        # Check if any PDF indicator is in the link text
                        if any(indicator in link_text for indicator in pdf_indicators):
                            # Remove xlsx and add pdf if not already there
                            if "pdf" not in download_info.file_formats:
                                download_info.file_formats = ["pdf"]
                                logging.info(f"[페이지 {post.post_id}] 파일 형식 수정: xlsx -> pdf (링크 텍스트 기반)")
                                break
            
            # Check all links for direct PDF indicators regardless of current format
            for link in download_info.download_links:
                url = link.get("url", "").lower()
                # Strong PDF indicators in URL take precedence over any other format
                if url.endswith(".pdf") or "/pdf/" in url:
                    if "pdf" not in download_info.file_formats:
                        download_info.file_formats = ["pdf"]
                        logging.info(f"[페이지 {post.post_id}] 파일 형식 수정: URL 기반으로 PDF 감지")
                        break
        
        # Filter out non-downloadable links by unified rule set
        download_info.download_links = [
            link for link in download_info.download_links
            if not any(pat in (link.get("url", "").lower() + link.get("text", "").lower()) for pat in EXCLUDE_PATTERNS)
        ]
        
        # Re-check has_download based on filtered links
        download_info.has_download = bool(download_info.download_links or download_info.download_buttons)
        
        # Ensure download_links is populated if has_download is True
        if download_info.has_download:
            # Initialize post.download_links if needed
            if not hasattr(post, 'download_links') or post.download_links is None:
                post.download_links = []
                
            # First handle existing download links
            if download_info.download_links:
                # Make sure all download_info links are in post.download_links
                for link in download_info.download_links:
                    link_exists = False
                    for existing_link in post.download_links:
                        if existing_link.get('url') == link.get('url'):
                            link_exists = True
                            break
                    if not link_exists:
                        post.download_links.append(link)
            
            # If no download links but we have buttons, create synthetic links
            if not download_info.download_links and download_info.download_buttons:
                for button in download_info.download_buttons:
                    button_text = button.get("text", "")
                    if button_text:
                        # Create a synthetic link with a default URL pattern
                        synthetic_link = {
                            "url": f"https://weolbu.com/download/{post.post_id}?type=button&text={button_text}",
                            "text": button_text
                        }
                        download_info.download_links.append(synthetic_link)
                        post.download_links.append(synthetic_link)
                        logging.info(f"[페이지 {post.post_id}] 합성 다운로드 링크 생성: {button_text}")
            
            # If still no download links but file_formats exists, create a generic link
            if not post.download_links and download_info.file_formats:
                for format in download_info.file_formats:
                    generic_link = {
                        "url": f"https://weolbu.com/download/{post.post_id}?format={format}",
                        "text": f"{format.upper()} 다운로드"
                    }
                    download_info.download_links.append(generic_link)
                    post.download_links.append(generic_link)
                    logging.info(f"[페이지 {post.post_id}] 파일 형식 기반 다운로드 링크 생성: {format}")
            
        # Skip if no downloads after correction
        if not download_info.has_download:
            return post
                    
        # Process each file
        for link in download_info.download_links:
            try:
                download_url = link["url"]
                
                # Skip links flagged by the unified exclude list
                if any(pat in (download_url.lower() + link.get("text", "").lower()) for pat in EXCLUDE_PATTERNS):
                    continue
                
                # Get filename
                filename = os.path.basename(download_url.split("?")[0])
                
                # If no filename or no extension, try to infer from context
                if not filename or not os.path.splitext(filename)[1]:
                    # Get link text and HTML content if available
                    link_text = link['text'].lower()
                    page_content = post.content.lower() if post.content else ""
                    
                    # 1. Try to infer from download_info
                    if download_info.file_formats and len(download_info.file_formats) > 0:
                        ext = download_info.file_formats[0]  # Use the first detected format
                        filename = f"{link['text']}.{ext}"
                    
                    # 2. Try to infer from link text
                    elif 'pdf' in link_text:
                        filename = f"{link['text']}.pdf"
                    elif any(ext in link_text for ext in ['ppt', 'pptx']):
                        filename = f"{link['text']}.pptx"
                    elif any(ext in link_text for ext in ['doc', 'docx']):
                        filename = f"{link['text']}.docx"
                    elif 'hwp' in link_text:
                        filename = f"{link['text']}.hwp"
                    elif 'xlsx' in link_text or 'excel' in link_text:
                        filename = f"{link['text']}.xlsx"
                    
                    # 3. For download buttons with no extension hints, check nearby context
                    elif '다운로드' in link_text or 'download' in link_text:
                        # Examine page content near the link text for file type hints
                        if 'pdf' in page_content:
                            filename = f"{link['text']}.pdf"
                        elif any(ext in page_content for ext in ['pptx', '.ppt']):
                            filename = f"{link['text']}.pptx"
                        elif any(ext in page_content for ext in ['docx', '.doc']):
                            filename = f"{link['text']}.docx"
                        elif '.hwp' in page_content:
                            filename = f"{link['text']}.hwp"
                        elif '.xlsx' in page_content:
                            filename = f"{link['text']}.xlsx"
                        else:
                            # Default to PDF for generic download buttons if no other clues
                            # This is a reasonable default for most document downloads
                            filename = f"{link['text']}.pdf"
                    
                    # 4. If still no format identified, use PDF as a sensible default
                    # Not having a default was causing valid downloads to be skipped
                    else:
                        filename = f"{link['text']}.pdf"  # Default to PDF as most common document type
                
                # Process file and add to post
                file_contents = self.file_processor.parse_file(download_url, post.post_id, filename)
                post.files.extend(file_contents)
                
            except Exception as e:
                logging.error(f"[페이지 {post.post_id}] 파일 처리 오류: {e}")
        
        return post
    
    def _create_driver(self) -> webdriver.Chrome:
        """
        Create a configured Chrome webdriver
        
        Returns:
            Chrome webdriver instance
        """
        options = Options()
        options.headless = Config.BROWSER_OPTIONS["headless"]
        
        if Config.BROWSER_OPTIONS["disable_automation"]:
            options.add_argument("--disable-blink-features=AutomationControlled")
        
        if Config.BROWSER_OPTIONS["no_sandbox"]:
            options.add_argument("--no-sandbox")
            
        if Config.BROWSER_OPTIONS["disable_shm"]:
            options.add_argument("--disable-dev-shm-usage")
            
        options.add_argument(f'user-agent={Config.USER_AGENT}')
        
        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
