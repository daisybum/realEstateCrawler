#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Content parser for real estate crawler
"""

import logging
import re
import time
from typing import List, Tuple, Dict, Any, Optional
from urllib.parse import urljoin

import cv2
import numpy as np
from paddleocr import PaddleOCR
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from requests_html import HTMLSession

from src.config import Config
from src.models.models import Post, Image, DownloadInfo


class ContentParser:
    """Handles parsing of web content"""
    
    def __init__(self, scraper=None, html_session=None, ocr=None):
        """
        Initialize content parser
        
        Args:
            scraper: Cloudscraper instance
            html_session: HTML session for JavaScript rendering
            ocr: PaddleOCR instance for image text extraction
        """
        self.scraper = scraper
        self.html_session = html_session or HTMLSession()
        self.ocr = ocr or PaddleOCR(lang="korean", show_log=False)
    
    def parse_post_content(self, driver: webdriver.Chrome, post: Post, download_info: DownloadInfo) -> Post:
        """
        Parse post content using browser
        
        Args:
            driver: Selenium webdriver
            post: Post object to populate
            download_info: Download information
            
        Returns:
            Updated Post object
        """
        try:
            # Update download information
            post.download_info = download_info
            post.update_download_summary()
            
            # Extract images
            self._extract_images_browser(driver, post)
            
            # Extract text content
            self._extract_text_browser(driver, post)
            
            # If no content found, try fallback methods
            if not post.content and not post.images:
                self._extract_content_fallback(driver, post)
            
        except Exception as e:
            logging.error(f"[페이지 {post.post_id}] 브라우저 처리 오류: {e}")
            post.error = f"Browser processing error: {str(e)}"
            
        return post
    
    def parse_post_content_api(self, html: str, url: str, post: Post, download_info: DownloadInfo) -> Post:
        """
        Parse post content using BeautifulSoup
        
        Args:
            html: HTML content
            url: Post URL
            post: Post object to populate
            download_info: Download information
            
        Returns:
            Updated Post object
        """
        try:
            # Parse HTML
            soup = BeautifulSoup(html, "html.parser")
            
            # Update download information
            post.download_info = download_info
            post.update_download_summary()
            
            # Extract content using various selectors
            content_selectors = [
                "div.post-content", "div.view-content", "div.content", "article.post", 
                "div.fr-view", "div.fr-element", "#post-content", "#view-content", 
                "#content", ".viewer_content", ".board-content"
            ]
            
            content_found = False
            for selector in content_selectors:
                content_div = soup.select_one(selector)
                if content_div:
                    # Extract text
                    text_content = content_div.get_text(" ", strip=True)
                    if text_content and len(text_content) > 50:
                        post.content = text_content
                        content_found = True
                    
                    # Extract images
                    images = content_div.find_all("img")
                    for idx, img in enumerate(images):
                        src = img.get("src", "")
                        if src and not src.startswith("data:") and not src.endswith(".svg"):
                            img_url = src if src.startswith("http") else urljoin(Config.BASE_URL, src)
                            post.images.append(Image(url=img_url, index=idx))
                            content_found = True
                
                if content_found:
                    break
            
            # Set error if no content found
            if not content_found:
                post.error = "Content not found with API method"
                
        except Exception as e:
            logging.error(f"[페이지 {post.post_id}] API 처리 오류: {e}")
            post.error = f"API error: {str(e)}"
            
        return post
    
    def ocr_image(self, url: str) -> str:
        """
        Perform OCR on an image
        
        Args:
            url: Image URL
            
        Returns:
            Extracted text or empty string
        """
        try:
            img_bytes = self.scraper.get(url, timeout=30).content
            arr = np.frombuffer(img_bytes, np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            res = self.ocr.ocr(img, cls=True)
            return " ".join(x[1][0] for x in res[0]) if res and res[0] else ""
        except Exception as e:
            logging.error(f"OCR 오류: {e}")
            return ""
    
    def _extract_images_browser(self, driver: webdriver.Chrome, post: Post) -> None:
        """
        Extract images from browser
        
        Args:
            driver: Selenium webdriver
            post: Post object to update
        """
        try:
            # Find images with various selectors
            images = driver.find_elements(By.CSS_SELECTOR, 
                ".post-content img, .view-content img, .content img, article img, .fr-view img, .fr-element img")
            
            for idx, img in enumerate(images):
                try:
                    src = img.get_attribute("src")
                    if src and not src.startswith("data:") and not src.endswith(".svg"):
                        img_url = src if src.startswith("http") else urljoin(Config.BASE_URL, src)
                        logging.info(f"[페이지 {post.post_id}] 이미지 발견: {img_url}")
                        post.images.append(Image(url=img_url, index=idx))
                except Exception as img_err:
                    logging.error(f"[페이지 {post.post_id}] 이미지 처리 오류: {img_err}")
        except Exception as img_section_err:
            logging.error(f"[페이지 {post.post_id}] 이미지 섹션 처리 오류: {img_section_err}")
    
    def _extract_text_browser(self, driver: webdriver.Chrome, post: Post) -> None:
        """
        Extract text content from browser
        
        Args:
            driver: Selenium webdriver
            post: Post object to update
        """
        try:
            # Try various selectors for content
            content_selectors = [
                ".post-content", ".view-content", ".content", "article", ".fr-view", ".fr-element",
                "#post-content", "#view-content", "#content", ".viewer_content", ".board-content"
            ]
            
            for selector in content_selectors:
                content_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if content_elements:
                    for element in content_elements:
                        text = element.text.strip()
                        if text and len(text) > 50:  # Only meaningful text
                            logging.info(f"[페이지 {post.post_id}] 본문 발견: {selector} ({len(text)} 글자)")
                            post.content = text
                            return
            
            # Fallback to body text if no content found
            if not post.content:
                body_text = driver.find_element(By.TAG_NAME, "body").text
                if body_text and len(body_text) > 100:
                    logging.info(f"[페이지 {post.post_id}] 본문 발견: body ({len(body_text)} 글자)")
                    post.content = body_text
                    
        except Exception as text_err:
            logging.error(f"[페이지 {post.post_id}] 텍스트 처리 오류: {text_err}")
    
    def _extract_content_fallback(self, driver: webdriver.Chrome, post: Post) -> None:
        """
        Fallback method for content extraction
        
        Args:
            driver: Selenium webdriver
            post: Post object to update
        """
        logging.warning(f"[페이지 {post.post_id}] 콘텐츠를 찾을 수 없습니다. HTML 구조를 분석합니다.")
        
        try:
            # Create BeautifulSoup from page source
            html_content = driver.page_source
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Search all div elements with classes
            for div in soup.find_all("div", class_=True):
                class_name = div.get("class", [])
                if class_name:
                    class_str = " ".join(class_name)
                    text = div.get_text(strip=True)
                    if text and len(text) > 100 and ("content" in class_str.lower() or "post" in class_str.lower() or "view" in class_str.lower()):
                        logging.info(f"[페이지 {post.post_id}] 추가 분석으로 콘텐츠 발견: div.{class_str}")
                        post.content = text
                        break
        except Exception as soup_err:
            logging.error(f"[페이지 {post.post_id}] BeautifulSoup 처리 오류: {soup_err}")
            post.error = f"BeautifulSoup 처리 오류: {soup_err}"


class ListParser:
    """Handles parsing of post lists"""
    
    def __init__(self, scraper=None):
        """
        Initialize list parser
        
        Args:
            scraper: Cloudscraper instance
        """
        self.scraper = scraper
    
    def parse_list_api(self, page: int, auth_headers: Dict[str, str], driver=None, size: int = 30) -> List[Tuple[str, str]]:
        """
        Parse post list using API
        
        Args:
            page: Page number
            auth_headers: Authentication headers
            driver: Optional Selenium webdriver for fallback
            size: Page size
            
        Returns:
            List of (title, url) tuples
        """
        # Try browser-based extraction if driver is provided
        if driver is not None:
            try:
                return self._parse_list_browser(page, driver)
            except Exception as e:
                logging.error(f"Browser API 실패: {e}")
        
        # Fall back to API
        params = dict(tab=Config.TAB, subTab=Config.SUBTAB, page=page, size=size)
        r = self.scraper.get(Config.API_URL, params=params, headers=auth_headers, timeout=Config.REQUEST_TIMEOUT)
        
        if r.status_code != 200 or "application/json" not in r.headers.get("content-type", ""):
            raise RuntimeError(f"API 실패: {r.status_code}")
            
        items = r.json().get("content", [])
        return [
            (item["title"], urljoin(Config.BASE_URL, f"/community/{item['id']}"))
            for item in items
        ]
    
    def _parse_list_browser(self, page: int, driver: webdriver.Chrome) -> List[Tuple[str, str]]:
        """
        Parse post list using browser
        
        Args:
            page: Page number
            driver: Selenium webdriver
            
        Returns:
            List of (title, url) tuples
        """
        url = f"{Config.LIST_URL}?tab={Config.TAB}&subTab={Config.SUBTAB}&page={page}"
        driver.get(url)
        time.sleep(Config.WAIT_PAGE_LOAD)  # Wait for page to load
        
        # Check if login required
        page_content = driver.execute_script("return document.body.innerText")
        if "로그인이 필요합니다" in page_content or "로그인" in page_content and "로그아웃" not in page_content:
            raise RuntimeError("Login required")
        
        # Find links
        links = driver.find_elements(By.CSS_SELECTOR, "a[href^='/community/']")
        posts = []
        seen = set()
        
        for link in links:
            href = link.get_attribute('href')
            title = link.text.strip()
            if href and re.match(r"^https://weolbu.com/community/\d+$", href) and href not in seen:
                posts.append((title, href))
                seen.add(href)
        
        return posts
