#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download detector for real estate crawler
"""
import re
import logging
import os
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import lxml.html
from selenium import webdriver
from selenium.webdriver.common.by import By

from src.models.models import DownloadInfo

class DownloadDetector:
    """Class for detecting downloadable files in HTML content"""
    
    def __init__(self):
        # 다운로드 링크를 찾기 위한 CSS 선택자
        self.download_selectors = [
            'a[href$=".pdf"]',
            'a[href$=".pptx"]',
            'a[href$=".ppt"]',
            'a[href$=".docx"]',
            'a[href$=".doc"]',
            'a[href$=".xlsx"]',
            'a[href$=".xls"]',
            'a[href$=".hwp"]',
            'a.download',
            'a.file',
            'a[download]',
            'a[data-download="true"]',
            'a.btn-download',
            'a.download-link',
            'a:contains("다운로드")',
            'a:contains("첨부파일")',
            'a:contains("Download")',
            'a:contains("Attachment")'
        ]
        
        # 다운로드 링크를 찾기 위한 XPath 패턴
        self.xpath_patterns = [
            '//a[contains(@href, "download")]',
            '//a[contains(@class, "download")]',
            '//a[contains(@class, "file")]',
            '//a[contains(@onclick, "download")]',
            '//a[contains(text(), "다운로드")]',
            '//a[contains(text(), "첨부파일")]',
            '//a[contains(text(), "Download")]',
            '//a[contains(text(), "Attachment")]'
        ]
        
        # 파일 확장자 매핑
        self.ext_mapping = {
            'pdf': 'pdf',
            'pptx': 'pptx', 'ppt': 'pptx', 'powerpoint': 'pptx', '프레젠테이션': 'pptx',
            'docx': 'docx', 'doc': 'docx', 'word': 'docx', '워드': 'docx',
            'xlsx': 'xlsx', 'xls': 'xlsx', 'excel': 'xlsx', '엑셀': 'xlsx',
            'hwp': 'hwp', '한글': 'hwp'
        }
    
    def detect_downloads(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Detect downloadable files in HTML content
        
        Args:
            html_content: HTML content to search for downloadable files
            
        Returns:
            List of dictionaries containing download information
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        return self.check_for_downloads_soup(soup)
    
    def check_for_downloads_soup(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Check for downloadable files in a BeautifulSoup object
        
        Args:
            soup: BeautifulSoup object to search
            
        Returns:
            List of dictionaries containing download information
        """
        downloads = []
        
        # CSS 선택자를 사용하여 다운로드 링크 찾기
        for selector in self.download_selectors:
            try:
                for link in soup.select(selector):
                    if not link.get('href'):
                        continue
                        
                    download_info = {
                        'url': link.get('href'),
                        'text': link.get_text(strip=True),
                        'element': 'a',
                        'method': 'css',
                        'page_content': str(soup)
                    }
                    
                    if download_info not in downloads:
                        downloads.append(download_info)
            except Exception as e:
                logging.warning(f"Error finding downloads with selector {selector}: {e}")
        
        # XPath를 사용하여 다운로드 링크 찾기
        try:
            html_tree = lxml.html.fromstring(str(soup))
            
            for xpath in self.xpath_patterns:
                try:
                    elements = html_tree.xpath(xpath)
                    
                    for element in elements:
                        href = element.get('href')
                        if not href:
                            continue
                            
                        download_info = {
                            'url': href,
                            'text': element.text_content().strip() if element.text_content() else '',
                            'element': 'a',
                            'method': 'xpath',
                            'page_content': str(soup)
                        }
                        
                        if download_info not in downloads:
                            downloads.append(download_info)
                except Exception as e:
                    logging.warning(f"Error finding downloads with XPath {xpath}: {e}")
        except Exception as e:
            logging.warning(f"Error parsing HTML for XPath: {e}")
        
        return downloads
    
    def extract_file_extension(self, text: str) -> str:
        """
        Extract file extension from text
        
        Args:
            text: Text to extract file extension from
            
        Returns:
            File extension or empty string if not found
        """
        text = text.lower()
        
        # 직접적인 확장자 언급 확인
        for keyword, ext in self.ext_mapping.items():
            if keyword in text:
                return ext
        
        # URL에서 확장자 추출 시도
        url_pattern = r'https?://[^\s]+\.([a-zA-Z0-9]+)(?:[?#]|$)'
        match = re.search(url_pattern, text)
        if match:
            ext = match.group(1).lower()
            if ext in ['pdf', 'pptx', 'ppt', 'docx', 'doc', 'xlsx', 'xls', 'hwp']:
                return ext
        
        return ""
        
    def check_for_downloads_browser(self, driver: webdriver.Chrome, url: str, pid: str) -> DownloadInfo:
        """
        Check for downloadable files using browser
        
        Args:
            driver: Selenium webdriver
            url: Page URL
            pid: Post ID
            
        Returns:
            DownloadInfo object
        """
        result = DownloadInfo()
        
        try:
            # 1. Find download buttons
            download_buttons = driver.find_elements(By.XPATH, "//span[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//a[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//button[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//div[contains(text(), '다운로드')]")
            
            # 2. Find links with file extensions or file-related text
            file_links = driver.find_elements(By.XPATH, 
                "//a[contains(@href, '.pptx') or contains(@href, '.pdf') or contains(@href, '.docx') or \
                contains(@href, '.hwp') or contains(@href, '.doc') or contains(@href, '.xlsx') or \
                contains(text(), 'PDF') or contains(text(), 'pdf') or contains(text(), 'ppt') or \
                contains(text(), 'PPT') or contains(text(), 'doc') or contains(text(), 'DOC') or \
                contains(text(), 'hwp') or contains(text(), 'HWP') or \
                contains(@download, 'pdf') or contains(@title, 'pdf')]") 
                
            # Also find generic download buttons that might be files
            download_buttons_links = driver.find_elements(By.XPATH, "//a[contains(text(), '다운로드') or contains(text(), 'download')]") 
            file_links.extend(download_buttons_links)
            
            # 2.5 직접 페이지 소스에서 파일명 패턴 찾기 (예: 월부_서울기초반_가형_임장보고서탬플릿_1주차.pdf)
            page_source = driver.page_source
            # 파일명 패턴 정의
            filename_pattern = re.compile(r"([가-힣a-zA-Z0-9_\-\[\]\(\)]+\.(pdf|pptx?|docx?|hwp|xlsx?))", re.IGNORECASE)
            filename_matches = filename_pattern.findall(page_source)
            
            for filename, ext in filename_matches:
                # 파일명이 발견되고 그 주변에 다운로드 관련 텍스트가 있는지 확인
                context_start = max(0, page_source.find(filename) - 50)
                context_end = min(len(page_source), page_source.find(filename) + len(filename) + 50)
                context = page_source[context_start:context_end].lower()
                
                # 다운로드 관련 단어가 주변에 있거나 <span class="text-sm font-semibold">다운로드</span> 패턴이 있는지 확인
                if "다운로드" in context or "download" in context or "첨부파일" in context or \
                   '<span class="text-sm font-semibold">다운로드</span>' in page_source:
                    result.has_download = True
                    
                    # 파일 형식 추가
                    file_type = ext.lower()
                    if file_type.startswith("ppt"):
                        file_type = "pptx"
                    elif file_type.startswith("doc"):
                        file_type = "docx"
                    elif file_type.startswith("xls"):
                        file_type = "xlsx"
                        
                    if file_type not in result.file_formats:
                        result.file_formats.append(file_type)
                    
                    # 링크 추가 (이미 있는지 확인)
                    synthetic_url = f"https://weolbu.com/api/download/{pid}/{filename}"
                    found = False
                    for link_info in result.download_links:
                        if link_info.get("url") == synthetic_url:
                            found = True
                            break
                            
                    if not found:
                        result.download_links.append({
                            "url": synthetic_url,
                            "text": filename
                        })
                        logging.info(f"[페이지 {pid}] 파일명 패턴 기반 다운로드 링크 추가: {filename}")
            
            # 3. Process download buttons
            for button in download_buttons:
                button_text = button.text.strip()
                if button_text:
                    result.has_download = True
                    logging.info(f"[페이지 {pid}] 다운로드 버튼 발견: {button_text}")
            
            # 4. Process file links
            for link in file_links:
                href = link.get_attribute("href")
                link_text = link.text.strip()
                
                if not href and not link_text:
                    continue
                    
                result.has_download = True
                
                # 파일 형식 추출
                file_type = ""
                if href:
                    if ".pdf" in href.lower() or "pdf" in link_text.lower():
                        file_type = "pdf"
                    elif any(ext in href.lower() for ext in [".pptx", ".ppt"]) or any(kw in link_text.lower() for kw in ["ppt", "pptx", "프레젠테이션"]):
                        file_type = "pptx"
                    elif any(ext in href.lower() for ext in [".docx", ".doc"]) or any(kw in link_text.lower() for kw in ["doc", "docx", "워드"]):
                        file_type = "docx"
                    elif ".hwp" in href.lower() or "hwp" in link_text.lower() or "한글" in link_text.lower():
                        file_type = "hwp"
                    elif any(ext in href.lower() for ext in [".xlsx", ".xls"]) or any(kw in link_text.lower() for kw in ["xls", "xlsx", "엑셀"]):
                        file_type = "xlsx"
                    else:
                        # 기본값으로 PDF 가정
                        file_type = "pdf"
                
                if file_type and file_type not in result.file_formats:
                    result.file_formats.append(file_type)
                
                # 링크 추가 (이미 있는지 확인)
                if href:
                    found = False
                    for link_info in result.download_links:
                        if link_info.get("url") == href:
                            found = True
                            break
                            
                    if not found:
                        result.download_links.append({
                            "url": href,
                            "text": link_text or "Download"
                        })
                        logging.info(f"[페이지 {pid}] 다운로드 링크 추가: {link_text or href}")
            
            return result
            
        except Exception as e:
            logging.error(f"[페이지 {pid}] 다운로드 검색 오류: {e}")
            return result
