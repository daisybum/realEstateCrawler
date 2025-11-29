#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download detector for real estate crawler
"""

import re
import logging
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By


@dataclass
class DownloadInfo:
    """다운로드 정보를 담는 클래스"""
    has_download: bool = False
    file_formats: List[str] = field(default_factory=list)
    download_links: List[Dict[str, str]] = field(default_factory=list)


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
        
        # 인증서 PDF 필터링을 위한 패턴
        self.certificate_patterns = [
            '원격평생교육원:(제 원-639호)',
            '원격평생교육원',
            'certificate',
            '인증서',
            '증명서'
        ]
        
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
        
        # 1. CSS 선택자로 다운로드 링크 찾기
        for selector in self.download_selectors:
            try:
                # BeautifulSoup의 CSS 선택자 기능은 제한적이므로 일부 선택자는 건너뛰기
                if ':contains(' in selector:
                    continue
                    
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    text = link.get_text(strip=True)
                    
                    if not href:
                        continue
                        
                    # 인증서 PDF 파일 무시
                    if self._is_certificate_pdf(href, text):
                        continue
                        
                    # 이미 추가된 링크인지 확인
                    if any(d.get('url') == href for d in downloads):
                        continue
                        
                    downloads.append({
                        'url': href,
                        'text': text or href.split('/')[-1]
                    })
            except Exception as e:
                logging.debug(f"Error with CSS selector {selector}: {e}")
        
        # 2. XPath 패턴으로 다운로드 링크 찾기 (lxml 사용)
        try:
            from lxml import etree
            html = etree.HTML(str(soup))
            
            for xpath in self.xpath_patterns:
                try:
                    elements = html.xpath(xpath)
                    for element in elements:
                        href = element.get('href')
                        text = ''.join(element.xpath('.//text()'))
                        
                        if not href:
                            continue
                            
                        # 인증서 PDF 파일 무시
                        if self._is_certificate_pdf(href, text):
                            continue
                            
                        # 이미 추가된 링크인지 확인
                        if any(d.get('url') == href for d in downloads):
                            continue
                            
                        downloads.append({
                            'url': href,
                            'text': text.strip() or href.split('/')[-1]
                        })
                except Exception as e:
                    logging.debug(f"Error with XPath pattern {xpath}: {e}")
        except ImportError:
            logging.debug("lxml not installed, skipping XPath patterns")
        
        return downloads
    
    def _is_certificate_pdf(self, url: str, text: str) -> bool:
        """
        Check if the URL or text refers to a certificate PDF
        
        Args:
            url: URL to check
            text: Text to check
            
        Returns:
            True if it's a certificate PDF, False otherwise
        """
        url_lower = url.lower()
        text_lower = text.lower()
        
        # 인증서 PDF 파일 필터링
        for pattern in self.certificate_patterns:
            if pattern.lower() in url_lower or pattern.lower() in text_lower:
                return True
                
        return False
        
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
    
    def check_content_for_file_references(self, content: str, pid: str) -> DownloadInfo:
        """
        Check post content text for file references
        
        Args:
            content: Post content text
            pid: Post ID
            
        Returns:
            DownloadInfo object with detected files
        """
        result = DownloadInfo()
        
        if not content:
            return result
            
        # 파일 확장자 패턴 (더 정확한 파일명 패턴)
        file_pattern = re.compile(r'([가-힣a-zA-Z0-9_\-\[\]\(\)]+\.(pdf|pptx?|docx?|hwp|xlsx?|xls))', re.IGNORECASE)
        matches = file_pattern.findall(content)
        
        for filename, ext in matches:
            # 인증서 PDF 파일 무시
            if self._is_certificate_pdf("", filename):
                logging.info(f"[페이지 {pid}] 인증서 PDF 파일 무시: {filename}")
                continue
                
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
            
            # CDN 직접 링크 추가
            cdn_url_pattern = r"https?://cdn\.weolbu\.com/([a-zA-Z0-9_\-]+/)?([가-힣a-zA-Z0-9_\-\[\]\(\)]+\.(pdf|pptx?|docx?|hwp|xlsx?|xls))"
            cdn_match = re.search(cdn_url_pattern, content)
            if cdn_match:
                cdn_url = cdn_match.group(0)
                result.download_links.append({
                    "url": cdn_url,
                    "text": filename
                })
                logging.info(f"[페이지 {pid}] CDN 직접 링크 추가: {cdn_url}")
                
        return result
        
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
            # 1. 가장 먼저 특정 다운로드 버튼 찾기 (<span class="text-sm font-semibold">다운로드</span>)
            specific_download_spans = driver.find_elements(By.XPATH, "//span[@class='text-sm font-semibold' and contains(text(), '다운로드')]")
            if specific_download_spans:
                logging.info(f"[페이지 {pid}] 특정 다운로드 버튼 발견: <span class='text-sm font-semibold'>다운로드</span>")
                result.has_download = True
            
            # 2. 일반 다운로드 버튼 찾기
            download_buttons = driver.find_elements(By.XPATH, "//span[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//a[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//button[contains(text(), '다운로드')]") + \
                              driver.find_elements(By.XPATH, "//div[contains(text(), '다운로드')]")
            
            # 다운로드 버튼이 있으면 다운로드 있음으로 표시 및 실제 링크 추출 시도
            if download_buttons:
                result.has_download = True
                for button in download_buttons:
                    button_text = button.text.strip()
                    if button_text:
                        logging.info(f"[페이지 {pid}] 다운로드 버튼 발견: {button_text}")
                    # 버튼 클릭 시도 (스크립트로) – 일부 페이지에서 a 태그가 동적 생성됨
                    try:
                        driver.execute_script("arguments[0].scrollIntoView(true);", button)
                        driver.execute_script("arguments[0].click();", button)
                        time.sleep(1)
                    except Exception as click_err:
                        logging.debug(f"[페이지 {pid}] 다운로드 버튼 클릭 실패: {click_err}")
                # 1차: 클릭 이후 DOM 에 CDN 링크가 생겼는지 확인
                anchor_elements = driver.find_elements(By.XPATH, "//a[contains(@href,'cdn.weolbu.com') and (contains(@href,'.pdf') or contains(@href,'.ppt') or contains(@href,'.doc') or contains(@href,'.hwp') or contains(@href,'.xls'))]")
                for a in anchor_elements:
                    href = a.get_attribute('href')
                    link_text = a.text.strip() or href.split('/')[-1]
                    if href and not self._is_certificate_pdf(href, link_text) and not any(link_info.get('url') == href for link_info in result.download_links):
                        result.download_links.append({'url': href, 'text': link_text})
                        file_ext = self.extract_file_extension(href)
                        if file_ext and file_ext not in result.file_formats:
                            result.file_formats.append(file_ext)
                        logging.info(f"[페이지 {pid}] 클릭 후 CDN 링크 발견(DOM): {href}")
                # 2차: 네트워크 로그에서 CDN 요청 추출
                try:
                    import json
                    perf_logs = driver.get_log('performance')
                    for entry in perf_logs:
                        msg = json.loads(entry.get('message', '{}')).get('message', {})
                        if msg.get('method') == 'Network.requestWillBeSent':
                            req_url = msg.get('params', {}).get('request', {}).get('url', '')
                            if 'cdn.weolbu.com' in req_url and re.search(r'\.(pdf|pptx?|docx?|hwp|xlsx?)', req_url, re.IGNORECASE):
                                if not self._is_certificate_pdf(req_url, '') and not any(link_info.get('url') == req_url for link_info in result.download_links):
                                    result.download_links.append({'url': req_url, 'text': req_url.split('/')[-1]})
                                    file_ext = self.extract_file_extension(req_url)
                                    if file_ext and file_ext not in result.file_formats:
                                        result.file_formats.append(file_ext)
                                    logging.info(f"[페이지 {pid}] 클릭 후 CDN 링크 발견(Net): {req_url}")
                except Exception as log_err:
                    logging.debug(f"[페이지 {pid}] 퍼포먼스 로그 파싱 오류: {log_err}")
            
            # 3. 파일 링크 찾기
            file_links = driver.find_elements(By.XPATH, 
                "//a[contains(@href, '.pptx') or contains(@href, '.pdf') or contains(@href, '.docx') or \
                contains(@href, '.hwp') or contains(@href, '.doc') or contains(@href, '.xlsx') or \
                contains(text(), 'PDF') or contains(text(), 'pdf') or contains(text(), 'ppt') or \
                contains(text(), 'PPT') or contains(text(), 'doc') or contains(text(), 'DOC') or \
                contains(text(), 'hwp') or contains(text(), 'HWP') or \
                contains(@download, 'pdf') or contains(@title, 'pdf')]") 
            
            # 일반 다운로드 링크도 추가
            download_buttons_links = driver.find_elements(By.XPATH, "//a[contains(text(), '다운로드') or contains(text(), 'download')]") 
            file_links.extend(download_buttons_links)
            
            # 파일 링크 처리
            for link in file_links:
                href = link.get_attribute("href")
                link_text = link.text.strip()
                
                if not href and not link_text:
                    continue
                    
                # 인증서 PDF 파일 무시
                if href and self._is_certificate_pdf(href, link_text or ""):
                    logging.info(f"[페이지 {pid}] 인증서 PDF 파일 무시: {link_text or href}")
                    continue
                    
                result.has_download = True
                
                # 파일 형식 추출 및 추가
                file_ext = self.extract_file_extension(href or link_text or "")
                if file_ext and file_ext not in result.file_formats:
                    result.file_formats.append(file_ext)
                
                # 링크 추가 (중복 방지)
                if href and not any(link_info.get('url') == href for link_info in result.download_links):
                    result.download_links.append({
                        'url': href,
                        'text': link_text or href.split('/')[-1]
                    })
                    logging.info(f"[페이지 {pid}] 다운로드 링크 추가: {href}")
            
            # 4. 페이지 소스에서 파일명 패턴 찾기
            page_source = driver.page_source
            filename_pattern = re.compile(r"([가-힣a-zA-Z0-9_\-\[\]\(\)]+\.(pdf|pptx?|docx?|hwp|xlsx?))", re.IGNORECASE)
            filename_matches = filename_pattern.findall(page_source)
            
            for filename, ext in filename_matches:
                # 파일명이 발견되고 그 주변에 다운로드 관련 텍스트가 있는지 확인
                context_start = max(0, page_source.find(filename) - 50)
                context_end = min(len(page_source), page_source.find(filename) + len(filename) + 50)
                context = page_source[context_start:context_end].lower()
                
                # 다운로드 관련 단어가 주변에 있는지 확인
                if "다운로드" in context or "download" in context or "첨부파일" in context:
                    # 인증서 PDF 파일 무시
                    if self._is_certificate_pdf("", filename):
                        logging.info(f"[페이지 {pid}] 인증서 PDF 파일 무시: {filename}")
                        continue
                        
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
                    
                    # CDN 직접 링크 추가
                    cdn_url_pattern = r"https?://cdn\.weolbu\.com/([a-zA-Z0-9_\-]+/)?([가-힣a-zA-Z0-9_\-\[\]\(\)]+\.(pdf|pptx?|docx?|hwp|xlsx?|xls))"
                    cdn_match = re.search(cdn_url_pattern, page_source)
                    if cdn_match:
                        cdn_url = cdn_match.group(0)
                        result.download_links.append({
                            "url": cdn_url,
                            "text": filename
                        })
                        logging.info(f"[페이지 {pid}] CDN 직접 링크 추가: {cdn_url}")
            
            # 5. 페이지 텍스트 콘텐츠에서 파일 참조 찾기 (새로 추가된 부분)
            page_text = driver.find_element(By.TAG_NAME, "body").text
            content_result = self.check_content_for_file_references(page_text, pid)
            
            # 결과 병합
            if content_result.has_download:
                result.has_download = True
                
                # 파일 형식 병합
                for file_format in content_result.file_formats:
                    if file_format not in result.file_formats:
                        result.file_formats.append(file_format)
                
                # 다운로드 링크 병합
                for link in content_result.download_links:
                    found = False
                    for existing_link in result.download_links:
                        if existing_link.get("url") == link.get("url"):
                            found = True
                            break
                    
                    if not found:
                        result.download_links.append(link)
            
            # 다운로드 있음/없음 로직 정리
            if result.has_download:
                logging.info(f"[페이지 {pid}] 다운로드 있음 처리 완료")
            else:
                logging.info(f"[페이지 {pid}] 다운로드 없음")
                
            return result
            
        except Exception as e:
            logging.error(f"[페이지 {pid}] 다운로드 검색 오류: {e}")
            return result
