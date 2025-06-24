#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File processing utilities for real estate crawler
"""

import os
import logging
import re
import time
import requests
from typing import List, Dict, Any
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from io import BytesIO

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

from src.models.models import FileContent, DownloadInfo
from src.config import Config
from src.parser.parser_document import DocumentParser
from src.parser.parser_pdf import PDFParser
from src.parser.parser_pptx import PPTXParser
from src.parser.parser_docx import DOCXParser


class FileProcessor:
    """Handles file detection and processing"""
    
    def __init__(self, scraper=None):
        """
        Initialize file processor
        
        Args:
            scraper: Cloudscraper instance for HTTP requests
        """
        self.scraper = scraper
        
        # Initialize document parsers
        try:
            # BytesIO for in-memory processing
            self.BytesIO = BytesIO
            
            # Initialize document parsers
            self.document_parser = DocumentParser()
            self.pdf_parser = PDFParser()
            self.pptx_parser = PPTXParser()
            self.docx_parser = DOCXParser()
            
            self.imports_successful = True
        except ImportError as e:
            logging.error(f"Failed to import document processing libraries: {e}")
            self.imports_successful = False
            
    def parse_file_bytes(self, file_obj, file_ext: str, filename: str = "", url: str = "") -> Dict[str, Any]:
        """
        Parse file bytes using appropriate document parser
        
        Args:
            file_obj: BytesIO object containing file data
            file_ext: File extension (e.g., '.pdf')
            filename: Optional filename for reference
            url: Optional URL for reference
            
        Returns:
            Dictionary with parsed content
        """
        if not self.imports_successful:
            return {
                "content": f"파일 다운로드 링크: {url}\n파일명: {filename}",
                "error": "Document processing libraries not available"
            }
        
        try:
            # 임시 파일을 생성하여 document_parser를 통해 처리
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as temp_file:
                # BytesIO 객체의 내용을 임시 파일에 쓰기
                file_obj.seek(0)  # 파일 포인터를 처음으로 이동
                temp_file.write(file_obj.read())
                temp_file_path = temp_file.name
            
            try:
                # DocumentParser를 사용하여 파일 파싱
                result = self.document_parser.parse_document(temp_file_path)
                
                # 파일 메타데이터 추가
                result["filename"] = filename
                result["url"] = url
                result["file_type"] = file_ext[1:] if file_ext.startswith('.') else file_ext
                
                return result
                
            finally:
                # 임시 파일 삭제
                try:
                    os.unlink(temp_file_path)
                except Exception as e:
                    logging.warning(f"Failed to delete temporary file {temp_file_path}: {e}")
            
        except Exception as e:
            logging.error(f"Error parsing file bytes: {e}")
            return {
                "content": f"파일 처리 오류: {e}\n파일 다운로드 링크: {url}\n파일명: {filename}",
                "error": str(e)
            }
    
    def parse_file(self, url: str, pid: str, filename: str) -> List[FileContent]:
        """
        Process a file based on its extension
        
        Args:
            url: URL of the file
            pid: Post ID
            filename: Filename
            
        Returns:
            List of FileContent objects
        """
        file_ext = os.path.splitext(filename)[1].lower()
        
        # Supported file extensions
        supported_extensions = {
            ".pdf", ".pptx", ".ppt", ".docx", ".doc", ".hwp"
        }
        
        # Process file if extension is supported
        if file_ext in supported_extensions:
            try:
                # Download file content
                if not self.scraper:
                    logging.error("Scraper not initialized")
                    return []
                    
                response = self.scraper.get(url, stream=True)
                response.raise_for_status()
                
                # Process file in-memory
                file_bytes = response.content
                file_obj = self.BytesIO(file_bytes)
                
                # Parse file using document parser
                parsed_content = self.parse_file_bytes(file_obj, file_ext, filename, url)
                
                # Create FileContent object
                file_content = FileContent(
                    filename=filename,
                    url=url,
                    file_type=file_ext[1:] if file_ext.startswith('.') else file_ext,
                    content=parsed_content.get("content", ""),
                    metadata=parsed_content.get("metadata", {}),
                    tables=parsed_content.get("tables", []),
                    images=parsed_content.get("images", [])
                )
                
                return [file_content]
            except Exception as e:
                logging.error(f"파일 처리 오류 ({file_ext}): {e}")
        
        return []
    
    def download_file(self, url: str, post_id: str, filename: str = "") -> str:
        """
        Download a file from URL and save it to the downloads directory
        
        Args:
            url: URL of the file to download
            post_id: Post ID for organizing downloads
            filename: Optional filename (will be extracted from URL if not provided)
            
        Returns:
            Path to the downloaded file or empty string if download failed
        """
        if not url:
            logging.error("Empty URL provided for download")
            return ""
            
        try:
            # Choose HTTP client (authenticated session if available)
            http_client = self.scraper if self.scraper is not None else requests
            # Extract filename from URL if not provided
            if not filename:
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path)
                if not filename or filename.endswith('/'):
                    # Generate a filename based on post_id and file extension
                    ext = self.extract_file_extension(url) or "pdf"
                    filename = f"file_{post_id}_{int(time.time())}.{ext}"
            
            # Create download directory structure based on Config settings
            config = Config.get_instance()
            download_root = str(config.download_dir)
            download_dir = os.path.join(download_root, post_id)
            os.makedirs(download_dir, exist_ok=True)
            
            # Full path for the downloaded file
            file_path = os.path.join(download_dir, filename)
            
            # Check if file already exists
            if os.path.exists(file_path):
                logging.info(f"File already exists: {file_path}")
                return file_path
            
            # Download the file
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            
            response = http_client.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
            
            # Save the file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logging.info(f"Downloaded file: {file_path}")
            return file_path
            
        except Exception as e:
            logging.error(f"Error downloading file from {url}: {e}")
            return ""
    
    # 불필요한 _parse_pdf 메서드 제거됨
    
    # 불필요한 _process_image_ocr 메서드 제거됨
    
    # 불필요한 parse_pdf_bytes 메서드 제거됨
    
    # 불필요한 _parse_pptx 메서드 제거됨
    


class DownloadDetector:
    """Detects downloadable files in web pages"""
    
    def __init__(self):
        """Initialize download detector"""
        # 파일 확장자 패턴
        self.file_ext_pattern = re.compile(r"\.(pdf|pptx?|docx?|hwp|xlsx?|zip|rar)($|\?)", re.IGNORECASE)
        # 파일명 패턴 (예: 월부_서울기초반_가형_임장보고서탬플릿_1주차.pdf)
        self.filename_pattern = re.compile(r"([가-힣a-zA-Z0-9_\-\[\]\(\)]+\.(pdf|pptx?|docx?|hwp|xlsx?))", re.IGNORECASE)
        self.pdf_pattern = re.compile(r"pdf", re.IGNORECASE)
        self.pptx_pattern = re.compile(r"pptx?|ppt", re.IGNORECASE)
        self.docx_pattern = re.compile(r"docx?|doc", re.IGNORECASE)
        self.hwp_pattern = re.compile(r"hwp", re.IGNORECASE)
    
    def extract_file_extension(self, url_or_filename):
        """Extract file extension from a URL or filename.
        
        This helper is robust to cases where the file name exists **only** inside
        the query-string (e.g. `https://host.com/download?file=abc.pdf`). If an
        extension cannot be found we fall back to keyword heuristics so that at
        least a sensible default (usually pdf) is returned.
        """
        
        # ---------- 1. Fast path: obvious patterns ----------
        url_lower = url_or_filename.lower()
        if url_lower.endswith(".pdf") or "/pdf/" in url_lower:
            return "pdf"
        
        # Strip query parameters and fragment for direct path inspection
        base_name = os.path.basename(url_or_filename.split("?")[0])
        _, ext = os.path.splitext(base_name)
        ext = ext.lower().lstrip(".")
        
        KNOWN_EXTS = {"pdf", "ppt", "pptx", "doc", "docx", "hwp", "xls", "xlsx"}
        
        if ext in KNOWN_EXTS:
            # Normal case: extension present in the path itself
            # Harmonise ppt -> pptx, doc -> docx, xls -> xlsx
            if ext == "ppt":
                return "pptx"
            if ext == "doc":
                return "docx"
            if ext == "xls":
                return "xlsx"
            return ext
        
        # ---------- 2. Check query string parameters ----------
        parsed = urlparse(url_or_filename)
        if parsed.query:
            for values in parse_qs(parsed.query, keep_blank_values=True).values():
                for val in values:
                    # Some sites double-encode the filename – decode once just in case
                    candidate = unquote(val).lower()
                    for e in KNOWN_EXTS:
                        if candidate.endswith(f".{e}"):
                            # Canonicalise short extensions
                            if e == "ppt":
                                return "pptx"
                            if e == "doc":
                                return "docx"
                            if e == "xls":
                                return "xlsx"
                            return e
        
        # ---------- 3. Keyword heuristics ----------
        lower_name = f"{base_name.lower()} {url_lower}"
        
        if any(k in lower_name for k in ["pdf", "보고서", "리포트", "첨부파일", "논문", "연구보고서"]):
            return "pdf"
        if any(k in lower_name for k in ["ppt", "프레젠테이션", "발표"]):
            return "pptx"
        if any(k in lower_name for k in ["doc", "워드", "문서"]):
            return "docx"
        if "hwp" in lower_name or "한글" in lower_name:
            return "hwp"
        if any(k in lower_name for k in ["xls", "엑셀", "스프레드시트"]):
            return "xlsx"
        
        # ---------- 4. Give up – return empty string (caller will default to pdf later) ----------
        return ""  # Unknown
    
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
            filename_matches = self.filename_pattern.findall(page_source)
            
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
                if not button_text:
                    continue
                    
                result.has_download = True
                result.download_buttons.append({
                    "text": button_text,
                    "element": button.tag_name
                })
                
                # Handle links
                if button.tag_name == "a":
                    href = button.get_attribute("href")
                    if href:
                        result.download_links.append({
                            "url": href,
                            "text": button_text
                        })
                        
                        # Extract file extension using the helper function
                        ext = self.extract_file_extension(href)
                        if ext and ext not in result.file_formats and \
                           ext not in ["jpg", "jpeg", "png", "gif"]:
                            result.file_formats.append(ext)
            
            # 4. Process file links
            for link in file_links:
                href = link.get_attribute("href")
                if not href:
                    continue
                    
                # Skip certificate PDFs
                if "certificate" in href and "원격평생교육원" in link.text:
                    continue
                    
                result.has_download = True
                
                # Check if this URL is already in download_links
                found = False
                for link_info in result.download_links:
                    if link_info.get("url") == href:
                        found = True
                        break
                        
                if not found:
                    result.download_links.append({
                        "url": href,
                        "text": link.text.strip() or os.path.basename(href.split("?")[0])
                    })
                
                # Extract file extension using the helper function
                ext = self.extract_file_extension(href)
                if ext and ext not in result.file_formats and \
                   ext not in ["jpg", "jpeg", "png", "gif"]:
                    result.file_formats.append(ext)
            
            # 5. Apply heuristics for download buttons without identifiable file formats
            if result.has_download and not result.file_formats:
                # Check if any download button text suggests a specific format
                format_found = False
                for button in result.download_buttons:
                    button_text = button["text"].lower()
                    if self.pdf_pattern.search(button_text):
                        result.file_formats.append("pdf")
                        format_found = True
                        break
                    elif self.pptx_pattern.search(button_text):
                        result.file_formats.append("pptx")
                        format_found = True
                        break
                    elif self.docx_pattern.search(button_text):
                        result.file_formats.append("docx")
                        format_found = True
                        break
                    elif self.hwp_pattern.search(button_text):
                        result.file_formats.append("hwp")
                        format_found = True
                        break
                
                # Check download links if no format found in buttons
                if not format_found and result.download_links:
                    for link in result.download_links:
                        link_text = link.get("text", "").lower()
                        if self.pdf_pattern.search(link_text):
                            result.file_formats.append("pdf")
                            format_found = True
                            break
                        elif self.pptx_pattern.search(link_text):
                            result.file_formats.append("pptx")
                            format_found = True
                            break
                        elif self.docx_pattern.search(link_text):
                            result.file_formats.append("docx")
                            format_found = True
                            break
                        elif self.hwp_pattern.search(link_text):
                            result.file_formats.append("hwp")
                            format_found = True
                            break
                
                # Set a default format if download detected but format unclear
                if not format_found and (result.download_buttons or result.download_links):
                    # Look at page context for clues
                    try:
                        page_text = driver.page_source.lower()
                        if "ppt" in page_text or "프레젠테이션" in page_text:
                            result.file_formats.append("pptx")
                        elif "hwp" in page_text or "한글" in page_text:
                            result.file_formats.append("hwp")
                        elif "excel" in page_text or "엑셀" in page_text:
                            result.file_formats.append("xlsx")
                        elif "word" in page_text or "워드" in page_text:
                            result.file_formats.append("docx")
                        else:
                            # PDF is most common default document format
                            result.file_formats.append("pdf")
                    except:
                        # Default to PDF if we can't access the page text
                        result.file_formats.append("pdf")
            
            # Log detailed information for debugging
            link_urls = [link.get("url", "no-url") for link in result.download_links]
            logging.info(f"[페이지 {pid}] 다운로드 검색 결과: {result.has_download}, 파일 형식: {result.file_formats}")
            logging.info(f"[페이지 {pid}] 다운로드 링크 수: {len(result.download_links)}, 버튼 수: {len(result.download_buttons)}")
            if result.download_links:
                logging.info(f"[페이지 {pid}] 첫 번째 링크 URL: {link_urls[0] if link_urls else 'None'}")
            
        except Exception as e:
            logging.error(f"[페이지 {pid}] 다운로드 검색 오류: {e}")
        
        return result
    
    def check_for_downloads_soup(self, soup: BeautifulSoup, url: str, pid: str) -> DownloadInfo:
        """
        Check for downloadable files using BeautifulSoup
        
        Args:
            soup: BeautifulSoup object
            url: Page URL
            pid: Post ID
            
        Returns:
            DownloadInfo object
        """
        result = DownloadInfo()
        
        try:
            # 1. Find download buttons by text
            download_buttons = soup.find_all(string=re.compile(r'다운로드|download|자료|첨부', re.IGNORECASE))
            
            # 2. Find links with file extensions
            file_links = soup.find_all('a', href=re.compile(r'\.(pdf|pptx?|docx?|hwp|xlsx?|zip|rar)($|\?)', re.IGNORECASE))
            
            # Find links that might have downloadable files but don't have the extension in URL
            pdf_links = soup.find_all('a', string=re.compile(r'pdf', re.IGNORECASE))
            ppt_links = soup.find_all('a', string=re.compile(r'pptx?|ppt|프레젠테이션', re.IGNORECASE))
            doc_links = soup.find_all('a', string=re.compile(r'docx?|doc|워드|문서', re.IGNORECASE))
            hwp_links = soup.find_all('a', string=re.compile(r'hwp|한글', re.IGNORECASE))
            excel_links = soup.find_all('a', string=re.compile(r'xlsx?|엑셀|스프레드시트', re.IGNORECASE))
            
            # Find links with common download-related attributes
            attr_links = soup.find_all('a', attrs={'download': True}) + \
                        soup.find_all('a', attrs={'data-downloadurl': True}) + \
                        soup.find_all('a', attrs={'target': '_blank', 'class': re.compile(r'down|file', re.IGNORECASE)})
            
            # Add all potential file links if not already in file_links
            for link_list in [pdf_links, ppt_links, doc_links, hwp_links, excel_links, attr_links]:
                file_links.extend([link for link in link_list if link not in file_links])
                
            # Find generic download buttons that might have files
            download_links = soup.find_all('a', string=re.compile(r'다운로드|download|자료|첨부', re.IGNORECASE))
            file_links.extend([link for link in download_links if link not in file_links])
            
            # 2.5 Find elements with download-related class names
            class_links = soup.find_all(class_=re.compile(r'download|file|attach', re.IGNORECASE))
            for el in class_links:
                if el.name == 'a' and el not in file_links and el.get('href'):
                    file_links.append(el)
            
            # 2.6 직접 HTML 소스에서 파일명 패턴 찾기 (예: 월부_서울기초반_가형_임장보고서탬플릿_1주차.pdf)
            html_source = str(soup)
            filename_matches = self.filename_pattern.findall(html_source)
            
            for filename, ext in filename_matches:
                # 파일명이 발견되고 그 주변에 다운로드 관련 텍스트가 있는지 확인
                context_start = max(0, html_source.find(filename) - 50)
                context_end = min(len(html_source), html_source.find(filename) + len(filename) + 50)
                context = html_source[context_start:context_end].lower()
                
                # 다운로드 관련 단어가 주변에 있거나 <span class="text-sm font-semibold">다운로드</span> 패턴이 있는지 확인
                if "다운로드" in context or "download" in context or "첨부파일" in context or \
                   '<span class="text-sm font-semibold">다운로드</span>' in html_source:
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
                parent = button.parent
                if not parent:
                    continue
                    
                result.has_download = True
                result.download_buttons.append({
                    "text": button.strip(),
                    "element": parent.name
                })
                
                # Handle links
                if parent.name == "a" and parent.get('href'):
                    href = parent.get('href')
                    full_url = href if href.startswith('http') else urljoin(url, href)
                    
                    # Skip certificate PDFs
                    if "certificate" in full_url and "원격평생교육원" in button.strip():
                        continue
                        
                    result.download_links.append({
                        "url": full_url,
                        "text": button.strip()
                    })
                    
                    # Extract file extension using the helper function
                    ext = self.extract_file_extension(full_url)
                    if ext and ext not in result.file_formats and \
                       ext not in ["jpg", "jpeg", "png", "gif"]:
                        result.file_formats.append(ext)
            
            # 4. Process file links
            for link in file_links:
                href = link.get('href')
                if not href:
                    continue
                    
                full_url = href if href.startswith('http') else urljoin(url, href)
                
                # Skip certificate PDFs
                if "certificate" in full_url and "원격평생교육원" in link.get_text(strip=True):
                    continue
                    
                result.has_download = True
                
                # Check if this URL is already in download_links
                found = False
                for link_info in result.download_links:
                    if link_info.get("url") == full_url:
                        found = True
                        break
                        
                if not found:
                    result.download_links.append({
                        "url": full_url,
                        "text": link.get_text(strip=True) or os.path.basename(full_url.split("?")[0])
                    })
                
                # Extract file extension using the helper function
                ext = self.extract_file_extension(full_url)
                if ext and ext not in result.file_formats and \
                   ext not in ["jpg", "jpeg", "png", "gif"]:
                    result.file_formats.append(ext)
            
            # 5. Apply heuristics for download buttons without identifiable file formats
            if result.has_download and not result.file_formats:
                # Check if any download button text suggests a specific format
                format_found = False
                for button in result.download_buttons:
                    button_text = button["text"].lower()
                    if self.pdf_pattern.search(button_text):
                        result.file_formats.append("pdf")
                        format_found = True
                        break
                    elif self.pptx_pattern.search(button_text):
                        result.file_formats.append("pptx")
                        format_found = True
                        break
                    elif self.docx_pattern.search(button_text):
                        result.file_formats.append("docx")
                        format_found = True
                        break
                    elif self.hwp_pattern.search(button_text):
                        result.file_formats.append("hwp")
                        format_found = True
                        break
                
                # Check download links if no format found in buttons
                if not format_found and result.download_links:
                    for link in result.download_links:
                        link_text = link.get("text", "").lower()
                        if self.pdf_pattern.search(link_text):
                            result.file_formats.append("pdf")
                            format_found = True
                            break
                        elif self.pptx_pattern.search(link_text):
                            result.file_formats.append("pptx")
                            format_found = True
                            break
                        elif self.docx_pattern.search(link_text):
                            result.file_formats.append("docx")
                            format_found = True
                            break
                        elif self.hwp_pattern.search(link_text):
                            result.file_formats.append("hwp")
                            format_found = True
                            break
                
                # Set a default format if download detected but format unclear
                if not format_found and (result.download_buttons or result.download_links):
                    # Check page context for clues
                    try:
                        page_text = soup.get_text().lower()
                        if "ppt" in page_text or "프레젠테이션" in page_text:
                            result.file_formats.append("pptx")
                        elif "hwp" in page_text or "한글" in page_text:
                            result.file_formats.append("hwp")
                        elif "excel" in page_text or "엑셀" in page_text:
                            result.file_formats.append("xlsx")
                        elif "word" in page_text or "워드" in page_text:
                            result.file_formats.append("docx")
                        else:
                            # PDF is most common default document format
                            result.file_formats.append("pdf")
                    except:
                        # Default to PDF if we can't access the page text
                        result.file_formats.append("pdf")
            
            # Log detailed information for debugging
            link_urls = [link.get("url", "no-url") for link in result.download_links]
            logging.info(f"[페이지 {pid}] API 다운로드 검색 결과: {result.has_download}, 파일 형식: {result.file_formats}")
            logging.info(f"[페이지 {pid}] API 다운로드 링크 수: {len(result.download_links)}, 버튼 수: {len(result.download_buttons)}")
            if result.download_links:
                logging.info(f"[페이지 {pid}] 첫 번째 링크 URL: {link_urls[0] if link_urls else 'None'}")
            
        except Exception as e:
            logging.error(f"[페이지 {pid}] API 다운로드 검색 오류: {e}")
            logging.exception(e)
        
        return result