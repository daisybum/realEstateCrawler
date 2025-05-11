#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File processing utilities for real estate crawler
"""

import os
import logging
import re
from typing import List, Optional, Dict, Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By

from src.config import Config
from src.models.models import FileContent, DownloadInfo


class FileProcessor:
    """Handles file detection and processing"""
    
    def __init__(self, scraper=None):
        """
        Initialize file processor
        
        Args:
            scraper: Cloudscraper instance for HTTP requests
        """
        self.scraper = scraper
        self.file_ext_pattern = re.compile(r"\.(pdf|pptx?|docx?|xlsx?|hwp)($|\?)", re.IGNORECASE)
    
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
        
        # Map file extensions to handlers
        handlers = {
            ".pdf": self._parse_pdf,
            ".pptx": self._parse_pptx,
            ".ppt": self._parse_pptx,
            ".docx": self._parse_docx,
            ".doc": self._parse_docx,
            ".hwp": self._parse_hwp
        }
        
        # Process file if handler exists
        if file_ext in handlers:
            try:
                return [handlers[file_ext](url, filename)]
            except Exception as e:
                logging.error(f"파일 처리 오류 ({file_ext}): {e}")
        
        return []
    
    def _parse_pdf(self, url: str, filename: str) -> FileContent:
        """
        Extract text from PDF file
        
        Args:
            url: PDF URL
            filename: PDF filename
            
        Returns:
            FileContent object with PDF data
        """
        # In a real implementation, download and process PDF
        # For now, just create a placeholder
        return FileContent(
            filename=filename,
            url=url,
            file_type="pdf",
            content=f"PDF 파일 다운로드 링크: {url}\n파일명: {filename}"
        )
    
    def _parse_pptx(self, url: str, filename: str) -> FileContent:
        """
        Extract text from PowerPoint file
        
        Args:
            url: PowerPoint URL
            filename: PowerPoint filename
            
        Returns:
            FileContent object with PowerPoint data
        """
        # In a real implementation, download and process PPTX
        return FileContent(
            filename=filename,
            url=url,
            file_type="pptx",
            content=f"PowerPoint 파일 다운로드 링크: {url}\n파일명: {filename}"
        )
    
    def _parse_docx(self, url: str, filename: str) -> FileContent:
        """
        Extract text from Word file
        
        Args:
            url: Word URL
            filename: Word filename
            
        Returns:
            FileContent object with Word data
        """
        # In a real implementation, download and process DOCX
        return FileContent(
            filename=filename,
            url=url,
            file_type="docx",
            content=f"Word 파일 다운로드 링크: {url}\n파일명: {filename}"
        )
    
    def _parse_hwp(self, url: str, filename: str) -> FileContent:
        """
        Extract text from HWP file
        
        Args:
            url: HWP URL
            filename: HWP filename
            
        Returns:
            FileContent object with HWP data
        """
        # In a real implementation, download and process HWP
        return FileContent(
            filename=filename,
            url=url,
            file_type="hwp",
            content=f"HWP 파일 다운로드 링크: {url}\n파일명: {filename}"
        )


class DownloadDetector:
    """Detects downloadable files in web pages"""
    
    def __init__(self):
        """Initialize download detector"""
        self.file_ext_pattern = re.compile(r"\.(pdf|pptx?|docx?|xlsx?|hwp)($|\?)", re.IGNORECASE)
    
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
            
            # 2. Find links with file extensions
            file_links = driver.find_elements(By.XPATH, 
                "//a[contains(@href, '.pptx') or contains(@href, '.pdf') or contains(@href, '.docx') or contains(@href, '.hwp')]")
            
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
                        
                        # Extract file extension
                        file_ext = os.path.splitext(href.split("?")[0])[1].lower()
                        if file_ext and file_ext[1:] not in result.file_formats and \
                           file_ext[1:] not in ["jpg", "jpeg", "png", "gif"]:
                            result.file_formats.append(file_ext[1:])
            
            # 4. Process file links
            for link in file_links:
                href = link.get_attribute("href")
                if not href:
                    continue
                    
                # Skip certificate PDFs
                if "certificate" in href and "원격평생교육원" in link.text:
                    continue
                    
                result.has_download = True
                result.download_links.append({
                    "url": href,
                    "text": link.text.strip() or os.path.basename(href.split("?")[0])
                })
                
                # Extract file extension
                file_ext = os.path.splitext(href.split("?")[0])[1].lower()
                if file_ext and file_ext[1:] not in result.file_formats and \
                   file_ext[1:] not in ["jpg", "jpeg", "png", "gif"]:
                    result.file_formats.append(file_ext[1:])
            
            # 5. Apply heuristics for download buttons without identifiable file formats
            if result.download_buttons and not result.file_formats:
                result.file_formats = ["pptx"]  # Default to PPTX
            
            logging.info(f"[페이지 {pid}] 다운로드 검색 결과: {result.has_download}, 파일 형식: {result.file_formats}")
            
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
            download_buttons = soup.find_all(string=re.compile(r'다운로드|download', re.IGNORECASE))
            
            # 2. Find links with file extensions
            file_links = soup.find_all('a', href=re.compile(r'\.(pdf|pptx?|docx?|hwp)($|\?)', re.IGNORECASE))
            
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
                    
                    # Extract file extension
                    file_ext = os.path.splitext(full_url.split("?")[0])[1].lower()
                    if file_ext and file_ext[1:] not in result.file_formats and \
                       file_ext[1:] not in ["jpg", "jpeg", "png", "gif"]:
                        result.file_formats.append(file_ext[1:])
            
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
                result.download_links.append({
                    "url": full_url,
                    "text": link.get_text(strip=True) or os.path.basename(full_url.split("?")[0])
                })
                
                # Extract file extension
                file_ext = os.path.splitext(full_url.split("?")[0])[1].lower()
                if file_ext and file_ext[1:] not in result.file_formats and \
                   file_ext[1:] not in ["jpg", "jpeg", "png", "gif"]:
                    result.file_formats.append(file_ext[1:])
            
            # 5. Apply heuristics for download buttons without identifiable file formats
            if result.download_buttons and not result.file_formats:
                result.file_formats = ["pptx"]  # Default to PPTX
            
            logging.info(f"[페이지 {pid}] API 다운로드 검색 결과: {result.has_download}, 파일 형식: {result.file_formats}")
            
        except Exception as e:
            logging.error(f"[페이지 {pid}] API 다운로드 검색 오류: {e}")
        
        return result
