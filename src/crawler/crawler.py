#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main crawler class for real estate crawler
"""

import re
import os
import time
import logging
import hashlib
import urllib.robotparser
from datetime import datetime
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Any, Tuple, Optional, Generator, Set

from bs4 import BeautifulSoup

import requests
import cloudscraper
from tqdm import tqdm
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from requests_html import HTMLSession

from src.config import Config
from src.crawler.auth import Authenticator, AuthenticationError
from src.parser.parser import ContentParser, ListParser
from src.models.models import Post, DownloadInfo, Image, FileContent
from src.storage.file_processor import FileProcessor, DownloadDetector
from src.storage.storage import JsonlStorage, CheckpointManager


# Patterns of words that indicate non-downloadable or irrelevant resources
# Centralised so that filtering rules stay consistent across the crawler.
EXCLUDE_PATTERNS = [
    "이미지", "사진", "갤러리", "썸네일", "미리보기", "광고", "배너", "로고",
    "certificate", "원격평생교육원", "인증서", "자격증", "수료증", "교육이수증",
    "학위증", "졸업증명서", "증명서", "인증", "logo", "banner", "thumbnail"
]


class CrawlerError(Exception):
    """Base exception for crawler errors"""
    pass


class RobotsError(CrawlerError):
    """Exception raised when a URL is not allowed by robots.txt"""
    pass


class Crawler:
    """Main crawler class that orchestrates the crawling process"""
    
    def __init__(self, config=None):
        """Initialize the crawler with all required components"""
        # Load configuration
        self.config = config or Config.get_instance()
        
        # Initialize core components
        self.scraper = cloudscraper.create_scraper()
        self.html_session = HTMLSession()
        self.session = requests.Session()
        
        # Set up robots.txt parser
        self.robots_parser = None
        self._init_robots_txt()
        
        # Rate limiting settings
        self.last_request_time = None
        self.request_count = 0
        self.rate_limit_window_start = datetime.now()
        
        # Create driver
        self.driver = self._create_driver()
        
        # Initialize component instances
        self.authenticator = Authenticator(self.config)
        self.content_parser = ContentParser(scraper=self.scraper, html_session=self.html_session)
        self.list_parser = ListParser(scraper=self.scraper)
        self.file_processor = FileProcessor(scraper=self.scraper)
        self.download_detector = DownloadDetector()
        self.storage = JsonlStorage()
        self.checkpoint_manager = CheckpointManager()
        
        # Authentication state
        self.auth_headers = None
        
        # URL tracking
        self.visited_urls = set()
        
    def _init_robots_txt(self):
        """Initialize robots.txt parser"""
        try:
            self.robots_parser = urllib.robotparser.RobotFileParser()
            robots_url = f"{self.config.base_url}/robots.txt"
            self.robots_parser.set_url(robots_url)
            self.robots_parser.read()
            logging.info(f"Loaded robots.txt from {robots_url}")
        except Exception as e:
            logging.warning(f"Could not load robots.txt: {e}. Will assume all URLs are allowed.")
            # Create a permissive parser as fallback
            self.robots_parser = urllib.robotparser.RobotFileParser()
            self.robots_parser.allow_all = True
    
    def _can_fetch(self, url):
        """Check if URL is allowed by robots.txt
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is allowed, False otherwise
        """
        if not self.robots_parser:
            return True
            
        try:
            return self.robots_parser.can_fetch(self.config.user_agent, url)
        except Exception as e:
            logging.warning(f"Error checking robots.txt for {url}: {e}. Assuming allowed.")
            return True
    
    def _rate_limit(self):
        """Apply rate limiting to avoid overloading the server
        
        This implements a token bucket algorithm for rate limiting
        """
        if not self.config.rate_limit_enabled:
            return
            
        now = datetime.now()
        
        # Reset counter if we're in a new time window
        window_duration = (now - self.rate_limit_window_start).total_seconds()
        if window_duration > self.config.rate_limit_period:
            self.request_count = 0
            self.rate_limit_window_start = now
            return
            
        # Check if we've exceeded our rate limit
        if self.request_count >= self.config.rate_limit_requests:
            # Calculate sleep time
            sleep_time = self.config.rate_limit_period - window_duration
            if sleep_time > 0:
                logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                # Reset after sleeping
                self.request_count = 0
                self.rate_limit_window_start = datetime.now()
        else:
            # Add a small delay between requests for politeness
            if self.last_request_time:
                elapsed = (now - self.last_request_time).total_seconds()
                if elapsed < self.config.wait_between_pages and elapsed > 0:
                    time.sleep(self.config.wait_between_pages - elapsed)
        
        # Update last request time
        self.last_request_time = datetime.now()
        self.request_count += 1
    
    def get_page(self, url: str, headers: Optional[Dict[str, str]] = None) -> str:
        """Get a page with rate limiting and robots.txt checking
        
        Args:
            url: URL to fetch
            headers: Optional headers to include
            
        Returns:
            Page content as string
            
        Raises:
            RobotsError: If URL is not allowed by robots.txt
            requests.RequestException: If request fails
        """
        # Check robots.txt
        if not self._can_fetch(url):
            raise RobotsError(f"URL not allowed by robots.txt: {url}")
            
        # Apply rate limiting
        self._rate_limit()
        
        # Ensure we have authentication
        self.auth_headers, self.driver = self.authenticator.ensure_authenticated()
        
        # Set default headers
        if headers is None:
            headers = {}
            
        # Add common headers
        headers.update({
            'User-Agent': self.config.user_agent,
            'Referer': self.config.base_url
        })
        
        # Add auth headers
        if self.auth_headers:
            headers.update(self.auth_headers)
        
        # Make request with retries
        max_retries = self.config.max_retries
        retry_delay = self.config.retry_delay
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, headers=headers, timeout=self.config.request_timeout)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    # Exponential backoff
                    sleep_time = retry_delay * (2 ** attempt)
                    logging.warning(f"Request failed: {e}. Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logging.error(f"Request failed after {max_retries} attempts: {e}")
                    raise
    
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
                # Construct page URL
                page_url = f"{self.config.specific_list_url}&page={page}"
                
                # Check if URL is allowed by robots.txt
                if not self._can_fetch(page_url):
                    logging.warning(f"Page URL not allowed by robots.txt: {page_url}")
                    break
                
                # Get post list for current page
                try:
                    posts = self.list_parser.parse_list_api(page, self.auth_headers, self.driver)
                except Exception as e:
                    logging.error(f"Page {page} failed: {e}")
                    # Try a few more times with exponential backoff
                    retry_success = False
                    for retry in range(self.config.max_retries):
                        backoff = self.config.retry_delay * (2 ** retry)
                        logging.info(f"Retrying page {page} in {backoff} seconds...")
                        time.sleep(backoff)
                        try:
                            posts = self.list_parser.parse_list_api(page, self.auth_headers, self.driver)
                            retry_success = True
                            break
                        except Exception as retry_e:
                            logging.error(f"Retry {retry+1} failed: {retry_e}")
                    
                    if not retry_success:
                        logging.error(f"Giving up on page {page} after {self.config.max_retries} retries")
                        break
                
                # Break if no posts found
                if not posts:
                    logging.info(f"No posts found on page {page}, ending crawl")
                    break
                
                # Process each post
                for title, link in tqdm(posts, desc=f"Posts p{page}", leave=False):
                    # Skip if URL is not allowed by robots.txt
                    if not self._can_fetch(link):
                        logging.warning(f"Post URL not allowed by robots.txt: {link}")
                        continue
                    
                    # Skip if we've already visited this URL
                    if link in self.visited_urls:
                        logging.info(f"Skipping already visited URL: {link}")
                        continue
                    
                    # Mark URL as visited
                    self.visited_urls.add(link)
                    
                    # Extract post ID from URL
                    pid_match = re.search(r"/community/(\d+)", link)
                    if not pid_match:
                        logging.warning(f"Could not extract post ID from {link}")
                        continue
                        
                    pid = pid_match.group(1)
                    
                    # Apply rate limiting
                    self._rate_limit()
                    
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
                
                # Apply rate limiting between pages
                self._rate_limit()
            
            pbar.close()
            
        finally:
            # Clean up
            if self.driver:
                self.driver.quit()
    
    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL by adding base URL if needed and handling relative paths
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        # Handle empty URLs
        if not url:
            return ""
            
        # Handle URLs that are already absolute
        if url.startswith('http://') or url.startswith('https://'):
            return url
            
        # Handle relative URLs
        return urljoin(self.config.base_url, url)
    
    def _is_valid_url(self, url: str) -> bool:
        """
        Check if URL is valid
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is valid, False otherwise
        """
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def _generate_url_hash(self, url: str) -> str:
        """
        Generate a hash for a URL to use as a unique identifier
        
        Args:
            url: URL to hash
            
        Returns:
            Hash of URL
        """
        return hashlib.md5(url.encode('utf-8')).hexdigest()
    
    def _detect_downloadable_files(self, html_content: str) -> List[Dict[str, str]]:
        """
        Detect downloadable files using the specified element
        
        Args:
            html_content: HTML content of the page
            
        Returns:
            List of dictionaries containing download information
        """
        download_links = []
        
        try:
            # Method 1: Using BeautifulSoup to find the download span
            soup = BeautifulSoup(html_content, 'html.parser')
            download_spans = soup.find_all('span', class_='text-sm font-semibold', string='다운로드')
            
            for span in download_spans:
                # Navigate to parent elements to find the actual download link
                parent_li = span.find_parent('li')
                if parent_li:
                    links = parent_li.find_all('a')
                    for link in links:
                        if link.get('href'):
                            download_links.append({
                                'href': link.get('href'),
                                'text': link.get_text(strip=True) or '다운로드 파일'
                            })
            
            # If no links found via BeautifulSoup, try XPath
            if not download_links:
                # Method 2: Using lxml's XPath
                import lxml.html
                tree = lxml.html.fromstring(html_content)
                download_elements = tree.xpath('/html/body/div[1]/div[3]/div/div/section[1]/div[4]/ul/li/div/div[2]/span')
                
                for element in download_elements:
                    # Find parent li and then find links
                    parent = element.getparent()
                    while parent is not None and parent.tag != 'li':
                        parent = parent.getparent()
                    
                    if parent is not None:
                        links = parent.xpath('.//a')
                        for link in links:
                            if link.get('href'):
                                download_links.append({
                                    'href': link.get('href'),
                                    'text': link.text_content().strip() or '다운로드 파일'
                                })
        except Exception as e:
            logging.error(f"Error detecting downloadable files: {e}")
        
        return download_links
    
    def _download_and_parse_file(self, url: str, filename: str) -> Dict[str, Any]:
        """
        Download and parse a file in-memory without saving to disk
        
        Args:
            url: URL of the file to download
            filename: Filename to use for the file
            
        Returns:
            Dictionary containing parsed content
        """
        try:
            # Apply rate limiting
            self._rate_limit()
            
            # Check robots.txt
            if not self._can_fetch(url):
                logging.warning(f"URL not allowed by robots.txt: {url}")
                return {"error": "URL not allowed by robots.txt"}
            
            # Download file in-memory
            response = self.session.get(
                url, 
                headers={
                    'User-Agent': self.config.user_agent,
                    'Referer': self.config.base_url
                },
                stream=True,
                timeout=self.config.request_timeout
            )
            response.raise_for_status()
            
            # Get file content as bytes
            file_bytes = response.content
            
            # Determine file type from filename
            file_ext = os.path.splitext(filename)[1].lower()
            
            # Parse file based on type using BytesIO (in-memory)
            from io import BytesIO
            file_obj = BytesIO(file_bytes)
            
            # Import DocumentParser from parser directory
            from src.parser.document_parser import DocumentParser
            
            # Create document parser instance if not already created
            if not hasattr(self, 'document_parser'):
                self.document_parser = DocumentParser()
            
            # Use the document parser to parse the file bytes
            parsed_content = {}
            
            # Create a temporary file to pass to the document parser
            # This is needed because some parsers require a file path
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=file_ext, delete=False) as temp_file:
                temp_file.write(file_bytes)
                temp_file_path = temp_file.name
            
            try:
                # Parse the document using the appropriate parser based on file extension
                parsed_result = self.document_parser.parse_document(temp_file_path)
                
                # Copy the parsed result to our output dictionary
                parsed_content = parsed_result
                
                # Add filename and URL to the parsed content
                parsed_content["filename"] = filename
                parsed_content["url"] = url
                
            finally:
                # Clean up the temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception as e:
                    logging.warning(f"Failed to delete temporary file {temp_file_path}: {e}")
            
            # Process OCR text from images if available
            if "images" in parsed_content and parsed_content["images"]:
                ocr_texts = []
                for img in parsed_content["images"]:
                    if "ocr_text" in img and img["ocr_text"].strip():
                        ocr_texts.append(f"Image OCR: {img['ocr_text']}")
                
                if ocr_texts:
                    # Add OCR text to content
                    if "content" in parsed_content:
                        parsed_content["content"] += "\n\n===== OCR Text from Images =====\n" + "\n\n".join(ocr_texts)
                    else:
                        parsed_content["content"] = "===== OCR Text from Images =====\n" + "\n\n".join(ocr_texts)
            
            # Process tables if available
            if "tables" in parsed_content and parsed_content["tables"]:
                table_texts = []
                for idx, table in enumerate(parsed_content["tables"]):
                    if "data" in table and table["data"]:
                        table_texts.append(f"Table {idx+1}:\n" + self._format_table_data(table["data"]))
                
                if table_texts:
                    # Add table text to content
                    if "content" in parsed_content:
                        parsed_content["content"] += "\n\n===== Tables =====\n" + "\n\n".join(table_texts)
                    else:
                        parsed_content["content"] = "===== Tables =====\n" + "\n\n".join(table_texts)
            
            return parsed_content
            
        except Exception as e:
            logging.error(f"Error downloading and parsing file: {e}")
            return {"error": str(e)}
    
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
        # Normalize URL
        url = self._normalize_url(url)
        
        # Validate URL
        if not self._is_valid_url(url):
            logging.error(f"Invalid URL: {url}")
            return []
        
        # Initialize post object
        post = Post(
            post_id=pid,
            title=title,
            url=url,
            content="",
            images=[],
            files=[]
        )
        
        try:
            # Apply rate limiting
            self._rate_limit()
            
            # Check robots.txt
            if not self._can_fetch(url):
                logging.warning(f"URL not allowed by robots.txt: {url}")
                post_dict = post.to_dict()
                post_dict["_download_summary"] = "[로봇 정책에 의해 차단됨]"
                return [post_dict]
            
            # Get content
            content_html = self.content_parser.get_content(url, self.auth_headers, self.driver)
            if not content_html:
                logging.warning(f"[페이지 {pid}] 내용을 가져올 수 없습니다")
                post_dict = post.to_dict()
                post_dict["_download_summary"] = "[내용 없음]"
                return [post_dict]
            
            # First, check for downloadable files using the specified element
            download_links = self._detect_downloadable_files(content_html)
            
            # If downloadable files are found, prioritize parsing those files
            if download_links:
                logging.info(f"[페이지 {pid}] 다운로드 파일 발견: {len(download_links)} 개")
                
                # Process each downloadable file in-memory
                parsed_files = []
                for link in download_links:
                    download_url = self._normalize_url(link.get('href', ''))
                    if not download_url:
                        continue
                        
                    filename = link.get('text', '') or f"file_{self._generate_url_hash(download_url)}"
                    if not os.path.splitext(filename)[1]:
                        # Add default extension if none exists
                        filename += ".pdf"  # Default to PDF if no extension
                    
                    # Download and parse file in-memory
                    parsed_content = self._download_and_parse_file(download_url, filename)
                    parsed_files.append(parsed_content)
                
                # Add parsed files to post
                post.parsed_files = parsed_files
                
                # Skip HTML content parsing since we're prioritizing document parsing
                post.content = ""  # Skip HTML content
                post.images = []   # Skip images
                
                # Create download summary
                file_names = [f.get("filename", "Unknown") for f in parsed_files if "error" not in f]
                if file_names:
                    download_summary = f"[다운로드 파일 처리: {', '.join(file_names)}]"
                else:
                    download_summary = "[다운로드 파일 처리 실패]"
            else:
                # No downloadable files found, proceed with normal HTML parsing
                # Parse content
                post.content = self.content_parser.extract_text(content_html)
                
                # Extract images with rate limiting
                images = self.content_parser.extract_images(content_html, pid)
                for img in images:
                    # Normalize image URL
                    img.url = self._normalize_url(img.url)
                    # Check robots.txt for image URL
                    if self._can_fetch(img.url):
                        # Apply rate limiting before fetching image
                        self._rate_limit()
                        post.images.append(img)
                    else:
                        logging.warning(f"Image URL not allowed by robots.txt: {img.url}")
                
                # Check for downloadable files using the old method
                download_info = self.download_detector.detect_downloads(content_html)
                if download_info:
                    post = self._process_files(post, download_info)
                    
                # Create download summary
                if post.files:
                    file_names = [f.filename for f in post.files]
                    download_summary = f"[다운로드: {', '.join(file_names)}]"
                else:
                    download_summary = "[다운로드 없음]"
            
            # Add download summary to post dict
            post_dict = post.to_dict()
            post_dict["_download_summary"] = download_summary
            
            return [post_dict]
            
        except Exception as e:
            logging.error(f"[페이지 {pid}] 처리 오류: {e}")
            post_dict = post.to_dict()
            post_dict["_download_summary"] = f"[오류: {str(e)}]"
            return [post_dict]
    
    def _format_table_data(self, table_data):
        """
        Format table data for display
        
        Args:
            table_data: Table data to format
            
        Returns:
            Formatted table string
        """
        if isinstance(table_data, list):
            if all(isinstance(row, dict) for row in table_data):
                # List of dictionaries format
                if not table_data:
                    return ""
                    
                # Get all keys from all dictionaries
                all_keys = set()
                for row in table_data:
                    all_keys.update(row.keys())
                    
                # Sort keys for consistent output
                keys = sorted(all_keys)
                
                # Format as table
                result = [" | ".join(keys)]
                result.append("-" * (sum(len(k) for k in keys) + 3 * (len(keys) - 1)))
                
                for row in table_data:
                    row_values = [str(row.get(k, "")) for k in keys]
                    result.append(" | ".join(row_values))
                    
                return "\n".join(result)
            else:
                # List of lists format
                if not table_data:
                    return ""
                    
                result = []
                for row in table_data:
                    if isinstance(row, list):
                        result.append(" | ".join(str(cell) for cell in row))
                    else:
                        result.append(str(row))
                        
                return "\n".join(result)
        else:
            # Unknown format
            return str(table_data)
    
    def _process_files(self, post: Post, download_info: Dict[str, Any]) -> Post:
        """
        Process downloadable files
        
        Args:
            post: Post object
            download_info: Download information
            
        Returns:
            Updated Post object
        """
        # Check if we have download links
        if not download_info or not hasattr(download_info, 'download_links') or not download_info.download_links:
            return post
            
        # UUID pattern for filename cleaning
        uuid_pattern = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
        
        # Files to exclude
        exclude_files = ["BC.pdf", "BC.docx", "BC.xlsx"]
        
        # Process each download link
        for link in download_info.download_links:
            try:
                # Extract download URL
                download_url = link.get('href', '')
                if not download_url:
                    logging.warning(f"[페이지 {post.post_id}] 다운로드 URL이 없습니다: {link}")
                    continue
                    
                # Normalize URL
                download_url = self._normalize_url(download_url)
                
                # Check if URL is valid
                if not self._is_valid_url(download_url):
                    logging.warning(f"[페이지 {post.post_id}] 유효하지 않은 URL: {download_url}")
                    continue
                    
                # Check robots.txt
                if not self._can_fetch(download_url):
                    logging.warning(f"[페이지 {post.post_id}] 로봇 정책에 의해 차단된 URL: {download_url}")
                    continue
                    
                # Apply rate limiting
                self._rate_limit()
                
                # Extract filename
                filename = None
                
                # 1. Try to extract from link text
                if link.get('text'):
                    # Remove UUID pattern
                    clean_text = uuid_pattern.sub('', link['text']).strip()
                    if clean_text:
                        # Check if it already has an extension
                        if any(clean_text.lower().endswith(ext) for ext in [".pdf", ".docx", ".pptx", ".xlsx", ".hwp"]):
                            filename = clean_text
                        else:
                            # Add extension based on URL or link text
                            if 'pdf' in download_url.lower() or 'pdf' in link['text'].lower():
                                filename = f"{clean_text}.pdf"
                            elif 'pptx' in download_url.lower() or 'ppt' in link['text'].lower():
                                filename = f"{clean_text}.pptx"
                            elif 'docx' in download_url.lower() or 'doc' in link['text'].lower():
                                filename = f"{clean_text}.docx"
                            elif 'xlsx' in download_url.lower() or 'excel' in link['text'].lower():
                                filename = f"{clean_text}.xlsx"
                            elif 'hwp' in download_url.lower() or 'hwp' in link['text'].lower():
                                filename = f"{clean_text}.hwp"
                
                # 2. Try to extract from URL
                if not filename and download_url:
                    url_parts = download_url.split('/')
                    if url_parts and url_parts[-1]:
                        # Check if it has an extension
                        if any(url_parts[-1].lower().endswith(ext) for ext in [".pdf", ".docx", ".pptx", ".xlsx", ".hwp"]):
                            filename = url_parts[-1]
                
                # 3. Infer from page content
                if not filename and link.get('text'):
                    page_content = getattr(download_info, 'page_content', '').lower()
                    if '.pdf' in page_content:
                        filename = f"{link['text']}.pdf"
                    elif '.pptx' in page_content or '.ppt' in page_content:
                        filename = f"{link['text']}.pptx"
                    elif '.docx' in page_content or '.doc' in page_content:
                        filename = f"{link['text']}.docx"
                    elif '.hwp' in page_content:
                        filename = f"{link['text']}.hwp"
                    elif '.xlsx' in page_content:
                        filename = f"{link['text']}.xlsx"
                    else:
                        # Default to PDF for generic download buttons
                        filename = f"{link['text']}.pdf"
                
                # 4. Use default if still no filename
                if not filename and link.get('text'):
                    filename = f"{link['text']}.pdf"  # Default to PDF as most common document type
                elif not filename:
                    # Generate a unique filename based on URL hash
                    url_hash = self._generate_url_hash(download_url)
                    filename = f"document_{url_hash}.pdf"
                
                # Skip excluded files
                if any(exclude in filename for exclude in exclude_files):
                    logging.info(f"[페이지 {post.post_id}] 제외된 파일 패턴: {filename}")
                    continue
                    
                # Process file and add to post
                logging.info(f"[페이지 {post.post_id}] 파일 다운로드 시도: {filename} ({download_url})")
                file_contents = self.file_processor.parse_file(download_url, post.post_id, filename)
                
                # Apply rate limiting after file download
                self._rate_limit()
                
                if file_contents:
                    post.files.extend(file_contents)
                    logging.info(f"[페이지 {post.post_id}] 파일 다운로드 성공: {filename}")
                else:
                    logging.warning(f"[페이지 {post.post_id}] 파일 다운로드 실패: {filename}")
                
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
