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
import traceback
import psutil
import gc
from datetime import datetime, timedelta
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
from src.storage.file_processor import FileProcessor
from src.crawler.download_detector import DownloadDetector
from src.storage.storage import JsonlStorage, CheckpointManager


# 상수 정의
# 다운로드 제외 패턴: 다운로드 링크로 감지하지 않을 패턴
EXCLUDE_PATTERNS = [
    "이미지", "사진", "갤러리", "썸네일", "미리보기", "광고", "배너", "로고",
    "certificate", "원격평생교육원", "인증서", "자격증", "수료증", "교육이수증",
    "학위증", "졸업증명서", "증명서", "인증", "logo", "banner", "thumbnail"
]

# 지원하는 파일 확장자
SUPPORTED_EXTENSIONS = [".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls", ".hwp"]

# 다운로드 관련 키워드
DOWNLOAD_KEYWORDS = ["첨부", "자료", "파일", "다운로드", "다운", "다운받기", "자료실", "문서"]


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
        
        # Performance monitoring
        self.start_time = datetime.now()
        self.memory_usage_samples = []
        self.cpu_usage_samples = []
        self.sample_interval = 60  # seconds between resource samples
        self.last_sample_time = None
        
        # Tracking visited URLs to avoid duplicates
        self.visited_urls = set()
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
        """Main crawling method for extracting posts from list pages and processing them"""
        start_time = datetime.now()
        total_posts_processed = 0
        total_files_processed = 0
        error_count = 0
        
        # Performance statistics
        performance_stats = {
            'network_errors': 0,
            'parsing_errors': 0,
            'file_processing_errors': 0,
            'request_times': [],
            'page_processing_times': [],
            'file_processing_times': []
        }
        
        try:
            logging.info(f"Starting crawl at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Login and get authentication headers
            try:
                self.auth_headers, self.driver = self.authenticator.login()
                logging.info("Authentication successful")
            except AuthenticationError as auth_e:
                logging.error(f"Authentication failed: {auth_e}")
                return
            except Exception as e:
                logging.error(f"Unexpected error during authentication: {e}")
                return
            
            # Get last processed page from checkpoint
            try:
                page = self.checkpoint_manager.get_last_page()
                logging.info(f"Resuming from page {page} based on checkpoint")
            except Exception as e:
                logging.warning(f"Could not get last page from checkpoint: {e}. Starting from page 1.")
                page = 1
            
            # Create progress bar
            pbar = tqdm(desc="Page", initial=page-1)
            
            # Track consecutive empty pages for better termination detection
            consecutive_empty_pages = 0
            max_consecutive_empty = self.config.max_consecutive_empty_pages if hasattr(self.config, 'max_consecutive_empty_pages') else 3
            
            # Process pages
            while True:
                # Construct page URL
                page_url = f"{self.config.specific_list_url}&page={page}"
                logging.info(f"Processing page {page}: {page_url}")
                
                # Check if URL is allowed by robots.txt
                if not self._can_fetch(page_url):
                    logging.warning(f"Page URL not allowed by robots.txt: {page_url}")
                    break
                
                # Get post list for current page
                try:
                    # Track page processing time
                    page_start_time = time.time()
                    
                    # Get page content first to extract pagination info
                    request_start_time = time.time()
                    page_content = self.get_page(page_url)
                    request_time = time.time() - request_start_time
                    performance_stats['request_times'].append(request_time)
                    
                    page_size = len(page_content)
                    logging.debug(f"Retrieved page {page} content ({page_size} bytes) in {request_time:.2f}s")
                    
                    if page_size < 100:  # Suspiciously small page
                        logging.warning(f"Page {page} content is suspiciously small ({page_size} bytes). Content: {page_content[:100]}")
                    
                    # Extract pagination info using the improved method
                    current_page, last_page, has_next = self._extract_pagination_info(page_content, page)
                    
                    # Log pagination info
                    logging.info(f"Page {current_page} of {last_page}, has next: {has_next}")
                    
                    # Check if we've reached the last page
                    if current_page >= last_page and not has_next:
                        logging.info(f"Reached last page ({last_page}), ending crawl")
                        break
                    
                    # Parse posts from the page
                    posts = self.parse_list_api(page, page_content)
                    
                    # Record page processing time
                    page_processing_time = time.time() - page_start_time
                    performance_stats['page_processing_times'].append(page_processing_time)
                    logging.debug(f"Processed page {page} in {page_processing_time:.2f}s")
                    
                except requests.exceptions.RequestException as req_e:
                    error_count += 1
                    performance_stats['network_errors'] += 1
                    logging.error(f"Network error on page {page}: {req_e}")
                    logging.debug(f"Network error details: {traceback.format_exc()}")
                    
                    # Monitor resources during error recovery
                    self._monitor_resources()
                    
                    # Try to recover with exponential backoff
                    retry_success = self._retry_page(page, page_url)
                    if not retry_success:
                        logging.error(f"Failed to recover from network error after retries, ending crawl")
                        break
                    continue
                    
                except Exception as e:
                    error_count += 1
                    performance_stats['parsing_errors'] += 1
                    logging.error(f"Error processing page {page}: {e}")
                    logging.debug(f"Error details: {traceback.format_exc()}")
                    
                    # Monitor resources during error recovery
                    self._monitor_resources()
                    
                    # Try to recover with exponential backoff
                    retry_success = self._retry_page(page, page_url)
                    if not retry_success:
                        logging.error(f"Failed to recover from parsing error after retries, ending crawl")
                        break
                    continue
                
                # Handle empty pages
                if not posts:
                    logging.info(f"No posts found on page {page}")
                    consecutive_empty_pages += 1
                    
                    if consecutive_empty_pages >= max_consecutive_empty:
                        logging.info(f"Found {consecutive_empty_pages} consecutive empty pages, ending crawl")
                        break
                else:
                    # Reset counter when we find posts
                    consecutive_empty_pages = 0
                    logging.info(f"Found {len(posts)} posts on page {page}")
                    total_posts_processed += len(posts)
                    
                    # Track file processing for statistics
                    files_on_page = 0
                
                # Monitor system resources periodically
                self._monitor_resources()
                
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
                    try:
                        post_records = self._parse_post(link, title, pid)
                        
                        # Count files processed
                        for rec in post_records:
                            if "parsed_files" in rec and rec["parsed_files"]:
                                files_on_page += len(rec["parsed_files"])
                                total_files_processed += len(rec["parsed_files"])
                        
                        # Get download summary
                        download_summary = "[다운로드 없음] "
                        for rec in post_records:
                            if "_download_summary" in rec:
                                download_summary = rec["_download_summary"]
                                break
                        
                        # Save checkpoint and posts
                        self.checkpoint_manager.save(page, download_summary)
                        self.storage.save_posts(post_records)
                    except Exception as e:
                        error_count += 1
                        logging.error(f"Error processing post {pid} ({link}): {e}", exc_info=True)
                        continue
                
                # Go to next page
                page += 1
                pbar.update(1)
                
                # Apply rate limiting between pages
                self._rate_limit()
            
            pbar.close()
            
            # Log crawl statistics
            end_time = datetime.now()
            duration = end_time - start_time
            logging.info(f"Crawl completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info(f"Crawl duration: {duration}")
            logging.info(f"Total pages processed: {page-1}")
            logging.info(f"Total posts processed: {total_posts_processed}")
            logging.info(f"Total files processed: {total_files_processed}")
            logging.info(f"Total errors encountered: {error_count}")
            
        finally:
            # Clean up resources
            logging.info("Cleaning up resources...")
            try:
                if self.driver:
                    self.driver.quit()
                    self.driver = None
                    logging.info("WebDriver closed successfully")
            except Exception as e:
                logging.error(f"Error closing WebDriver: {e}")
                
            # Force garbage collection
            gc.collect()
            
            # Log final resource usage
            try:
                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                memory_usage_mb = memory_info.rss / 1024 / 1024
                cpu_percent = process.cpu_percent(interval=0.1)
                logging.info(f"Final memory usage: {memory_usage_mb:.2f} MB, CPU usage: {cpu_percent:.2f}%")
            except Exception as e:
                logging.error(f"Error getting resource usage: {e}")
    
    def _retry_page(self, page: int, page_url: str) -> bool:
        """Retry fetching and processing a page with exponential backoff
        
        Args:
            page: Page number to retry
            page_url: URL of the page to retry
            
        Returns:
            True if retry was successful, False otherwise
        """
        retry_success = False
        for retry in range(self.config.max_retries):
            backoff = self.config.retry_delay * (2 ** retry)
            logging.info(f"Retrying page {page} in {backoff} seconds... (Attempt {retry+1}/{self.config.max_retries})")
            time.sleep(backoff)
            try:
                page_content = self.get_page(page_url)
                posts = self.parse_list_api(page, page_content)
                retry_success = True
                logging.info(f"Retry {retry+1} successful for page {page}")
                break
            except Exception as retry_e:
                logging.error(f"Retry {retry+1} failed for page {page}: {retry_e}")
        
        if not retry_success:
            logging.error(f"Giving up on page {page} after {self.config.max_retries} retries")
            # Save checkpoint before exiting
            try:
                self.checkpoint_manager.save_checkpoint(page - 1)
                logging.info(f"Saved checkpoint at page {page-1}")
            except Exception as e:
                logging.error(f"Failed to save checkpoint: {e}")
        
        return retry_success
        
    def _monitor_resources(self) -> None:
        """Monitor system resources and log if thresholds are exceeded
        
        This method samples CPU and memory usage at regular intervals and logs warnings
        if usage exceeds configured thresholds. It also performs garbage collection
        if memory usage is high.
        """
        now = datetime.now()
        
        # Only sample at configured intervals
        if self.last_sample_time and (now - self.last_sample_time).total_seconds() < self.sample_interval:
            return
            
        self.last_sample_time = now
        
        # Get current memory usage
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        memory_usage_mb = memory_info.rss / 1024 / 1024
        self.memory_usage_samples.append(memory_usage_mb)
        
        # Get CPU usage
        cpu_percent = process.cpu_percent(interval=0.1)
        self.cpu_usage_samples.append(cpu_percent)
        
        # Log resource usage
        logging.debug(f"Memory usage: {memory_usage_mb:.2f} MB, CPU usage: {cpu_percent:.2f}%")
        
        # Check if memory usage is high
        memory_threshold = getattr(self.config, 'memory_threshold_mb', 1000)  # Default 1GB
        if memory_usage_mb > memory_threshold:
            logging.warning(f"High memory usage detected: {memory_usage_mb:.2f} MB > {memory_threshold} MB threshold")
            logging.info("Performing garbage collection to free memory")
            gc.collect()
            
        # Check if CPU usage is high
        cpu_threshold = getattr(self.config, 'cpu_threshold_percent', 90)  # Default 90%
        if cpu_percent > cpu_threshold:
            logging.warning(f"High CPU usage detected: {cpu_percent:.2f}% > {cpu_threshold}% threshold")
            
        # Limit the number of samples we keep
        max_samples = 100
        if len(self.memory_usage_samples) > max_samples:
            self.memory_usage_samples = self.memory_usage_samples[-max_samples:]
        if len(self.cpu_usage_samples) > max_samples:
            self.cpu_usage_samples = self.cpu_usage_samples[-max_samples:]
                
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
                try:
                    file_processing_start = time.time()
                    post_records = self._parse_post(link, title, pid)
                    file_processing_time = time.time() - file_processing_start
                    performance_stats['file_processing_times'].append(file_processing_time)
                    
                    # Count files processed
                    files_in_post = 0
                    for rec in post_records:
                        if "parsed_files" in rec and rec["parsed_files"]:
                            files_in_post += len(rec["parsed_files"])
                            total_files_processed += len(rec["parsed_files"])
                    
                    logging.debug(f"Processed {files_in_post} files from post {pid} in {file_processing_time:.2f}s")
                    
                    # Get download summary
                    download_summary = "[다운로드 없음] "
                    for rec in post_records:
                        if "_download_summary" in rec:
                            download_summary = rec["_download_summary"]
                            break
                    
                    # Save checkpoint and posts
                    self.checkpoint_manager.save(page, download_summary)
                    self.storage.save_posts(post_records)
                except Exception as e:
                    error_count += 1
                    performance_stats['file_processing_errors'] += 1
                    logging.error(f"Error processing post {pid} ({link}): {e}")
                    logging.debug(f"Error details: {traceback.format_exc()}")
                    continue
            
            # Go to next page
            page += 1
            pbar.update(1)
            
            # Apply rate limiting between pages
            self._rate_limit()
        
            pbar.close()
            
            # Log crawl statistics
            end_time = datetime.now()
            duration = end_time - start_time
            logging.info(f"Crawl completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info(f"Crawl duration: {duration}")
            logging.info(f"Total pages processed: {page-1}")
            logging.info(f"Total posts processed: {total_posts_processed}")
            logging.info(f"Total files processed: {total_files_processed}")
            logging.info(f"Total errors encountered: {error_count}")
            
            # Log performance statistics
            if performance_stats['request_times']:
                avg_request_time = sum(performance_stats['request_times']) / len(performance_stats['request_times'])
                logging.info(f"Average request time: {avg_request_time:.2f}s")
            
            if performance_stats['page_processing_times']:
                avg_page_time = sum(performance_stats['page_processing_times']) / len(performance_stats['page_processing_times'])
                logging.info(f"Average page processing time: {avg_page_time:.2f}s")
            
            if performance_stats['file_processing_times']:
                avg_file_time = sum(performance_stats['file_processing_times']) / len(performance_stats['file_processing_times'])
                logging.info(f"Average file processing time: {avg_file_time:.2f}s")
            
            logging.info(f"Network errors: {performance_stats['network_errors']}")
            logging.info(f"Parsing errors: {performance_stats['parsing_errors']}")
            logging.info(f"File processing errors: {performance_stats['file_processing_errors']}")
            
            # Clean up resources
            logging.info("Cleaning up resources...")
            try:
                if self.driver:
                    self.driver.quit()
                    self.driver = None
                    logging.info("WebDriver closed successfully")
            except Exception as e:
                logging.error(f"Error closing WebDriver: {e}")
                
            # Force garbage collection
            gc.collect()
            
            # Log final resource usage
            try:
                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                memory_usage_mb = memory_info.rss / 1024 / 1024
                cpu_percent = process.cpu_percent(interval=0.1)
                logging.info(f"Final memory usage: {memory_usage_mb:.2f} MB, CPU usage: {cpu_percent:.2f}%")
            except Exception as e:
                logging.error(f"Error getting resource usage: {e}")
    
    def _extract_pagination_info(self, html_content: str, current_page: int) -> Tuple[int, int, bool]:
        """
        Extract pagination information from HTML content
        
        Args:
            html_content: HTML content to extract pagination from
            current_page: Current page number
            
        Returns:
            Tuple of (current_page, last_page, has_next)
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Find pagination elements using common selectors
            pagination = soup.select('.pagination a, .pagination span, .paging a, .paging span, .paginate a, .paginate span, .page-numbers, .page-item a')
            if not pagination:
                # Try broader selectors if specific ones don't match
                pagination = soup.select('a[href*="page="], a[href*="pageIndex="], a[href*="pageNo="]')
                
            if not pagination:
                return current_page, current_page, False
            
            # Extract page numbers
            page_numbers = []
            has_next = False
            active_page = None
            
            # Korean pagination terms
            next_terms = ['다음', '다음 페이지', '다음페이지', '>', '>>', '→']
            prev_terms = ['이전', '이전 페이지', '이전페이지', '<', '<<', '←']
            
            for item in pagination:
                text = item.text.strip()
                
                # Check for active/current page
                if item.parent and item.parent.get('class') and any(c in ['active', 'current', 'selected'] for c in item.parent.get('class')):
                    try:
                        active_page = int(text)
                    except ValueError:
                        pass
                
                # Check for next page indicator
                if text in next_terms or any(term in text for term in next_terms):
                    has_next = True
                    continue
                
                # Skip previous page indicators
                if text in prev_terms or any(term in text for term in prev_terms):
                    continue
                    
                # Try to extract page number
                try:
                    # Handle cases like "1/10" format
                    if '/' in text:
                        parts = text.split('/')
                        if len(parts) == 2 and parts[1].strip().isdigit():
                            page_numbers.append(int(parts[1].strip()))
                            continue
                    
                    # Regular page number
                    page_number = int(text)
                    page_numbers.append(page_number)
                except ValueError:
                    # Skip non-numeric text that isn't a pagination control
                    pass
            
            # Use active page if found, otherwise use provided current_page
            if active_page is not None:
                current_page = active_page
            
            # Determine last page
            last_page = max(page_numbers) if page_numbers else current_page
            
            # If we're on the last page, there's no next page
            if current_page >= last_page:
                has_next = False
            
            # Sanity check: if current_page > last_page, adjust last_page
            if current_page > last_page:
                last_page = current_page
            
            # Look for text like "Page 1 of 10" if we still don't have a last page
            if last_page == current_page and not page_numbers:
                for elem in soup.select('div, p, span'):
                    page_text = elem.get_text()
                    match = re.search(r'(?:Page|페이지)\s*(\d+)\s*(?:of|중|/)\s*(\d+)', page_text)
                    if match:
                        try:
                            current_page = int(match.group(1))
                            last_page = int(match.group(2))
                            has_next = current_page < last_page
                            break
                        except (ValueError, TypeError):
                            continue
            
            return current_page, last_page, has_next
            
        except Exception as e:
            logging.error(f"Error extracting pagination info: {e}")
            return current_page, current_page, False
        
    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL by adding base URL if needed and handling relative paths
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        # Handle relative URLs
        if url.startswith('/'):
            return urljoin(self.config.base_url, url)
        # Handle URLs without scheme
        elif not url.startswith(('http://', 'https://')):
            return urljoin(self.config.base_url, url)
        # Return as is if already absolute
        return urljoin(self.config.base_url, url)
    
    def parse_list_api(self, page: int, page_content: str) -> List[Tuple[str, str]]:
        """
        Parse the list page to extract post titles and URLs
        
        Args:
            page: Page number
            page_content: HTML content of the page
            
        Returns:
            List of tuples containing (title, url) for each post
        """
        posts = []
        soup = BeautifulSoup(page_content, 'html.parser')
        
        # Try multiple strategies to find post containers
        strategies = [
            # Strategy 1: Common class-based selectors
            lambda s: s.select('.post-item, .board-list tr, article.post, .list-item, .board-item'),
            
            # Strategy 2: Table-based layouts
            lambda s: s.select('table.board-list tbody tr, table.list tbody tr, table.board tbody tr'),
            
            # Strategy 3: List-based layouts
            lambda s: s.select('ul.board-list li, ol.board-list li, div.board-list > div'),
            
            # Strategy 4: Generic table rows with title/subject cells
            lambda s: s.select('tr:has(td.title), tr:has(td.subject), tr:has(a.title), tr:has(a.subject)'),
            
            # Strategy 5: Find all links that might be post titles
            lambda s: [a.parent for a in s.select('a[href*="/community/"], a[href*="/board/"], a[href*="/post/"]')]
        ]
        
        # Try each strategy until we find post containers
        post_containers = []
        for strategy in strategies:
            post_containers = strategy(soup)
            if post_containers:
                logging.debug(f"Found {len(post_containers)} post containers using strategy {strategies.index(strategy)+1}")
                break
        
        # If we still don't have containers, try a more aggressive approach
        if not post_containers:
            # Look for any table rows that might be posts
            tables = soup.select('table')
            for table in tables:
                rows = table.select('tr')
                if len(rows) > 1:  # Skip tables with only one row
                    # Skip header row
                    post_containers.extend(rows[1:])
        
        # Process each post container
        for container in post_containers:
            # Multiple strategies to extract title and URL
            link_elem = None
            
            # Strategy 1: Look for specific title elements
            for selector in ['a.title', 'td.title a', '.subject a', 'h3.title a', '.list-title a', '.post-title a']:
                link_elem = container.select_one(selector)
                if link_elem:
                    break
            
            # Strategy 2: Look for any link with post-like URL patterns
            if not link_elem:
                for link in container.select('a[href]'):
                    href = link.get('href', '')
                    if any(pattern in href for pattern in ['/community/', '/board/', '/post/', '/view/', '/read/']):
                        link_elem = link
                        break
            
            # Strategy 3: Just take the first link if nothing else worked
            if not link_elem and container.select('a[href]'):
                link_elem = container.select('a[href]')[0]
            
            if link_elem:
                # Extract title, handling various formats
                title = link_elem.get_text(strip=True)
                
                # If title is empty, try to find it elsewhere in the container
                if not title:
                    title_elem = container.select_one('.title, .subject, .post-title, .list-title')
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                
                # Extract URL
                url = link_elem.get('href', '')
                
                # Normalize URL
                url = self._normalize_url(url)
                
                # Validate and add to results
                if title and url and self._is_valid_url(url):
                    # Check for duplicates before adding
                    if not any(existing_url == url for _, existing_url in posts):
                        posts.append((title, url))
        
        logging.info(f"Found {len(posts)} posts on page {page}")
        return posts
    
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
        """Detect downloadable files in a page using DownloadDetector.
        Args:
            html_content: Raw HTML of the page.
        Returns:
            A deduplicated list of dictionaries with keys ``href`` and ``text``.
        """
        try:
            detections = self.download_detector.detect_downloads(html_content)
            unique: Dict[str, Dict[str, str]] = {}
            for d in detections:
                href = d.get("url")
                if not href:
                    continue
                # Prefer the first non-empty text we encounter for a given href
                if href not in unique or (not unique[href]["text"] and d.get("text")):
                    unique[href] = {
                        "href": href,
                        "text": d.get("text") or "다운로드 파일",
                    }
            return list(unique.values())
        except Exception as e:
            logging.error(f"Error detecting downloadable files: {e}")
            return []
    
    def _download_and_parse_file(self, url: str, filename: str) -> Dict[str, Any]:
        """
        Download and parse a file in-memory without saving to disk using the FileProcessor
        
        Args:
            url: URL of the file to download
            filename: Filename to use for the file
            
        Returns:
            Dictionary containing parsed content and content hash
        """
        download_start_time = time.time()
        file_bytes = None
        content_hash = None
        file_size = 0
        
        try:
            # Apply rate limiting
            self._rate_limit()
            
            # Check robots.txt
            if not self._can_fetch(url):
                logging.warning(f"URL not allowed by robots.txt: {url}")
                return {"error": "URL not allowed by robots.txt", "filename": filename, "url": url}
            
            # Download file content using requests to get raw bytes for hash calculation
            try:
                logging.debug(f"Downloading file from {url}")
                response = requests.get(url, headers=self.auth_headers, timeout=self.config.request_timeout)
                response.raise_for_status()
                file_bytes = response.content
                file_size = len(file_bytes)
                
                # Calculate content hash
                hash_start_time = time.time()
                content_hash = self._calculate_content_hash(file_bytes)
                hash_time = time.time() - hash_start_time
                
                logging.debug(f"Downloaded {file_size} bytes, hash: {content_hash} (calculated in {hash_time:.2f}s)")
            except requests.exceptions.RequestException as download_error:
                logging.error(f"Network error downloading file {url}: {download_error}")
                return {
                    "error": f"Download error: {str(download_error)}", 
                    "filename": filename, 
                    "url": url,
                    "file_size": 0,
                    "download_time": time.time() - download_start_time
                }
            except Exception as download_error:
                logging.error(f"Error downloading file {url}: {download_error}")
                logging.debug(f"Error details: {traceback.format_exc()}")
                return {
                    "error": f"Download error: {str(download_error)}", 
                    "filename": filename, 
                    "url": url,
                    "file_size": 0,
                    "download_time": time.time() - download_start_time
                }
            
            # Parse file using FileProcessor
            parse_start_time = time.time()
            try:
                file_contents = self.file_processor.parse_file(url, "", filename)
                parse_time = time.time() - parse_start_time
                logging.debug(f"Parsed file in {parse_time:.2f}s")
            except Exception as parse_error:
                logging.error(f"Error parsing file {filename} from {url}: {parse_error}")
                logging.debug(f"Error details: {traceback.format_exc()}")
                return {
                    "error": f"Parse error: {str(parse_error)}", 
                    "filename": filename, 
                    "url": url,
                    "hash": content_hash,
                    "file_size": file_size,
                    "download_time": parse_start_time - download_start_time,
                    "parse_time": 0
                }
            
            # Handle empty parsing results
            if not file_contents or len(file_contents) == 0:
                return {
                    "error": "Failed to parse file", 
                    "filename": filename, 
                    "url": url,
                    "hash": content_hash,  # Still include hash even if parsing failed
                    "file_size": file_size,
                    "download_time": parse_start_time - download_start_time,
                    "parse_time": time.time() - parse_start_time
                }
            
            # Get first file content object
            file_content = file_contents[0]
            
            # Calculate total processing time
            total_processing_time = time.time() - download_start_time
            
            # Extract file properties safely
            parsed_content = {
                "filename": filename,
                "url": url,
                "content": getattr(file_content, 'content', ""),
                "metadata": getattr(file_content, 'metadata', {}),
                "file_type": getattr(file_content, 'file_type', os.path.splitext(filename)[1][1:] or 'pdf'),
                "hash": content_hash,
                "file_size": file_size,
                "download_time": parse_start_time - download_start_time,
                "parse_time": time.time() - parse_start_time,
                "total_processing_time": total_processing_time
            }
            
            # Log processing metrics
            logging.debug(f"File processing metrics for {filename}: size={file_size} bytes, " 
                         f"download={parsed_content['download_time']:.2f}s, " 
                         f"parse={parsed_content['parse_time']:.2f}s, " 
                         f"total={total_processing_time:.2f}s")
            
            # Process images if available
            images = getattr(file_content, 'images', [])
            if images:
                parsed_content["images"] = []
                ocr_texts = []
                
                for img in images:
                    img_data = {}
                    if hasattr(img, 'data'):
                        img_data["data"] = img.data
                    
                    ocr_text = getattr(img, 'ocr_text', "").strip()
                    if ocr_text:
                        img_data["ocr_text"] = ocr_text
                        ocr_texts.append(f"Image OCR: {ocr_text}")
                    
                    parsed_content["images"].append(img_data)
                
                # Add OCR text to content if available
                if ocr_texts:
                    ocr_section = "\n\n===== OCR Text from Images =====\n" + "\n\n".join(ocr_texts)
                    parsed_content["content"] = (parsed_content["content"] + ocr_section) if parsed_content["content"] else ocr_section
            
            # Process tables if available
            tables = getattr(file_content, 'tables', [])
            if tables:
                parsed_content["tables"] = []
                table_texts = []
                
                for idx, table in enumerate(tables):
                    table_data = {}
                    if hasattr(table, 'data') and table.data:
                        table_data["data"] = table.data
                        table_texts.append(f"Table {idx+1}:\n" + self._format_table_data(table.data))
                    
                    parsed_content["tables"].append(table_data)
                
                # Add table text to content if available
                if table_texts:
                    table_section = "\n\n===== Tables =====\n" + "\n\n".join(table_texts)
                    parsed_content["content"] = (parsed_content["content"] + table_section) if parsed_content["content"] else table_section
            
            return parsed_content
            
        except Exception as e:
            logging.error(f"Error downloading and parsing file {url}: {e}")
            return {"error": str(e), "filename": filename, "url": url}
    
    def _calculate_content_hash(self, file_bytes: Optional[bytes]) -> str:
        """
        Calculate a hash for file content
        
        Args:
            file_bytes: File content as bytes
            
        Returns:
            SHA-256 hash of file content as hexadecimal string
        """
        if not file_bytes:
            logging.warning("Cannot calculate hash for empty content")
            return ""
            
        try:
            # Use SHA-256 for a good balance of speed and collision resistance
            hash_obj = hashlib.sha256()
            
            # Process in chunks to handle large files more efficiently
            chunk_size = 8192  # 8KB chunks
            if isinstance(file_bytes, bytes):
                # Process bytes directly if already in memory
                for i in range(0, len(file_bytes), chunk_size):
                    hash_obj.update(file_bytes[i:i+chunk_size])
            else:
                # Fallback for other types (shouldn't happen, but just in case)
                logging.warning(f"Unexpected type for file_bytes: {type(file_bytes)}")
                hash_obj.update(str(file_bytes).encode('utf-8', errors='ignore'))
                
            # Return the hexadecimal digest
            return hash_obj.hexdigest()
        except Exception as e:
            logging.error(f"Error calculating content hash: {e}")
            logging.debug(f"Error details: {traceback.format_exc()}")
            return f"hash_error_{int(time.time())}"  # Return a timestamp-based placeholder
    
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
                        # DownloadDetector의 extract_file_extension 메서드를 활용하여 파일 확장자 추출
                        # 링크 텍스트와 URL 모두 고려
                        combined_text = clean_text + ' ' + download_url
                        
                        # 파일 확장자가 이미 있는지 확인
                        if any(clean_text.lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                            filename = clean_text
                        else:
                            # DownloadDetector의 extract_file_extension 메서드를 활용하여 파일 형식 추측
                            ext = self.download_detector.extract_file_extension(combined_text)
                            
                            if ext:
                                filename = f"{clean_text}.{ext}"
                            else:
                                # 기본값으로 PDF 사용
                                filename = f"{clean_text}.pdf"
                
                # 2. Try to extract from URL
                if not filename and download_url:
                    url_parts = download_url.split('/')
                    if url_parts and url_parts[-1]:
                        # 파일 확장자 확인
                        if any(url_parts[-1].lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                            filename = url_parts[-1]
                
                # 3. 페이지 내용에서 파일 형식 추론
                if not filename and link.get('text'):
                    page_content = getattr(download_info, 'page_content', '').lower()
                    
                    # 링크 텍스트와 페이지 콘텐츠 모두 고려
                    combined_text = link['text'] + ' ' + page_content
                    
                    # DownloadDetector의 extract_file_extension 메서드를 활용하여 파일 형식 추측
                    ext = self.download_detector.extract_file_extension(combined_text)
                    
                    if ext:
                        filename = f"{link['text']}.{ext}"
                    else:
                        # 기본값으로 PDF 사용
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
