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
        # Skip if no downloads
        if not download_info.has_download or not download_info.download_links:
            return post
        
        # Process each file
        for link in download_info.download_links:
            try:
                download_url = link["url"]
                
                # Skip certificate PDFs
                if "certificate" in download_url:
                    continue
                    
                # Get filename
                filename = os.path.basename(download_url.split("?")[0])
                if not filename:
                    filename = f"{link['text']}.pptx"
                
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
