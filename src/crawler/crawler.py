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
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from src.config import Config
from src.crawler.auth import Authenticator

class CrawlerError(Exception):
    """Base exception for crawler errors"""
    pass

class Crawler:
    """
    Main crawler class that handles listing and parsing posts from the real estate community
    """
    
    def __init__(self, config: Config = None):
        """Initialize the crawler with configuration"""
        self.config = config or Config.get_instance()
        self.authenticator = Authenticator(self.config)
        self.session = requests.Session()
        self.driver = None
        self.auth_headers = None
        self.visited_urls = set()
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _create_driver(self) -> webdriver.Chrome:
        """Create and configure a Chrome WebDriver instance"""
        options = Options()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f'user-agent={self.config.user_agent}')
        
        if self.config.headless:
            options.add_argument("--headless")
        
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    
    def ensure_authenticated(self):
        """Ensure we have valid authentication"""
        if not self.auth_headers:
            self.auth_headers, self.driver = self.authenticator.ensure_authenticated()
        return self.auth_headers, self.driver
    
    def list_posts(self, page: int) -> List[Tuple[str, str]]:
        """
        List posts from the community using browser rendering
        
        Args:
            page: Page number to fetch
            
        Returns:
            List of (title, url) tuples for each post
        """
        return self.list_posts_render(page)
    
    def list_posts_api(self, page: int) -> List[Tuple[str, str]]:
        """
        Fetch posts using the community API
        """
        params = {
            'tab': self.config.tab,
            'subTab': self.config.subtab,
            'page': page,
            'size': 30
        }
        
        # Ensure we have authentication
        auth_headers, _ = self.ensure_authenticated()
        
        try:
            response = self.session.get(
                f"{self.config.base_url}/api/v1/community/posts",
                params=params,
                headers={
                    'User-Agent': self.config.user_agent,
                    **(auth_headers or {})
                },
                timeout=30
            )
            
            if response.status_code == 200 and "application/json" in response.headers.get("content-type", ""):
                items = response.json().get("content", [])
                return [
                    (item["title"], urljoin(self.config.base_url, f"/community/{item['id']}"))
                    for item in items
                ]
            else:
                raise CrawlerError(f"API request failed with status {response.status_code}")
                
        except Exception as e:
            self.logger.error(f"API request failed: {e}")
            raise
    
    def list_posts_render(self, page: int) -> List[Tuple[str, str]]:
        """
        Fetch posts by rendering the page with Selenium WebDriver
        
        Args:
            page: Page number to fetch
            
        Returns:
            List of (title, url) tuples for each post
        """
        if not hasattr(self, 'driver') or not self.driver:
            self.driver = self._create_driver()
        
        try:
            # Use the specific_list_url from config and add page parameter
            url = f"{self.config.specific_list_url}&page={page}"
            
            self.logger.info(f"Rendering page {page}: {url}")
            self.driver.get(url)
            time.sleep(3)  # Wait for initial page load
            
            # Check if login is required
            page_content = self.driver.execute_script("return document.body.innerText")
            if "로그인이 필요합니다" in page_content or ("로그인" in page_content and "로그아웃" not in page_content):
                self.logger.warning("Session expired. Re-authenticating...")
                self.auth_headers, self.driver = self.authenticator.ensure_authenticated()
                self.driver.get(url)  # Reload the page after login
                time.sleep(3)  # Wait for page to load after login
            
            # Extract post links
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='/community/']")
            posts = []
            seen = set()
            
            for link in links:
                try:
                    href = link.get_attribute('href')
                    title = link.text.strip()
                    
                    # Match only post detail URLs (e.g., /community/12345)
                    if (href and 
                        re.match(rf"^{self.config.base_url}/community/\d+$", href) and 
                        href not in seen and 
                        title):
                        posts.append((title, href))
                        seen.add(href)
                except Exception as e:
                    self.logger.warning(f"Error processing link: {e}")
            
            self.logger.info(f"Found {len(posts)} posts on page {page}")
            return posts
            
        except Exception as e:
            self.logger.error(f"Error in list_posts_render for page {page}: {e}")
            # Take screenshot for debugging
            try:
                timestamp = int(time.time())
                screenshot_dir = Path("screenshots")
                screenshot_dir.mkdir(exist_ok=True)
                screenshot_path = screenshot_dir / f"error_page_{page}_{timestamp}.png"
                self.driver.save_screenshot(str(screenshot_path))
                self.logger.info(f"Saved screenshot to {screenshot_path}")
                
                # Save page source for debugging
                page_source_path = screenshot_dir / f"error_page_{page}_{timestamp}.html"
                with open(page_source_path, 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)
                self.logger.info(f"Saved page source to {page_source_path}")
            except Exception as screenshot_error:
                self.logger.error(f"Failed to save debug information: {screenshot_error}")
            
            raise
    
    def close(self):
        """Clean up resources"""
        try:
            if self.driver:
                self.driver.quit()
        except Exception as e:
            self.logger.error(f"Error closing WebDriver: {e}")
        
        try:
            self.session.close()
        except Exception as e:
            self.logger.error(f"Error closing session: {e}")

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def _process_post(self, url: str) -> Dict[str, Any]:
        """Process a single post by its URL"""
        try:
            # Extract post ID from URL
            post_id = url.split('/')[-1]
            
            # Skip API attempt and go directly to browser rendering
            # Ensure we have a driver
            if not hasattr(self, 'driver') or not self.driver:
                self.driver = self._create_driver()
            
            # Make sure we're using the full URL
            if not url.startswith('http'):
                url = f"{self.config.base_url}/community/{post_id}"
                
            self.logger.info(f"Navigating to post: {url}")
            self.driver.get(url)
            time.sleep(3)  # Wait for page to load
            
            # Verify we're on the correct page
            current_url = self.driver.current_url
            if f"/community/{post_id}" not in current_url:
                self.logger.warning(f"Unexpected redirect: {current_url}. Attempting to navigate directly to post.")
                # Try again with explicit community post URL
                direct_url = f"{self.config.base_url}/community/{post_id}"
                self.driver.get(direct_url)
                time.sleep(3)  # Wait for page to load
                
                # Check if we need to re-authenticate
                page_content = self.driver.execute_script("return document.body.innerText")
                if "로그인이 필요합니다" in page_content or ("로그인" in page_content and "로그아웃" not in page_content):
                    self.logger.warning("Session expired. Re-authenticating...")
                    self.auth_headers, self.driver = self.authenticator.ensure_authenticated()
                    # Navigate back to the post after re-authentication
                    self.driver.get(direct_url)
                    time.sleep(3)  # Wait for page to load
            
            # Extract post content
            post_data = {
                'id': post_id,
                'url': url,
                'title': self.driver.title,
                'content': self.driver.find_element(By.CSS_SELECTOR, '.post-content').text if self.driver.find_elements(By.CSS_SELECTOR, '.post-content') else '',
                'author': self.driver.find_element(By.CSS_SELECTOR, '.author').text if self.driver.find_elements(By.CSS_SELECTOR, '.author') else '',
                'created_at': self.driver.find_element(By.CSS_SELECTOR, '.created-at').get_attribute('datetime') if self.driver.find_elements(By.CSS_SELECTOR, '.created-at') else '',
                'attachments': []
            }
            
            # Extract attachments
            for attachment in self.driver.find_elements(By.CSS_SELECTOR, '.attachment a'):
                attachment_url = attachment.get_attribute('href')
                if attachment_url:
                    post_data['attachments'].append({
                        'url': urljoin(self.config.base_url, attachment_url) if not attachment_url.startswith('http') else attachment_url,
                        'filename': attachment.text.strip() or attachment_url.split('/')[-1]
                    })
            
            return post_data
            
        except Exception as e:
            self.logger.error(f"Error processing post {url}: {e}")
            return {
                'id': url.split('/')[-1],
                'url': url,
                'error': str(e)
            }
    
    def _save_results(self, results: List[Dict[str, Any]]) -> None:
        """Save results to JSONL file with consistent format"""
        output_file = Path(self.config.out_jsonl)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'a', encoding='utf-8') as f:
            for result in results:
                # Extract post data from the result
                data = result.get('data', {})
                post_id = data.get('id') or result.get('url', '').split('/')[-1]
                
                # Create a properly formatted post entry
                post = {
                    'post_id': post_id,
                    '_download_summary': '[다운로드 없음] ',
                    'src': result.get('url', ''),
                    'title': data.get('title', '').replace('\n', ' ').strip(),
                    'type': 'text_content',
                    'has_download': False,
                    'file_formats': [],
                    'download_links': [],
                    'content': data.get('content', '')
                }
                
                # Add error information if present
                if 'error' in result:
                    post['error'] = str(result['error'])
                    post['type'] = 'error'
                
                # Handle attachments if any
                if data.get('attachments'):
                    post['has_download'] = True
                    for attachment in data['attachments']:
                        url = attachment.get('url', '')
                        if url:
                            post['download_links'].append({
                                'url': url,
                                'filename': attachment.get('filename', url.split('/')[-1])
                            })
                            # Extract file format from URL
                            if '.' in url:
                                fmt = url.split('.')[-1].lower()
                                if fmt in ['pdf', 'pptx', 'docx', 'xlsx'] and fmt not in post['file_formats']:
                                    post['file_formats'].append(fmt)
                    
                    if post['file_formats']:
                        post['_download_summary'] = f"[다운로드 가능: {', '.join(post['file_formats'])}] "
                
                f.write(json.dumps(post, ensure_ascii=False) + '\n')
    
    def _save_checkpoint(self, page: int, download_summary: str = "[다운로드 없음] ") -> None:
        """Save current page and download summary to checkpoint"""
        checkpoint_dir = Path(self.config.checkpoint_file).parent
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        with open(self.config.checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump({
                'last_page': page,
                'last_post_download_summary': download_summary,
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            }, f)
    
    def _load_checkpoint(self) -> int:
        """Load last processed page from checkpoint"""
        try:
            if Path(self.config.checkpoint_file).exists():
                with open(self.config.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('last_page', 1)
        except Exception as e:
            self.logger.error(f"Error loading checkpoint: {e}")
        return 1
    
    def crawl(self) -> None:
        """Main crawling method that handles pagination and post processing"""
        try:
            # Ensure authenticated
            self.auth_headers, self.driver = self.ensure_authenticated()
            
            # Load checkpoint
            start_page = self._load_checkpoint()
            self.logger.info(f"Starting from page {start_page}")
            
            page = start_page
            from tqdm import tqdm
            pbar = tqdm(desc="Page", initial=page-1)
            
            while True:
                try:
                    self.logger.info(f"Processing page {page}...")
                    
                    # Get posts from current page
                    posts = self.list_posts(page)
                    if not posts:
                        self.logger.info(f"No more posts found on page {page}")
                        break
                    
                    for title, url in tqdm(posts, desc=f"Posts p{page}", leave=False):
                        try:
                            self.logger.info(f"Processing post: {title}")
                            post_id = url.split('/')[-1]
                            post_data = self._process_post(url)
                            
                            # Create a record with the post data
                            result = {
                                'title': title,
                                'url': url,
                                'data': post_data,
                                'crawled_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                            }
                            
                            # Get download summary if available
                            download_summary = "[다운로드 없음] "
                            if post_data.get('attachments'):
                                file_formats = set()
                                for attachment in post_data['attachments']:
                                    url = attachment.get('url', '')
                                    if '.' in url:
                                        fmt = url.split('.')[-1].lower()
                                        if fmt in ['pdf', 'pptx', 'docx', 'xlsx']:
                                            file_formats.add(fmt)
                                
                                if file_formats:
                                    download_summary = f"[다운로드 가능: {', '.join(file_formats)}] "
                            
                            # Save checkpoint with download summary
                            self._save_checkpoint(page, download_summary)
                            
                            # Save the result
                            self._save_results([result])
                            
                        except Exception as e:
                            self.logger.error(f"Error processing post {url}: {e}")
                    
                    # Move to next page
                    page += 1
                    pbar.update(1)
                    time.sleep(1)  # polite delay
                    
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {e}")
                    # Wait before retry
                    time.sleep(5)
            
            pbar.close()
            self.logger.info("Crawling completed successfully")
            
        except Exception as e:
            self.logger.error(f"Fatal error during crawling: {e}", exc_info=True)
            raise
        finally:
            # Close browser when done
            self.close()