#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main crawler class for real estate crawler
"""

import re
import time
import logging
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm

from src.config import Config
from src.crawler.auth import Authenticator
from src.crawler.download_detector import DownloadDetector
from src.storage.file_processor import FileProcessor
from src.storage.storage import CheckpointManager


class CrawlerError(Exception):
    """Base exception for crawler errors"""
    pass


class CrawlerSelectors:
    """CSS/XPath selectors for crawler"""
    POST_LINK = "a[href^='/community/']"
    TITLE_MAIN = '.post-title, .view-title, h1.title, .board-title'
    CONTENT_AREAS = [
        ".post-content", ".view-content", ".content", "article", ".fr-view", ".fr-element",
        "#post-content", "#view-content", "#content", ".viewer_content", ".board-content"
    ]
    AUTHOR = '.author, .writer, .user-info'
    DATE = '.date, .created-at, .post-date, .write-date, li[title]'
    IMAGES = [".post-content img", ".view-content img", ".content img", "article img", ".fr-view img"]


class NoOpStorage:
    """Dummy storage for download-only mode"""
    def save_posts(self, posts):
        pass


class Crawler:
    """
    Main crawler class that handles listing and parsing posts from the real estate community
    """
    
    def __init__(self, config: Optional[Config] = None):
        """Initialize the crawler with configuration"""
        self.config = config or Config.get_instance()
        self.authenticator = Authenticator(self.config)
        self.session = requests.Session()
        self.driver: Optional[webdriver.Chrome] = None
        self.auth_headers: Optional[Dict[str, str]] = None
        self.visited_urls: Set[str] = set()
        self.download_detector = DownloadDetector()
        self.checkpoint_manager = CheckpointManager(config=self.config)
        
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
        
        if self.config.browser_options.get("headless"):
            options.add_argument("--headless")
        
        service = Service(ChromeDriverManager().install())
        return webdriver.Chrome(service=service, options=options)
    
    def ensure_authenticated(self) -> Tuple[Dict[str, str], Optional[webdriver.Chrome]]:
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
        self._ensure_driver()
        
        try:
            url = f"{self.config.specific_list_url}&page={page}"
            self.logger.info(f"Rendering page {page}: {url}")
            
            self.driver.get(url)
            time.sleep(3)  # Wait for initial page load
            
            self._check_and_handle_reauth(url)
            
            return self._extract_post_links(page)
            
        except Exception as e:
            self._handle_error(e, page)
            raise

    def _ensure_driver(self) -> None:
        """Initialize webdriver if needed"""
        if not hasattr(self, 'driver') or not self.driver:
            self.driver = self._create_driver()

    def _check_and_handle_reauth(self, current_url: str) -> None:
        """Check if re-authentication is needed and handle it"""
        page_content = self.driver.execute_script("return document.body.innerText")
        if "로그인이 필요합니다" in page_content or ("로그인" in page_content and "로그아웃" not in page_content):
            self.logger.warning("Session expired. Re-authenticating...")
            self.auth_headers, self.driver = self.authenticator.ensure_authenticated()
            self.driver.get(current_url)
            time.sleep(3)

    def _extract_post_links(self, page: int) -> List[Tuple[str, str]]:
        """Extract post links from the current page"""
        links = self.driver.find_elements(By.CSS_SELECTOR, CrawlerSelectors.POST_LINK)
        posts = []
        seen = set()
        
        for link in links:
            try:
                href = link.get_attribute('href')
                title = link.text.strip()
                
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

    def _handle_error(self, error: Exception, page: int) -> None:
        """Handle errors during crawling, including taking screenshots"""
        self.logger.error(f"Error in list_posts for page {page}: {error}")
        try:
            timestamp = int(time.time())
            screenshot_dir = Path("screenshots")
            screenshot_dir.mkdir(exist_ok=True)
            
            screenshot_path = screenshot_dir / f"error_page_{page}_{timestamp}.png"
            self.driver.save_screenshot(str(screenshot_path))
            self.logger.info(f"Saved screenshot to {screenshot_path}")
            
            page_source_path = screenshot_dir / f"error_page_{page}_{timestamp}.html"
            with open(page_source_path, 'w', encoding='utf-8') as f:
                f.write(self.driver.page_source)
        except Exception as e:
            self.logger.error(f"Failed to save debug information: {e}")

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
            post_id = url.split('/')[-1]
            self._ensure_driver()
            
            # Normalize URL
            if not url.startswith('http'):
                url = f"{self.config.base_url}/community/{post_id}"
                
            self.logger.info(f"Navigating to post: {url}")
            self._navigate_to_post(url, post_id)
            
            # Initialize post data
            post_data = {
                'id': post_id,
                'url': url,
                'title': '',
                'content': '',
                'author': '',
                'created_at': '',
                'attachments': [],
                'date_limit_reached': False
            }
            
            # Extract data
            post_data['title'] = self._extract_title()
            post_data['content'] = self._extract_content()
            
            author, created_at = self._extract_metadata()
            post_data['author'] = author
            post_data['created_at'] = created_at
            
            # Detect downloads
            post_data['attachments'] = self._detect_downloads(url, post_id, post_data['content'])
            
            # Extract images (append to content)
            image_text = self._extract_images()
            if image_text:
                post_data['content'] += f"\n\n{image_text}"
            
            # Check date limit
            if created_at and re.match(r"^\d{2}\.\d{2}\.\d{2}$", created_at.strip()):
                post_data['date_limit_reached'] = True
                
            return post_data
            
        except Exception as e:
            self.logger.error(f"Error processing post {url}: {e}")
            return {
                'id': url.split('/')[-1],
                'url': url,
                'error': str(e)
            }

    def _navigate_to_post(self, url: str, post_id: str) -> None:
        """Navigate to the post URL and handle redirects/reauth"""
        self.driver.get(url)
        time.sleep(3)
        
        current_url = self.driver.current_url
        if f"/community/{post_id}" not in current_url:
            self.logger.warning(f"Unexpected redirect: {current_url}. Attempting direct navigation.")
            direct_url = f"{self.config.base_url}/community/{post_id}"
            self.driver.get(direct_url)
            time.sleep(3)
            self._check_and_handle_reauth(direct_url)

    def _extract_title(self) -> str:
        """Extract post title"""
        try:
            # Try specific elements first
            title_elements = self.driver.find_elements(By.CSS_SELECTOR, CrawlerSelectors.TITLE_MAIN)
            if title_elements:
                return title_elements[0].text.strip()
            # Fallback to page title
            return self.driver.title.replace(' : 월급쟁이부자들', '').strip()
        except Exception as e:
            self.logger.warning(f"Error extracting title: {e}")
            return ""

    def _extract_content(self) -> str:
        """Extract post content using multiple strategies"""
        # Strategy 1: Known selectors
        for selector in CrawlerSelectors.CONTENT_AREAS:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text and len(text) > 50:
                        self.logger.info(f"Found content using selector: {selector} ({len(text)} chars)")
                        return text
            except Exception:
                continue
        
        # Strategy 2: Body text heuristic
        try:
            return self._extract_content_from_body()
        except Exception as e:
            self.logger.warning(f"Error extracting body text: {e}")
            
        # Strategy 3: BeautifulSoup fallback
        try:
            return self._extract_content_bs4()
        except Exception as e:
            self.logger.warning(f"BeautifulSoup parsing error: {e}")
            
        return ""

    def _extract_content_from_body(self) -> str:
        """Extract content from body text by filtering UI elements"""
        body_text = self.driver.find_element(By.TAG_NAME, "body").text
        lines = body_text.split('\n')
        content_lines = []
        in_content = False
        
        for line in lines:
            if len(line.strip()) < 5:
                continue
            if any(x in line.lower() for x in ['로그인', '회원가입', '메뉴', '검색', '홈', '마이페이지']):
                continue
            if len(line.strip()) > 30:
                in_content = True
            if in_content:
                content_lines.append(line)
        
        if content_lines:
            return '\n'.join(content_lines)
        return ""

    def _extract_content_bs4(self) -> str:
        """Extract content using BeautifulSoup as fallback"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        for div in soup.find_all("div", class_=True):
            class_name = div.get("class", [])
            if class_name:
                class_str = " ".join(class_name)
                text = div.get_text(strip=True)
                if text and len(text) > 100 and any(x in class_str.lower() for x in ["content", "post", "view"]):
                    return text
        return ""

    def _extract_metadata(self) -> Tuple[str, str]:
        """Extract author and creation date"""
        author = ""
        created_at = ""
        
        try:
            author_elements = self.driver.find_elements(By.CSS_SELECTOR, CrawlerSelectors.AUTHOR)
            if author_elements:
                author = author_elements[0].text.strip()
            
            date_elements = self.driver.find_elements(By.CSS_SELECTOR, CrawlerSelectors.DATE)
            if date_elements:
                for elem in date_elements:
                    title_attr = elem.get_attribute('title')
                    if title_attr and re.match(r'^\d{4}-\d{2}-\d{2}', title_attr):
                        created_at = title_attr.strip()
                        break
                if not created_at:
                    created_at = date_elements[0].text.strip()
        except Exception as e:
            self.logger.debug(f"Error extracting metadata: {e}")
            
        return author, created_at

    def _detect_downloads(self, url: str, post_id: str, content: str) -> List[Dict[str, str]]:
        """Detect attachments and download links"""
        attachments = []
        try:
            # Browser check
            download_info = self.download_detector.check_for_downloads_browser(self.driver, url, post_id)
            
            # Content text check
            if content and not download_info.has_download:
                content_download_info = self.download_detector.check_content_for_file_references(content, post_id)
                if content_download_info.has_download:
                    download_info.has_download = True
                    # Merge links
                    for link in content_download_info.download_links:
                        if not any(existing.get("url") == link.get("url") for existing in download_info.download_links):
                            download_info.download_links.append(link)
            
            if download_info.has_download:
                for link in download_info.download_links:
                    attachment_url = link.get('url')
                    filename = link.get('text') or attachment_url.split('/')[-1]
                    
                    if attachment_url and not any(a['url'] == attachment_url for a in attachments):
                        full_url = attachment_url if attachment_url.startswith('http') else f"{self.config.base_url}{attachment_url}"
                        attachments.append({
                            'url': full_url,
                            'filename': filename
                        })
        except Exception as e:
            self.logger.debug(f"Error detecting downloads: {e}")
            
        return attachments

    def _extract_images(self) -> str:
        """Extract images and return formatted string"""
        image_text = ""
        try:
            for selector in CrawlerSelectors.IMAGES:
                images = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for img in images:
                    src = img.get_attribute("src")
                    if src and not src.startswith("data:") and not src.endswith(".svg"):
                        img_url = src if src.startswith("http") else f"{self.config.base_url}{src}"
                        if img_url:
                            image_text += f"[이미지: {img_url}]\n"
        except Exception as e:
            self.logger.debug(f"Error extracting images: {e}")
        return image_text.strip()

    def _save_results(self, results: List[Dict[str, Any]]) -> None:
        """Save results to JSONL file with consistent format"""
        output_file = Path(self.config.out_jsonl)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'a', encoding='utf-8') as f:
            for result in results:
                f.write(json.dumps(self._format_result_for_save(result), ensure_ascii=False) + '\n')

    def _format_result_for_save(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Format a single result for saving"""
        data = result.get('data', {})
        post_id = data.get('id') or result.get('url', '').split('/')[-1]
        
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
        
        if 'error' in result:
            post['error'] = str(result['error'])
            post['type'] = 'error'
        
        if data.get('attachments'):
            post['has_download'] = True
            for attachment in data['attachments']:
                url = attachment.get('url', '')
                if url:
                    post['download_links'].append({
                        'url': url,
                        'filename': attachment.get('filename', url.split('/')[-1])
                    })
                    if '.' in url:
                        fmt = url.split('.')[-1].lower()
                        if fmt in ['pdf', 'pptx', 'docx', 'xlsx'] and fmt not in post['file_formats']:
                            post['file_formats'].append(fmt)
            
            if post['file_formats']:
                post['_download_summary'] = f"[다운로드 가능: {', '.join(post['file_formats'])}] "
                
        return post

    def crawl(self, start_page: int = None, max_pages: int = None) -> Dict[str, Any]:
        """Main crawling method that handles pagination and post processing"""
        stats = {
            'pages_processed': 0,
            'posts_processed': 0,
            'posts_with_downloads': 0,
            'files_processed': 0,
            'errors': 0
        }
        
        # Use authenticator session (with cookies) for HTTP downloads
        file_processor = FileProcessor(scraper=self.authenticator.session, download_dir=self.config.download_dir)
        
        try:
            self.ensure_authenticated()
            start_page = start_page or 1
            self.logger.info(f"Starting from page {start_page}")
            
            page = start_page
            pbar = tqdm(desc="Page", initial=page-1)
            
            while True:
                if max_pages and stats['pages_processed'] >= max_pages:
                    self.logger.info(f"Reached maximum pages limit ({max_pages})")
                    break
                    
                try:
                    self.logger.info(f"Processing page {page}...")
                    posts = self.list_posts(page)
                    if not posts:
                        self.logger.info(f"No more posts found on page {page}")
                        break
                    
                    self._process_page_posts(posts, page, stats, file_processor)
                    
                    # Save checkpoint
                    self.checkpoint_manager.save(page, f"Processed page {page}")
                    
                    page += 1
                    stats['pages_processed'] += 1
                    pbar.update(1)
                    time.sleep(1)
                    
                except StopIteration as stop_exc:
                    self.logger.info(str(stop_exc))
                    break
                except Exception as e:
                    self.logger.error(f"Error processing page {page}: {e}")
                    stats['errors'] += 1
                    time.sleep(5)
            
            pbar.close()
            self.logger.info(f"Crawling completed. Statistics: {stats}")
            return stats
            
        except Exception as e:
            self.logger.error(f"Fatal error during crawling: {e}", exc_info=True)
            stats['errors'] += 1
            raise
        finally:
            self.close()

    def _process_page_posts(self, posts: List[Tuple[str, str]], page: int, stats: Dict[str, Any], file_processor: FileProcessor) -> None:
        """Process all posts on a single page"""
        for title, url in tqdm(posts, desc=f"Posts p{page}", leave=False):
            try:
                self.logger.info(f"Processing post: {title}")
                post_data = self._process_post(url)
                
                if post_data.get('date_limit_reached'):
                    raise StopIteration("Date limit reached")
                
                result = self._create_result_record(post_data, title, url)
                self._handle_downloads(result, post_data, file_processor, stats)
                
                self._save_results([{'data': result, 'url': url}])
                stats['posts_processed'] += 1
                
            except StopIteration:
                raise
            except Exception as e:
                self.logger.error(f"Error processing post {url}: {e}")
                stats['errors'] += 1

    def _create_result_record(self, post_data: Dict[str, Any], title: str, url: str) -> Dict[str, Any]:
        """Create a standardized result record"""
        return {
            'post_id': post_data.get('id'),
            'title': title,
            'src': url,
            'author': post_data.get('author', ''),
            'date': post_data.get('created_at', ''),
            'content': post_data.get('content', ''),
            'crawl_timestamp': datetime.now().isoformat(),
            'has_download': False,
            'file_formats': []
        }

    def _handle_downloads(self, result: Dict[str, Any], post_data: Dict[str, Any], file_processor: FileProcessor, stats: Dict[str, Any]) -> None:
        """Handle file downloads for a post"""
        if not post_data.get('attachments'):
            return

        self._sync_cookies_to_session(file_processor)
        
        file_formats = set()
        file_sources = []
        
        for attachment in post_data['attachments']:
            attachment_url = attachment.get('url', '')
            if not attachment_url:
                continue
                
            if '.' in attachment_url:
                fmt = attachment_url.split('.')[-1].lower()
                if fmt in ['pdf', 'pptx', 'docx', 'xlsx']:
                    file_formats.add(fmt)
            
            try:
                downloaded_file = file_processor.download_file(
                    url=attachment_url,
                    post_id=post_data.get('id'),
                    filename=attachment.get('filename', '')
                )
                if downloaded_file:
                    file_sources.append(attachment_url)
            except Exception as e:
                self.logger.error(f"Error downloading file {attachment_url}: {e}")
                stats['errors'] += 1
        
        if file_formats:
            result['has_download'] = True
            result['file_formats'] = list(file_formats)
            stats['posts_with_downloads'] += 1
            stats['files_processed'] += len(file_sources)

    def _sync_cookies_to_session(self, file_processor: FileProcessor) -> None:
        """Sync Selenium cookies to the file processor's session"""
        try:
            if self.driver and hasattr(file_processor, 'scraper') and file_processor.scraper:
                for cookie in self.driver.get_cookies():
                    file_processor.scraper.cookies.set(cookie['name'], cookie['value'])
        except Exception:
            pass
