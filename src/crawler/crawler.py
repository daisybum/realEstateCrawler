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
from src.storage.storage import CheckpointManager


class CrawlerError(Exception):
    """Base exception for crawler errors"""
    pass


class CrawlerSelectors:
    """CSS/XPath selectors for crawler"""
    POST_LINK = "a[href^='/community/']"
    TITLE_MAIN = [
        r"body > div.min-w-\[1200px\].max-w-\[2560px\].mx-auto.isolate > div.bg-\[\#f2f2f2\].pt-4.pb-20 > div.flex.mx-auto.max-w-\[1200px\].px-2\.5 > div > section:nth-child(1) > div.flex.justify-between.px-8.pt-8 > div > h1",
        '.post-title', '.view-title', 'h1.title', '.board-title'
    ]
    CONTENT_AREAS = [
        r"body > div.min-w-\[1200px\].max-w-\[2560px\].mx-auto.isolate > div.bg-\[\#f2f2f2\].pt-4.pb-20 > div.flex.mx-auto.max-w-\[1200px\].px-2\.5 > div > section:nth-child(1) > div.relative.overflow-hidden > section > div > div",
        r"body > div.min-w-\[1200px\].max-w-\[2560px\].mx-auto.isolate > div.bg-\[\#f2f2f2\].pt-4.pb-20 > div.flex.mx-auto.max-w-\[1200px\].px-2\.5 > div > section:nth-child(1) > div.relative.overflow-hidden > section > div > div > section",
        ".post-content", ".view-content", ".content", "article", ".fr-view", ".fr-element",
        "#post-content", "#view-content", "#content", ".viewer_content", ".board-content"
    ]
    AUTHOR = '.author, .writer, .user-info'
    AUTHOR_POST_COUNT = [
        r"body > div.min-w-\[1200px\].max-w-\[2560px\].mx-auto.isolate > div.bg-\[\#f2f2f2\].pt-4.pb-20 > div.flex.mx-auto.max-w-\[1200px\].px-2\.5 > aside > div.sticky.top-\[90px\].w-\[383px\] > div > div > div > div:nth-child(2) > div > a:nth-child(2) > span.text-center.font-semibold.text-nowrap",
        "/html/body/div[3]/div[3]/div[2]/aside/div[1]/div/div/div/div[2]/div/a[2]/span[2]"
    ]
    VIP_SIDEBAR = [
        r"body > div.min-w-\[1200px\].max-w-\[2560px\].mx-auto.isolate > div.bg-\[\#f2f2f2\].pt-4.pb-20 > div.flex.mx-auto.max-w-\[1200px\].px-2\.5 > aside",
        "/html/body/div[3]/div[3]/div[2]/aside"
    ]
    DATE = '.date, .created-at, .post-date, .write-date, li[title]'
    IMAGES = [
        r"body > div.min-w-\[1200px\].max-w-\[2560px\].mx-auto.isolate > div.bg-\[\#f2f2f2\].pt-4.pb-20 > div.flex.mx-auto.max-w-\[1200px\].px-2\.5 > div > section:nth-child(1) > div.relative.overflow-hidden > section > div > div > section img",
        r"body > div.min-w-\[1200px\].max-w-\[2560px\].mx-auto.isolate > div.bg-\[\#f2f2f2\].pt-4.pb-20 > div.flex.mx-auto.max-w-\[1200px\].px-2\.5 > div > section:nth-child(1) > div.relative.overflow-hidden > section > div > div > section > figure img",
        ".post-content img", ".view-content img", ".content img", "article img", ".fr-view img"
    ]


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
        
        # Configure download directory to /dev/null and block downloads
        prefs = {
            "download.default_directory": "/dev/null",
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_settings.popups": 0,
            "download_restrictions": 3  # 3 = Block all downloads
        }
        options.add_experimental_option("prefs", prefs)
        
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
    
    def _process_post(self, url: str, session: requests.Session) -> Dict[str, Any]:
        """Process a single post by its URL"""
        try:
            post_id = url.split('/')[-1]
            self._ensure_driver()
            
            # Normalize URL
            if not url.startswith('http'):
                url = f"{self.config.base_url}/community/{post_id}"
                
            self.logger.info(f"Navigating to post: {url}")
            self._navigate_to_post(url, post_id)
            
            # --- 0. Check VIP Status & Post Count ---
            is_vip = False
            post_count = None
            
            # Check VIP (Creator/Ace in sidebar)
            try:
                vip_selector = CrawlerSelectors.VIP_SIDEBAR[0]
                sidebar_elements = self.driver.find_elements(By.CSS_SELECTOR, vip_selector)
                if sidebar_elements:
                    sidebar_text = sidebar_elements[0].text
                    if '크리에이터' in sidebar_text or '에이스' in sidebar_text:
                        is_vip = True
                        self.logger.info(f"VIP Author detected for post {post_id}")
            except Exception as e:
                self.logger.warning(f"Error checking VIP status: {e}")

            # Check Post Count
            try:
                post_count_selector = CrawlerSelectors.AUTHOR_POST_COUNT[0]
                post_count_element = self.driver.find_elements(By.CSS_SELECTOR, post_count_selector)
                
                if post_count_element:
                    count_text = post_count_element[0].text.strip().replace(',', '')
                    count_match = re.search(r'\d+', count_text)
                    if count_match:
                        post_count = int(count_match.group())
            except Exception as e:
                self.logger.warning(f"Error checking post count: {e}")

            # --- Skip Logic ---
            # Skip ONLY if:
            # 1. Not VIP
            # 2. Post count was successfully found
            # 3. Post count < 100
            if not is_vip and post_count is not None and post_count < 100:
                self.logger.info(f"Skipping post {post_id} because author has only {post_count} posts (< 100) and is not VIP.")
                return {'id': post_id, 'skipped': True, 'reason': 'low_post_count'}
            
            if is_vip:
                self.logger.info(f"Processing post {post_id} (VIP Author).")
            elif post_count is None:
                self.logger.info(f"Processing post {post_id} (Post count not found).")
            else:
                self.logger.info(f"Processing post {post_id} (Author has {post_count} posts).")

            # --- Processing (Unconditional) ---
            
            # 1. Extract Content & Title (Always)
            content = ""
            title = ""
            try:
                content = self._extract_content()
                title = self._extract_title()
                # Save text content
                self._save_post_text(post_id, title, content)
            except Exception as e:
                self.logger.warning(f"Error extracting/saving content: {e}")

            # 2. Extract & Save Images (Always)
            try:
                self._extract_and_save_images(post_id, session)
            except Exception as e:
                self.logger.warning(f"Error extracting images: {e}")

            # 3. Check & Download Files (If present)
            try:
                download_info = self.download_detector.check_for_downloads_browser(self.driver, url, post_id)
                
                # Check content for file references too
                if content:
                    content_download_info = self.download_detector.check_content_for_file_references(content, post_id)
                    if content_download_info.has_download:
                        download_info.has_download = True
                
                if download_info.has_download:
                    self.logger.info(f"Downloads found for {post_id}. Downloading files...")
                    self._download_files(post_id, download_info, session)
            except Exception as e:
                self.logger.warning(f"Error handling downloads: {e}")
            
            return {'id': post_id, 'skipped': False, 'processed': True}
            
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
            for selector in CrawlerSelectors.TITLE_MAIN:
                try:
                    title_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if title_elements:
                        return title_elements[0].text.strip()
                except Exception:
                    continue
                    
            # Fallback to page title
            return self.driver.title.replace(' : 월급쟁이부자들', '').strip()
        except Exception as e:
            self.logger.warning(f"Error extracting title: {e}")
            return ""

    def _extract_content(self) -> str:
        """Extract post content using specific selector"""
        # Only use the first selector as requested by user
        selector = CrawlerSelectors.CONTENT_AREAS[0]
        try:
            elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                text = elements[0].text.strip()
                if text:
                    self.logger.info(f"Found content using selector: {selector} ({len(text)} chars)")
                    return text
        except Exception as e:
            self.logger.warning(f"Error extracting content with specific selector: {e}")
            
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

    def _set_download_behavior(self, download_path: str) -> None:
        """Set download behavior using CDP"""
        try:
            params = {
                "behavior": "allow",
                "downloadPath": str(Path(download_path).absolute())
            }
            self.driver.execute_cdp_cmd("Page.setDownloadBehavior", params)
            self.logger.info(f"Set download path to: {download_path}")
        except Exception as e:
            self.logger.error(f"Error setting download behavior: {e}")

    def _download_files(self, post_id: str, download_info: Any, session: requests.Session) -> None:
        """Download files by clicking buttons"""
        try:
            output_dir = Path("output") / post_id
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Set download path dynamically
            self._set_download_behavior(str(output_dir))
            
            # Find and click download buttons
            user_selector = r"body > div.min-w-\[1200px\].max-w-\[2560px\].mx-auto.isolate > div.bg-\[\#f2f2f2\].pt-4.pb-20 > div.flex.mx-auto.max-w-\[1200px\].px-2\.5 > div > section:nth-child(1) > div.space-y-6.px-8.py-10 > ul > li > div > div.text-primary-600.flex.items-center.space-x-1\.5.py-2\.5 > span"
            
            buttons = self.driver.find_elements(By.CSS_SELECTOR, user_selector)
            if not buttons:
                self.logger.warning(f"No download buttons found for {post_id} despite detection.")
                return

            for i, btn in enumerate(buttons):
                try:
                    self.logger.info(f"Clicking download button {i+1}...")
                    # Scroll into view and click
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                    time.sleep(0.5)
                    self.driver.execute_script("arguments[0].click();", btn)
                    
                    # Wait for download to start/finish
                    time.sleep(3) 
                except Exception as e:
                    self.logger.error(f"Error clicking button {i+1}: {e}")
            
            # Wait a bit more for downloads to complete
            time.sleep(2)
            
        except Exception as e:
            self.logger.error(f"Error in _download_files: {e}")

    def _save_post_text(self, post_id: str, title: str, content: str) -> None:
        """Save post title and content to a text file"""
        try:
            output_dir = Path("output") / post_id
            output_dir.mkdir(parents=True, exist_ok=True)
            
            filepath = output_dir / f"{post_id}.txt"
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"Title: {title}\n\n")
                f.write(content)
                
            self.logger.info(f"Saved text content to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving text content: {e}")

    def _extract_and_save_images(self, post_id: str, session: requests.Session) -> None:
        """Extract images and save them to output/<post_id>/"""
        try:
            image_urls = [] # Use list to preserve order
            seen_urls = set()
            
            # Strategy 1: Find images within the content area (preserves document order)
            found_in_content = False
            for selector in CrawlerSelectors.CONTENT_AREAS:
                try:
                    content_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if content_elements:
                        # Use the first matching content area
                        container = content_elements[0]
                        images = container.find_elements(By.TAG_NAME, "img")
                        if images:
                            self.logger.info(f"Found {len(images)} images in content area: {selector}")
                            for img in images:
                                src = img.get_attribute("src")
                                if src and not src.startswith("data:") and not src.endswith(".svg"):
                                    img_url = src if src.startswith("http") else f"{self.config.base_url}{src}"
                                    if img_url not in seen_urls:
                                        image_urls.append(img_url)
                                        seen_urls.add(img_url)
                            found_in_content = True
                            break
                except Exception:
                    continue
            
            # Strategy 2: Fallback to IMAGES selectors if no images found in content area
            if not found_in_content:
                for selector in CrawlerSelectors.IMAGES:
                    images = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for img in images:
                        src = img.get_attribute("src")
                        if src and not src.startswith("data:") and not src.endswith(".svg"):
                            img_url = src if src.startswith("http") else f"{self.config.base_url}{src}"
                            if img_url not in seen_urls:
                                image_urls.append(img_url)
                                seen_urls.add(img_url)
            
            if not image_urls:
                self.logger.info(f"No images found for post {post_id}")
                return

            self.logger.info(f"Found {len(image_urls)} images for post {post_id}")
            
            # Sync cookies for downloading
            self._sync_cookies_to_session(session)
            
            # Create output directory for this post
            output_dir = Path("output") / post_id
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Download images
            for i, img_url in enumerate(image_urls):
                try:
                    # Determine extension
                    ext = "jpg"
                    if "." in img_url.split("/")[-1]:
                        possible_ext = img_url.split("/")[-1].split(".")[-1].split("?")[0]
                        if possible_ext.lower() in ["png", "jpeg", "jpg", "gif", "webp"]:
                            ext = possible_ext
                    
                    filename = f"image_{i+1}.{ext}"
                    filepath = output_dir / filename
                    
                    self.logger.info(f"Downloading image {img_url} to {filepath}")
                    
                    # Use session for download
                    response = session.get(img_url, stream=True, timeout=10)
                        
                    if response.status_code == 200:
                        with open(filepath, 'wb') as f:
                            for chunk in response.iter_content(chunk_size=8192):
                                f.write(chunk)
                    else:
                        self.logger.warning(f"Failed to download image {img_url}: Status {response.status_code}")
                        
                except Exception as e:
                    self.logger.error(f"Error downloading image {img_url}: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error extracting/saving images: {e}")

    def _save_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Save results to JSONL file.
        DISABLED for Image Crawler Mode as per user request.
        """
        pass

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
                    
                    self._process_page_posts(posts, page, stats, self.authenticator.session)
                    
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

    def _process_page_posts(self, posts: List[Tuple[str, str]], page: int, stats: Dict[str, Any], session: requests.Session) -> None:
        """Process all posts on a single page"""
        for title, url in tqdm(posts, desc=f"Posts p{page}", leave=False):
            try:
                self.logger.info(f"Processing post: {title}")
                # Pass session to _process_post
                result = self._process_post(url, session)
                
                if result.get('skipped'):
                    stats['posts_skipped'] = stats.get('posts_skipped', 0) + 1
                else:
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

    def _sync_cookies_to_session(self, session: requests.Session) -> None:
        """Sync Selenium cookies to the requests session"""
        try:
            if self.driver and session:
                for cookie in self.driver.get_cookies():
                    session.cookies.set(cookie['name'], cookie['value'])
        except Exception:
            pass
