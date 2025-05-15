#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for the Crawler class
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, Mock
import hashlib
import tempfile
from bs4 import BeautifulSoup

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.crawler.crawler import Crawler, RobotsError
from src.models.post import Post


class TestCrawler(unittest.TestCase):
    """Test cases for the Crawler class"""

    def setUp(self):
        """Set up test fixtures"""
        # Mock config
        self.config_mock = MagicMock()
        self.config_mock.base_url = "https://example.com"
        self.config_mock.user_agent = "TestBot/1.0"
        self.config_mock.wait_between_pages = 1
        self.config_mock.rate_limit_requests = 10
        self.config_mock.rate_limit_period = 60
        self.config_mock.request_timeout = 10
        self.config_mock.max_retries = 3
        self.config_mock.retry_delay = 1
        self.config_mock.allowed_domains = ["example.com"]
        
        # Create crawler with mocked config
        with patch('src.crawler.crawler.Config') as config_cls_mock:
            config_cls_mock.get_instance.return_value = self.config_mock
            self.crawler = Crawler()
        
        # Mock the components
        self.crawler.authenticator = MagicMock()
        self.crawler.content_parser = MagicMock()
        self.crawler.file_processor = MagicMock()
        self.crawler.download_detector = MagicMock()
        self.crawler.storage = MagicMock()
        self.crawler.checkpoint_manager = MagicMock()
        self.crawler.session = MagicMock()
        self.crawler.robots_parser = MagicMock()
        self.crawler.driver = MagicMock()

    def test_init_robots_txt(self):
        """Test initialization of robots.txt parser"""
        # Mock the robotparser
        with patch('src.crawler.crawler.robotparser.RobotFileParser') as mock_parser:
            # Create a new instance to trigger _init_robots_txt
            crawler = Crawler(self.config_mock)
            
            # Verify that the robot parser was initialized correctly
            mock_parser.assert_called_once()
            mock_parser.return_value.set_url.assert_called_once_with(
                f"{self.config_mock.base_url}/robots.txt"
            )
            mock_parser.return_value.read.assert_called_once()

    def test_can_fetch(self):
        """Test _can_fetch method"""
        # Set up the mock
        self.crawler.robots_parser.can_fetch.return_value = True
        
        # Test with an allowed URL
        url = "https://example.com/allowed"
        result = self.crawler._can_fetch(url)
        
        # Verify the result and that the mock was called correctly
        self.assertTrue(result)
        self.crawler.robots_parser.can_fetch.assert_called_once_with(
            self.config_mock.user_agent, url
        )
        
        # Reset the mock and test with a disallowed URL
        self.crawler.robots_parser.can_fetch.reset_mock()
        self.crawler.robots_parser.can_fetch.return_value = False
        
        url = "https://example.com/disallowed"
        result = self.crawler._can_fetch(url)
        
        # Verify the result and that the mock was called correctly
        self.assertFalse(result)
        self.crawler.robots_parser.can_fetch.assert_called_once_with(
            self.config_mock.user_agent, url
        )

    def test_rate_limit(self):
        """Test rate limiting functionality"""
        with patch('src.crawler.crawler.time.sleep') as mock_sleep:
            # Test normal delay between requests
            self.crawler._rate_limit()
            
            # Should not sleep if this is the first request
            mock_sleep.assert_not_called()
            
            # Test with a previous request
            with patch('src.crawler.crawler.datetime') as mock_datetime:
                # Set up the mock to simulate time passing
                mock_datetime.now.return_value = MagicMock()
                
                # Call rate limit again
                self.crawler._rate_limit()
                
                # Should sleep for the configured delay
                mock_sleep.assert_called()

    def test_normalize_url(self):
        """Test URL normalization"""
        # Test with relative URL
        relative_url = "/path/to/page"
        normalized = self.crawler._normalize_url(relative_url)
        self.assertEqual(normalized, f"{self.config_mock.base_url}{relative_url}")
        
        # Test with absolute URL
        absolute_url = "https://example.com/path/to/page"
        normalized = self.crawler._normalize_url(absolute_url)
        self.assertEqual(normalized, absolute_url)
        
        # Test with URL without scheme
        no_scheme_url = "example.com/path/to/page"
        normalized = self.crawler._normalize_url(no_scheme_url)
        self.assertEqual(normalized, f"{self.config_mock.base_url}/{no_scheme_url}")

    def test_is_valid_url(self):
        """Test URL validation"""
        # Test with valid URL
        valid_url = "https://example.com/path"
        self.assertTrue(self.crawler._is_valid_url(valid_url))
        
        # Test with empty URL
        empty_url = ""
        self.assertFalse(self.crawler._is_valid_url(empty_url))
        
        # Test with non-HTTP URL
        non_http_url = "ftp://example.com/path"
        self.assertFalse(self.crawler._is_valid_url(non_http_url))
        
        # Test with URL from non-allowed domain
        non_allowed_domain = "https://other-domain.com/path"
        self.assertFalse(self.crawler._is_valid_url(non_allowed_domain))

    def test_generate_url_hash(self):
        """Test URL hash generation"""
        url = "https://example.com/path/to/page"
        expected_hash = hashlib.md5(url.encode()).hexdigest()
        
        hash_result = self.crawler._generate_url_hash(url)
        self.assertEqual(hash_result, expected_hash)

    def test_calculate_content_hash(self):
        """Test content hash calculation"""
        # Test with valid bytes
        test_bytes = b"test content"
        expected_hash = hashlib.sha256(test_bytes).hexdigest()
        
        hash_result = self.crawler._calculate_content_hash(test_bytes)
        self.assertEqual(hash_result, expected_hash)
        
        # Test with None
        hash_result = self.crawler._calculate_content_hash(None)
        self.assertEqual(hash_result, "")

    def test_extract_pagination_info(self):
        """Test pagination information extraction"""
        # Create a sample HTML with pagination
        html = """
        <html>
            <body>
                <div class="pagination">
                    <a href="?page=1">1</a>
                    <a href="?page=2" class="current">2</a>
                    <a href="?page=3">3</a>
                    <a href="?page=4">4</a>
                </div>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Test extraction
        pagination_info = self.crawler._extract_pagination_info(soup)
        
        # Verify the extracted information
        self.assertIn('current_page', pagination_info)
        self.assertEqual(pagination_info['current_page'], 2)
        self.assertIn('total_pages', pagination_info)
        self.assertEqual(pagination_info['total_pages'], 4)
        
        # Test with no pagination
        html_no_pagination = "<html><body><div>No pagination here</div></body></html>"
        soup_no_pagination = BeautifulSoup(html_no_pagination, 'html.parser')
        
        pagination_info = self.crawler._extract_pagination_info(soup_no_pagination)
        self.assertEqual(pagination_info, {})

    def test_parse_list_api(self):
        """Test parsing of list page"""
        # Create a sample HTML with posts
        html = """
        <html>
            <body>
                <div class="board-list">
                    <div class="post-item">
                        <a href="/community/123" class="title">Test Post 1</a>
                    </div>
                    <div class="post-item">
                        <a href="/community/456" class="title">Test Post 2</a>
                    </div>
                </div>
            </body>
        </html>
        """
        
        # Mock the normalize_url and is_valid_url methods
        self.crawler._normalize_url = lambda url: f"https://example.com{url}"
        self.crawler._is_valid_url = lambda url: True
        
        # Test parsing
        posts = self.crawler.parse_list_api(1, html)
        
        # Verify the parsed posts
        self.assertEqual(len(posts), 2)
        self.assertEqual(posts[0][0], "Test Post 1")
        self.assertEqual(posts[0][1], "https://example.com/community/123")
        self.assertEqual(posts[1][0], "Test Post 2")
        self.assertEqual(posts[1][1], "https://example.com/community/456")
        
        # Test with no posts
        html_no_posts = "<html><body><div>No posts here</div></body></html>"
        posts = self.crawler.parse_list_api(1, html_no_posts)
        self.assertEqual(posts, [])

    @patch('src.crawler.crawler.requests.Session')
    def test_get_page(self, mock_session):
        """Test page retrieval with rate limiting and robots.txt checking"""
        # Set up mocks
        self.crawler._can_fetch = MagicMock(return_value=True)
        self.crawler._rate_limit = MagicMock()
        self.crawler.authenticator.ensure_authenticated = MagicMock(return_value=(None, None))
        
        mock_response = MagicMock()
        mock_response.text = "Page content"
        self.crawler.session.get.return_value = mock_response
        
        # Test successful page retrieval
        url = "https://example.com/page"
        result = self.crawler.get_page(url)
        
        # Verify the result and that the mocks were called correctly
        self.assertEqual(result, "Page content")
        self.crawler._can_fetch.assert_called_once_with(url)
        self.crawler._rate_limit.assert_called_once()
        self.crawler.authenticator.ensure_authenticated.assert_called_once()
        self.crawler.session.get.assert_called_once()
        
        # Test with robots.txt disallowing the URL
        self.crawler._can_fetch.reset_mock()
        self.crawler._can_fetch.return_value = False
        
        with self.assertRaises(RobotsError):
            self.crawler.get_page(url)
        
        self.crawler._can_fetch.assert_called_once_with(url)


if __name__ == '__main__':
    unittest.main()
