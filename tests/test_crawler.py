#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for the Crawler class - Task 3 features
"""
import os
import sys
import unittest
import time
import io
import logging
from unittest.mock import patch, MagicMock, mock_open
import hashlib
from datetime import datetime, timedelta

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import with try/except to handle missing dependencies in test environment
try:
    from src.crawler.crawler import Crawler
    from src.models.models import Post
except ImportError as e:
    print(f"Import error: {e}")


class TestCrawler(unittest.TestCase):
    """Test cases for the Crawler class - Task 3 features"""

    def setUp(self):
        """Set up test fixtures"""
        # Skip tests if imports failed
        if 'Crawler' not in globals():
            self.skipTest("Required modules not available")
            
        # Create a mock config
        self.config_mock = MagicMock()
        self.config_mock.base_url = "https://example.com"
        self.config_mock.rate_limit_enabled = False
        self.config_mock.request_timeout = 10
        self.config_mock.max_retries = 3
        self.config_mock.retry_delay = 1
        self.config_mock.memory_threshold_mb = 1000
        self.config_mock.cpu_threshold_percent = 90
        
        # Mock Chrome driver
        self.driver_mock = MagicMock()
        
        # Create crawler with patched dependencies
        with patch('src.crawler.crawler.Config') as config_cls_mock, \
             patch('src.crawler.crawler.cloudscraper.create_scraper') as scraper_mock, \
             patch('src.crawler.crawler.Crawler._create_driver', return_value=self.driver_mock), \
             patch('src.crawler.crawler.JsonlStorage') as storage_mock, \
             patch('src.crawler.crawler.CheckpointManager') as checkpoint_mock, \
             patch('src.crawler.crawler.Authenticator') as auth_mock, \
             patch('src.crawler.crawler.FileProcessor') as file_processor_mock, \
             patch('src.crawler.crawler.DownloadDetector') as download_detector_mock:
            
            # Set up mock returns
            config_cls_mock.get_instance.return_value = self.config_mock
            storage_mock.return_value = MagicMock()
            checkpoint_mock.return_value = MagicMock()
            auth_mock.return_value = MagicMock()
            file_processor_mock.return_value = MagicMock()
            download_detector_mock.return_value = MagicMock()
            
            # Create crawler instance
            self.crawler = Crawler()

    def test_ensure_authenticated(self):
        """Test authentication check"""
        # Setup mock authenticator
        self.crawler.authenticator.ensure_authenticated.return_value = ({"Cookie": "test"}, self.driver_mock)
        
        # Call method
        headers, driver = self.crawler.ensure_authenticated()
        
        # Verify results
        self.assertEqual(headers, {"Cookie": "test"})
        self.assertEqual(driver, self.driver_mock)
        self.crawler.authenticator.ensure_authenticated.assert_called_once()

if __name__ == '__main__':
    unittest.main()
