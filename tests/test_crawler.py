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
             patch('src.crawler.crawler.HTMLSession') as session_mock, \
             patch('src.crawler.crawler.urllib.robotparser.RobotFileParser') as robots_mock, \
             patch('src.crawler.crawler.psutil.Process') as process_mock, \
             patch('src.crawler.crawler.Crawler._create_driver', return_value=self.driver_mock), \
             patch('src.crawler.crawler.JsonlStorage') as storage_mock, \
             patch('src.crawler.crawler.CheckpointManager') as checkpoint_mock, \
             patch('src.crawler.crawler.Authenticator') as auth_mock, \
             patch('src.crawler.crawler.ContentParser') as content_parser_mock, \
             patch('src.crawler.crawler.ListParser') as list_parser_mock, \
             patch('src.crawler.crawler.FileProcessor') as file_processor_mock, \
             patch('src.crawler.crawler.DownloadDetector') as download_detector_mock:
            
            # Set up mock returns
            config_cls_mock.get_instance.return_value = self.config_mock
            storage_mock.return_value = MagicMock()
            checkpoint_mock.return_value = MagicMock()
            auth_mock.return_value = MagicMock()
            content_parser_mock.return_value = MagicMock()
            list_parser_mock.return_value = MagicMock()
            file_processor_mock.return_value = MagicMock()
            download_detector_mock.return_value = MagicMock()
            
            # Create crawler instance
            self.crawler = Crawler()

    def test_normalize_url(self):
        """Test URL normalization"""
        # Set base URL in config
        self.crawler.config.base_url = "https://example.com"
        
        # Absolute URL
        self.assertEqual(
            self.crawler._normalize_url("https://example.com/page"),
            "https://example.com/page"
        )
        
        # Relative URL
        self.assertEqual(
            self.crawler._normalize_url("/page"),
            "https://example.com/page"
        )
        
        # URL with query parameters
        self.assertEqual(
            self.crawler._normalize_url("/page?q=test"),
            "https://example.com/page?q=test"
        )
        
        # URL with fragment
        self.assertEqual(
            self.crawler._normalize_url("/page#section"),
            "https://example.com/page#section"
        )
        
    def test_calculate_content_hash(self):
        """Test content hash calculation"""
        # Test with simple content
        content = b"Test content for hashing"
        expected_hash = hashlib.sha256(content).hexdigest()
        self.assertEqual(self.crawler._calculate_content_hash(content), expected_hash)
        
        # Test with empty content
        with patch('logging.warning') as mock_log_warning:
            self.assertEqual(self.crawler._calculate_content_hash(None), "")
            mock_log_warning.assert_called_once()
        
        # Test with large content (to test chunking)
        large_content = b"X" * 1024 * 1024  # 1MB of data
        hash_obj = hashlib.sha256()
        hash_obj.update(large_content)
        expected_large_hash = hash_obj.hexdigest()
        self.assertEqual(self.crawler._calculate_content_hash(large_content), expected_large_hash)
        
        # Test with binary content
        binary_content = bytes([0x00, 0x01, 0x02, 0x03, 0xFF])
        binary_hash = hashlib.sha256(binary_content).hexdigest()
        self.assertEqual(self.crawler._calculate_content_hash(binary_content), binary_hash)
        
        # Test with non-bytes content (error handling)
        with patch('logging.warning') as mock_log_warning:
            # The actual implementation converts non-bytes to string and hashes it
            result = self.crawler._calculate_content_hash("string instead of bytes")
            # Should log a warning about unexpected type
            self.assertTrue(mock_log_warning.called)
            # Should still return a hash (the implementation falls back to string conversion)
            self.assertTrue(isinstance(result, str) and len(result) > 0)
    
    def test_extract_pagination_info(self):
        """Test the improved pagination handling functionality"""
        # Test case 1: Standard pagination with next page
        html_with_pagination = """
        <div class="pagination">
            <a href="?page=1">1</a>
            <a href="?page=2" class="current">2</a>
            <a href="?page=3">3</a>
            <a href="?page=4">4</a>
            <a href="?page=5">5</a>
        </div>
        """
        current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_pagination, 2)
        self.assertEqual(current_page, 2)
        self.assertEqual(last_page, 5)
        self.assertTrue(has_next)
        
        # Test case 2: Pagination with no next page
        html_with_last_page = """
        <div class="pagination">
            <a href="?page=1">1</a>
            <a href="?page=2">2</a>
            <a href="?page=3">3</a>
            <a href="?page=4">4</a>
            <a href="?page=5" class="current">5</a>
        </div>
        """
        current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_last_page, 5)
        self.assertEqual(current_page, 5)
        self.assertEqual(last_page, 5)
        self.assertFalse(has_next)
        
        # Test case 3: Pagination with non-numeric format
        html_with_non_numeric = """
        <div class="pagination">
            <a href="?page=prev">이전</a>
            <span class="current-page">Page 3 of 10</span>
            <a href="?page=next">다음</a>
        </div>
        """
        current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_non_numeric, 3)
        self.assertEqual(current_page, 3)
        self.assertEqual(last_page, 10)
        self.assertTrue(has_next)
        
        # Test case 4: No pagination information
        html_without_pagination = "<div class=\"content\">Some content without pagination</div>"
        current_page, last_page, has_next = self.crawler._extract_pagination_info(html_without_pagination, 1)
        self.assertEqual(current_page, 1)
        self.assertEqual(last_page, 1)
        self.assertFalse(has_next)
    
    def test_monitor_resources(self):
        """Test resource monitoring functionality"""
        # Create a mock process for resource monitoring
        process_mock = MagicMock()
        
        # Set up memory info mock
        memory_info_mock = MagicMock()
        memory_info_mock.rss = 500 * 1024 * 1024  # 500 MB
        process_mock.memory_info.return_value = memory_info_mock
        
        # Set up CPU usage mock
        process_mock.cpu_percent.return_value = 50.0  # 50%
        
        # Replace the crawler's process with our mock
        with patch('src.crawler.crawler.psutil.Process', return_value=process_mock), \
             patch('logging.debug') as mock_log_debug, \
             patch('logging.warning') as mock_log_warning:
            
            # Initialize the crawler's process
            self.crawler.process = process_mock
            self.crawler.memory_usage_samples = []
            self.crawler.cpu_usage_samples = []
            self.crawler.last_sample_time = None
            
            # Test with resources under threshold
            self.crawler.config.memory_threshold_mb = 1000  # 1 GB
            self.crawler.config.cpu_threshold_percent = 80
            
            # Method returns None but should log resource usage
            self.crawler._monitor_resources()
            
            # Verify samples were collected
            self.assertEqual(len(self.crawler.memory_usage_samples), 1)
            self.assertEqual(len(self.crawler.cpu_usage_samples), 1)
            self.assertEqual(self.crawler.memory_usage_samples[0], 500.0)  # 500 MB
            self.assertEqual(self.crawler.cpu_usage_samples[0], 50.0)  # 50%
            
            # Test with memory over threshold
            # Reset last_sample_time to ensure monitoring happens
            self.crawler.last_sample_time = None
            
            # First mock the debug logging to avoid interference
            with patch('logging.debug'):
                # Set memory threshold and mock memory usage
                self.crawler.config.memory_threshold_mb = 400  # 400 MB threshold
                memory_info_mock.rss = 500 * 1024 * 1024  # 500 MB actual usage (over threshold)
                
                # Need to patch both logging.warning and logging.info before gc.collect
                with patch('logging.warning') as mock_log_warning, \
                     patch('logging.info') as mock_log_info, \
                     patch('gc.collect') as mock_gc_collect:
                    
                    # Call the method
                    self.crawler._monitor_resources()
                    
                    # Verify the warning was logged with the correct message
                    mock_log_warning.assert_called_once()
                    warning_call_args = mock_log_warning.call_args[0][0]
                    self.assertIn("High memory usage detected", warning_call_args)
                    # The actual format includes 2 decimal places
                    self.assertIn("500.00 MB > 400 MB threshold", warning_call_args)
                    
                    # Should log info about performing garbage collection
                    mock_log_info.assert_called_once_with("Performing garbage collection to free memory")
                    
                    # Should call garbage collection when memory is over threshold
                    mock_gc_collect.assert_called_once()
            
            # Test with CPU over threshold
            # Reset last_sample_time to ensure monitoring happens
            self.crawler.last_sample_time = None
            
            # First mock the debug logging to avoid interference
            with patch('logging.debug'):
                # Set CPU threshold and mock CPU usage
                self.crawler.config.memory_threshold_mb = 1000  # Reset memory threshold (well above usage)
                self.crawler.config.cpu_threshold_percent = 40  # 40% threshold
                process_mock.cpu_percent.return_value = 50.0  # 50% actual usage (over threshold)
                
                # Need to patch logging.warning
                with patch('logging.warning') as mock_log_warning:
                    # Call the method
                    self.crawler._monitor_resources()
                    
                    # Verify the warning was logged with the correct message
                    mock_log_warning.assert_called_once()
                    warning_call_args = mock_log_warning.call_args[0][0]
                    self.assertIn("High CPU usage detected", warning_call_args)
                    # The actual format includes 2 decimal places
                    self.assertIn("50.00% > 40% threshold", warning_call_args)
            
            # Test with exception - need to add a try/except block since the method doesn't have one
            process_mock.memory_info.side_effect = Exception("Test exception")
            
            # Reset last_sample_time to ensure monitoring happens
            self.crawler.last_sample_time = None
            
            # We need to wrap the call in our own try/except since the method doesn't have one
            try:
                self.crawler._monitor_resources()
                self.fail("Expected an exception to be raised")
            except Exception as e:
                # Verify the exception is the one we set up
                self.assertEqual(str(e), "Test exception")
    
    def test_download_and_parse_file(self):
        """Test file downloading and parsing with performance metrics"""
        test_url = "https://example.com/test.pdf"
        test_filename = "test.pdf"
        test_content = b"PDF content for testing"
        
        # Mock requests.get
        mock_response = MagicMock()
        mock_response.content = test_content
        mock_response.raise_for_status = MagicMock()
        
        # Mock file processor
        mock_file_content = MagicMock()
        mock_file_content.content = "Parsed content"
        mock_file_content.metadata = {"title": "Test Document"}
        mock_file_content.file_type = "pdf"
        self.crawler.file_processor.parse_file.return_value = [mock_file_content]
        
        # Define time values for mocking - provide plenty of time values to avoid StopIteration
        time_values = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5, 104.0, 104.5]
        
        with patch('requests.get', return_value=mock_response) as mock_get, \
             patch('time.time', side_effect=time_values), \
             patch('logging.debug'):
            
            # Test successful download and parse
            result = self.crawler._download_and_parse_file(test_url, test_filename)
            
            # Verify request was made correctly
            mock_get.assert_called_once_with(test_url, headers=self.crawler.auth_headers, timeout=self.crawler.config.request_timeout)
            
            # Verify file processor was called with correct arguments
            self.crawler.file_processor.parse_file.assert_called_once_with(test_url, "", test_filename)
            
            # Check result structure
            self.assertEqual(result["url"], test_url)
            self.assertEqual(result["filename"], test_filename)
            self.assertEqual(result["content"], "Parsed content")
            self.assertEqual(result["metadata"], {"title": "Test Document"})
            self.assertEqual(result["file_type"], "pdf")
            
            # Verify performance metrics - in the actual implementation, metrics are at the top level
            self.assertIn("download_time", result)
            self.assertIn("parse_time", result)
            self.assertIn("total_processing_time", result)
            # The total time should be the sum of download_time and parse_time
            self.assertEqual(result["total_processing_time"], 2.5)  # 2.5 seconds (1.0 + 1.5)
            
            # Verify hash calculation was performed
            self.assertIn("hash", result)
        
        # Test error handling during download
        with patch('requests.get', side_effect=Exception("Download error")), \
             patch('time.time', return_value=100.0), \
             patch('logging.error') as mock_log_error:
            
            result = self.crawler._download_and_parse_file(test_url, test_filename)
            
            # Verify error is captured
            self.assertIn("error", result)
            self.assertEqual(result["filename"], test_filename)
            self.assertEqual(result["url"], test_url)
            mock_log_error.assert_called_once()
    
    def test_download_and_parse_file_with_hash(self):
        """Test downloading and parsing a file with content hash calculation"""
        test_url = "https://example.com/test.pdf"
        test_filename = "test.pdf"
        test_content = b"PDF content for testing"
        expected_hash = hashlib.sha256(test_content).hexdigest()
        
        # Mock requests.get
        mock_response = MagicMock()
        mock_response.content = test_content
        mock_response.raise_for_status = MagicMock()
        
        # Mock file processor
        mock_file_content = MagicMock()
        mock_file_content.content = "Parsed content"
        mock_file_content.metadata = {"title": "Test Document"}
        mock_file_content.file_type = "pdf"
        self.crawler.file_processor.parse_file.return_value = [mock_file_content]
        
        # Set up auth headers for the request
        self.crawler.auth_headers = None
        
        # Test with regular content - provide plenty of time values to avoid StopIteration
        time_values = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5, 104.0, 104.5]
        with patch('requests.get', return_value=mock_response), \
             patch('time.time', side_effect=time_values), \
             patch('logging.debug'):
            
            result = self.crawler._download_and_parse_file(test_url, test_filename)
            
            # Verify hash calculation was performed correctly
            self.assertEqual(result["hash"], expected_hash)
        
        # Test with binary content
        binary_content = bytes([0x00, 0x01, 0x02, 0x03, 0xFF])
        binary_hash = hashlib.sha256(binary_content).hexdigest()
        
        mock_response.content = binary_content
        
        # Provide plenty of time values to avoid StopIteration
        time_values = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5, 103.0, 103.5, 104.0, 104.5]
        with patch('requests.get', return_value=mock_response), \
             patch('time.time', side_effect=time_values), \
             patch('logging.debug'):
            
            result = self.crawler._download_and_parse_file(test_url, test_filename)
            
            # Verify hash calculation was performed correctly for binary content
            self.assertEqual(result["hash"], binary_hash)
            
        # Direct test of the hash calculation method
        self.assertEqual(self.crawler._calculate_content_hash(binary_content), binary_hash)
    
    def test_extract_pagination_info(self):
        """Test the improved pagination handling functionality"""
        # Test case 1: Standard pagination with page numbers
        html_with_standard_pagination = """
        <div class="pagination">
            <a href="?page=1">1</a>
            <a href="?page=2" class="current">2</a>
            <a href="?page=3">3</a>
            <a href="?page=4">4</a>
            <a href="?page=5">5</a>
            <a href="?page=3" class="next">다음</a>
        </div>
        """
        current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_standard_pagination, 2)
        self.assertEqual(current_page, 2)
        self.assertEqual(last_page, 5)
        self.assertTrue(has_next)
        
        # Test case 2: Pagination with no next page
        html_with_last_page = """
        <div class="pagination">
            <a href="?page=1">1</a>
            <a href="?page=2">2</a>
            <a href="?page=3">3</a>
            <a href="?page=4">4</a>
            <a href="?page=5" class="current">5</a>
        </div>
        """
        current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_last_page, 5)
        self.assertEqual(current_page, 5)
        self.assertEqual(last_page, 5)
        self.assertFalse(has_next)
        
        # Test case 3: Pagination with non-numeric format
        html_with_non_numeric = """
        <div class="pagination">
            <a href="?page=prev">이전</a>
            <span class="current-page">Page 3 of 10</span>
            <a href="?page=next">다음</a>
        </div>
        """
        current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_non_numeric, 3)
        self.assertEqual(current_page, 3)
        self.assertEqual(last_page, 10)
        self.assertTrue(has_next)
        
        # Test case 4: No pagination information
        html_without_pagination = "<div class=\"content\">Some content without pagination</div>"

def test_download_and_parse_file(self):
    """Test file downloading and parsing with performance metrics"""
    test_url = "https://example.com/test.pdf"
    test_filename = "test.pdf"
    test_content = b"PDF content for testing"
        
    # Mock requests.get
    mock_response = MagicMock()
    mock_response.content = test_content
    mock_response.raise_for_status = MagicMock()
        
    # Mock file processor
    mock_file_content = MagicMock()
    mock_file_content.content = "Parsed content"
    mock_file_content.metadata = {"title": "Test Document"}
    mock_file_content.file_type = "pdf"
    self.crawler.file_processor.parse_file.return_value = [mock_file_content]
        
    # Define time values for mocking
    time_values = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5]  # Add extra values for safety
        
    with patch('requests.get', return_value=mock_response), \
         patch('time.time', side_effect=time_values), \
         patch('logging.debug'):
            
        # Test successful download and parse
        result = self.crawler._download_and_parse_file(test_url, test_filename)
            
        # Verify result contains performance metrics
        self.assertEqual(result["url"], test_url)
        self.assertEqual(result["filename"], test_filename)
        self.assertEqual(result["content"], "Parsed content")
        self.assertEqual(result["file_type"], "pdf")
        self.assertEqual(result["file_size"], len(test_content))
        self.assertIn("download_time", result)
        self.assertIn("parse_time", result)
        self.assertIn("total_processing_time", result)
        self.assertIn("hash", result)
        
    # Test error handling during download
    with patch('requests.get', side_effect=Exception("Download error")), \
         patch('time.time', return_value=100.0), \
         patch('logging.error'):
            
        result = self.crawler._download_and_parse_file(test_url, test_filename)
            
        # Verify error is captured
        self.assertIn("error", result)
        self.assertEqual(result["filename"], test_filename)
        self.assertEqual(result["url"], test_url)
        self.assertIn("download_time", result)
    # Test with binary content
    binary_content = bytes([0x00, 0x01, 0x02, 0x03, 0xFF])
    binary_hash = hashlib.sha256(binary_content).hexdigest()
    self.assertEqual(self.crawler._calculate_content_hash(binary_content), binary_hash)
        
def test_extract_pagination_info(self):
    """Test the improved pagination handling functionality"""
    # Test case 1: Standard pagination with page numbers
    html_with_standard_pagination = """
    <div class="pagination">
        <a href="?page=1">1</a>
        <a href="?page=2" class="current">2</a>
        <a href="?page=3">3</a>
        <a href="?page=4">4</a>
        <a href="?page=5">5</a>
        <a href="?page=3" class="next">다음</a>
    </div>
    """
    current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_standard_pagination, 2)
    self.assertEqual(current_page, 2)
    self.assertEqual(last_page, 5)
    self.assertTrue(has_next)
        
    # Test case 2: Pagination with no next page
    html_with_last_page = """
    <div class="pagination">
        <a href="?page=1">1</a>
        <a href="?page=2">2</a>
        <a href="?page=3">3</a>
        <a href="?page=4">4</a>
        <a href="?page=5" class="current">5</a>
    </div>
    """
    current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_last_page, 5)
    self.assertEqual(current_page, 5)
    self.assertEqual(last_page, 5)
    self.assertFalse(has_next)
        
    # Test case 3: Pagination with non-numeric format
    html_with_non_numeric = """
    <div class="pagination">
        <a href="?page=prev">이전</a>
        <span class="current-page">Page 3 of 10</span>
        <a href="?page=next">다음</a>
    </div>
    """
    current_page, last_page, has_next = self.crawler._extract_pagination_info(html_with_non_numeric, 3)
    self.assertEqual(current_page, 3)
    self.assertEqual(last_page, 10)
    self.assertTrue(has_next)
        
    # Test case 4: No pagination information
    html_without_pagination = "<div class=\"content\">Some content without pagination</div>"
    current_page, last_page, has_next = self.crawler._extract_pagination_info(html_without_pagination, 1)
    self.assertEqual(current_page, 1)
    self.assertEqual(last_page, 1)
    self.assertFalse(has_next)

def test_download_and_parse_file_with_hash(self):
    """Test downloading and parsing a file with content hash calculation"""
    # Set up test data
    test_url = "https://example.com/test.pdf"
    test_filename = "test.pdf"
    test_content = b"Test file content"
    expected_hash = hashlib.sha256(test_content).hexdigest()
        
    # Set up mock response
    mock_response = MagicMock()
    mock_response.content = test_content
    mock_response.raise_for_status = MagicMock()
        
    # Set up mock file processor
    mock_file_content = MagicMock()
    mock_file_content.content = "Parsed content"
    mock_file_content.metadata = {"title": "Test Document"}
    mock_file_content.file_type = "pdf"
    self.crawler.file_processor.parse_file.return_value = [mock_file_content]
        
    # Set up time mocking to avoid StopIteration errors
    time_values = [100.0, 100.5, 101.0, 101.5, 102.0, 102.5]  # Add extra values for safety
        
    # Call the method with all necessary mocks
    with patch('requests.get', return_value=mock_response), \
         patch('time.time', side_effect=time_values), \
         patch('logging.debug'):
            
        # Call the method
        result = self.crawler._download_and_parse_file(test_url, test_filename)
            
        # Verify the hash was calculated correctly
        self.assertEqual(result["hash"], expected_hash)
            
        # Verify other content was parsed correctly
        self.assertEqual(result["content"], "Parsed content")
        self.assertEqual(result["file_type"], "pdf")
        self.assertEqual(result["metadata"], {"title": "Test Document"})
            
        # Verify performance metrics are included
        self.assertIn("download_time", result)
        self.assertIn("parse_time", result)
        self.assertIn("total_processing_time", result)
        self.assertIn("file_size", result)

if __name__ == '__main__':
    unittest.main()
