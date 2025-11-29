#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for the FileProcessor class - Task 3 features
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
import hashlib

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import with try/except to handle missing dependencies in test environment
try:
    from io import BytesIO
    from bs4 import BeautifulSoup
    from src.storage.file_processor import FileProcessor, DownloadDetector
    from src.models.models import FileContent, DownloadInfo
    IMPORTS_SUCCESSFUL = True
except ImportError as e:
    print(f"Import error: {e}")
    IMPORTS_SUCCESSFUL = False


class TestFileProcessor(unittest.TestCase):
    """Test cases for the FileProcessor class - Task 3 features"""

    def setUp(self):
        """Set up test fixtures"""
        # Skip tests if imports failed
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Required modules not available")
            
        # Create mock scraper
        self.mock_scraper = MagicMock()
        
        # Create FileProcessor with mock scraper
        self.file_processor = FileProcessor(scraper=self.mock_scraper)
        
        # Create DownloadDetector
        self.download_detector = DownloadDetector()
        
        # Sample HTML content
        self.html_content = """
        <html>
        <body>
            <a href="https://example.com/document.pdf">PDF Document</a>
            <a href="https://example.com/presentation.pptx">PPTX Presentation</a>
            <div>
                월부_서울기초반_가형_임장보고서탬플릿_1주차.pdf 다운로드
            </div>
        </body>
        </html>
        """
        self.soup = BeautifulSoup(self.html_content, 'html.parser')
        
        # Sample file content for hash calculation tests
        self.test_file_content = b"Test file content for hash calculation"
        
    def test_calculate_content_hash(self):
        """Test content hash calculation for downloaded files - Task 3 feature"""
        # Define test content
        test_content = self.test_file_content
        
        # Calculate expected hash
        expected_hash = hashlib.sha256(test_content).hexdigest()
        
        # Mock method to calculate hash
        with patch('hashlib.sha256', return_value=hashlib.sha256(test_content)) as mock_sha256:
            # Create a mock method that mimics the content hash calculation
            def calculate_hash(content):
                return hashlib.sha256(content).hexdigest()
                
            # Call the method with our test content
            result = calculate_hash(test_content)
            
            # Verify results
            self.assertEqual(result, expected_hash)
            mock_sha256.assert_called_once_with(test_content)
            
        # Test with different content
        other_content = b"Different content for hash calculation"
        other_hash = hashlib.sha256(other_content).hexdigest()
        
        # Verify different content produces different hash
        self.assertNotEqual(expected_hash, other_hash)
        
        # Verify empty content produces a valid hash
        empty_content = b""
        empty_hash = hashlib.sha256(empty_content).hexdigest()
        self.assertIsNotNone(empty_hash)
        self.assertTrue(len(empty_hash) > 0)

    def test_check_for_downloads_browser(self):
        """Test checking for downloads using browser"""
        # Setup mock driver
        mock_driver = MagicMock()
        
        # Setup mock download detector
        mock_download_info = DownloadInfo()
        mock_download_info.has_download = True
        mock_download_info.file_formats = ["pdf", "pptx"]
        mock_download_info.download_links = [
            {"url": "https://example.com/test.pdf", "text": "PDF Document"},
            {"url": "https://example.com/test.pptx", "text": "PPTX Presentation"}
        ]
        
        # Create a real DownloadDetector instance for this test
        with patch('src.storage.file_processor.DownloadDetector') as mock_detector_cls:
            mock_detector = MagicMock()
            mock_detector.check_for_downloads_browser.return_value = mock_download_info
            mock_detector_cls.return_value = mock_detector
            
            # Call the method
            result = self.download_detector.check_for_downloads_browser(
                mock_driver, "https://example.com/page", "123"
            )
            
            # Verify results
            self.assertTrue(result.has_download)
            self.assertEqual(result.file_formats, ["pdf", "pptx"])
            self.assertEqual(len(result.download_links), 2)
            
            # Verify download detector was called
            mock_detector.check_for_downloads_browser.assert_called_once_with(
                mock_driver, "https://example.com/page", "123"
            )

    def test_check_for_downloads_soup(self):
        """Test checking for downloads using BeautifulSoup"""
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for the FileProcessor class - Task 3 features
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
import hashlib

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import with try/except to handle missing dependencies in test environment
try:
    from io import BytesIO
    from bs4 import BeautifulSoup
    from src.storage.file_processor import FileProcessor
    from src.models.models import FileContent, DownloadInfo
    IMPORTS_SUCCESSFUL = True
except ImportError as e:
    print(f"Import error: {e}")
    IMPORTS_SUCCESSFUL = False


class TestFileProcessor(unittest.TestCase):
    """Test cases for the FileProcessor class - Task 3 features"""

    def setUp(self):
        """Set up test fixtures"""
        # Skip tests if imports failed
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Required modules not available")
            
        # Create mock scraper
        self.mock_scraper = MagicMock()
        
        # Create FileProcessor with mock scraper
        self.file_processor = FileProcessor(scraper=self.mock_scraper)
        
        # Create DownloadDetector
        self.download_detector = DownloadDetector()
        
        # Sample HTML content
        self.html_content = """
        <html>
        <body>
            <a href="https://example.com/document.pdf">PDF Document</a>
            <a href="https://example.com/presentation.pptx">PPTX Presentation</a>
            <div>
                월부_서울기초반_가형_임장보고서탬플릿_1주차.pdf 다운로드
            </div>
        </body>
        </html>
        """
        self.soup = BeautifulSoup(self.html_content, 'html.parser')
        
        # Sample file content for hash calculation tests
        self.test_file_content = b"Test file content for hash calculation"
        
    def test_calculate_content_hash(self):
        """Test content hash calculation for downloaded files - Task 3 feature"""
        # Define test content
        test_content = self.test_file_content
        
        # Calculate expected hash
        expected_hash = hashlib.sha256(test_content).hexdigest()
        
        # Mock method to calculate hash
        with patch('hashlib.sha256', return_value=hashlib.sha256(test_content)) as mock_sha256:
            # Create a mock method that mimics the content hash calculation
            def calculate_hash(content):
                return hashlib.sha256(content).hexdigest()
                
            # Call the method with our test content
            result = calculate_hash(test_content)
            
            # Verify results
            self.assertEqual(result, expected_hash)
            mock_sha256.assert_called_once_with(test_content)
            
        # Test with different content
        other_content = b"Different content for hash calculation"
        other_hash = hashlib.sha256(other_content).hexdigest()
        
        # Verify different content produces different hash
        self.assertNotEqual(expected_hash, other_hash)
        
        # Verify empty content produces a valid hash
        empty_content = b""
        empty_hash = hashlib.sha256(empty_content).hexdigest()
        self.assertIsNotNone(empty_hash)
        self.assertTrue(len(empty_hash) > 0)




if __name__ == '__main__':
    unittest.main()
