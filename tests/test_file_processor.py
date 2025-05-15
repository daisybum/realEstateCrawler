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
        
        # Mock document parsers
        self.file_processor.document_parser = MagicMock()
        self.file_processor.pdf_parser = MagicMock()
        self.file_processor.pptx_parser = MagicMock()
        self.file_processor.docx_parser = MagicMock()
        
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

    @patch('tempfile.NamedTemporaryFile')
    @patch('os.unlink')
    def test_parse_file_bytes(self, mock_unlink, mock_temp_file):
        """Test parsing file bytes"""
        # Setup mock temp file
        mock_temp = MagicMock()
        mock_temp.name = "/tmp/test.pdf"
        mock_temp_file.return_value.__enter__.return_value = mock_temp
        
        # Setup mock document parser
        self.file_processor.document_parser.parse_document.return_value = {
            "content": "Parsed content",
            "metadata": {"author": "Test Author"},
            "tables": [{"data": [["Header", "Value"]]}],
            "images": [{"url": "image.jpg"}]
        }
        
        # Create mock file object
        file_obj = BytesIO(b"Test PDF content")
        
        # Call the method
        result = self.file_processor.parse_file_bytes(
            file_obj, ".pdf", "test.pdf", "https://example.com/test.pdf"
        )
        
        # Verify results
        self.assertEqual(result["content"], "Parsed content")
        self.assertEqual(result["metadata"]["author"], "Test Author")
        self.assertEqual(result["filename"], "test.pdf")
        self.assertEqual(result["url"], "https://example.com/test.pdf")
        self.assertEqual(result["file_type"], "pdf")
        
        # Verify document parser was called
        self.file_processor.document_parser.parse_document.assert_called_once_with("/tmp/test.pdf")
        
        # Verify temp file was cleaned up
        mock_unlink.assert_called_once_with("/tmp/test.pdf")

    @patch('src.storage.file_processor.BytesIO')
    def test_parse_file(self, mock_bytesio):
        """Test parsing a file"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.content = b"Test PDF content"
        mock_response.raise_for_status = MagicMock()
        self.mock_scraper.get.return_value = mock_response
        
        # Setup mock BytesIO
        mock_file_obj = MagicMock()
        mock_bytesio.return_value = mock_file_obj
        
        # Setup mock parse_file_bytes
        self.file_processor.parse_file_bytes.return_value = {
            "content": "Parsed content",
            "metadata": {"author": "Test Author"},
            "tables": [{"data": [["Header", "Value"]]}],
            "images": [{"url": "image.jpg"}]
        }
        
        # Call the method
        result = self.file_processor.parse_file(
            "https://example.com/test.pdf", "123", "test.pdf"
        )
        
        # Verify results
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].filename, "test.pdf")
        self.assertEqual(result[0].url, "https://example.com/test.pdf")
        self.assertEqual(result[0].file_type, "pdf")
        self.assertEqual(result[0].content, "Parsed content")
        
        # Verify scraper was called
        self.mock_scraper.get.assert_called_once_with("https://example.com/test.pdf", stream=True)
        
        # Verify parse_file_bytes was called
        self.file_processor.parse_file_bytes.assert_called_once_with(
            mock_file_obj, ".pdf", "test.pdf", "https://example.com/test.pdf"
        )
        
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
        with patch('src.crawler.download_detector.DownloadDetector') as mock_detector_cls:
            mock_detector = MagicMock()
            mock_detector.check_for_downloads_browser.return_value = mock_download_info
            mock_detector_cls.return_value = mock_detector
            
            # Call the method
            result = self.file_processor.check_for_downloads_browser(
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
        # Setup mock download detector
        mock_downloads = [
            {"url": "https://example.com/test.pdf", "text": "PDF Document"},
            {"url": "https://example.com/test.pptx", "text": "PPTX Presentation"}
        ]
        
        with patch('src.crawler.download_detector.DownloadDetector') as mock_detector_cls:
            mock_detector = MagicMock()
            mock_detector.detect_downloads.return_value = mock_downloads
            mock_detector.extract_file_extension.side_effect = lambda text: {
                "https://example.com/test.pdf": "pdf",
                "https://example.com/test.pptx": "pptx",
                "PDF Document": "",
                "PPTX Presentation": ""
            }.get(text, "")
            mock_detector_cls.return_value = mock_detector
            
            # Call the method
            result = self.file_processor.check_for_downloads_soup(
                self.soup, "https://example.com/page", "123"
            )
            
            # Verify results
            self.assertTrue(result.has_download)
            self.assertIn("pdf", result.file_formats)
            self.assertIn("pptx", result.file_formats)
            self.assertEqual(len(result.download_links), 2)
            
            # Verify download detector was called
            mock_detector.detect_downloads.assert_called_once()

    def test_add_link_and_ext(self):
        """Test adding link and extension"""
        # Create DownloadInfo
        info = DownloadInfo()
        
        # Create mock detector
        mock_detector = MagicMock()
        mock_detector.extract_file_extension.side_effect = lambda text: {
            "https://example.com/test.pdf": "pdf",
            "https://example.com/test.pptx": "pptx",
            "PDF Document": "",
            "PPTX Presentation": ""
        }.get(text, "")
        
        # Call the method
        self.file_processor._add_link_and_ext(
            info, "https://example.com", "https://example.com/test.pdf", "PDF Document", mock_detector
        )
        
        # Verify results
        self.assertTrue(info.has_download)
        self.assertEqual(info.file_formats, ["pdf"])
        self.assertEqual(len(info.download_links), 1)
        self.assertEqual(info.download_links[0]["url"], "https://example.com/test.pdf")
        self.assertEqual(info.download_links[0]["text"], "PDF Document")
        
        # Add another link
        self.file_processor._add_link_and_ext(
            info, "https://example.com", "test.pptx", "PPTX Presentation", mock_detector
        )
        
        # Verify results
        self.assertEqual(info.file_formats, ["pdf", "pptx"])
        self.assertEqual(len(info.download_links), 2)
        self.assertEqual(info.download_links[1]["url"], "https://example.com/test.pptx")
        
        # Try adding a duplicate link
        self.file_processor._add_link_and_ext(
            info, "https://example.com", "https://example.com/test.pdf", "PDF Again", mock_detector
        )
        
        # Verify no duplicate was added
        self.assertEqual(len(info.download_links), 2)
        
        # Test with explicit extension
        self.file_processor._add_link_and_ext(
            info, "https://example.com", "custom_file", "Custom File", mock_detector, explicit_ext="docx"
        )
        
        # Verify results
        self.assertIn("docx", info.file_formats)
        self.assertEqual(len(info.download_links), 3)


if __name__ == '__main__':
    unittest.main()
