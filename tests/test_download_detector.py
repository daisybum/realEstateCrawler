#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for the DownloadDetector class - Task 3 features
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import with try/except to handle missing dependencies in test environment
try:
    from bs4 import BeautifulSoup
    import lxml.html
    from src.crawler.download_detector import DownloadDetector
    from src.models.models import DownloadInfo
    IMPORTS_SUCCESSFUL = True
except ImportError as e:
    print(f"Import error: {e}")
    IMPORTS_SUCCESSFUL = False


class TestDownloadDetector(unittest.TestCase):
    """Test cases for the DownloadDetector class - Task 3 features"""

    def setUp(self):
        """Set up test fixtures"""
        # Skip tests if imports failed
        if not IMPORTS_SUCCESSFUL:
            self.skipTest("Required modules not available")
            
        self.detector = DownloadDetector()
        
        # Sample HTML content
        self.html_content = """
        <html>
        <body>
            <a href="https://example.com/document.pdf">PDF Document</a>
            <a href="https://example.com/presentation.pptx">PPTX Presentation</a>
            <a href="https://example.com/document.docx">Word Document</a>
            <a href="https://example.com/spreadsheet.xlsx">Excel Spreadsheet</a>
            <a href="https://example.com/한글문서.hwp">한글 문서</a>
            <a href="https://example.com/image.jpg">Image</a>
            <a href="https://example.com/download" class="download">Download Link</a>
            <a href="https://example.com/file" class="file">File Link</a>
            <a href="https://example.com/attachment" download>Attachment</a>
            <a href="javascript:void(0)" onclick="download('file.pdf')">JS Download</a>
            <a href="https://example.com/다운로드">다운로드</a>
            <a href="https://example.com/첨부파일">첨부파일</a>
            <div>월부_서울기초반_가형_임장보고서탬플릿_1주차.pdf 다운로드</div>
        </body>
        </html>
        """
        
        # Create BeautifulSoup object if imports successful
        if IMPORTS_SUCCESSFUL:
            self.soup = BeautifulSoup(self.html_content, 'html.parser')

    def test_extract_file_extension(self):
        """Test extraction of file extensions from text"""
        # Test with direct extensions
        self.assertEqual(self.detector.extract_file_extension("document.pdf"), "pdf")
        self.assertEqual(self.detector.extract_file_extension("presentation.pptx"), "pptx")
        self.assertEqual(self.detector.extract_file_extension("document.docx"), "docx")
        self.assertEqual(self.detector.extract_file_extension("spreadsheet.xlsx"), "xlsx")
        self.assertEqual(self.detector.extract_file_extension("한글문서.hwp"), "hwp")
        
        # Test with keywords
        self.assertEqual(self.detector.extract_file_extension("PDF 문서"), "pdf")
        self.assertEqual(self.detector.extract_file_extension("파워포인트 프레젠테이션"), "pptx")
        self.assertEqual(self.detector.extract_file_extension("워드 문서"), "docx")
        self.assertEqual(self.detector.extract_file_extension("엑셀 스프레드시트"), "xlsx")
        self.assertEqual(self.detector.extract_file_extension("한글 문서"), "hwp")
        
        # Test with URLs
        self.assertEqual(
            self.detector.extract_file_extension("https://example.com/document.pdf"),
            "pdf"
        )
        self.assertEqual(
            self.detector.extract_file_extension("https://example.com/presentation.pptx?q=test"),
            "pptx"
        )
        
        # Test with unsupported extension
        self.assertEqual(self.detector.extract_file_extension("image.jpg"), "")
        self.assertEqual(self.detector.extract_file_extension("https://example.com/page"), "")

    def test_detect_downloads(self):
        """Test detection of downloadable files"""
        downloads = self.detector.detect_downloads(self.html_content)
        
        # Should find all downloadable files
        self.assertGreaterEqual(len(downloads), 10)
        
        # Verify PDF link was found
        pdf_link = next((d for d in downloads if d.get("url") == "https://example.com/document.pdf"), None)
        self.assertIsNotNone(pdf_link)
        self.assertEqual(pdf_link.get("text"), "PDF Document")
        
        # Verify PPTX link was found
        pptx_link = next((d for d in downloads if d.get("url") == "https://example.com/presentation.pptx"), None)
        self.assertIsNotNone(pptx_link)
        self.assertEqual(pptx_link.get("text"), "PPTX Presentation")
        
        # Verify HWP link was found
        hwp_link = next((d for d in downloads if d.get("url") == "https://example.com/한글문서.hwp"), None)
        self.assertIsNotNone(hwp_link)
        self.assertEqual(hwp_link.get("text"), "한글 문서")
        
        # Verify download class link was found
        download_link = next((d for d in downloads if d.get("url") == "https://example.com/download"), None)
        self.assertIsNotNone(download_link)
        
        # Verify Korean text links were found
        kr_link = next((d for d in downloads if d.get("url") == "https://example.com/다운로드"), None)
        self.assertIsNotNone(kr_link)
        self.assertEqual(kr_link.get("text"), "다운로드")

    def test_check_for_downloads_soup(self):
        """Test checking for downloads using BeautifulSoup"""
        downloads = self.detector.check_for_downloads_soup(self.soup)
        
        # Should find all downloadable files
        self.assertGreaterEqual(len(downloads), 10)
        
        # Verify PDF link was found
        pdf_link = next((d for d in downloads if d.get("url") == "https://example.com/document.pdf"), None)
        self.assertIsNotNone(pdf_link)
        self.assertEqual(pdf_link.get("text"), "PDF Document")

    @patch('src.crawler.download_detector.webdriver')
    def test_check_for_downloads_browser(self, mock_webdriver):
        """Test checking for downloads using browser"""
        # Setup mock driver
        mock_driver = MagicMock()
        mock_driver.page_source = self.html_content
        
        # Setup mock elements
        mock_pdf_link = MagicMock()
        mock_pdf_link.get_attribute.side_effect = lambda attr: {
            "href": "https://example.com/document.pdf",
            "text": "PDF Document"
        }.get(attr, "")
        mock_pdf_link.text = "PDF Document"
        
        mock_download_button = MagicMock()
        mock_download_button.text = "다운로드"
        
        # Setup mock find_elements
        mock_driver.find_elements.side_effect = lambda by, selector: {
            "//span[contains(text(), '다운로드')]": [mock_download_button],
            "//a[contains(text(), '다운로드')]": [mock_download_button],
            "//button[contains(text(), '다운로드')]": [],
            "//div[contains(text(), '다운로드')]": [],
            "//a[contains(@href, '.pptx') or contains(@href, '.pdf') or contains(@href, '.docx') or contains(@href, '.hwp') or contains(@href, '.doc') or contains(@href, '.xlsx') or contains(text(), 'PDF') or contains(text(), 'pdf') or contains(text(), 'ppt') or contains(text(), 'PPT') or contains(text(), 'doc') or contains(text(), 'DOC') or contains(text(), 'hwp') or contains(text(), 'HWP') or contains(@download, 'pdf') or contains(@title, 'pdf')]": [mock_pdf_link],
            "//a[contains(text(), '다운로드') or contains(text(), 'download')]": [mock_download_button]
        }.get(selector, [])
        
        # Call the method
        result = self.detector.check_for_downloads_browser(mock_driver, "https://example.com/page", "123")
        
        # Verify results
        self.assertTrue(result.has_download)
        self.assertIn("pdf", result.file_formats)
        self.assertGreaterEqual(len(result.download_links), 1)


if __name__ == '__main__':
    unittest.main()
