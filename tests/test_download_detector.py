#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for the DownloadDetector class
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.crawler.download_detector import DownloadDetector


class TestDownloadDetector(unittest.TestCase):
    """Test cases for the DownloadDetector class"""

    def setUp(self):
        """Set up test fixtures"""
        self.detector = DownloadDetector()

    def test_extract_file_extension(self):
        """Test file extension extraction from text"""
        # Test with PDF keyword
        text = "This is a PDF document"
        ext = self.detector.extract_file_extension(text)
        self.assertEqual(ext, "pdf")
        
        # Test with PPTX keyword
        text = "This is a PowerPoint presentation (pptx)"
        ext = self.detector.extract_file_extension(text)
        self.assertEqual(ext, "pptx")
        
        # Test with DOCX keyword
        text = "This is a Word document (docx)"
        ext = self.detector.extract_file_extension(text)
        self.assertEqual(ext, "docx")
        
        # Test with HWP keyword
        text = "This is a Hangul document (hwp)"
        ext = self.detector.extract_file_extension(text)
        self.assertEqual(ext, "hwp")
        
        # Test with Excel keyword
        text = "This is an Excel spreadsheet"
        ext = self.detector.extract_file_extension(text)
        self.assertEqual(ext, "xlsx")
        
        # Test with no recognized keyword
        text = "This is a plain text document"
        ext = self.detector.extract_file_extension(text)
        self.assertEqual(ext, "")
        
        # Test with multiple keywords (should return the first match)
        text = "This is both a PDF and a DOCX"
        ext = self.detector.extract_file_extension(text)
        self.assertEqual(ext, "pdf")

    def test_check_for_downloads_soup(self):
        """Test download detection from BeautifulSoup object"""
        # Create a sample HTML with download links
        html = """
        <html>
            <body>
                <div class="content">
                    <a href="https://example.com/document.pdf" class="download">Download PDF</a>
                    <a href="https://example.com/presentation.pptx">Download Presentation</a>
                    <a href="https://example.com/spreadsheet.xlsx">Download Spreadsheet</a>
                </div>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Test detection
        downloads = self.detector.check_for_downloads_soup(soup)
        
        # Verify the detected downloads
        self.assertEqual(len(downloads), 3)
        
        # Check the first download
        self.assertEqual(downloads[0]['url'], "https://example.com/document.pdf")
        self.assertEqual(downloads[0]['text'], "Download PDF")
        
        # Check the second download
        self.assertEqual(downloads[1]['url'], "https://example.com/presentation.pptx")
        self.assertEqual(downloads[1]['text'], "Download Presentation")
        
        # Check the third download
        self.assertEqual(downloads[2]['url'], "https://example.com/spreadsheet.xlsx")
        self.assertEqual(downloads[2]['text'], "Download Spreadsheet")
        
        # Test with no download links
        html_no_downloads = "<html><body><div>No downloads here</div></body></html>"
        soup_no_downloads = BeautifulSoup(html_no_downloads, 'html.parser')
        
        downloads = self.detector.check_for_downloads_soup(soup_no_downloads)
        self.assertEqual(downloads, [])

    def test_detect_downloads(self):
        """Test download detection from HTML string"""
        # Create a sample HTML with download links
        html = """
        <html>
            <body>
                <div class="content">
                    <a href="https://example.com/document.pdf" class="download">Download PDF</a>
                    <a href="https://example.com/presentation.pptx">Download Presentation</a>
                </div>
            </body>
        </html>
        """
        
        # Test detection
        downloads = self.detector.detect_downloads(html)
        
        # Verify the detected downloads
        self.assertEqual(len(downloads), 2)
        
        # Check the first download
        self.assertEqual(downloads[0]['url'], "https://example.com/document.pdf")
        self.assertEqual(downloads[0]['text'], "Download PDF")
        
        # Check the second download
        self.assertEqual(downloads[1]['url'], "https://example.com/presentation.pptx")
        self.assertEqual(downloads[1]['text'], "Download Presentation")
        
        # Test with no download links
        html_no_downloads = "<html><body><div>No downloads here</div></body></html>"
        
        downloads = self.detector.detect_downloads(html_no_downloads)
        self.assertEqual(downloads, [])

    def test_detect_downloads_with_xpath(self):
        """Test download detection using XPath patterns"""
        # Create a sample HTML with download links matching XPath patterns
        html = """
        <html>
            <body>
                <div class="content">
                    <a href="https://example.com/document.pdf" data-download="true">Download PDF</a>
                    <div class="download-container">
                        <a href="https://example.com/presentation.pptx">Download Presentation</a>
                    </div>
                </div>
            </body>
        </html>
        """
        
        # Set custom XPath patterns
        self.detector.xpath_patterns = [
            "//a[@data-download='true']",
            "//div[@class='download-container']/a"
        ]
        
        # Test detection
        downloads = self.detector.detect_downloads(html)
        
        # Verify the detected downloads
        self.assertEqual(len(downloads), 2)
        
        # Check the first download
        self.assertEqual(downloads[0]['url'], "https://example.com/document.pdf")
        self.assertEqual(downloads[0]['text'], "Download PDF")
        
        # Check the second download
        self.assertEqual(downloads[1]['url'], "https://example.com/presentation.pptx")
        self.assertEqual(downloads[1]['text'], "Download Presentation")


if __name__ == '__main__':
    unittest.main()
