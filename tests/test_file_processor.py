#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unit tests for the FileProcessor class
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock, Mock
import tempfile
import hashlib

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.storage.file_processor import FileProcessor


class TestFileProcessor(unittest.TestCase):
    """Test cases for the FileProcessor class"""

    def setUp(self):
        """Set up test fixtures"""
        # Create a mock scraper
        self.scraper_mock = MagicMock()
        
        # Create FileProcessor with mock scraper
        self.file_processor = FileProcessor(scraper=self.scraper_mock)
        
        # Mock the document parsers
        self.file_processor.pdf_parser = MagicMock()
        self.file_processor.pptx_parser = MagicMock()
        self.file_processor.docx_parser = MagicMock()
        self.file_processor.image_parser = MagicMock()

    def test_parse_file(self):
        """Test file parsing with different file types"""
        # Mock the parse_file_bytes method
        self.file_processor.parse_file_bytes = MagicMock(return_value={"content": "parsed content"})
        
        # Mock the scraper get method
        mock_response = MagicMock()
        mock_response.content = b"file content"
        self.scraper_mock.get.return_value = mock_response
        
        # Test with PDF file
        url = "https://example.com/document.pdf"
        file_ext = "pdf"
        filename = "document.pdf"
        
        result = self.file_processor.parse_file(url, file_ext, filename)
        
        # Verify the result and that the mocks were called correctly
        self.assertEqual(result, {"content": "parsed content"})
        self.scraper_mock.get.assert_called_once_with(url, stream=True)
        self.file_processor.parse_file_bytes.assert_called_once_with(
            mock_response.content, file_ext, filename, url
        )
        
        # Reset mocks and test with PPTX file
        self.scraper_mock.get.reset_mock()
        self.file_processor.parse_file_bytes.reset_mock()
        
        url = "https://example.com/presentation.pptx"
        file_ext = "pptx"
        filename = "presentation.pptx"
        
        result = self.file_processor.parse_file(url, file_ext, filename)
        
        # Verify the result and that the mocks were called correctly
        self.assertEqual(result, {"content": "parsed content"})
        self.scraper_mock.get.assert_called_once_with(url, stream=True)
        self.file_processor.parse_file_bytes.assert_called_once_with(
            mock_response.content, file_ext, filename, url
        )
        
        # Test with error in request
        self.scraper_mock.get.reset_mock()
        self.scraper_mock.get.side_effect = Exception("Request failed")
        
        result = self.file_processor.parse_file(url, file_ext, filename)
        
        # Verify the result contains an error
        self.assertIn("error", result)
        self.assertEqual(result["error"], "Request failed")

    def test_parse_file_bytes(self):
        """Test parsing file bytes with different file types"""
        # Create sample file content
        file_content = b"file content"
        
        # Test with PDF file
        file_ext = "pdf"
        filename = "document.pdf"
        url = "https://example.com/document.pdf"
        
        # Mock the PDF parser
        mock_pdf_result = MagicMock()
        mock_pdf_result.text = "PDF content"
        mock_pdf_result.metadata = {"Author": "Test Author"}
        mock_pdf_result.file_type = "pdf"
        mock_pdf_result.raw_bytes = file_content
        self.file_processor.pdf_parser.parse.return_value = [mock_pdf_result]
        
        # Create a temporary file for testing
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name
        
        try:
            # Test parsing PDF
            with patch('tempfile.NamedTemporaryFile') as mock_temp_file:
                # Set up the mock to return our temporary file
                mock_temp_file_instance = MagicMock()
                mock_temp_file_instance.__enter__.return_value = temp_file
                mock_temp_file.return_value = mock_temp_file_instance
                
                result = self.file_processor.parse_file_bytes(file_content, file_ext, filename, url)
                
                # Verify the result and that the PDF parser was called
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0].text, "PDF content")
                self.assertEqual(result[0].metadata, {"Author": "Test Author"})
                self.assertEqual(result[0].file_type, "pdf")
                self.assertEqual(result[0].raw_bytes, file_content)
                self.file_processor.pdf_parser.parse.assert_called_once()
            
            # Reset mocks and test with PPTX file
            self.file_processor.pdf_parser.parse.reset_mock()
            
            file_ext = "pptx"
            filename = "presentation.pptx"
            url = "https://example.com/presentation.pptx"
            
            # Mock the PPTX parser
            mock_pptx_result = MagicMock()
            mock_pptx_result.text = "PPTX content"
            mock_pptx_result.metadata = {"Author": "Test Author"}
            mock_pptx_result.file_type = "pptx"
            mock_pptx_result.raw_bytes = file_content
            self.file_processor.pptx_parser.parse.return_value = [mock_pptx_result]
            
            # Test parsing PPTX
            with patch('tempfile.NamedTemporaryFile') as mock_temp_file:
                # Set up the mock to return our temporary file
                mock_temp_file_instance = MagicMock()
                mock_temp_file_instance.__enter__.return_value = temp_file
                mock_temp_file.return_value = mock_temp_file_instance
                
                result = self.file_processor.parse_file_bytes(file_content, file_ext, filename, url)
                
                # Verify the result and that the PPTX parser was called
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0].text, "PPTX content")
                self.assertEqual(result[0].metadata, {"Author": "Test Author"})
                self.assertEqual(result[0].file_type, "pptx")
                self.assertEqual(result[0].raw_bytes, file_content)
                self.file_processor.pptx_parser.parse.assert_called_once()
            
            # Test with unsupported file type
            file_ext = "xyz"
            filename = "unknown.xyz"
            url = "https://example.com/unknown.xyz"
            
            result = self.file_processor.parse_file_bytes(file_content, file_ext, filename, url)
            
            # Verify that an empty result is returned for unsupported file type
            self.assertEqual(len(result), 0)
            
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)


if __name__ == '__main__':
    unittest.main()
