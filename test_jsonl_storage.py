#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for JSONL storage functionality
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime

from src.storage.storage import JsonlStorage
from src.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

def main():
    # Create test output directory
    test_output_dir = Path("test_output")
    test_output_dir.mkdir(exist_ok=True)
    test_jsonl_file = test_output_dir / "test_storage.jsonl"
    
    # Remove existing test file if it exists
    if test_jsonl_file.exists():
        os.remove(test_jsonl_file)
        logger.info(f"Removed existing test file: {test_jsonl_file}")
    
    # Create test records
    test_records = [
        {
            "post_id": "12345",
            "src": "https://example.com/post/12345",
            "title": "Test Property Listing",
            "author": "Test User",
            "date": "2025-06-04",
            "content": "This is a test property listing with some content.\nMultiple lines of text.\nWith Unicode characters: 한글 테스트",
            "has_download": True,
            "file_formats": ["pdf", "pptx"],
            "download_links": [
                {"url": "https://example.com/files/document1.pdf", "filename": "document1.pdf"},
                {"url": "https://example.com/files/presentation.pptx", "filename": "presentation.pptx"}
            ],
            "type": "text_content"
        },
        {
            "post_id": "67890",
            "src": "https://example.com/post/67890",
            "title": "Another Property",
            "author": "Another User",
            "date": "2025-06-03",
            "content": "This is another test property listing.",
            "has_download": False,
            "file_formats": [],
            "download_links": [],
            "type": "text_content"
        },
        {
            "post_id": "54321",
            "src": "https://example.com/post/54321",
            "title": "Property with Parsed Content",
            "author": "PDF User",
            "date": "2025-06-02",
            "content": "Original content",
            "parsed_content": "This is parsed content from a PDF file.\nWith table data and extracted text.",
            "has_download": True,
            "file_formats": ["pdf"],
            "download_links": [
                {"url": "https://example.com/files/property_document.pdf", "filename": "property_document.pdf"}
            ],
            "type": "pdf_extract"
        }
    ]
    
    # Create storage with test file
    storage = JsonlStorage(filename=test_jsonl_file)
    
    # Save records
    logger.info(f"Saving {len(test_records)} test records to {test_jsonl_file}")
    storage.save_posts(test_records)
    
    # Verify export
    if test_jsonl_file.exists():
        with open(test_jsonl_file, "r", encoding="utf-8") as f:
            saved_records = [json.loads(line) for line in f]
        
        logger.info(f"Successfully saved {len(saved_records)} records")
        
        # Display saved records
        logger.info("Saved record structure:")
        for i, record in enumerate(saved_records):
            logger.info(f"Record {i+1}:")
            logger.info(f"  URL: {record.get('url', '')}")
            logger.info(f"  Meta: {record.get('meta', {})}")
            logger.info(f"  Post ID: {record.get('post_id', '')}")
            logger.info(f"  Body length: {len(record.get('body', ''))} characters")
            logger.info(f"  Parsed content length: {len(record.get('parsed_content', ''))} characters")
            logger.info(f"  File sources: {record.get('file_sources', [])}")
            logger.info(f"  Crawl timestamp: {record.get('crawl_timestamp', '')}")
            logger.info("")
        
        # Test duplicate handling by saving the same records again
        logger.info("Testing duplicate handling by saving the same records again...")
        storage.save_posts(test_records)
        
        # Verify no duplicates were added
        with open(test_jsonl_file, "r", encoding="utf-8") as f:
            new_saved_records = [json.loads(line) for line in f]
        
        if len(new_saved_records) == len(saved_records):
            logger.info("✅ Duplicate handling works correctly - no duplicates were added")
        else:
            logger.error(f"❌ Duplicate handling failed - expected {len(saved_records)} records but got {len(new_saved_records)}")
    else:
        logger.error(f"❌ Save failed - file {test_jsonl_file} does not exist")

if __name__ == "__main__":
    main()
