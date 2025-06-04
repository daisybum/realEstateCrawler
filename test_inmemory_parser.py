#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for in-memory PPTX parsing
Tests both direct PPTXParser and DocumentParser with in-memory bytes
"""

import logging
from pathlib import Path
import io

from src.parser.parser_pptx import PPTXParser
from src.parser.parser_document import DocumentParser

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_inmemory_parsing():
    """Test in-memory PPTX parsing with both parsers"""
    # Path to example PPTX file
    example_path = Path("data/example.pptx")
    
    if not example_path.exists():
        logger.error(f"Example file not found: {example_path}")
        return
    
    logger.info(f"Testing in-memory parsing with file: {example_path}")
    
    # Read file into memory
    with open(example_path, "rb") as f:
        file_bytes = f.read()
    
    # Create BytesIO object
    file_obj = io.BytesIO(file_bytes)
    
    # Test PPTXParser directly
    logger.info("Testing PPTXParser with in-memory bytes...")
    pptx_parser = PPTXParser()
    pptx_result = pptx_parser.extract_content(file_bytes)
    
    # Test DocumentParser with BytesIO
    logger.info("Testing DocumentParser with BytesIO...")
    doc_parser = DocumentParser()
    
    # We need to add a method to DocumentParser to handle BytesIO objects
    # For now, we'll just test the PPTXParser directly
    
    # Print summary of extracted content from PPTXParser
    print("\n=== In-Memory PPTX Parser Results ===")
    
    content = pptx_result.get("content", "")
    print(f"Content length: {len(content)} characters")
    
    # Print first part of content
    print("\nContent preview:")
    print(content[:500] + "..." if len(content) > 500 else content)

if __name__ == "__main__":
    test_inmemory_parsing()
