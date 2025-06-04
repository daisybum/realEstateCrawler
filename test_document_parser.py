#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for DocumentParser with PPTX files
"""

import logging
from pathlib import Path

from src.parser.parser_document import DocumentParser

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_document_parser():
    """Test DocumentParser with example PPTX file"""
    # Path to example PPTX file
    example_path = Path("data/example.pptx")
    
    if not example_path.exists():
        logger.error(f"Example file not found: {example_path}")
        return
    
    logger.info(f"Testing DocumentParser with file: {example_path}")
    
    # Initialize parser
    parser = DocumentParser()
    
    # Parse the PPTX file
    result = parser.parse_document(str(example_path))
    
    # Print summary of extracted content
    print("\n=== Document Parser Results ===")
    print(f"File type: {result.get('file_type', 'unknown')}")
    
    content = result.get("content", "")
    print(f"Content length: {len(content)} characters")
    
    # Print first part of content
    print("\nContent preview:")
    print(content[:500] + "..." if len(content) > 500 else content)

if __name__ == "__main__":
    test_document_parser()
