#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for PPTX parser with in-memory processing
"""

import io
import json
import logging
from pathlib import Path

from src.parser.parser_pptx import PPTXParser
from src.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_pptx_parser():
    """Test PPTX parser with example file"""
    # Path to example PPTX file
    example_path = Path("data/example.pptx")
    
    if not example_path.exists():
        logger.error(f"Example file not found: {example_path}")
        return
    
    logger.info(f"Testing PPTX parser with file: {example_path}")
    
    # Initialize parser
    parser = PPTXParser()
    
    # Test 1: Parse from file path
    logger.info("Test 1: Parsing from file path")
    result_from_path = parser.extract_content(str(example_path))
    
    # Test 2: Parse from bytes (in-memory)
    logger.info("Test 2: Parsing from bytes (in-memory)")
    with open(example_path, 'rb') as f:
        file_bytes = io.BytesIO(f.read())
    
    result_from_bytes = parser.extract_content(file_bytes)
    
    # Compare results
    logger.info("Comparing results from both methods")
    path_content_len = len(result_from_path.get("content", ""))
    bytes_content_len = len(result_from_bytes.get("content", ""))
    
    logger.info(f"Content length from path: {path_content_len}")
    logger.info(f"Content length from bytes: {bytes_content_len}")
    
    # Save results to file for inspection
    output_path = Path("test_pptx_parser_results.json")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({
            "from_path": result_from_path,
            "from_bytes": result_from_bytes
        }, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Results saved to: {output_path}")
    
    # Print summary of extracted content
    print("\n=== PPTX Parsing Results ===")
    print(f"Metadata: {json.dumps(result_from_path.get('metadata', {}), indent=2)}")
    print(f"Number of slides: {len(result_from_path.get('slides', []))}")
    print(f"Number of elements: {len(result_from_path.get('elements', []))}")
    print(f"Content length: {path_content_len} characters")
    
    # Print first part of content
    content = result_from_path.get("content", "")
    print("\nContent preview:")
    print(content[:500] + "..." if len(content) > 500 else content)

if __name__ == "__main__":
    test_pptx_parser()
