#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for PDF Parser
Tests both file path and in-memory bytes input
"""

import os
import json
import time
from io import BytesIO

from src.parser.parser_pdf import PDFParser

def main():
    # Path to example PDF file
    example_pdf_path = "data/example.pdf"
    
    # Check if the file exists
    if not os.path.exists(example_pdf_path):
        print(f"Example PDF file not found at {example_pdf_path}")
        print("Please provide a valid PDF file path")
        return
    
    # Initialize PDF parser
    pdf_parser = PDFParser()
    
    # Test with file path
    print(f"Testing PDF parser with file path: {example_pdf_path}")
    start_time = time.time()
    result = pdf_parser.extract_content(example_pdf_path)
    elapsed_time = time.time() - start_time
    
    # Print summary of extracted content
    print("\n=== PDF Parser Results (File Path) ===")
    print(f"Processing time: {elapsed_time:.2f} seconds")
    print(f"Content length: {len(result.get('content', ''))} characters")
    print(f"Pages: {len(result.get('pages', []))}")
    print(f"Tables: {len(result.get('tables', []))}")
    print(f"Images: {len(result.get('images', []))}")
    
    # Print first part of content
    content = result.get("content", "")
    print("\nContent preview:")
    print(content[:500] + "..." if len(content) > 500 else content)
    
    # Save results to JSON file
    with open("test_pdf_parser_results.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print("\nResults saved to test_pdf_parser_results.json")
    
    # Test with in-memory bytes
    print("\nTesting PDF parser with in-memory bytes")
    with open(example_pdf_path, "rb") as f:
        pdf_bytes = f.read()
    
    start_time = time.time()
    bytes_result = pdf_parser.extract_content(pdf_bytes)
    elapsed_time = time.time() - start_time
    
    # Print summary of extracted content
    print("\n=== PDF Parser Results (In-Memory Bytes) ===")
    print(f"Processing time: {elapsed_time:.2f} seconds")
    print(f"Content length: {len(bytes_result.get('content', ''))} characters")
    print(f"Pages: {len(bytes_result.get('pages', []))}")
    print(f"Tables: {len(bytes_result.get('tables', []))}")
    print(f"Images: {len(bytes_result.get('images', []))}")
    
    # Compare results
    if bytes_result.get("content") == result.get("content"):
        print("\n✅ In-memory parsing produces identical content to file path parsing")
    else:
        print("\n❌ In-memory parsing produces different content from file path parsing")
    
    # Test with BytesIO
    print("\nTesting PDF parser with BytesIO")
    bytes_io = BytesIO(pdf_bytes)
    
    start_time = time.time()
    bytesio_result = pdf_parser.extract_content(bytes_io)
    elapsed_time = time.time() - start_time
    
    # Print summary of extracted content
    print("\n=== PDF Parser Results (BytesIO) ===")
    print(f"Processing time: {elapsed_time:.2f} seconds")
    print(f"Content length: {len(bytesio_result.get('content', ''))} characters")
    
    # Compare results
    if bytesio_result.get("content") == result.get("content"):
        print("\n✅ BytesIO parsing produces identical content to file path parsing")
    else:
        print("\n❌ BytesIO parsing produces different content from file path parsing")

if __name__ == "__main__":
    main()
