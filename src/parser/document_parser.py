#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Document Parser for Real Estate Crawler
Handles parsing of different document types (PDF, PPTX, DOCX)
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.config import Config
from src.parser.pdf_parser import PDFParser
from src.parser.pptx_parser import PPTXParser
from src.parser.docx_parser import DOCXParser


class DocumentParser:
    """Parser for various document types"""
    
    def __init__(self, config=None):
        """Initialize the document parser"""
        self.config = config or Config.get_instance()
        self.logger = logging.getLogger(__name__)
        
        # Initialize parsers
        self.pdf_parser = PDFParser(self.config)
        self.pptx_parser = PPTXParser(self.config)
        self.docx_parser = DOCXParser(self.config)
    
    def parse_document(self, file_path: str) -> Dict[str, Any]:
        """Parse a document based on its file extension
        
        Args:
            file_path: Path to the document file
            
        Returns:
            Dictionary containing extracted content
        """
        if not os.path.exists(file_path):
            self.logger.error(f"File not found: {file_path}")
            return {"error": f"File not found: {file_path}"}
        
        # Get file extension
        file_ext = Path(file_path).suffix.lower()
        
        # Parse based on file type
        try:
            if file_ext == ".pdf":
                self.logger.info(f"Parsing PDF file: {file_path}")
                return self.pdf_parser.extract_content(file_path)
            
            elif file_ext in [".pptx", ".ppt"]:
                self.logger.info(f"Parsing PPTX file: {file_path}")
                return self.pptx_parser.extract_content(file_path)
            
            elif file_ext in [".docx", ".doc"]:
                self.logger.info(f"Parsing DOCX file: {file_path}")
                return self.docx_parser.extract_content(file_path)
            
            else:
                self.logger.error(f"Unsupported file type: {file_ext}")
                return {"error": f"Unsupported file type: {file_ext}"}
        
        except Exception as e:
            self.logger.error(f"Error parsing document: {e}", exc_info=True)
            return {"error": f"Error parsing document: {str(e)}"}
    
    def is_supported_file_type(self, file_path: str) -> bool:
        """Check if the file type is supported
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if the file type is supported, False otherwise
        """
        file_ext = Path(file_path).suffix.lower()
        return file_ext in self.config.supported_file_types
    
    def get_parser_for_file_type(self, file_path: str):
        """Get the appropriate parser for a file type
        
        Args:
            file_path: Path to the file
            
        Returns:
            Parser instance or None if not supported
        """
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == ".pdf":
            return self.pdf_parser
        elif file_ext in [".pptx", ".ppt"]:
            return self.pptx_parser
        elif file_ext in [".docx", ".doc"]:
            return self.docx_parser
        else:
            return None
