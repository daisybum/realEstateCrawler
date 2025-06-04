#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parser module for Real Estate Crawler
"""

from src.parser.parser_pdf import PDFParser
from src.parser.parser_pptx import PPTXParser
from src.parser.parser_docx import DOCXParser
from src.parser.parser_document import DocumentParser
from src.parser.parser import ContentParser

__all__ = [
    'PDFParser', 'PPTXParser', 'DOCXParser', 'DocumentParser',
    'ContentParser'
]
