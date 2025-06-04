#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF Parser for Real Estate Crawler
Handles extraction of text, tables, and images from PDF files using Docling and PaddleOCR
Optimized for in-memory processing without saving to disk
"""

import os
import io
import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, BinaryIO, Union

import fitz  # PyMuPDF
import camelot
import cv2
import numpy as np
from PIL import Image
from paddleocr import PaddleOCR

from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import PdfFormatOption

from src.config import Config


class PDFParser:
    """Parser for PDF files using Docling and PaddleOCR, optimized for in-memory processing"""
    
    def __init__(self, config=None):
        """Initialize the PDF parser"""
        self.config = config or Config.get_instance()
        self.logger = logging.getLogger(__name__)
        
        # Initialize PaddleOCR
        self.ocr = PaddleOCR(use_angle_cls=True, lang='korean', show_log=False)
        
        # Initialize Docling DocumentConverter with custom options
        pipeline_options = PdfPipelineOptions(
            do_ocr=True,
            extract_text_from_pdf=True,
            extract_images=True,
            do_table_detection=True,
            force_ocr=False
        )
        
        self.doc_converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
            }
        )
    
    def extract_content(self, file_path_or_bytes: Union[str, bytes, BinaryIO]) -> Dict[str, Any]:
        """
        Extract content from a PDF file or bytes
        
        Args:
            file_path_or_bytes: Path to the PDF file or bytes/BytesIO object
            
        Returns:
            Dictionary with parsed content
        """
        try:
            # Handle different input types
            if isinstance(file_path_or_bytes, str):
                self.logger.info(f"Parsing PDF file: {file_path_or_bytes}")
                return self._parse_from_path(file_path_or_bytes)
            elif isinstance(file_path_or_bytes, (bytes, io.BytesIO)):
                self.logger.info("Parsing PDF from bytes/BytesIO")
                return self._parse_from_bytes(file_path_or_bytes)
            else:
                raise ValueError(f"Unsupported input type: {type(file_path_or_bytes)}")
            
        except Exception as e:
            self.logger.error(f"Error parsing PDF: {e}")
            return {"error": str(e), "content": f"PDF 파일 처리 오류: {e}"}
    
    def _parse_from_path(self, file_path: str) -> Dict[str, Any]:
        """Parse PDF from file path"""
        result = {
            "content": "",
            "metadata": {},
            "tables": [],
            "images": [],
            "pages": []
        }
        
        # Use PyMuPDF (fitz) for direct text and metadata extraction
        doc = fitz.open(file_path)
        
        # Extract metadata
        result["metadata"] = self._extract_metadata(doc)
        
        # Extract text content and process pages
        self._process_pages(doc, result)
        
        # Extract tables using Camelot
        self._extract_tables_with_camelot(file_path, result)
        
        doc.close()
        return result
    
    def _parse_from_bytes(self, bytes_data: Union[bytes, BinaryIO]) -> Dict[str, Any]:
        """
        Parse PDF directly from bytes or BytesIO without saving to disk
        
        Args:
            bytes_data: PDF data as bytes or BytesIO
            
        Returns:
            Dictionary with parsed content
        """
        result = {
            "content": "",
            "metadata": {},
            "tables": [],
            "images": [],
            "pages": []
        }
        
        # Convert to BytesIO if needed
        if isinstance(bytes_data, bytes):
            bytes_io = io.BytesIO(bytes_data)
        else:
            bytes_io = bytes_data
            bytes_io.seek(0)  # Ensure we're at the start of the stream
        
        # Use PyMuPDF (fitz) for direct in-memory processing
        doc = fitz.open(stream=bytes_io.read(), filetype="pdf")
        bytes_io.seek(0)  # Reset for potential reuse
        
        # Extract metadata
        result["metadata"] = self._extract_metadata(doc)
        
        # Extract text content and process pages
        self._process_pages(doc, result)
        
        # For tables, we need a temporary file since Camelot doesn't support BytesIO directly
        # But we'll only create this temp file if tables are needed
        if not result["tables"]:
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=True) as temp_file:
                bytes_io.seek(0)
                temp_file.write(bytes_io.read())
                temp_file.flush()
                
                # Extract tables using Camelot
                self._extract_tables_with_camelot(temp_file.name, result)
        
        doc.close()
        return result
    
    def _extract_metadata(self, doc: fitz.Document) -> Dict[str, Any]:
        """Extract metadata from a PyMuPDF document"""
        metadata = doc.metadata
        if metadata:
            return {
                'title': metadata.get('title', ''),
                'author': metadata.get('author', ''),
                'subject': metadata.get('subject', ''),
                'creator': metadata.get('creator', ''),
                'producer': metadata.get('producer', ''),
                'page_count': len(doc)
            }
        return {}
    
    def _process_pages(self, doc: fitz.Document, result: Dict[str, Any]) -> None:
        """Process pages from a PyMuPDF document"""
        all_text = []
        
        for page_idx in range(len(doc)):
            page = doc[page_idx]
            page_text = page.get_text()
            all_text.append(page_text)
            
            page_content = {
                "page_num": page_idx + 1,
                "text": page_text,
                "images": [],
                "tables": []
            }
            
            # Extract images from page
            image_list = page.get_images(full=True)
            for img_idx, img in enumerate(image_list):
                try:
                    xref = img[0]
                    base_image = doc.extract_image(xref)
                    image_bytes = base_image["image"]
                    
                    # Run OCR on image
                    ocr_text = self._run_paddle_ocr(image_bytes)
                    
                    image_data = {
                        "index": img_idx,
                        "width": base_image.get("width", 0),
                        "height": base_image.get("height", 0),
                        "ocr_text": ocr_text
                    }
                    
                    page_content["images"].append(image_data)
                    result["images"].append({
                        "page": page_idx + 1,
                        **image_data
                    })
                except Exception as e:
                    self.logger.warning(f"Error processing image {img_idx} on page {page_idx+1}: {e}")
            
            result["pages"].append(page_content)
        
        result["content"] = "\n\n".join(all_text)
    
    def _run_paddle_ocr(self, image_data: bytes) -> str:
        """
        Run PaddleOCR on image data
        
        Args:
            image_data: Image data as bytes
            
        Returns:
            Extracted text from the image
        """
        try:
            # Convert bytes to numpy array for OpenCV
            nparr = np.frombuffer(image_data, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if img is None or img.size == 0:
                return ""
            
            # Check if image is too small for meaningful OCR
            if img.shape[0] < 50 or img.shape[1] < 50:
                return ""  # Skip tiny images like icons
            
            # Preprocess image for better OCR results
            preprocessed_img = self._preprocess_image(img)
            
            # Run PaddleOCR
            ocr_result = self.ocr.ocr(preprocessed_img, cls=True)
            
            if not ocr_result or not ocr_result[0]:
                return ""
            
            # Extract text from OCR result
            text_results = []
            for line in ocr_result[0]:
                if len(line) >= 2 and line[1] and len(line[1]) >= 2:
                    text, confidence = line[1]
                    if confidence > 0.5:  # Only include confident results
                        text_results.append(text)
            
            return " ".join(text_results)
            
        except Exception as e:
            self.logger.error(f"Error running PaddleOCR: {e}")
            return ""
    
    def _preprocess_image(self, img: np.ndarray) -> np.ndarray:
        """
        Preprocess image for better OCR results
        
        Args:
            img: OpenCV image
            
        Returns:
            Preprocessed image
        """
        try:
            # Convert to grayscale
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Apply CLAHE (Contrast Limited Adaptive Histogram Equalization)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
            gray = clahe.apply(gray)
            
            # Apply adaptive thresholding
            thresh = cv2.adaptiveThreshold(
                gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                cv2.THRESH_BINARY, 11, 2
            )
            
            # Denoise
            denoised = cv2.fastNlMeansDenoising(thresh, None, 10, 7, 21)
            
            return denoised
            
        except Exception as e:
            self.logger.warning(f"Image preprocessing error: {e}")
            return img
    
    def _extract_tables_with_camelot(self, file_path: str, result: Dict[str, Any]) -> None:
        """
        Extract tables using Camelot
        
        Args:
            file_path: Path to the PDF file
            result: Result dictionary to update
        """
        try:
            # Extract tables using Camelot
            tables = camelot.read_pdf(file_path, pages='all', flavor='stream')
            
            if tables.n > 0:
                for i, table in enumerate(tables):
                    if table.df.empty:
                        continue
                    
                    # Convert DataFrame to list of dictionaries
                    headers = table.df.iloc[0].tolist()
                    table_data = []
                    
                    for _, row in table.df.iloc[1:].iterrows():
                        row_data = {}
                        for j, cell in enumerate(row):
                            if j < len(headers):
                                row_data[headers[j]] = str(cell).strip() if cell else ""
                        table_data.append(row_data)
                    
                    result["tables"].append({
                        'page': table.page,
                        'index': i,
                        'headers': headers,
                        'data': table_data,
                        'html': table.df.to_html(index=False)
                    })
                    
        except Exception as e:
            self.logger.warning(f"Error extracting tables with Camelot: {e}")
    
    def parse_bytes(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """
        Parse PDF from BytesIO object (alias for extract_content for backward compatibility)
        
        Args:
            file_obj: BytesIO object containing PDF data
            
        Returns:
            Dictionary with parsed content
        """
        return self.extract_content(file_obj)
