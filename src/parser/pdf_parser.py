#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF Parser for Real Estate Crawler
Handles extraction of text, tables, and images from PDF files
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import fitz  # PyMuPDF
import pdfplumber
import camelot
import pytesseract
from PIL import Image
import cv2
import numpy as np
import tempfile

from src.config import Config


class PDFParser:
    """Parser for PDF files"""
    
    def __init__(self, config=None):
        """Initialize the PDF parser"""
        self.config = config or Config.get_instance()
        self.logger = logging.getLogger(__name__)
        
    def parse_bytes(self, file_obj) -> Dict[str, Any]:
        """
        Parse PDF from BytesIO object
        
        Args:
            file_obj: BytesIO object containing PDF data
            
        Returns:
            Dictionary with parsed content
        """
        try:
            result = {
                "content": "",
                "metadata": {},
                "tables": [],
                "images": []
            }
            
            # Extract text using PyMuPDF (fitz)
            doc = fitz.open(stream=file_obj.read(), filetype="pdf")
            
            # Extract text
            text_content = []
            for page_num in range(len(doc)):
                page = doc[page_num]
                text_content.append(page.get_text())
            
            result["content"] = "\n\n".join(text_content)
            
            # Extract metadata
            metadata = doc.metadata
            if metadata:
                result["metadata"] = {
                    'title': metadata.get('title', ''),
                    'author': metadata.get('author', ''),
                    'subject': metadata.get('subject', ''),
                    'creator': metadata.get('creator', ''),
                    'producer': metadata.get('producer', ''),
                    'page_count': len(doc)
                }
            
            # Extract images
            for page_num in range(len(doc)):
                page = doc[page_num]
                image_list = page.get_images(full=True)
                
                for img_index, img in enumerate(image_list):
                    try:
                        xref = img[0]
                        base_image = doc.extract_image(xref)
                        image_bytes = base_image["image"]
                        image_ext = base_image["ext"]
                        
                        # Process image with OCR
                        ocr_text = self._process_image_ocr(image_bytes)
                        
                        # Add to images list
                        result["images"].append({
                            'page': page_num + 1,
                            'index': img_index,
                            'width': base_image.get('width', 0),
                            'height': base_image.get('height', 0),
                            'format': image_ext,
                            'ocr_text': ocr_text
                        })
                    except Exception as e:
                        self.logger.warning(f"Error extracting image {img_index} from page {page_num+1}: {e}")
            
            # Extract tables using pdfplumber
            try:
                file_obj.seek(0)  # Reset file pointer
                with pdfplumber.open(file_obj) as pdf:
                    for page_num, page in enumerate(pdf.pages):
                        page_tables = page.extract_tables()
                        if page_tables:
                            for table_idx, table in enumerate(page_tables):
                                # Convert table to list of dictionaries
                                if table and len(table) > 1:  # Has header and data
                                    headers = [str(h).strip() for h in table[0]]
                                    table_data = []
                                    for row in table[1:]:
                                        row_data = {}
                                        for i, cell in enumerate(row):
                                            if i < len(headers):
                                                row_data[headers[i]] = str(cell).strip() if cell else ""
                                        table_data.append(row_data)
                                    
                                    result["tables"].append({
                                        'page': page_num + 1,
                                        'index': table_idx,
                                        'headers': headers,
                                        'data': table_data
                                    })
            except Exception as e:
                self.logger.warning(f"Error extracting tables from PDF: {e}")
            
            doc.close()
            return result
            
        except Exception as e:
            self.logger.error(f"Error parsing PDF bytes: {e}")
            return {"error": str(e), "content": f"PDF 파일 처리 오류: {e}"}
    
    def _process_image_ocr(self, image_bytes):
        """
        Process image with OCR to extract text
        
        Args:
            image_bytes: Image data as bytes
            
        Returns:
            Extracted text from the image
        """
        try:
            # Convert bytes to numpy array for OpenCV
            nparr = np.frombuffer(image_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if img is None or img.size == 0:
                return ""
                
            # Check if image is too small for meaningful OCR
            if img.shape[0] < 50 or img.shape[1] < 50:
                return ""  # Skip tiny images like icons
                
            # Preprocess image for better OCR results
            # Convert to grayscale
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Apply adaptive thresholding
            thresh = cv2.adaptiveThreshold(
                gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                cv2.THRESH_BINARY, 11, 2
            )
            
            # Try PaddleOCR first (better for Korean text)
            paddle_result = ""
            try:
                result = PaddleOCR(use_angle_cls=True, lang='korean', show_log=False).ocr(img, cls=True)
                if result and result[0]:
                    for line in result[0]:
                        if len(line) >= 2 and line[1] and len(line[1]) >= 2:
                            text, confidence = line[1]
                            if confidence > 0.5:  # Only include confident results
                                paddle_result += text + " "
            except Exception as e:
                self.logger.warning(f"PaddleOCR failed: {e}")
                
            # If PaddleOCR failed, try pytesseract as fallback
            if not paddle_result.strip():
                try:
                    # Convert OpenCV image to PIL Image for pytesseract
                    pil_img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
                    pytess_result = pytesseract.image_to_string(pil_img, lang='kor+eng')
                    return pytess_result.strip()
                except Exception as e:
                    self.logger.warning(f"Pytesseract OCR failed: {e}")
                    return ""
            
            return paddle_result.strip()
            
        except Exception as e:
            self.logger.error(f"Image OCR processing error: {e}")
            return ""
    
    def extract_content(self, pdf_path: str) -> Dict[str, Any]:
        """
        Extract content from a PDF file
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Dictionary containing extracted content
        """
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return {"error": f"PDF file not found: {pdf_path}"}
        
        try:
            result = {
                "text": "",
                "tables": [],
                "images": [],
                "metadata": {},
                "pages": 0
            }
            
            # Extract text and metadata using PyMuPDF
            text, metadata, pages = self._extract_with_pymupdf(pdf_path)
            result["text"] = text
            result["metadata"] = metadata
            result["pages"] = pages
            
            # Extract tables using Camelot
            tables = self._extract_tables(pdf_path)
            result["tables"] = tables
            
            # Extract images
            images = self._extract_images(pdf_path)
            result["images"] = images
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error extracting content from PDF: {e}", exc_info=True)
            return {"error": f"Error extracting content from PDF: {str(e)}"}
    
    def _extract_with_pymupdf(self, pdf_path: str) -> Tuple[str, Dict[str, Any], int]:
        """
        Extract text and metadata using PyMuPDF
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Tuple of (text, metadata, page_count)
        """
        text = ""
        metadata = {}
        
        try:
            doc = fitz.open(pdf_path)
            
            # Extract metadata
            metadata = {
                "title": doc.metadata.get("title", ""),
                "author": doc.metadata.get("author", ""),
                "subject": doc.metadata.get("subject", ""),
                "keywords": doc.metadata.get("keywords", ""),
                "creator": doc.metadata.get("creator", ""),
                "producer": doc.metadata.get("producer", ""),
                "creation_date": doc.metadata.get("creationDate", ""),
                "modification_date": doc.metadata.get("modDate", ""),
            }
            
            # Extract text from each page
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                page_text = page.get_text("text")
                text += f"\n--- Page {page_num + 1} ---\n{page_text}\n"
            
            return text, metadata, len(doc)
        
        except Exception as e:
            self.logger.error(f"Error extracting with PyMuPDF: {e}", exc_info=True)
            
            # Fallback to pdfplumber if PyMuPDF fails
            try:
                self.logger.info("Falling back to pdfplumber for text extraction")
                with pdfplumber.open(pdf_path) as pdf:
                    pages_text = []
                    for i, page in enumerate(pdf.pages):
                        page_text = page.extract_text() or ""
                        pages_text.append(f"\n--- Page {i + 1} ---\n{page_text}\n")
                    
                    return "".join(pages_text), metadata, len(pdf.pages)
            
            except Exception as inner_e:
                self.logger.error(f"Error in fallback text extraction: {inner_e}", exc_info=True)
                return "", {}, 0
    
    def _extract_tables(self, pdf_path: str) -> List[Dict[str, Any]]:
        """
        Extract tables from PDF using Camelot
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            List of tables as dictionaries
        """
        tables_data = []
        
        try:
            # Try to extract tables using Camelot
            tables = camelot.read_pdf(pdf_path, pages='all', flavor='lattice')
            
            for i, table in enumerate(tables):
                if table.df.empty:
                    continue
                
                tables_data.append({
                    "page": table.page,
                    "index": i,
                    "data": table.df.to_dict('records'),
                    "accuracy": table.accuracy,
                    "whitespace": table.whitespace,
                })
            
            # If no tables found with lattice, try stream flavor
            if not tables_data:
                tables = camelot.read_pdf(pdf_path, pages='all', flavor='stream')
                
                for i, table in enumerate(tables):
                    if table.df.empty:
                        continue
                    
                    tables_data.append({
                        "page": table.page,
                        "index": i,
                        "data": table.df.to_dict('records'),
                        "accuracy": table.accuracy,
                        "whitespace": table.whitespace,
                    })
        
        except Exception as e:
            self.logger.error(f"Error extracting tables from PDF: {e}", exc_info=True)
            
            # Fallback to pdfplumber for table extraction
            try:
                self.logger.info("Falling back to pdfplumber for table extraction")
                with pdfplumber.open(pdf_path) as pdf:
                    for i, page in enumerate(pdf.pages):
                        tables = page.extract_tables()
                        for j, table in enumerate(tables):
                            if not table:
                                continue
                            
                            # Convert table to list of dictionaries
                            headers = table[0]
                            data = []
                            for row in table[1:]:
                                row_dict = {}
                                for k, header in enumerate(headers):
                                    if k < len(row):
                                        row_dict[header] = row[k]
                                data.append(row_dict)
                            
                            tables_data.append({
                                "page": i + 1,
                                "index": j,
                                "data": data,
                                "accuracy": None,
                                "whitespace": None,
                            })
            
            except Exception as inner_e:
                self.logger.error(f"Error in fallback table extraction: {inner_e}", exc_info=True)
        
        return tables_data
    
    def _extract_images(self, pdf_path: str) -> List[Dict[str, Any]]:
        """
        Extract images from PDF
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            List of images as dictionaries with OCR text
        """
        images_data = []
        temp_dir = tempfile.mkdtemp()
        
        try:
            doc = fitz.open(pdf_path)
            
            for page_index in range(len(doc)):
                page = doc[page_index]
                
                # Get images
                image_list = page.get_images(full=True)
                
                for img_index, img in enumerate(image_list):
                    xref = img[0]
                    
                    try:
                        base_image = doc.extract_image(xref)
                        image_bytes = base_image["image"]
                        image_ext = base_image["ext"]
                        
                        # Save image to temporary file
                        image_path = os.path.join(temp_dir, f"page{page_index+1}_img{img_index+1}.{image_ext}")
                        with open(image_path, "wb") as img_file:
                            img_file.write(image_bytes)
                        
                        # Perform OCR on the image
                        ocr_text = ""
                        try:
                            ocr_text = pytesseract.image_to_string(Image.open(image_path), lang='kor+eng')
                        except Exception as ocr_e:
                            self.logger.warning(f"OCR failed for image: {ocr_e}")
                        
                        images_data.append({
                            "page": page_index + 1,
                            "index": img_index + 1,
                            "width": base_image["width"],
                            "height": base_image["height"],
                            "ext": image_ext,
                            "ocr_text": ocr_text.strip(),
                            "temp_path": image_path
                        })
                    
                    except Exception as img_e:
                        self.logger.warning(f"Could not extract image: {img_e}")
        
        except Exception as e:
            self.logger.error(f"Error extracting images from PDF: {e}", exc_info=True)
        
        return images_data
    
    def split_pdf(self, pdf_path: str, output_dir: str) -> List[str]:
        """
        Split a PDF file into individual pages
        
        Args:
            pdf_path: Path to the PDF file
            output_dir: Directory to save the split pages
            
        Returns:
            List of paths to the split pages
        """
        output_paths = []
        
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Open the PDF
            doc = fitz.open(pdf_path)
            
            # Get the base filename without extension
            base_name = Path(pdf_path).stem
            
            # Split each page
            for page_num in range(len(doc)):
                output_path = os.path.join(output_dir, f"{base_name}_page{page_num+1}.pdf")
                
                # Create a new PDF with just this page
                new_doc = fitz.open()
                new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
                new_doc.save(output_path)
                new_doc.close()
                
                output_paths.append(output_path)
            
            doc.close()
            
        except Exception as e:
            self.logger.error(f"Error splitting PDF: {e}", exc_info=True)
        
        return output_paths
