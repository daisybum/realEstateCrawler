#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PPTX Parser for Real Estate Crawler
Handles extraction of text, tables, and images from PPTX files
"""

import os
import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

import pytesseract
from PIL import Image
from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE

from src.config import Config


class PPTXParser:
    """Parser for PPTX files"""
    
    def __init__(self, config=None):
        """Initialize the PPTX parser"""
        self.config = config or Config.get_instance()
        self.logger = logging.getLogger(__name__)
    
    def extract_content(self, pptx_path: str) -> Dict[str, Any]:
        """
        Extract content from a PPTX file
        
        Args:
            pptx_path: Path to the PPTX file
            
        Returns:
            Dictionary containing extracted content
        """
        if not os.path.exists(pptx_path):
            self.logger.error(f"PPTX file not found: {pptx_path}")
            return {"error": f"PPTX file not found: {pptx_path}"}
        
        try:
            result = {
                "text": "",
                "slides": [],
                "images": [],
                "metadata": {},
                "slide_count": 0
            }
            
            # Extract text, metadata, and images
            text, slides, images, metadata, slide_count = self._extract_pptx_content(pptx_path)
            result["text"] = text
            result["slides"] = slides
            result["images"] = images
            result["metadata"] = metadata
            result["slide_count"] = slide_count
            
            return result
        
        except Exception as e:
            self.logger.error(f"Error extracting content from PPTX: {e}", exc_info=True)
            return {"error": f"Error extracting content from PPTX: {str(e)}"}
    
    def _extract_pptx_content(self, pptx_path: str) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], int]:
        """
        Extract content from PPTX file
        
        Args:
            pptx_path: Path to the PPTX file
            
        Returns:
            Tuple of (text, slides, images, metadata, slide_count)
        """
        text = ""
        slides = []
        images = []
        metadata = {}
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Open the presentation
            presentation = Presentation(pptx_path)
            
            # Extract metadata
            core_props = presentation.core_properties
            metadata = {
                "title": core_props.title or "",
                "author": core_props.author or "",
                "subject": core_props.subject or "",
                "keywords": core_props.keywords or "",
                "created": str(core_props.created) if core_props.created else "",
                "modified": str(core_props.modified) if core_props.modified else "",
                "last_modified_by": core_props.last_modified_by or "",
                "revision": core_props.revision or 0,
                "category": core_props.category or "",
                "content_status": core_props.content_status or "",
                "language": core_props.language or "",
            }
            
            # Process each slide
            for slide_index, slide in enumerate(presentation.slides):
                slide_text = ""
                slide_shapes = []
                
                # Process shapes in the slide
                for shape_index, shape in enumerate(slide.shapes):
                    # Extract text from shape
                    if shape.has_text_frame:
                        shape_text = ""
                        for paragraph in shape.text_frame.paragraphs:
                            for run in paragraph.runs:
                                shape_text += run.text + " "
                        
                        slide_text += shape_text.strip() + "\n"
                        
                        slide_shapes.append({
                            "type": "text",
                            "index": shape_index,
                            "text": shape_text.strip()
                        })
                    
                    # Extract tables
                    elif shape.has_table:
                        table_data = []
                        for row in shape.table.rows:
                            row_data = []
                            for cell in row.cells:
                                cell_text = ""
                                for paragraph in cell.text_frame.paragraphs:
                                    cell_text += paragraph.text + " "
                                row_data.append(cell_text.strip())
                            table_data.append(row_data)
                        
                        slide_text += f"[Table with {len(shape.table.rows)} rows and {len(shape.table.columns)} columns]\n"
                        
                        slide_shapes.append({
                            "type": "table",
                            "index": shape_index,
                            "rows": len(shape.table.rows),
                            "columns": len(shape.table.columns),
                            "data": table_data
                        })
                    
                    # Extract images
                    elif shape.shape_type == MSO_SHAPE_TYPE.PICTURE:
                        try:
                            # Save image to temporary file
                            image_path = os.path.join(temp_dir, f"slide{slide_index+1}_img{shape_index+1}.png")
                            
                            # Get image blob and save it
                            image_blob = shape.image.blob
                            with open(image_path, "wb") as img_file:
                                img_file.write(image_blob)
                            
                            # Perform OCR on the image
                            ocr_text = ""
                            try:
                                ocr_text = pytesseract.image_to_string(Image.open(image_path), lang='kor+eng')
                            except Exception as ocr_e:
                                self.logger.warning(f"OCR failed for image: {ocr_e}")
                            
                            images.append({
                                "slide": slide_index + 1,
                                "index": shape_index + 1,
                                "ocr_text": ocr_text.strip(),
                                "temp_path": image_path
                            })
                            
                            slide_text += f"[Image: {ocr_text.strip() if ocr_text.strip() else 'No text detected'}]\n"
                            
                            slide_shapes.append({
                                "type": "image",
                                "index": shape_index,
                                "ocr_text": ocr_text.strip()
                            })
                        
                        except Exception as img_e:
                            self.logger.warning(f"Could not extract image: {img_e}")
                
                # Add slide information
                slides.append({
                    "index": slide_index + 1,
                    "text": slide_text.strip(),
                    "shapes": slide_shapes
                })
                
                # Add slide text to overall text
                text += f"\n--- Slide {slide_index + 1} ---\n{slide_text}\n"
            
            return text, slides, images, metadata, len(presentation.slides)
        
        except Exception as e:
            self.logger.error(f"Error extracting PPTX content: {e}", exc_info=True)
            return "", [], [], {}, 0
    
    def extract_slide_thumbnails(self, pptx_path: str, output_dir: str, size: Tuple[int, int] = (800, 600)) -> List[str]:
        """
        Extract slide thumbnails from PPTX file
        
        Args:
            pptx_path: Path to the PPTX file
            output_dir: Directory to save thumbnails
            size: Thumbnail size as (width, height)
            
        Returns:
            List of paths to the thumbnails
        """
        thumbnail_paths = []
        
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Get the base filename without extension
            base_name = Path(pptx_path).stem
            
            # This is a placeholder since python-pptx doesn't directly support
            # rendering slides as images. In a real implementation, you might:
            # 1. Use a library like comtypes to automate PowerPoint (Windows only)
            # 2. Use a service like LibreOffice to convert PPTX to PDF, then render PDF pages
            # 3. Use a cloud API for conversion
            
            self.logger.warning("Slide thumbnail extraction not fully implemented")
            
            # For now, just return an empty list
            return thumbnail_paths
            
        except Exception as e:
            self.logger.error(f"Error extracting slide thumbnails: {e}", exc_info=True)
            
        return thumbnail_paths
