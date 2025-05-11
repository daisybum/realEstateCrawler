#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data models for real estate crawler
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass
class DownloadInfo:
    """Download information for a post"""
    has_download: bool = False
    file_formats: List[str] = field(default_factory=list)
    download_links: List[Dict[str, str]] = field(default_factory=list)
    download_buttons: List[Dict[str, str]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "has_download": self.has_download,
            "file_formats": self.file_formats,
            "download_links": self.download_links,
            "download_buttons": self.download_buttons
        }


@dataclass
class Image:
    """Image information"""
    url: str
    index: int
    ocr_text: str = ""
    
    def to_dict(self, post_id: str, src: str, title: str) -> Dict[str, Any]:
        """Convert to dictionary with post information"""
        result = {
            "post_id": post_id,
            "src": src,
            "title": title,
            "type": "image",
            "idx": self.index,
            "img_url": self.url
        }
        
        if self.ocr_text:
            result["ocr_text"] = self.ocr_text
            
        return result


@dataclass
class FileContent:
    """File content information"""
    filename: str
    url: str
    file_type: str
    content: str = ""
    
    def to_dict(self, post_id: str) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "post_id": post_id,
            "type": f"{self.file_type}_extract",
            "filename": self.filename,
            "content": self.content or f"{self.file_type.upper()} 파일 다운로드 링크: {self.url}\n파일명: {self.filename}"
        }


@dataclass
class Post:
    """Post information"""
    post_id: str
    title: str
    url: str
    download_info: Optional[DownloadInfo] = None
    content: str = ""
    images: List[Image] = field(default_factory=list)
    files: List[FileContent] = field(default_factory=list)
    download_summary: str = "[다운로드 없음] "
    error: str = ""
    
    def to_records(self) -> List[Dict[str, Any]]:
        """Convert to a list of records for storage"""
        records = []
        
        # Basic post information
        post_info = {
            "post_id": self.post_id,
            "src": self.url,
            "title": self.title,
            "type": "post_info",
            "_download_summary": self.download_summary
        }
        records.append(post_info)
        
        # Download information if present
        if self.download_info and self.download_info.has_download:
            download_rec = {
                "post_id": self.post_id,
                "src": self.url,
                "title": self.title,
                "type": "download_info",
                "_download_summary": self.download_summary,
                "has_download": True,
                "file_formats": self.download_info.file_formats,
                "download_links": self.download_info.download_links
            }
            records.append(download_rec)
        else:
            # No downloads
            download_rec = {
                "post_id": self.post_id,
                "src": self.url,
                "title": self.title,
                "type": "download_info",
                "_download_summary": self.download_summary,
                "has_download": False
            }
            records.append(download_rec)
        
        # Add content if present
        if self.content:
            content_rec = {
                "post_id": self.post_id,
                "src": self.url,
                "title": self.title,
                "type": "text_content",
                "content": self.content
            }
            records.append(content_rec)
        
        # Add images
        for img in self.images:
            img_rec = img.to_dict(self.post_id, self.url, self.title)
            img_rec["_download_summary"] = self.download_summary
            records.append(img_rec)
        
        # Add file content
        for file in self.files:
            file_rec = file.to_dict(self.post_id)
            file_rec["_download_summary"] = self.download_summary
            records.append(file_rec)
        
        # Add error if present
        if self.error:
            error_rec = {
                "post_id": self.post_id,
                "src": self.url,
                "title": self.title,
                "type": "error",
                "message": self.error
            }
            records.append(error_rec)
        
        return records
    
    def update_download_summary(self) -> None:
        """Update download summary based on download info"""
        if self.download_info and self.download_info.has_download and self.download_info.file_formats:
            formats_str = ", ".join(self.download_info.file_formats)
            self.download_summary = f"[다운로드 파일: {formats_str}] "
        else:
            self.download_summary = "[다운로드 없음] "
