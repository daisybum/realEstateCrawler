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
    metadata: Dict[str, Any] = field(default_factory=dict)
    tables: List[Dict[str, Any]] = field(default_factory=list)
    images: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self, post_id: str) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {
            "post_id": post_id,
            "type": f"{self.file_type}_extract",
            "filename": self.filename,
            "content": self.content or f"{self.file_type.upper()} 파일 다운로드 링크: {self.url}\n파일명: {self.filename}"
        }
        
        # Add metadata if available
        if self.metadata:
            result["metadata"] = self.metadata
            
        # Add tables if available
        if self.tables:
            result["tables"] = self.tables
            
        # Add images if available
        if self.images:
            result["images"] = self.images
            
        return result


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
    parsed_files: List[Dict[str, Any]] = field(default_factory=list)
    download_summary: str = "[다운로드 없음] "
    error: str = ""
    
    def to_records(self) -> List[Dict[str, Any]]:
        """Convert to a list of records for storage"""
        records = []
        
        # 다운로드 요약 정보 업데이트
        if self.download_info and self.download_info.has_download:
            # 실제 다운로드 링크 수를 기준으로 요약 정보 생성
            link_count = len(self.download_info.download_links) if self.download_info.download_links else 0
            formats = ", ".join(self.download_info.file_formats) if self.download_info.file_formats else "알 수 없음"
            self.download_summary = f"[다운로드 파일: {formats}, {link_count}개 파일] "
        else:
            self.download_summary = "[다운로드 없음] "
        
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
            
        # Add parsed files content
        if self.parsed_files:
            for parsed_file in self.parsed_files:
                # Create a record for each parsed file
                parsed_file_rec = {
                    "post_id": self.post_id,
                    "src": self.url,
                    "title": self.title,
                    "type": "parsed_file",
                    "_download_summary": self.download_summary,
                }
                # Add all parsed file data
                parsed_file_rec.update(parsed_file)
                records.append(parsed_file_rec)
        
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
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        result = {
            "post_id": self.post_id,
            "url": self.url,
            "title": self.title,
            "content": self.content,
        }
        
        # Add error if present
        if self.error:
            result["error"] = self.error
            
        # Add images if present
        if self.images:
            result["images"] = [img.to_dict(self.post_id, self.url, self.title) for img in self.images]
            
        # Add files if present
        if self.files:
            result["files"] = [f.to_dict(self.post_id) for f in self.files]
            
        # Add parsed files if present
        if self.parsed_files:
            result["parsed_files"] = self.parsed_files
            
        return result
    
    def update_download_summary(self) -> None:
        """Update download summary based on download info"""
        if self.download_info and self.download_info.has_download:
            links_count = len(self.download_info.download_links) if self.download_info.download_links else 0
            buttons_count = len(self.download_info.download_buttons) if self.download_info.download_buttons else 0
            total_count = links_count + buttons_count
            
            if self.download_info.file_formats:
                # Case 1: Download available with identified file formats
                formats_str = ", ".join(self.download_info.file_formats)
                if total_count > 0:
                    self.download_summary = f"[다운로드 파일: {formats_str}, {total_count}개 파일] "
                else:
                    self.download_summary = f"[다운로드 파일: {formats_str}] "
            else:
                # Case 2: Download available but format couldn't be identified
                if total_count > 0:
                    self.download_summary = f"[다운로드 가능: {total_count}개 파일] "
                else:
                    # This case occurs when has_download is true but we couldn't determine formats or count
                    self.download_summary = "[다운로드 감지됨] "
                    
                # Add a note in log for debugging purposes
                import logging
                logging.warning(f"[포스트 {self.post_id}] 다운로드 감지되었지만 파일 형식 불명함")
        else:
            # Case 3: No download detected
            self.download_summary = "[다운로드 없음] "
