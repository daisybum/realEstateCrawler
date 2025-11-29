#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Storage utilities for real estate crawler
"""

import json
import logging
import os
import fcntl
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from src.config import Config


class JsonlStorage:
    """JSONL storage for post data"""
    
    def __init__(self, filename: Optional[Path] = None, config=None):
        """
        Initialize JSONL storage
        
        Args:
            filename: Path to JSONL file (defaults to config.out_jsonl)
            config: Config instance (optional)
        """
        self.config = config or Config.get_instance()
        self.filename = filename or self.config.out_jsonl
        self.logger = logging.getLogger(__name__)
    
    def save_posts(self, posts: List[Dict[str, Any]]) -> None:
        """
        Save post records to JSONL file, avoiding duplicates
        
        Args:
            posts: List of post records to save
        """
        # Ensure output directory exists
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing records to avoid duplicates
        existing_records = self._load_existing_records()
        
        # Filter out checkpoint records, keep only post records
        post_records = [rec for rec in posts if "_checkpoint_page" not in rec and (rec.get("post_id") or rec.get("id") or rec.get("url"))]
        
        # Group and merge records by post_id
        posts_by_id = self._merge_records_by_id(post_records)
        
        # Check if there are new records to save
        new_records = {post_id: post for post_id, post in sorted(posts_by_id.items()) if post_id not in existing_records}
        
        if not new_records:
            self.logger.info("No new records to save")
            return
        
        # Save only new records with consistent field ordering and file locking
        try:
            with open(self.filename, "a", encoding="utf-8") as f:
                # Acquire exclusive lock
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                
                for post_id, post in sorted(new_records.items()):
                    # Create a new dict with fields in consistent order
                    ordered_post = {
                        "url": post.get("src", "") or post.get("url", ""),
                        "meta": {
                            "title": post.get("title", ""),
                            "author": post.get("author", ""),
                            "date": post.get("date", "")
                        },
                        "body": post.get("content", ""),
                        "parsed_content": post.get("parsed_content", "") or post.get("content", ""),
                        "file_sources": self._extract_file_sources(post),
                        "crawl_timestamp": post.get("crawl_timestamp", datetime.now().isoformat()),
                        "post_id": post_id,
                        "_download_summary": post.get("_download_summary", "[다운로드 없음] "),
                        "has_download": post.get("has_download", False),
                        "file_formats": post.get("file_formats", [])
                    }
                    
                    # Add error field if present
                    if "error" in post:
                        ordered_post["error"] = post["error"]
                    
                    f.write(json.dumps(ordered_post, ensure_ascii=False) + "\n")
                
                # Release lock (automatically done when file is closed)
            
            self.logger.info(f"Exported {len(new_records)} records to {self.filename}")
        except Exception as e:
            self.logger.error(f"Error exporting records to {self.filename}: {e}")
            raise
    
    def _load_existing_records(self) -> Dict[str, Dict[str, Any]]:
        """
        Load existing records from file
        
        Returns:
            Dictionary of post_id/url -> record
        """
        existing_records = {}
        if self.filename.exists():
            try:
                with open(self.filename, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            record = json.loads(line)
                            # Use post_id or url as the key
                            key = record.get("post_id") or record.get("url")
                            if key:
                                existing_records[key] = record
                        except json.JSONDecodeError:
                            self.logger.warning(f"Invalid JSON in {self.filename}: {line[:50]}...")
            except Exception as e:
                self.logger.error(f"Error loading existing records from {self.filename}: {e}")
        return existing_records
    
    def _merge_records_by_id(self, records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Merge records with the same post_id/url and ensure consistent output format
        
        Args:
            records: List of post records
        
        Returns:
            Dictionary of merged records by post_id/url with consistent format
        """
        posts_by_id = {}
        
        for rec in records:
            # Use post_id, id, or url as the key
            post_id = rec.get("post_id") or rec.get("id") or rec.get("url")
            if not post_id:
                continue
                
            if post_id not in posts_by_id:
                # Initialize with default values in the correct order
                posts_by_id[post_id] = {
                    "post_id": post_id,
                    "_download_summary": "[다운로드 없음] ",
                    "src": "",
                    "title": "",
                    "type": "text_content",
                    "has_download": False,
                    "file_formats": [],
                    "download_links": [],
                    "content": ""
                }
            
            # Update fields from current record, preserving the original structure
            current_post = posts_by_id[post_id]
            
            # Handle URL/source
            if "src" in rec and rec["src"]:
                current_post["src"] = rec["src"]
            elif "url" in rec and rec["url"]:
                current_post["src"] = rec["url"]
            
            # Handle title
            if "title" in rec and rec["title"]:
                current_post["title"] = rec["title"]
            
            # Handle content
            if "content" in rec and rec["content"]:
                current_post["content"] = rec["content"]
            
            # Handle download information
            if "_download_summary" in rec:
                current_post["_download_summary"] = rec["_download_summary"]
            
            if "has_download" in rec:
                current_post["has_download"] = rec["has_download"]
            
            if "file_formats" in rec and rec["file_formats"]:
                current_post["file_formats"] = list(set(current_post["file_formats"] + rec["file_formats"]))
            
            if "download_links" in rec and rec["download_links"]:
                # Merge download links, avoiding duplicates
                existing_urls = {link.get("url") for link in current_post["download_links"]}
                for link in rec["download_links"]:
                    if isinstance(link, dict) and link.get("url") and link["url"] not in existing_urls:
                        current_post["download_links"].append(link)
            
            # Handle type (pdf_extract, pptx_extract, text_content, error)
            if "type" in rec and rec["type"]:
                current_post["type"] = rec["type"]
            
            # Handle errors
            if "error" in rec and rec["error"]:
                current_post["error"] = rec["error"]
                current_post["type"] = "error"
        
        return posts_by_id
        
    def _extract_file_sources(self, post: Dict[str, Any]) -> List[str]:
        """
        Extract file sources from post record
        
        Args:
            post: Post record
            
        Returns:
            List of file source URLs
        """
        file_sources = []
        
        # Extract from download_links
        if "download_links" in post and post["download_links"]:
            for link in post["download_links"]:
                if isinstance(link, dict) and link.get("url"):
                    file_sources.append(link["url"])
        
        # Extract from file_sources if already present
        if "file_sources" in post and isinstance(post["file_sources"], list):
            file_sources.extend(post["file_sources"])
        
        # Remove duplicates while preserving order
        seen = set()
        return [url for url in file_sources if not (url in seen or seen.add(url))]


class CheckpointManager:
    """Manages crawler checkpoints for resumable crawling"""
    
    def __init__(self, filename: Path = None, config=None):
        """
        Initialize checkpoint manager
        
        Args:
            filename: Path to checkpoint file (defaults to config.checkpoint_file)
            config: Config instance (optional)
        """
        self.config = config or Config.get_instance()
        self.filename = filename or self.config.checkpoint_file
        self.jsonl_file = self.config.out_jsonl
    
    def save(self, page: int, download_summary: str) -> None:
        """
        Save checkpoint information
        
        Args:
            page: Current page number
            download_summary: Download summary string
        """
        checkpoint_data = {
            "page": page,
            "download_summary": download_summary,
            "timestamp": datetime.now().isoformat()
        }
        
        with open(self.filename, "w", encoding="utf-8") as f:
            json.dump(checkpoint_data, f, ensure_ascii=False)
            
    def save_checkpoint(self, page: int, download_summary: str = "") -> None:
        """
        Save checkpoint information (alias for save method for compatibility)
        
        Args:
            page: Current page number
            download_summary: Download summary string
        """
        self.save(page, download_summary)
    
    def get_last_page(self) -> int:
        """
        Get the last processed page number from checkpoint
        
        Returns:
            Last processed page number, or 1 if no checkpoint found
        """
        try:
            # Check new checkpoint file format
            if self.filename.exists():
                with open(self.filename, "r", encoding="utf-8") as f:
                    checkpoint_data = json.load(f)
                    return checkpoint_data["page"] + 1
            
            # Fall back to legacy format (in JSONL)
            elif Path(self.jsonl_file).exists():
                with open(self.jsonl_file, "r", encoding="utf-8") as f:
                    last_checkpoint = None
                    for line in f:
                        try:
                            rec = json.loads(line)
                            if "_checkpoint_page" in rec:
                                last_checkpoint = rec
                        except json.JSONDecodeError:
                            pass
                    
                    if last_checkpoint:
                        return last_checkpoint["_checkpoint_page"] + 1
        except Exception as e:
            logging.error(f"체크포인트 확인 실패: {e}")
        
        # Default: start from page 1
        return 1
