#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Storage utilities for real estate crawler
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

from src.config import Config


class JsonlStorage:
    """JSONL storage for post data"""
    
    def __init__(self, filename: Path = None, config=None):
        """
        Initialize JSONL storage
        
        Args:
            filename: Path to JSONL file (defaults to config.out_jsonl)
            config: Config instance (optional)
        """
        self.config = config or Config.get_instance()
        self.filename = filename or self.config.out_jsonl
    
    def save_posts(self, posts: List[Dict[str, Any]]) -> None:
        """
        Save post records to JSONL file, avoiding duplicates
        
        Args:
            posts: List of post records to save
        """
        # Load existing records to avoid duplicates
        existing_records = self._load_existing_records()
        
        # Filter out checkpoint records, keep only post records
        post_records = [rec for rec in posts if "_checkpoint_page" not in rec and (rec.get("post_id") or rec.get("id"))]
        
        # Group and merge records by post_id
        posts_by_id = self._merge_records_by_id(post_records)
        
        # Ensure output directory exists
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        
        # Save only new records with consistent field ordering
        with open(self.filename, "a", encoding="utf-8") as f:
            for post_id, post in sorted(posts_by_id.items()):
                if post_id not in existing_records:
                    # Create a new dict with fields in consistent order
                    ordered_post = {
                        "post_id": post_id,
                        "_download_summary": post.get("_download_summary", "[다운로드 없음] "),
                        "src": post.get("src", ""),
                        "title": post.get("title", ""),
                        "type": post.get("type", "text_content"),
                        "has_download": post.get("has_download", False),
                        "file_formats": post.get("file_formats", []),
                        "download_links": post.get("download_links", []),
                        "content": post.get("content", "")
                    }
                    
                    # Add error field if present
                    if "error" in post:
                        ordered_post["error"] = post["error"]
                    
                    f.write(json.dumps(ordered_post, ensure_ascii=False) + "\n")
    
    def _load_existing_records(self) -> Dict[str, Dict[str, Any]]:
        """
        Load existing records from file
        
        Returns:
            Dictionary of post_id -> record
        """
        existing_records = {}
        if self.filename.exists():
            with open(self.filename, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        post_id = record.get("post_id")
                        if post_id:
                            existing_records[post_id] = record
                    except json.JSONDecodeError:
                        logging.warning(f"Invalid JSON in {self.filename}: {line[:50]}...")
        return existing_records
    
    def _merge_records_by_id(self, records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Merge records with the same post_id and ensure consistent output format
        
        Args:
            records: List of post records
        
        Returns:
            Dictionary of merged records by post_id with consistent format
        """
        posts_by_id = {}
        
        for rec in records:
            post_id = rec.get("post_id") or rec.get("id")
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
