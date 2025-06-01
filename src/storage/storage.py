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
        post_records = [rec for rec in posts if "_checkpoint_page" not in rec and rec.get("post_id")]
        
        # Group and merge records by post_id
        posts_by_id = self._merge_records_by_id(post_records)
        
        # Save only new records
        with open(self.filename, "a", encoding="utf-8") as f:
            for post_id, post in posts_by_id.items():
                if post_id not in existing_records:
                    f.write(json.dumps(post, ensure_ascii=False) + "\n")
    
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
        Merge records with the same post_id
        
        Args:
            records: List of post records
        
        Returns:
            Dictionary of merged records by post_id
        """
        posts_by_id = {}
        for rec in records:
            post_id = rec.get("post_id")
            if post_id not in posts_by_id:
                posts_by_id[post_id] = {
                    "post_id": post_id,
                    "_download_summary": rec.get("_download_summary", "[다운로드 없음] "),
                    "src": rec.get("src", ""),
                    "title": rec.get("title", "")
                }
            
            # Add remaining fields, with special handling for certain types
            for key, value in rec.items():
                if key not in ["post_id", "src", "title", "_download_summary"]:
                    # Download-related information
                    if key in ["has_download", "file_formats", "download_links"]:
                        posts_by_id[post_id][key] = value
                    # Handle type field
                    elif key == "type":
                        if value == "download_info" and "_download_summary" in rec:
                            posts_by_id[post_id]["_download_summary"] = rec["_download_summary"]
                        posts_by_id[post_id]["type"] = value
                    # All other fields
                    else:
                        posts_by_id[post_id][key] = value
        
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
