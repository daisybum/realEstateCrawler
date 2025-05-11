#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration settings for the Real Estate Crawler
"""

from pathlib import Path

class Config:
    # Base URLs
    BASE_URL = "https://weolbu.com"
    LIST_URL = f"{BASE_URL}/community"
    SPECIFIC_LIST_URL = f"{LIST_URL}?tab=100143&subTab=5"  # 특정 탭과 서브탭이 있는 URL
    API_URL = f"{BASE_URL}/api/v1/community/posts"   # API endpoint for posts
    LOGIN_URL = f"{BASE_URL}/api/v1/auth/login"      # Login API endpoint
    
    # Community parameters
    TAB = 100143
    SUBTAB = 5
    
    # Output files
    OUT_JSONL = Path("output/weolbu_posts.jsonl")
    CHECKPOINT_FILE = Path("output/checkpoint.json")
    
    # Login credentials (consider moving to environment variables in production)
    LOGIN_ID = "hirvahapjh@naver.com"
    LOGIN_PW = "Wuss1256!@"
    
    # User agent
    USER_AGENT = "Mozilla/5.0 (WeolbuCrawler/0.5)"
    
    # Browser settings
    BROWSER_OPTIONS = {
        "headless": False,  # Set to True in production
        "disable_automation": True,
        "no_sandbox": True,
        "disable_shm": True
    }
    
    # Request timeout (seconds)
    REQUEST_TIMEOUT = 20
    
    # Wait times (seconds)
    WAIT_AFTER_LOGIN = 5
    WAIT_PAGE_LOAD = 3
    WAIT_BETWEEN_PAGES = 1
    
    # File types
    SUPPORTED_FILE_TYPES = [".pdf", ".pptx", ".docx", ".hwp", ".ppt", ".doc", ".xlsx", ".xls"]
    EXCLUDED_FILE_TYPES = [".jpg", ".jpeg", ".png", ".gif", ".svg"]
