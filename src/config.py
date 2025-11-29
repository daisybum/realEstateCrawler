#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration settings for the Real Estate Crawler
"""

import os
import logging
import configparser
from pathlib import Path
from typing import Dict, List, Any, Optional


class ConfigLoader:
    """
    Configuration loader for the Real Estate Crawler.
    Handles loading configuration from environment variables and config files.
    """
    def __init__(self, config_file: Optional[str] = None):
        self.config = {}
        self.config_file = config_file
        self._load_defaults()
        
        # Load from config file if provided
        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)
            
        # Override with environment variables
        self._load_from_env()
        
        # Validate configuration
        self._validate_config()
    
    def _load_defaults(self) -> None:
        """Load default configuration settings"""
        self.config = {
            # Directory/Path Management
            'output_dir': 'output',
            'jsonl_file': 'weolbu_posts.jsonl',
            'checkpoint_file': 'checkpoint.json',
            'download_dir': 'downloads',
            
            # URL & API Settings
            'base_url': 'https://weolbu.com',
            'community_path': '/community',
            'api_path': '/api/v1/community/posts',
            'login_path': '/api/v1/auth/login',
            
            # Community parameters
            'tab': 100143,
            'subtab': 5,
            
            # Credentials (empty by default, should be set via env vars)
            'login_id': '',
            'login_pw': '',
            
            # User-Agent
            'user_agent': 'Mozilla/5.0 (WeolbuCrawler/0.5)',
            
            # Browser/Driver Settings
            'browser_headless': True,
            'disable_automation': True,
            'no_sandbox': True,
            'disable_shm': True,
            
            # Request/Timeout Settings
            'request_timeout': 20,  # seconds
            
            # Wait Times
            'wait_after_login': 5,  # seconds
            'wait_page_load': 3,    # seconds
            'wait_between_pages': 1,  # seconds
            
            # Rate Limiting
            'rate_limit_enabled': True,
            'rate_limit_requests': 5,
            'rate_limit_period': 10,  # seconds
            
            # File Type Settings
            'supported_file_types': [
                '.pdf', '.pptx', '.docx', '.hwp', '.ppt', '.doc', '.xlsx', '.xls'
            ],
            'excluded_file_types': [
                '.jpg', '.jpeg', '.png', '.gif', '.svg'
            ],
            
            # Retry Settings
            'max_retries': 3,
            'retry_delay': 5,  # seconds
        }
    
    def _load_from_file(self, config_file: str) -> None:
        """Load configuration from a file"""
        parser = configparser.ConfigParser()
        try:
            parser.read(config_file)
            
            # Parse sections and update config
            for section in parser.sections():
                for key, value in parser[section].items():
                    # Convert string values to appropriate types
                    if key in self.config:
                        if isinstance(self.config[key], bool):
                            self.config[key] = parser[section].getboolean(key)
                        elif isinstance(self.config[key], int):
                            self.config[key] = parser[section].getint(key)
                        elif isinstance(self.config[key], float):
                            self.config[key] = parser[section].getfloat(key)
                        elif isinstance(self.config[key], list):
                            self.config[key] = [item.strip() for item in value.split(',')]
                        else:
                            self.config[key] = value
        except Exception as e:
            logging.error(f"Error loading config file: {e}")
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables"""
        env_mapping = {
            'WEOLBU_OUTPUT_DIR': 'output_dir',
            'WEOLBU_JSONL_FILE': 'jsonl_file',
            'WEOLBU_CHECKPOINT_FILE': 'checkpoint_file',
            'WEOLBU_DOWNLOAD_DIR': 'download_dir',
            'WEOLBU_BASE_URL': 'base_url',
            'WEOLBU_TAB': 'tab',
            'WEOLBU_SUBTAB': 'subtab',
            'WEOLBU_LOGIN_ID': 'login_id',
            'WEOLBU_LOGIN_PW': 'login_pw',
            'WEOLBU_USER_AGENT': 'user_agent',
            'WEOLBU_BROWSER_HEADLESS': 'browser_headless',
            'WEOLBU_REQUEST_TIMEOUT': 'request_timeout',
            'WEOLBU_WAIT_AFTER_LOGIN': 'wait_after_login',
            'WEOLBU_WAIT_PAGE_LOAD': 'wait_page_load',
            'WEOLBU_WAIT_BETWEEN_PAGES': 'wait_between_pages',
            'WEOLBU_RATE_LIMIT_ENABLED': 'rate_limit_enabled',
            'WEOLBU_RATE_LIMIT_REQUESTS': 'rate_limit_requests',
            'WEOLBU_RATE_LIMIT_PERIOD': 'rate_limit_period',
            'WEOLBU_MAX_RETRIES': 'max_retries',
            'WEOLBU_RETRY_DELAY': 'retry_delay',
        }
        
        for env_var, config_key in env_mapping.items():
            if env_var in os.environ:
                value = os.environ[env_var]
                
                # Convert to appropriate type
                if isinstance(self.config[config_key], bool):
                    self.config[config_key] = value.lower() in ('true', 'yes', '1', 'y')
                elif isinstance(self.config[config_key], int):
                    self.config[config_key] = int(value)
                elif isinstance(self.config[config_key], float):
                    self.config[config_key] = float(value)
                elif isinstance(self.config[config_key], list):
                    self.config[config_key] = [item.strip() for item in value.split(',')]
                else:
                    self.config[config_key] = value
    
    def _validate_config(self) -> None:
        """Validate the configuration"""
        # Check required settings
        required_settings = ['base_url', 'output_dir']
        missing = [key for key in required_settings if not self.config.get(key)]
        
        if missing:
            raise ValueError(f"Missing required configuration settings: {', '.join(missing)}")
        
        # Validate URL format
        if not self.config['base_url'].startswith(('http://', 'https://')):
            self.config['base_url'] = 'https://' + self.config['base_url']
            logging.warning(f"Base URL modified to include https://: {self.config['base_url']}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value"""
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a configuration value"""
        self.config[key] = value
    
    def get_all(self) -> Dict[str, Any]:
        """Get all configuration values"""
        return self.config.copy()


class Config:
    """Configuration settings for the Real Estate Crawler"""
    # Initialize with default configuration
    _instance = None
    _config_loader = None
    
    @classmethod
    def initialize(cls, config_file: Optional[str] = None):
        """Initialize the configuration"""
        cls._config_loader = ConfigLoader(config_file)
        cls._instance = cls()
    
    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        if cls._instance is None:
            cls.initialize()
        return cls._instance
    
    def __init__(self):
        if Config._instance is not None:
            raise RuntimeError("Use Config.get_instance() instead of creating a new instance")
        
        if Config._config_loader is None:
            Config._config_loader = ConfigLoader()
        
        # Load configuration
        config = Config._config_loader.get_all()
        
        # Set properties from configuration
        self.output_dir = Path(config['output_dir'])
        self.download_dir = Path(config['output_dir']) / config['download_dir']
        self.out_jsonl = self.output_dir / config['jsonl_file']
        self.checkpoint_file = self.output_dir / config['checkpoint_file']
        
        # URL settings
        self.base_url = config['base_url']
        self.list_url = f"{self.base_url}{config['community_path']}"
        self.specific_list_url = f"{self.list_url}?tab={config['tab']}&subTab={config['subtab']}"
        self.api_url = f"{self.base_url}{config['api_path']}"
        self.login_url = f"{self.base_url}{config['login_path']}"
        
        # Community parameters
        self.tab = config['tab']
        self.subtab = config['subtab']
        
        # Credentials
        self.login_id = config['login_id']
        self.login_pw = config['login_pw']
        
        # User-Agent
        self.user_agent = config['user_agent']
        
        # Browser/Driver Settings
        self.browser_options = {
            "headless": config['browser_headless'],
            "disable_automation": config['disable_automation'],
            "no_sandbox": config['no_sandbox'],
            "disable_shm": config['disable_shm']
        }
        
        # Request/Timeout Settings
        self.request_timeout = config['request_timeout']
        
        # Wait Times
        self.wait_after_login = config['wait_after_login']
        self.wait_page_load = config['wait_page_load']
        self.wait_between_pages = config['wait_between_pages']
        
        # Rate Limiting
        self.rate_limit_enabled = config['rate_limit_enabled']
        self.rate_limit_requests = config['rate_limit_requests']
        self.rate_limit_period = config['rate_limit_period']
        
        # File Type Settings
        self.supported_file_types = config['supported_file_types']
        self.excluded_file_types = config['excluded_file_types']
        
        # Retry Settings
        self.max_retries = config['max_retries']
        self.retry_delay = config['retry_delay']
    
    @staticmethod
    def ensure_directories():
        """Ensure all necessary directories exist"""
        config = Config.get_instance()
        for path in [config.output_dir]:
            os.makedirs(path, exist_ok=True)
