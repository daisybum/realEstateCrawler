#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
월급쟁이부자들(tab=100143, subTab=5) 크롤러 - 리팩토링 버전
──────────────────────────────────────────────────────────────
0) 로그인 처리
1) REST API → 실패 시 2) Headless 렌더링(Fallback)
2) 첨부파일(pdf/pptx/docx) 있으면 파일만, 없으면 본문·이미지 OCR
3) JSONL 체크포인트 저장
──────────────────────────────────────────────────────────────
"""

import os
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv
from src.config import Config
from src.crawler.crawler import Crawler


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Real Estate Crawler for Weolbu.com")
    parser.add_argument(
        "-c", "--config", 
        help="Path to configuration file", 
        default=None
    )
    parser.add_argument(
        "-o", "--output", 
        help="Output directory for crawled data", 
        default=None
    )
    parser.add_argument(
        "--headless", 
        help="Run browser in headless mode", 
        action="store_true"
    )
    parser.add_argument(
        "--debug", 
        help="Enable debug logging", 
        action="store_true"
    )
    return parser.parse_args()


def setup_logging(debug=False):
    """Configure logging"""
    log_level = logging.DEBUG if debug else logging.INFO
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=log_level,
        format=log_format
    )
    
    # Create a file handler for logging to a file
    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler("logs/crawler.log")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(log_format))
    
    # Add the file handler to the root logger
    logging.getLogger().addHandler(file_handler)


def main():
    """Main entry point"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Configure logging
    setup_logging(args.debug)
    
    # Load environment variables from .env.crawler
    env_path = Path(__file__).parent / '.env.crawler'
    if env_path.exists():
        load_dotenv(env_path, override=True)
        logging.info(f"Loaded environment variables from {env_path}")
    else:
        logging.warning(f"No .env.crawler file found at {env_path}")
    
    # Initialize configuration
    Config.initialize(args.config)
    config = Config.get_instance()
    
    # Debug: Print loaded credentials (don't log passwords in production)
    if config.login_id:
        logging.info(f"Loaded login ID: {config.login_id}")
        logging.info("Login password: [REDACTED]")
    else:
        logging.warning("No login credentials found in environment variables")
    
    # Override configuration with command line arguments
    if args.output:
        config._config_loader.set('output_dir', args.output)
    
    if args.headless:
        config._config_loader.set('browser_headless', True)
    
    # Ensure directories exist
    Config.ensure_directories()
    
    # Log configuration
    logging.info(f"Starting crawler with output directory: {config.output_dir}")
    logging.info(f"JSONL output file: {config.out_jsonl}")
    logging.info(f"Checkpoint file: {config.checkpoint_file}")
    
    # Create and run crawler
    try:
        crawler = Crawler()
        crawler.crawl()
        
        # Print completion message
        print(f"✅ 완료 → {config.out_jsonl.resolve()}")
    except Exception as e:
        logging.error(f"Error during crawling: {e}", exc_info=True)
        print(f"❌ 오류 발생: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
