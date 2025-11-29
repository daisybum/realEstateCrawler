#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
월급쟁이부자들(tab=100143, subTab=5) 크롤러 - 리팩토링 버전
──────────────────────────────────────────────────────────────
0) 로그인 처리
1) REST API → 실패 시 2) Headless 렌더링(Fallback)
2) 첨부파일(pdf/pptx/docx) 있으면 파일만, 없으면 본문·이미지 OCR
3) JSONL 체크포인트 저장
4) 크롤링 오케스트레이션 및 JSONL 내보내기
──────────────────────────────────────────────────────────────
"""

import os
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv
from src.config import Config
from src.crawler.crawler import Crawler
from src.storage.storage import JsonlStorage, CheckpointManager


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
    parser.add_argument(
        "--start-page",
        help="Start crawling from this page number",
        type=int,
        default=None
    )
    parser.add_argument(
        "--max-pages",
        help="Maximum number of pages to crawl",
        type=int,
        default=None
    )
    parser.add_argument(
        "--export-only",
        help="Only export existing data without crawling",
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
    config.ensure_directories()
    
    # Log configuration
    logging.info(f"Starting crawler with output directory: {config.output_dir}")
    logging.info(f"JSONL output file: {config.out_jsonl}")
    logging.info(f"Checkpoint file: {config.checkpoint_file}")
    
    try:
        # Initialize checkpoint manager
        checkpoint = CheckpointManager()
        
        if args.export_only:
            # Export only mode - just process existing data
            logging.info("Export-only mode: processing existing data")
            storage = JsonlStorage()
            last_page = checkpoint.get_last_page()
            logging.info(f"Last processed page from checkpoint: {last_page}")
            print(f"✅ 내보내기 완료 → {config.out_jsonl.resolve()}")
        else:
            # Full crawling mode with Crawler
            crawler = Crawler(config)
            
            # Set crawling parameters
            start_page = args.start_page or checkpoint.get_last_page()
            max_pages = args.max_pages
            
            # Start crawling
            logging.info(f"Starting crawl from page {start_page}")
            stats = crawler.crawl(start_page=start_page, max_pages=max_pages)
            
            # Print completion message
            print(f"✅ 크롤링 완료 → {config.out_jsonl.resolve()}")
            if hasattr(stats, 'get'):
                print(f"   - 처리된 페이지: {stats.get('pages_processed', 'N/A')}")
                print(f"   - 처리된 포스트: {stats.get('posts_processed', 'N/A')}")
                print(f"   - 다운로드 포스트: {stats.get('posts_with_downloads', 'N/A')}")
                print(f"   - 처리된 파일: {stats.get('files_processed', 'N/A')}")
                print(f"   - 오류: {stats.get('errors', 'N/A')}")
            else:
                print("   - 크롤링 완료 (상세 통계 없음)")
                print(f"   - 마지막 페이지: {checkpoint.get_last_page()}")
                print(f"   - 출력 파일: {config.out_jsonl}")
                print(f"   - 체크포인트: {config.checkpoint_file}")
                print(f"   - 출력 디렉토리: {config.output_dir}")
                print(f"   - 다운로드 디렉토리: {config.download_dir}")
    except Exception as e:
        logging.error(f"Error during crawling: {e}", exc_info=True)
        print(f"❌ 오류 발생: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
