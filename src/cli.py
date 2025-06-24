#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command Line Interface for Real Estate Crawler
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from src.config import Config, ConfigLoader
from src.crawler.orchestrator import CrawlerOrchestrator


def setup_logging() -> None:
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        force=True
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Real Estate Crawler')
    
    # Main commands
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Crawl command
    crawl_parser = subparsers.add_parser('crawl', help='Start crawling')
    crawl_parser.add_argument('--start-page', type=int, help='Page number to start crawling from')
    crawl_parser.add_argument('--max-pages', type=int, help='Maximum number of pages to crawl')
    crawl_parser.add_argument('--config', type=str, help='Path to configuration file')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show crawler status')
    status_parser.add_argument('--config', type=str, help='Path to configuration file')
    
    # Reset command
    reset_parser = subparsers.add_parser('reset', help='Reset crawler state')
    reset_parser.add_argument('--config', type=str, help='Path to configuration file')
    reset_parser.add_argument('--confirm', action='store_true', help='Confirm reset without prompting')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export data to JSONL')
    export_parser.add_argument('--output', type=str, help='Output file path')
    export_parser.add_argument('--config', type=str, help='Path to configuration file')
    
    return parser.parse_args()


def load_config(config_path: Optional[str] = None) -> Config:
    """
    Load configuration from file or environment
    
    Args:
        config_path: Path to configuration file (optional)
    
    Returns:
        Config instance
    """
    if config_path:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        config_loader = ConfigLoader(config_path)
        return config_loader.load()
    
    return Config.get_instance()


def command_crawl(args: argparse.Namespace) -> None:
    """
    Execute crawl command
    
    Args:
        args: Command line arguments
    """
    config = load_config(args.config)
    
    # Create orchestrator
    orchestrator = CrawlerOrchestrator(config)
    
    # Start crawling
    stats = orchestrator.crawl(
        start_page=args.start_page,
        max_pages=args.max_pages
    )
    
    # Print summary
    print("\nCrawling completed!")
    print(f"Pages processed: {stats['pages_processed']}")
    print(f"Posts processed: {stats['posts_processed']}")
    print(f"Posts with downloads: {stats['posts_with_downloads']}")
    print(f"Files processed: {stats['files_processed']}")
    print(f"Errors: {stats['errors']}")
    
    if "duration_seconds" in stats:
        duration = stats["duration_seconds"]
        hours, remainder = divmod(duration, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"Duration: {int(hours)}h {int(minutes)}m {int(seconds)}s")


def command_status(args: argparse.Namespace) -> None:
    """
    Execute status command
    
    Args:
        args: Command line arguments
    """
    config = load_config(args.config)
    
    # Check checkpoint file
    checkpoint_path = config.output_dir / config.checkpoint_file
    if not checkpoint_path.exists():
        print("No crawler state found. Crawler has not been run yet.")
        return
    
    # Load checkpoint
    try:
        import json
        with open(checkpoint_path, 'r', encoding='utf-8') as f:
            checkpoint = json.load(f)
        
        print(f"Last crawled page: {checkpoint.get('last_page', 'Unknown')}")
        print(f"Last update: {checkpoint.get('timestamp', 'Unknown')}")
        print(f"Status: {checkpoint.get('status', 'Unknown')}")
    except Exception as e:
        print(f"Error loading checkpoint: {e}")


def command_reset(args: argparse.Namespace) -> None:
    """
    Execute reset command
    
    Args:
        args: Command line arguments
    """
    config = load_config(args.config)
    
    # Check checkpoint file
    checkpoint_path = config.output_dir / config.checkpoint_file
    if not checkpoint_path.exists():
        print("No crawler state found. Nothing to reset.")
        return
    
    # Confirm reset
    if not args.confirm:
        confirm = input("Are you sure you want to reset crawler state? (y/N): ")
        if confirm.lower() != 'y':
            print("Reset cancelled.")
            return
    
    # Delete checkpoint file
    try:
        os.remove(checkpoint_path)
        print("Crawler state reset successfully.")
    except Exception as e:
        print(f"Error resetting crawler state: {e}")


def command_export(args: argparse.Namespace) -> None:
    """
    Execute export command
    
    Args:
        args: Command line arguments
    """
    config = load_config(args.config)
    
    # Check if output file exists
    output_path = args.output or config.out_jsonl
    if not os.path.exists(output_path):
        print(f"Output file not found: {output_path}")
        return
    
    print(f"Data already exported to: {output_path}")
    print(f"File size: {os.path.getsize(output_path) / 1024:.2f} KB")


def main() -> None:
    """Main entry point"""
    # Parse command line arguments
    args = parse_args()
    
    # Set up logging
    setup_logging()
    
    # Execute command
    if args.command == 'crawl':
        command_crawl(args)
    elif args.command == 'status':
        command_status(args)
    elif args.command == 'reset':
        command_reset(args)
    elif args.command == 'export':
        command_export(args)
    else:
        print("Please specify a command. Use --help for more information.")
        sys.exit(1)


if __name__ == '__main__':
    main()
