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

import logging
from src.config import Config
from src.crawler.crawler import Crawler


def main():
    """Main entry point"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create and run crawler
    crawler = Crawler()
    crawler.crawl()
    
    # Print completion message
    print(f"✅ 완료 → {Config.OUT_JSONL.resolve()}")


if __name__ == "__main__":
    main()
