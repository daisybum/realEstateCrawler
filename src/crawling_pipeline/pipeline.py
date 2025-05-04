# src/weolbu_pretrain_pipeline/pipeline.py
from io import BytesIO
from typing import List
from pathlib import Path
from datetime import datetime

from .crawler import fetch_attachments
from .extractor_pdf import extract_text_from_pdf, extract_images_from_pdf
from .extractor_pptx import extract_text_from_pptx, extract_images_from_pptx
from .ocr import ocr_image
from .preprocess import clean_text
from .io_writer import write_jsonl_gzip

BATCH_SIZE: int = 20
OUTPUT_PATH: str = "outputs/weolbu_dataset.jsonl.gz"

def process_attachment(file_bytes: BytesIO, ext: str) -> str:
    """
    첨부파일 BytesIO를 처리하여 텍스트를 추출합니다.
    :param file_bytes: 첨부파일 BytesIO
    :param ext: 파일 확장자 ('.pdf' 또는 '.pptx')
    :return: 추출된 원시 텍스트
    """
    pass

def flush_batch(batch: List[dict], is_first: bool) -> None:
    """
    배치 데이터를 gzip JSONL 파일에 저장합니다.
    :param batch: 데이터 dict 리스트
    :param is_first: 첫 저장 여부 ('wb' vs 'ab')
    """
    mode = "wb" if is_first else "ab"
    write_jsonl_gzip(batch, OUTPUT_PATH, mode)

def run_pipeline(source_urls: list[str]) -> None:
    """
    주어진 게시글 URL 리스트를 대상으로 전체 파이프라인을 실행하고,
    일정 BATCH_SIZE마다 중간 결과를 저장하여 중단 시 복구 가능하도록 합니다.
    :param source_urls: 게시글 URL 리스트
    """
    batch: List[dict] = []
    is_first_write: bool = True
    for url in source_urls:
        attachments = fetch_attachments([url])
        for file_bytes in attachments:
            ext = ".pdf" if b"%PDF" in file_bytes.getbuffer()[:4] else ".pptx"
            raw_text = process_attachment(file_bytes, ext)
            cleaned = clean_text(raw_text)
            batch.append({
                "src": url,
                "timestamp": datetime.utcnow().isoformat(),
                "text": cleaned,
            })
            if len(batch) >= BATCH_SIZE:
                flush_batch(batch, is_first_write)
                is_first_write = False
                batch.clear()
    if batch:
        flush_batch(batch, is_first_write)

if __name__ == "__main__":
    # 예시: source_urls를 로드하거나 크롤러에서 직접 가져올 수 있음
    source_urls_example: list[str] = []
    run_pipeline(source_urls_example)
