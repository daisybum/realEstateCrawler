from io import BytesIO
from typing import List, Dict, Any, Tuple, Optional
import os
import tempfile

# PDF 처리 라이브러리
import pypdf2
import fitz  # PyMuPDF
import pdfplumber
import camelot

# 이미지 및 OCR 처리 라이브러리
import numpy as np
import cv2
import pytesseract


def extract_text_from_pdf(pdf_bytes: BytesIO) -> str:
    """
    PDF 문서(BytesIO)에서 텍스트를 추출합니다.
    PyPDF를 사용하여 텍스트 레이어가 있는 경우 추출하고,
    그렇지 않은 경우 PyMuPDF와 OCR을 사용합니다.
    
    :param pdf_bytes: PDF 파일 내용을 담은 BytesIO 객체
    :return: 추출된 텍스트 문자열
    """
    # PyPDF를 사용한 텍스트 추출 시도
    reader = pypdf2.PdfReader(pdf_bytes)
    text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text and page_text.strip():
            text += page_text + "\n\n"
    
    # 텍스트가 충분히 추출되지 않은 경우 OCR 사용
    if not text or len(text.strip()) < 100:  # 임의의 기준값
        text = extract_text_with_ocr(pdf_bytes)
    
    return text.strip()


def extract_text_with_ocr(pdf_bytes: BytesIO) -> str:
    """
    PyMuPDF와 pytesseract를 사용하여 PDF에서 OCR을 통해 텍스트를 추출합니다.
    
    :param pdf_bytes: PDF 파일 내용을 담은 BytesIO 객체
    :return: OCR로 추출된 텍스트 문자열
    """
    # 임시 파일로 저장 (PyMuPDF가 파일 경로를 필요로 할 수 있음)
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
        temp_file.write(pdf_bytes.getvalue())
        temp_path = temp_file.name
    
    try:
        # PyMuPDF로 PDF 열기
        doc = fitz.open(temp_path)
        full_text = []
        
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            
            # 페이지를 이미지로 변환 (300 DPI)
            pix = page.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
            img_data = pix.samples
            
            # NumPy 배열로 변환
            img_array = np.frombuffer(img_data, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
            
            # BGR 형식으로 변환 (OpenCV 형식)
            if pix.n == 4:  # RGBA
                img_array = cv2.cvtColor(img_array, cv2.COLOR_RGBA2BGR)
            
            # OCR 수행 (한국어 + 영어)
            page_text = pytesseract.image_to_string(img_array, lang='kor+eng')
            full_text.append(page_text)
        
        return "\n\n".join(full_text)
    
    finally:
        # 임시 파일 삭제
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def extract_images_from_pdf(pdf_bytes: BytesIO) -> List[BytesIO]:
    """
    PDF 문서에서 이미지를 추출합니다.
    PyMuPDF를 사용하여 고품질 이미지를 추출합니다.
    
    :param pdf_bytes: PDF 파일 내용을 담은 BytesIO 객체
    :return: 추출된 이미지들을 담은 BytesIO 객체 리스트
    """
    # 임시 파일로 저장
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
        temp_file.write(pdf_bytes.getvalue())
        temp_path = temp_file.name
    
    try:
        # PyMuPDF로 PDF 열기
        doc = fitz.open(temp_path)
        images = []
        
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            
            # 페이지에서 이미지 추출
            image_list = page.get_images(full=True)
            
            for img_index, img_info in enumerate(image_list):
                xref = img_info[0]  # 이미지 참조 번호
                base_image = doc.extract_image(xref)
                image_bytes = base_image["image"]
                
                # BytesIO로 변환
                image_stream = BytesIO(image_bytes)
                images.append(image_stream)
        
        return images
    
    finally:
        # 임시 파일 삭제
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def extract_tables_from_pdf(pdf_bytes: BytesIO) -> List[Dict[str, Any]]:
    """
    PDF 문서에서 테이블을 추출합니다.
    pdfplumber와 camelot을 함께 사용하여 더 정확한 테이블 추출을 시도합니다.
    
    :param pdf_bytes: PDF 파일 내용을 담은 BytesIO 객체
    :return: 추출된 테이블 데이터 리스트 (각 테이블은 딕셔너리 형태)
    """
    # 임시 파일로 저장 (camelot이 파일 경로를 필요로 함)
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
        temp_file.write(pdf_bytes.getvalue())
        temp_path = temp_file.name
    
    try:
        all_tables = []
        
        # 1. pdfplumber로 테이블 추출 시도
        with pdfplumber.open(temp_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                
                for table_num, table_data in enumerate(tables):
                    # 헤더와 데이터 분리 (첫 번째 행을 헤더로 가정)
                    if table_data and len(table_data) > 0:
                        headers = table_data[0]
                        data = table_data[1:] if len(table_data) > 1 else []
                        
                        all_tables.append({
                            "page": page_num + 1,
                            "table_num": table_num + 1,
                            "method": "pdfplumber",
                            "headers": headers,
                            "data": data
                        })
        
        # 2. camelot으로 테이블 추출 시도 (pdfplumber가 찾지 못한 경우)
        if not all_tables:
            tables = camelot.read_pdf(temp_path, pages='all', flavor='lattice')
            
            for table_num, table in enumerate(tables):
                df = table.df
                headers = df.iloc[0].tolist()
                data = df.iloc[1:].values.tolist()
                
                all_tables.append({
                    "page": table.page,
                    "table_num": table_num + 1,
                    "method": "camelot",
                    "headers": headers,
                    "data": data
                })
        
        return all_tables
    
    finally:
        # 임시 파일 삭제
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def get_pdf_metadata(pdf_bytes: BytesIO) -> Dict[str, str]:
    """
    PDF 문서의 메타데이터를 추출합니다.
    
    :param pdf_bytes: PDF 파일 내용을 담은 BytesIO 객체
    :return: 메타데이터 딕셔너리
    """
    reader = pypdf2.PdfReader(pdf_bytes)
    metadata = reader.metadata
    
    result = {}
    if metadata:
        # 일반적인 메타데이터 필드 추출
        for key in metadata:
            if metadata[key]:
                # '/키' 형식에서 '키'만 추출
                clean_key = key.strip('/') if isinstance(key, str) else str(key)
                result[clean_key] = str(metadata[key])
    
    # 페이지 수 추가
    result['PageCount'] = len(reader.pages)
    
    return result


def split_pdf_by_pages(pdf_bytes: BytesIO, page_ranges: List[Tuple[int, int]]) -> List[BytesIO]:
    """
    PDF 문서를 지정된 페이지 범위에 따라 여러 개의 PDF로 분할합니다.
    
    :param pdf_bytes: PDF 파일 내용을 담은 BytesIO 객체
    :param page_ranges: 분할할 페이지 범위 리스트 [(시작 페이지, 끝 페이지), ...]
    :return: 분할된 PDF 파일들을 담은 BytesIO 객체 리스트
    """
    reader = pypdf2.PdfReader(pdf_bytes)
    split_pdfs = []
    
    for start_page, end_page in page_ranges:
        # 페이지 범위 검증
        if start_page < 0 or end_page >= len(reader.pages) or start_page > end_page:
            continue
        
        # 새 PDF 생성
        writer = pypdf2.PdfWriter()
        
        # 지정된 페이지 범위 추가
        for page_num in range(start_page, end_page + 1):
            writer.add_page(reader.pages[page_num])
        
        # BytesIO에 저장
        output = BytesIO()
        writer.write(output)
        output.seek(0)
        
        split_pdfs.append(output)
    
    return split_pdfs
