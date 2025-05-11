#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Docling PDF Extractor

PDF 파일에서 텍스트와 이미지를 추출하고 OCR을 적용하여 JSONL 형식으로 변환하는 도구입니다.
"""

# 표준 라이브러리
import os
import json
import logging
import tempfile
import uuid
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, TypedDict
from dataclasses import dataclass

# 서드파티 라이브러리
import cv2
import numpy as np
from PIL import Image
import camelot
from paddleocr import PaddleOCR

# Docling 라이브러리
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions,
    TableStructureOptions,
    TesseractOcrOptions,
    TableFormerMode,
)
from docling.datamodel.base_models import InputFormat
from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling_core.types.doc.document import DocItem

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 타입 정의
class ImageInfo(TypedDict):
    """이미지 정보를 저장하는 타입"""
    identifier: str
    image_data: np.ndarray
    page: int
    index: int
    width: int
    height: int
    format: str
    position: Optional[Dict[str, float]]
    path: Optional[str]

class OcrResult(TypedDict):
    """OCR 결과를 저장하는 타입"""
    text: str
    confidence: float
    bbox: List[List[float]]

@dataclass
class PdfProcessingConfig:
    """PDF 처리 설정"""
    # 일반 설정
    tessdata_prefix: str = '/usr/share/tesseract-ocr/4.00/tessdata/'
    min_image_width: int = 100
    min_image_height: int = 100
    
    # OCR 설정
    use_korean: bool = True
    detect_tables: bool = True
    ocr_confidence_threshold: float = 0.6
    
    # 이미지 처리 설정
    max_image_dimension: int = 2000
    table_detection_min_lines: int = 5
    table_detection_horizontal_threshold: int = 10
    table_detection_vertical_threshold: int = 10
    
    # 페이지 경계 감지 설정
    page_boundary_threshold: int = 500

class PdfImageExtractor:
    """PDF에서 이미지를 추출하는 클래스"""
    
    def __init__(self, config: PdfProcessingConfig):
        """PdfImageExtractor 초기화
        
        Args:
            config: PDF 처리 설정
        """
        self.config = config
        self._import_dependencies()
    
    def _import_dependencies(self) -> None:
        """PyMuPDF 등 필요한 라이브러리 임포트"""
        try:
            import fitz  # PyMuPDF
            self.fitz = fitz
        except ImportError:
            logging.error("PyMuPDF(fitz) 라이브러리가 설치되지 않았습니다. 'pip install pymupdf'를 실행하세요.")
            raise
    
    def extract_images(self, pdf_path: str, output_dir: Optional[str] = None) -> List[ImageInfo]:
        """PDF에서 이미지를 추출하여 메모리에 저장합니다.
        
        Args:
            pdf_path: PDF 파일 경로
            output_dir: 디버깅용 이미지 저장 디렉토리 (선택적, None이면 저장하지 않음)
        
        Returns:
            추출된 이미지 정보 목록
        """
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        extracted_images: List[ImageInfo] = []
        
        try:
            # PDF 파일 열기
            doc = self.fitz.open(pdf_path)
            
            # 각 페이지에서 이미지 추출
            for page_idx, page in enumerate(doc):
                logging.info(f"페이지 {page_idx+1}/{len(doc)} 처리 중...")
                
                # 페이지에서 이미지 추출
                image_list = page.get_images(full=True)
                
                # 페이지에 이미지가 없는 경우
                if not image_list:
                    logging.info(f"페이지 {page_idx+1}에 이미지가 없습니다.")
                    continue
                
                # 각 이미지 처리
                extracted_images.extend(self._process_page_images(doc, page, page_idx, image_list, output_dir))
            
            return extracted_images
        
        except Exception as e:
            logging.error(f"PDF 이미지 추출 오류: {e}")
            return []
    
    def _process_page_images(self, doc, page, page_idx: int, image_list: list, output_dir: Optional[str]) -> List[ImageInfo]:
        """PDF 페이지의 이미지를 처리합니다.
        
        Args:
            doc: PyMuPDF 문서 객체
            page: 페이지 객체
            page_idx: 페이지 인덱스
            image_list: 페이지에서 추출한 이미지 목록
            output_dir: 디버깅용 이미지 저장 디렉토리
            
        Returns:
            추출된 이미지 정보 목록
        """
        page_images: List[ImageInfo] = []
        
        for img_idx, img_info in enumerate(image_list):
            xref = img_info[0]  # 이미지 참조 번호
            base_img = doc.extract_image(xref)
            image_bytes = base_img["image"]
            image_ext = base_img["ext"]
            
            # 이미지 메타데이터
            width = base_img.get("width", 0)
            height = base_img.get("height", 0)
            
            # 너무 작은 이미지는 건너뛰기
            if width < self.config.min_image_width or height < self.config.min_image_height:
                logging.debug(f"이미지가 너무 작아 건너뛰니다: {width}x{height}")
                continue
            
            # 이미지 식별자 생성
            img_identifier = f"page{page_idx+1:03d}_img{img_idx+1:03d}"
            
            # 이미지 바이트를 numpy 배열로 변환
            nparr = np.frombuffer(image_bytes, np.uint8)
            img_array = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            # 디버깅용 이미지 저장 (선택적)
            img_path = None
            if output_dir:
                img_filename = f"{img_identifier}.{image_ext}"
                img_path = os.path.join(output_dir, img_filename)
                with open(img_path, "wb") as img_file:
                    img_file.write(image_bytes)
                logging.info(f"이미지 추출 완료: {img_path} ({width}x{height})")
            else:
                logging.info(f"이미지 추출 완료: {img_identifier} ({width}x{height})")
            
            # 이미지 정보 저장
            image_info: ImageInfo = {
                "identifier": img_identifier,
                "image_data": img_array,  # 메모리에 이미지 데이터 저장
                "page": page_idx + 1,
                "index": img_idx + 1,
                "width": width,
                "height": height,
                "format": image_ext,
                "position": None,
                "path": img_path
            }
            
            # 이미지 위치 정보 추출 시도
            try:
                # 이미지 위치 찾기 (근사값)
                for img_rect in page.get_image_rects(xref):
                    image_info["position"] = {
                        "x": img_rect.x0,
                        "y": img_rect.y0,
                        "width": img_rect.width,
                        "height": img_rect.height
                    }
                    break
            except Exception as e:
                logging.warning(f"이미지 위치 정보 추출 실패: {e}")
            
            page_images.append(image_info)
        
        return page_images

class ImagePreprocessor:
    """OCR을 위한 이미지 전처리 클래스"""
    
    def __init__(self, config: PdfProcessingConfig):
        """ImagePreprocessor 초기화
        
        Args:
            config: PDF 처리 설정
        """
        self.config = config
    
    def preprocess(self, image_data: np.ndarray, identifier: str = None, is_table: bool = False, debug_dir: Optional[str] = None) -> Optional[np.ndarray]:
        """OCR 정확도 향상을 위한 이미지 전처리
        
        Args:
            image_data: 이미지 데이터 (numpy 배열)
            identifier: 이미지 식별자 (디버깅용)
            is_table: 표 이미지 여부 (표 이미지일 경우 특별한 전처리 적용)
            debug_dir: 디버깅용 이미지 저장 디렉토리 (선택적)
        
        Returns:
            전처리된 이미지 또는 오류 발생시 None
        """
        # 이미지 데이터 확인
        if image_data is None or len(image_data) == 0:
            logging.error(f"이미지 데이터가 없습니다.")
            return None
        
        try:
            # 이미지 리사이징 처리
            resized_img = self._resize_if_needed(image_data)
            
            # 그레이스케일 변환
            gray_img = self._convert_to_grayscale(resized_img)
            
            # 이미지 타입에 따른 처리
            if is_table:
                result = self._process_table_image(gray_img)
            else:
                result = self._process_normal_image(gray_img)
            
            # 디버깅용 저장
            self._save_debug_image(result, identifier, debug_dir)
            
            return result
        
        except Exception as e:
            logging.error(f"이미지 전처리 오류: {e}")
            return None
    
    def _resize_if_needed(self, image: np.ndarray) -> np.ndarray:
        """필요한 경우 이미지 크기 조정"""
        img = image.copy()  # 원본 데이터 보존
        h, w = img.shape[:2]
        max_dim = self.config.max_image_dimension
        
        if h > max_dim or w > max_dim:
            scale = min(max_dim/h, max_dim/w)
            img = cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_AREA)
            logging.info(f"이미지 리사이징: {w}x{h} -> {int(w*scale)}x{int(h*scale)}")
        
        return img
    
    def _convert_to_grayscale(self, image: np.ndarray) -> np.ndarray:
        """이미지를 그레이스케일로 변환"""
        if len(image.shape) == 3 and image.shape[2] == 3:
            return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        return image.copy()
    
    def _process_table_image(self, gray_image: np.ndarray) -> np.ndarray:
        """표 이미지 전처리"""
        # 1. 경색성 개선 - CLAHE 적용 (적응형 히스토그램 평활화)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray_image)
        
        # 2. 경미한 노이즈 제거 - 작은 커널 사용
        denoised = cv2.fastNlMeansDenoising(enhanced, None, 10, 7, 21)
        
        # 3. 경색성 강화 - 어둠게 하지 않는 정도의 경색성 강화
        alpha = 1.2  # 대비 증가 계수
        beta = 10    # 밝기 증가 계수
        return cv2.convertScaleAbs(denoised, alpha=alpha, beta=beta)
    
    def _process_normal_image(self, gray_image: np.ndarray) -> np.ndarray:
        """일반 이미지 전처리"""
        # 1. 경색성 개선 - CLAHE 적용
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray_image)
        
        # 2. 경미한 가우시안 블러링 - 노이즈 제거
        blurred = cv2.GaussianBlur(enhanced, (3, 3), 0)
        
        # 3. 선택적 색상 강화 - 텍스트와 배경의 대비 강화
        alpha = 1.1  # 대비 증가 계수
        beta = 5     # 밝기 증가 계수
        return cv2.convertScaleAbs(blurred, alpha=alpha, beta=beta)
    
    def _save_debug_image(self, image: np.ndarray, identifier: Optional[str], debug_dir: Optional[str]) -> None:
        """디버깅용 이미지 저장"""
        if debug_dir and identifier:
            os.makedirs(debug_dir, exist_ok=True)
            preprocessed_path = os.path.join(debug_dir, f"{identifier}_preprocessed.png")
            cv2.imwrite(preprocessed_path, image)
            logging.debug(f"전처리된 이미지 저장: {preprocessed_path}")

# ─── 헬퍼 함수들 ────────────────────────────────────────
def numpy_to_pil(img_array: np.ndarray) -> Image.Image:
    """OpenCV BGR 배열을 PIL Image(RGB)로 변환"""
    rgb = cv2.cvtColor(img_array, cv2.COLOR_BGR2RGB)
    return Image.fromarray(rgb)

def image_to_temp_pdf(pil_img: Image.Image) -> Path:
    """PIL Image를 임시 PDF로 저장하고 Path 반환"""
    tmp = Path(tempfile.gettempdir()) / f"{uuid.uuid4()}.pdf"
    pil_img.convert("RGB").save(tmp, "PDF", resolution=300.0)
    return tmp

# ─── 기존 run_ocr_on_images 함수 교체 ──────────────────────
def run_ocr_on_images(
    image_info_list: List[Dict[str, Any]],
    lang: str = "korean",
    table_score_th: float = 0.5
) -> Dict[str, Dict[str, Any]]:
    """PaddleOCR와 Camelot으로 이미지 OCR 및 테이블 추출"""
    # 1) OCR 모델 초기화 (한 번만)
    ocr = PaddleOCR(lang=lang, use_angle_cls=True, show_log=False)

    ocr_results: Dict[str, Dict[str, Any]] = {}

    for img_info in image_info_list:
        identifier = img_info["identifier"]
        img_cv = img_info["image_data"]  # BGR numpy array

        # 2) OCR 수행
        ocr_res = ocr.ocr(img_cv, cls=True)
        ocr_text = " ".join(w[1][0] for line in ocr_res for w in line) if ocr_res else ""

        # 3) Camelot으로 테이블 검출
        tables_list: List[List[List[str]]] = []
        try:
            pil_img = numpy_to_pil(img_cv)
            tmp_pdf = image_to_temp_pdf(pil_img)
            camelot_tables = camelot.read_pdf(str(tmp_pdf), pages="1", flavor="stream")
            for tb in camelot_tables:
                if tb.accuracy >= table_score_th * 100:
                    tables_list.append(tb.df.values.tolist())
            tmp_pdf.unlink(missing_ok=True)
        except Exception:
            # 테이블 추출 실패 시 무시
            pass

        # 4) 결과 저장
        ocr_results[identifier] = {
            "ocr_text": ocr_text,
            "tables": tables_list
        }

    return ocr_results


def convert_pdf_to_jsonl(pdf_path: str, output_path: str) -> Tuple[int, int]:
    """PDF 파일을 JSONL 형식으로 변환합니다.

    Args:
        pdf_path: 변환할 PDF 파일의 경로
        output_path: 출력 JSONL 파일의 경로
        
    Returns:
        총 항목 수와 텍스트 항목 수를 포함한 튜플
    """
    logging.info(f"PDF 변환 시작: {pdf_path}")
    
    # Tesseract OCR 데이터 경로 설정
    os.environ['TESSDATA_PREFIX'] = '/usr/share/tesseract-ocr/4.00/tessdata/'
    table_struct_options = TableStructureOptions(mode=TableFormerMode.ACCURATE)

    pipeline_options = PdfPipelineOptions(
        do_ocr=True,  # OCR 활성화
        ocr_options=TesseractOcrOptions(lang=["eng", "kor"]),  # 영어와 한국어 OCR 설정
        extract_text_from_pdf=True,  # PDF에서 텍스트 직접 추출 활성화 (OCR 전에 시도)
        do_table_structure=True,  # 표 구조 인식 활성화
        table_structure_options=table_struct_options,
        force_ocr=False,  # 텍스트 추출이 가능한 경우 OCR 건너뛰기
        use_pdf_text_extraction_fallback=True  # PDF 텍스트 추출 실패 시 대체 방법 사용
    )

    converter = DocumentConverter(format_options={
        InputFormat.PDF: PdfFormatOption(
            backend=PyPdfiumDocumentBackend,  
            pipeline_options=pipeline_options 
        )
    })

    result = converter.convert(source=pdf_path)
    doc = result.document

    # 추출된 아이템 수 추적
    item_counts = {}
    total_items = 0
    text_items = 0
    
    logging.info(f"변환 완료, 결과 처리 시작")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        for item_tuple in doc.iterate_items(): 
            actual_item = item_tuple[0]
            if not isinstance(actual_item, DocItem):
                continue
                
            total_items += 1
            item_type = actual_item.label
            item_counts[item_type] = item_counts.get(item_type, 0) + 1
            
            # 위치 정보 추출
            position_data = None
            if hasattr(actual_item, 'prov') and actual_item.prov and isinstance(actual_item.prov, list) and len(actual_item.prov) > 0:
                first_prov = actual_item.prov[0]
                if hasattr(first_prov, 'bbox') and first_prov.bbox:
                    try:
                        position_data = first_prov.bbox.model_dump()
                    except AttributeError:
                        # Pydantic v1 호환성
                        if hasattr(first_prov.bbox, 'dict'):
                            position_data = first_prov.bbox.dict()
            
            # 텍스트 콘텐츠 추출
            has_text = hasattr(actual_item, 'text') and actual_item.text and isinstance(actual_item.text, str)
            
            # 텍스트 항목 처리 (모든 텍스트 포함 항목 처리)
            if has_text:
                text_items += 1
                json_output = {
                    "type": "text",
                    "content": actual_item.text,
                    "label": actual_item.label,  # 원래 라벨 유지
                    "position": position_data
                }
                f.write(json.dumps(json_output, ensure_ascii=False) + '\n')
            
            # 이미지 항목 처리
            elif actual_item.label == "picture" or actual_item.label == "image":
                ocr_text = getattr(actual_item, 'text', "")
                json_output = {
                    "type": "image",
                    "position": position_data,
                    "ocr_text": ocr_text if ocr_text else ""
                }
                f.write(json.dumps(json_output, ensure_ascii=False) + '\n')
            
            # 테이블 항목 처리
            elif actual_item.label == "table":
                markdown_table = ""
                if hasattr(actual_item, 'export_to_markdown'):
                    try:
                        markdown_table = actual_item.export_to_markdown(doc=doc)
                    except Exception as e:
                        logging.warning(f"테이블 마크다운 변환 실패: {e}")
                
                table_text = getattr(actual_item, 'text', "")
                json_output = {
                    "type": "table",
                    "markdown_content": markdown_table,
                    "text_content": table_text if table_text else "",
                    "position": position_data
                }
                f.write(json.dumps(json_output, ensure_ascii=False) + '\n')
    
    logging.info(f"총 {total_items}개 항목 처리 완료, 텍스트 항목: {text_items}개")
    logging.info(f"항목 유형별 개수: {item_counts}")
    return total_items, text_items

def extract_raw_text_from_pdf(pdf_path: str, output_path: str) -> int:
    """PDF에서 가능한 모든 텍스트를 추출하는 보조 함수
    
    Args:
        pdf_path: PDF 파일 경로
        output_path: 추출된 텍스트를 저장할 파일 경로
        
    Returns:
        추출된 텍스트 항목 수
    """
    logging.info(f"PDF에서 원시 텍스트 추출 시작: {pdf_path}")
    
    # Tesseract OCR 데이터 경로 설정
    os.environ['TESSDATA_PREFIX'] = '/usr/share/tesseract-ocr/4.00/tessdata/'
    
    # 텍스트 추출에 최적화된 파이프라인 옵션
    pipeline_options = PdfPipelineOptions(
        do_ocr=True,
        ocr_options=TesseractOcrOptions(lang=["eng", "kor"]),
        extract_text_from_pdf=True,
        force_ocr=True,  # 모든 페이지에 OCR 강제 적용
        use_pdf_text_extraction_fallback=True
    )
    
    converter = DocumentConverter(format_options={
        InputFormat.PDF: PdfFormatOption(
            backend=PyPdfiumDocumentBackend,
            pipeline_options=pipeline_options
        )
    })
    
    result = converter.convert(source=pdf_path)
    doc = result.document
    
    all_text = []
    for item_tuple in doc.iterate_items():
        actual_item = item_tuple[0]
        if hasattr(actual_item, 'text') and actual_item.text:
            all_text.append(actual_item.text)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(all_text))
    
    logging.info(f"원시 텍스트 추출 완료: {len(all_text)}개 텍스트 항목 추출")
    return len(all_text)

def update_jsonl_with_ocr(jsonl_path: str, image_ocr_results: Dict[str, List[Dict[str, Any]]], output_jsonl_path: Optional[str] = None) -> str:
    """JSONL 파일의 이미지 항목에 OCR 결과를 추가합니다.
    
    Args:
        jsonl_path: 원본 JSONL 파일 경로
        image_ocr_results: OCR 결과 디클래스 (이미지 식별자를 키로 사용)
        output_jsonl_path: 출력 JSONL 파일 경로, None이면 자동 생성
    
    Returns:
        업데이트된 JSONL 파일 경로
    """
    if output_jsonl_path is None:
        output_jsonl_path = jsonl_path.replace('.jsonl', '_with_ocr.jsonl')
    
    # OCR 결과 저장 (원본 텍스트 및 전체 결과)
    image_ocr_texts = {}  # 텍스트만 저장
    image_ocr_full = {}   # 전체 OCR 결과 저장
    
    # 실제 추출된 이미지 ID 집합 (실제 존재하는 이미지만 처리하기 위해)
    extracted_image_ids = set(image_ocr_results.keys())
    logging.info(f"실제 추출된 이미지 ID 수: {len(extracted_image_ids)}")
    
    # OCR 결과 처리
    for img_id, ocr_results in image_ocr_results.items():
        if ocr_results:  # 결과가 있는 경우에만 처리
            texts = [item['text'] for item in ocr_results if item.get('text')]
            if texts:  # 텍스트가 있는 경우에만 처리
                image_ocr_texts[img_id] = ' '.join(texts)
                image_ocr_full[img_id] = ocr_results
            else:
                image_ocr_texts[img_id] = ""
                image_ocr_full[img_id] = []
        else:
            image_ocr_texts[img_id] = ""
            image_ocr_full[img_id] = []
    
    # OCR 결과 로깅
    logging.info(f"OCR 결과 사전 처리: {len(image_ocr_texts)}개 이미지, 텍스트 있는 이미지: {sum(1 for t in image_ocr_texts.values() if t)}개")
    
    # 이미지 식별자에서 페이지 번호와 이미지 번호 추출
    page_img_map = {}
    for img_id in image_ocr_texts.keys():
        # 식별자 형식: page{page_num}_img{img_num}
        parts = img_id.split('_')
        if len(parts) >= 2:
            page_num = parts[0].replace('page', '')
            img_num = parts[1].replace('img', '')
            try:
                page_num = int(page_num)
                img_num = int(img_num)
                if page_num not in page_img_map:
                    page_img_map[page_num] = {}
                page_img_map[page_num][img_num] = img_id
            except ValueError:
                logging.warning(f"이미지 식별자 파싱 오류: {img_id}")
    
    # 이미지 항목 카운터 초기화
    img_counter = {}  # 페이지별 이미지 카운터
    
    # JSONL 파일 읽기 및 업데이트
    updated_lines = []
    updated_count = 0
    
    # 첫 번째 패스: 이미지 항목에 식별자 추가
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 페이지 번호 추적
    current_page = 1
    page_positions = []  # 페이지 시작 위치 추적
    
    # 페이지 경계 찾기 (페이지 번호가 변경되는 위치)
    for line_idx, line in enumerate(lines):
        try:
            item = json.loads(line.strip())
            position = item.get('position', {})
            
            # 페이지 경계 감지 (t 값이 갑자기 크게 증가하면 새 페이지로 간주)
            if position and line_idx > 0:
                prev_item = json.loads(lines[line_idx-1].strip())
                prev_pos = prev_item.get('position', {})
                
                if prev_pos and position.get('t', 0) > 0 and prev_pos.get('t', 0) > 0:
                    # t 값이 갑자기 크게 증가하면 새 페이지로 간주 (페이지 경계)
                    if position.get('t', 0) - prev_pos.get('t', 0) > 500:  # 임계값 조정 가능
                        current_page += 1
                        page_positions.append(line_idx)
            
        except (json.JSONDecodeError, KeyError):
            continue
    
    # 페이지 번호 할당
    page_ranges = []
    for i in range(len(page_positions)):
        if i == 0:
            page_ranges.append((0, page_positions[i] - 1, 1))
        else:
            page_ranges.append((page_positions[i-1], page_positions[i] - 1, i + 1))
    
    if page_positions:
        page_ranges.append((page_positions[-1], len(lines) - 1, len(page_positions) + 1))
    else:
        # 페이지 경계를 찾지 못한 경우 모든 항목을 페이지 1로 간주
        page_ranges.append((0, len(lines) - 1, 1))
    
    # 추출되지 않은 이미지 항목을 제거하기 위한 새로운 라인 목록
    new_lines = []
    removed_count = 0
    
    # 각 페이지 내에서 이미지 항목 식별 및 번호 할당
    for start_idx, end_idx, page_num in page_ranges:
        img_count = 0
        
        # 페이지 내 항목 처리
        for line_idx in range(start_idx, end_idx + 1):
            try:
                item = json.loads(lines[line_idx].strip())
                
                # 이미지 항목인 경우
                if item.get('type') == 'image':
                    img_count += 1
                    img_id = f"page{page_num:03d}_img{img_count:03d}"
                    
                    # 실제 추출된 이미지인지 확인
                    if img_id in extracted_image_ids:
                        # 실제 추출된 이미지인 경우만 포함
                        item['image_id'] = img_id
                        item['ocr_text'] = image_ocr_texts[img_id]
                        item['ocr_results'] = image_ocr_full[img_id]
                        item['extracted'] = True
                        new_lines.append(json.dumps(item, ensure_ascii=False) + '\n')
                        updated_count += 1
                    else:
                        # 실제 추출되지 않은 이미지는 제외
                        removed_count += 1
                        logging.info(f"이미지 항목 제거: {img_id} (실제 추출되지 않음)")
                else:
                    # 이미지가 아닌 항목은 그대로 유지
                    new_lines.append(lines[line_idx])
            except json.JSONDecodeError:
                # JSON 파싱 오류가 있는 라인은 그대로 유지
                new_lines.append(lines[line_idx])
                continue
    
    # 기존 lines 배열을 새로운 배열로 교체
    lines = new_lines
    logging.info(f"실제 추출되지 않은 이미지 항목 {removed_count}개 제거됨")
    
    # 업데이트된 JSONL 파일 저장
    with open(output_jsonl_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)
    
    logging.info(f"OCR 결과가 추가된 JSONL 파일 저장: {output_jsonl_path}")
    logging.info(f"총 {updated_count}개 이미지 항목 업데이트됨")
    
    # OCR 결과 JSON 파일로 저장 (디버깅용)
    ocr_results_path = os.path.join(os.path.dirname(output_jsonl_path), 'ocr_results.json')
    with open(ocr_results_path, 'w', encoding='utf-8') as f:
        json.dump(image_ocr_results, f, ensure_ascii=False, indent=2)
    
    logging.info(f"OCR 결과 JSON 파일 저장: {ocr_results_path}")
    
    return output_jsonl_path

def process_pdf_with_ocr(pdf_path: str, output_dir: str = "output", jsonl_output_path: Optional[str] = None, debug_dir: Optional[str] = None, detect_tables: bool = True) -> Tuple[str, int, int, int]:
    """PDF를 처리하고 이미지 OCR을 적용하는 통합 함수
    
    Args:
        pdf_path: PDF 파일 경로
        output_dir: 출력 디렉토리
        jsonl_output_path: JSONL 출력 파일 경로, None이면 자동 생성
        debug_dir: 디버깅용 이미지 저장 디렉토리 (선택적)
        detect_tables: 표 감지 및 특별 처리 여부
        
    Returns:
        (JSONL 파일 경로, 총 항목 수, 텍스트 항목 수, 이미지 수) 포함한 튜플
    """
    # 설정 객체 생성
    config = PdfProcessingConfig(
        tessdata_prefix='/usr/share/tesseract-ocr/4.00/tessdata/',
        use_korean=True,
        detect_tables=detect_tables
    )
    
    # 클래스 인스턴스 생성
    image_extractor = PdfImageExtractor(config)
    image_preprocessor = ImagePreprocessor(config)
    
    # 출력 디렉토리 생성
    os.makedirs(output_dir, exist_ok=True)
    
    # 디버깅 디렉토리 생성 (지정된 경우)
    if debug_dir:
        os.makedirs(debug_dir, exist_ok=True)
    
    # 기본 JSONL 출력 경로 설정
    if jsonl_output_path is None:
        jsonl_output_path = os.path.join(output_dir, "output.jsonl")
    
    # 1. Docling으로 기본 처리
    logging.info(f"1. Docling으로 PDF 처리 시작: {pdf_path}")
    total_items, text_items = convert_pdf_to_jsonl(pdf_path, jsonl_output_path)
    logging.info(f"Docling 처리 완료: 총 {total_items}개 항목, {text_items}개 텍스트 항목")
    
    # 2. PyMuPDF로 PDF에서 이미지 추출 (메모리에 저장)
    logging.info("2. PDF에서 이미지 추출 시작")
    extracted_images = image_extractor.extract_images(pdf_path, debug_dir)
    logging.info(f"이미지 추출 완료: {len(extracted_images)}개 이미지")
    
    # 3. PaddleOCR로 이미지 텍스트 추출
    if extracted_images:
        logging.info("3. 추출된 이미지에 OCR 적용 시작")
        # 이미지 전처리를 위해 run_ocr_on_images 함수 수정
        ocr_results = run_ocr_on_images(
            extracted_images, 
            use_preprocessing=True,
            use_korean=config.use_korean, 
            debug_dir=debug_dir,
            detect_tables=config.detect_tables,
            image_preprocessor=image_preprocessor  # 이미지 전처리기 전달
        )
        
        # OCR 결과 저장
        ocr_json_path = os.path.join(output_dir, "ocr_results.json")
        with open(ocr_json_path, 'w', encoding='utf-8') as f:
            json.dump(ocr_results, f, ensure_ascii=False, indent=2)
        logging.info(f"OCR 결과 JSON 저장: {ocr_json_path}")
        
        # 4. JSONL 파일 업데이트
        logging.info("4. JSONL 파일에 OCR 결과 추가")
        updated_jsonl = update_jsonl_with_ocr(jsonl_output_path, ocr_results)
        logging.info(f"JSONL 업데이트 완료: {updated_jsonl}")
        
        return updated_jsonl, total_items, text_items, len(extracted_images)
    else:
        logging.warning("PDF에서 이미지를 추출할 수 없습니다.")
        return jsonl_output_path, total_items, text_items, 0

def main() -> None:
    """PDF 처리 및 OCR 적용 메인 함수"""
    # 경로 설정
    pdf_file_path = "test_data/content.pdf"  
    output_dir = "enhanced_output"
    debug_dir = os.path.join(output_dir, "debug_images")  # 디버깅용 이미지 저장 디렉토리 (선택적)
    
    # 디렉토리 생성
    if not os.path.exists("test_data"):
        os.makedirs("test_data")
    
    # 테스트용 PDF 생성 (필요한 경우)
    if not os.path.exists(pdf_file_path):
        try:
            from pypdf import PdfWriter
            writer = PdfWriter()
            writer.add_blank_page(width=210, height=297) 
            with open(pdf_file_path, "wb") as f_dummy:
                writer.write(f_dummy)
            print(f"테스트용 더미 PDF 생성 완료: {pdf_file_path}")
        except ImportError:
            print(f"'{pdf_file_path}'에 유효한 PDF를 배치하거나 더미 PDF 생성을 위해 pypdf를 설치하세요.")
            return
    
    # Tesseract OCR 데이터 경로 설정 (메모리에 저장)
    os.environ['TESSDATA_PREFIX'] = '/usr/share/tesseract-ocr/4.00/tessdata/'
    
    # PDF 처리 및 OCR 적용
    try:
        jsonl_path, total_items, text_items, image_items = process_pdf_with_ocr(
            pdf_file_path, 
            output_dir, 
            jsonl_output_path="output.jsonl", 
            debug_dir=debug_dir,  # 디버깅용 이미지 저장 디렉토리 (선택적)
            detect_tables=True  # 표 감지 및 특별 처리 활성화
        )
        print(f"\n===== 처리 완료 =====")
        print(f"PDF 파일: {pdf_file_path}")
        print(f"JSONL 출력 파일: {jsonl_path}")
        print(f"총 항목 수: {total_items}개")
        print(f"텍스트 항목 수: {text_items}개")
        print(f"추출된 이미지 수: {image_items}개")
        print(f"\n이제 PDF의 모든 텍스트와 이미지 OCR 결과가 JSONL 파일에 포함되어 있습니다.")
        print(f"출력 디렉토리: {os.path.abspath(output_dir)}")
    except Exception as e:
        logging.error(f"PDF 처리 중 오류 발생: {e}")
        print(f"PDF 처리 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()