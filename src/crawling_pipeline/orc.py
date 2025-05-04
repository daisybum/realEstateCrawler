from io import BytesIO

def ocr_image(image_bytes: BytesIO, lang: str = "kor+eng") -> str:
    """
    이미지(BytesIO)에 대해 OCR 수행 후 텍스트 리턴.
    :param image_bytes: 이미지 데이터를 담은 BytesIO
    :param lang: Tesseract OCR 언어 설정
    :return: 추출된 텍스트
    """
    pass
