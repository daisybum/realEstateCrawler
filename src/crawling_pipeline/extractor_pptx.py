from io import BytesIO

def extract_text_from_pptx(pptx_bytes: BytesIO) -> str:
    """
    PPTX 파일(BytesIO)에서 텍스트를 추출합니다.
    모든 입출력은 BytesIO 기반으로 처리됩니다.
    :param pptx_bytes: PPTX 파일 내용을 담은 BytesIO 객체
    :return: 추출된 텍스트 문자열
    """
    pass

def extract_images_from_pptx(pptx_bytes: BytesIO) -> list[BytesIO]:
    """
    PPTX 파일에서 삽입된 이미지들을 추출합니다.
    :param pptx_bytes: PPTX 파일 내용을 담은 BytesIO 객체
    :return: 추출된 이미지들을 담은 BytesIO 객체 리스트
    """
    pass
