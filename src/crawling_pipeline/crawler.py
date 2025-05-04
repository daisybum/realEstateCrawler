from io import BytesIO

def fetch_url_to_bytesio(url: str) -> BytesIO:
    """
    주어진 URL의 콘텐츠(첨부파일 등)를 메모리(BytesIO)로 다운로드합니다.
    모든 입출력은 BytesIO 기반으로 처리됩니다.
    :param url: 다운로드할 자원(URL) 경로
    :return: 요청한 파일 데이터를 담은 BytesIO 객체
    """
    response = requests.get(url)
    return BytesIO(response.content)

def fetch_attachments(urls: list[str]) -> list[BytesIO]:
    """
    URL 리스트에 있는 첨부파일들을 순회하며 크롤링하여 BytesIO 리스트를 반환합니다.
    :param urls: 첨부파일이 포함된 URL들의 리스트
    :return: 각 첨부파일 데이터를 담은 BytesIO 객체들의 리스트
    """
    return [fetch_url_to_bytesio(url) for url in urls]
