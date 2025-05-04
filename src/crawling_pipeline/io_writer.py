# src/weolbu_pretrain_pipeline/io_writer.py
import gzip
import json
from pathlib import Path
from typing import Iterable

def write_jsonl_gzip(data_iter: Iterable[dict], output_path: str, mode: str = "wb") -> None:
    """
    JSONL 데이터를 gzip 파일에 기록합니다. 'ab' 모드로 호출 시 이어쓰기(append) 가능합니다.
    :param data_iter: JSON 직렬화 가능한 dict의 반복자
    :param output_path: 출력 gzip 파일 경로
    :param mode: 파일 모드 ("wb" | "ab")
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output_path, mode) as gz_file:
        for item in data_iter:
            line = json.dumps(item, ensure_ascii=False) + "\n"
            gz_file.write(line.encode("utf-8"))
