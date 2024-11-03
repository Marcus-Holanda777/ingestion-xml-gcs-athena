from ingestion_xml_gcs_athena.connect import iter_notes
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from ingestion_xml_gcs_athena.cloud import Storage
from ingestion_xml_gcs_athena.parsexml import FileXml
from ingestion_xml_gcs_athena import (
    BUCKET_NAME,
    RAW_BUCKET
)
import os
import logging


logger = logging.getLogger(__name__)


def comand_raw(
    start: datetime,
    end: datetime,
    notes: list[str],
    credentials: str = None
) -> list[str]:
    
    logger.info(f'Start logs RAW: {datetime.now()}')
    
    storage = Storage(
        credentials=credentials
    )
    
    gen_notas = [
        *enumerate(
            iter_notes(
                tips=notes,
                start=start,
                end=end
            )
        )
    ]

    def upload_bytes_xml(datas: tuple) -> str:
        pos, data = datas
        file = FileXml(*data)
        file_to, file_bytes = file.export_file_xml()
        
        file_raw_to = f'{RAW_BUCKET}/{file_to}'
        storage.upload_streaming(file_bytes, BUCKET_NAME, file_raw_to)
        logger.info(f'File: {file_raw_to}, Pos: {pos}')

        return file_raw_to
    
    logging.info(f'Total list raw: {len(gen_notas)}')
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        rst = executor.map(upload_bytes_xml, gen_notas)
    
    logger.info(f'End logs RAW: {datetime.now()}')
    
    return list(rst)