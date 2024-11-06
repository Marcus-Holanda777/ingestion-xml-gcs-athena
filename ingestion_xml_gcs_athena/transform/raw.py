from ingestion_xml_gcs_athena.connect import iter_notes
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from ingestion_xml_gcs_athena.cloud import Storage
from ingestion_xml_gcs_athena.parsexml import FileXml
from ingestion_xml_gcs_athena import (
    BUCKET_NAME,
    RAW_BUCKET,
    BRONZE_BUCKET,
    LOTES_BUCKET
)
import os
import logging
import json
from itertools import islice
from time import sleep


logger = logging.getLogger(__name__)


def export_lotes_json(list_raw: list[str]) -> None:
    st = Storage()

    def mod_path(path: str):
        cam = '/'.join(path.split('/')[1:])
        return BRONZE_BUCKET + '/' + os.path.splitext(cam)[0] + '.parquet'
    
    data = {'data': [*map(mod_path, list_raw)]}
    blob_name = f'{LOTES_BUCKET}/{datetime.now():%Y%m%d_%H%M%S}.json'

    st.upload_file(
        json.dumps(data, indent=4),
        bucket_name=BUCKET_NAME,
        blob_name=blob_name,
        content_type='application/json'
    )

    return blob_name


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

    gen_notas = iter(gen_notas)
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        while lotes := list(islice(gen_notas, 1_000)):
            rst = executor.map(upload_bytes_xml, lotes)
            logger.info(f'End logs RAW: {datetime.now()}')
            list_raw = list(rst)

            json_to = export_lotes_json(list_raw)
            logger.info(f'Export json: {datetime.now()}, {json_to=}, sleep 60 seconds ...')
            sleep(60.0)