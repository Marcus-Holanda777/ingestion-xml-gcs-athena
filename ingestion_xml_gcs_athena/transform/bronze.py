from ingestion_xml_gcs_athena import (
    BUCKET_NAME,
    RAW_BUCKET,
    BRONZE_BUCKET
)
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from ingestion_xml_gcs_athena.cloud import Storage
from ingestion_xml_gcs_athena.parsexml import ParseXml
from ingestion_xml_gcs_athena.write import Write
from ingestion_xml_gcs_athena import (
    BUCKET_NAME,
    RAW_BUCKET
)
from datetime import datetime
from google.cloud.storage import Blob
import os
import logging
import io


logger = logging.getLogger(__name__)


def comand_bronze(list_raw: list[str] = []) -> list[str]:
    logger.info(f'Start logs BRONZE: {datetime.now()}')

    storage = Storage()

    if not list_raw:
        list_raw = storage.list_files(BUCKET_NAME, RAW_BUCKET)
    else:
        logger.info(f'List raw: {len(list_raw)}')
    
    def inner_bronze(blob: Blob | str):
        if not isinstance(blob, Blob):
            blob = storage.get_blob_file(BUCKET_NAME, blob)

        if not blob.name.endswith('/'):

            file_bytes = io.BytesIO(blob.download_as_bytes())
            __, controle, *__, name = blob.name.split('/')
            
            file_xml = ParseXml(controle, file_bytes)
            
            data = (
                file_xml.df()
                .assign(
                    controle = controle.strip().lower(),
                    status = int(name.split('_')[1]),
                    year = lambda _df: _df.dh_emi.dt.year,
                    month = lambda _df: _df.dh_emi.dt.month
                )
            )
            
            key_to = f"{BRONZE_BUCKET}/{'/'.join(blob.name.split('/')[1:])}"
            write = Write(data, BUCKET_NAME, storage)
            out_key = write.write_parquet_buffer(key_to)

            logger.info(f'Key bronze to: {out_key}')

            return out_key
    
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        rst = executor.map(inner_bronze, list_raw)
    
    logger.info(f'End logs BRONZE: {datetime.now()}')
    
    return list(rst)