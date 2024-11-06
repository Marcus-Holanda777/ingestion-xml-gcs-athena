from ingestion_xml_gcs_athena.writedelta import WriteDelta
from ingestion_xml_gcs_athena import (
    BUCKET_NAME,
    BRONZE_BUCKET
)
from ingestion_xml_gcs_athena.cloud import Storage
import logging
from datetime import datetime
from itertools import islice


logger = logging.getLogger(__name__)


def comand_silver() -> None:
    # TODO: Iterar a cada 1_000 arquivos
    logger.info(f'Start logs SILVER: {datetime.now()}')
    
    st = Storage()
    files = st.list_files(BUCKET_NAME, BRONZE_BUCKET)
    dt = WriteDelta(BUCKET_NAME, 'notas')

    while lotes := list(
        filter(
            lambda s: not s.endswith('/'), 
            map(lambda b: b.name, islice(files, 100))
        )
    ):
        is_delta = dt.delta_is_table()

        if is_delta:
            logger.info(f'Merge delta table: {datetime.now()}, lotes = {len(lotes)}')
            dt.delta_merge(lotes)
        else:
            logger.info(f'Create delta table: {datetime.now()}, lotes = {len(lotes)}')
            dt.delta_write(lotes)
        
    dt.delta_optimize()
    logger.info(f'End logs SILVER: {datetime.now()}')