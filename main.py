from ingestion_xml_gcs_athena.transform.raw import comand_raw
from datetime import datetime
import logging
from datetime import datetime


logging.basicConfig(
    level=logging.INFO
)


if __name__ == '__main__':
    comand_raw(
        start=datetime(2024, 7, 1),
        end=datetime(2024, 10, 31),
        notes=['INCINERACAO', 'ESTORNO-INCINERACAO']
    )