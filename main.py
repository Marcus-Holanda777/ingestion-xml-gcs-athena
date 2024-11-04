from ingestion_xml_gcs_athena.transform.raw import comand_raw
from datetime import datetime
import logging


logging.basicConfig(
    level=logging.INFO
)


if __name__ == '__main__':
    list_raw = comand_raw(
        start=datetime(2024, 4, 8),
        end=datetime(2024, 4, 30),
        notes=['INCINERACAO', 'ESTORNO-INCINERACAO']
    )