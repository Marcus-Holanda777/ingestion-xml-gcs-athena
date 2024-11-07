from ingestion_xml_gcs_athena.transform.raw import comand_raw
from datetime import datetime
import logging
from datetime import datetime
import argparse
import sys
from dateutil.relativedelta import relativedelta


logging.basicConfig(
    level=logging.INFO
)


DEFAULT_END = datetime.now()
DEFAULT_START = datetime.now().replace(day=1)

if DEFAULT_END.day <= 5:
    DEFAULT_START = DEFAULT_START - relativedelta(months=1) 


parser = argparse.ArgumentParser(
    prog='Export XML',
    description='Extrai dados xml do sql server para Athena aws',
    epilog="Des. Marcus holanda, System: %(prog)s"
)

parser.add_argument(
    '-v', 
    '--version', 
    action='version'
)
parser.add_argument(
    '-s',
    '--start', 
    help="Data inicial", 
    type=lambda d: datetime.strptime(d, "%d/%m/%Y"),
    default=DEFAULT_START
)
parser.add_argument(
    '-e',
    '--end',
    help="Data final",
    type=lambda d: datetime.strptime(d, "%d/%m/%Y"),
    default=DEFAULT_END
)


if __name__ == '__main__':
    try:

        args = parser.parse_args()
    
    except Exception:
        parser.print_help()
        sys.exit()

    else:
        comand_raw(
            start=args.start,
            end=args.end,
            notes=['INCINERACAO', 'ESTORNO-INCINERACAO']
        )