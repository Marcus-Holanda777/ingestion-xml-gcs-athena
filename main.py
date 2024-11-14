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


DEFAULT_END = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
DEFAULT_START = DEFAULT_END.replace(day=1)

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
    'notes',
    help='Lista com o tipo de nota fiscal',
    metavar='[INCINERACAO, ...]',
    type=str,
    nargs='+'
)
parser.add_argument(
    '-s',
    '--start', 
    help=f"Data inicial DEFAULT: {DEFAULT_START:%d/%m/%Y}", 
    type=lambda d: datetime.strptime(d, "%d/%m/%Y"),
    default=DEFAULT_START,
)
parser.add_argument(
    '-e',
    '--end',
    help=f"Data final DEFAULT: {DEFAULT_END:%d/%m/%Y}",
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
        logging.info(f'Ops: {args.start=}, {args.end=}, {args.notes}')
        
        comand_raw(
            start=args.start,
            end=args.end,
            notes=args.notes
        )