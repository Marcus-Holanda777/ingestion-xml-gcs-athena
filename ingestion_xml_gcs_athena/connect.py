from contextlib import contextmanager
import pyodbc
from datetime import datetime

from dotenv import load_dotenv
import os


load_dotenv()


@contextmanager
def do_connect(*args, **kwargs):
    con = pyodbc.connect(*args, **kwargs)
    con.autocommit = True

    con.set_attr(
        pyodbc.SQL_ATTR_TXN_ISOLATION,
        pyodbc.SQL_TXN_READ_UNCOMMITTED
    )

    con.autocommit = False
    cursor = con.cursor()

    try:
        yield cursor
    except Exception:
        con.rollback()
        raise
    else:
        con.commit()
    finally:
        cursor.close()
        con.close()


def iter_notes(
    *,
    tips: list[str],
    start: datetime,
    end: datetime,
    font: str = 'dbnfe'
):
    DRIVER = (
        'Driver={ODBC Driver 18 for Sql Server};'
        f'Server={os.getenv('SERVER')};'
        f'Database={os.getenv('DATABASE')};'
        'TrustServerCertificate=Yes;'
        'Authentication=ActiveDirectoryIntegrated;'
    )
    
    query = os.getenv('QUERY')
    params = {
        'tips': ','.join(f'{c!r}' for c in tips),
        'start': start,
        'end': end,
        'font': font
    }

    with do_connect(DRIVER) as cursor:
        cursor.execute(query.format(**params))
        
        for row in cursor:
            yield row
