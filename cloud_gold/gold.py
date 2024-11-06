from writedelta import WriteDelta
from cloud import Storage
import json
from athena_mvsh import (
    Athena, 
    CursorParquetDuckdb
)
import pyarrow.dataset as ds
import os
from datetime import datetime
from contextlib import chdir


def comand_gold(bucket_name, key) -> None:

    print(f'Start table export ATHENA, {datetime.now()}')
    
    # TODO: Leitura do periodo
    st = Storage()
    blob = st.get_blob_file(bucket_name, key)
    file_json = json.loads(blob.download_as_text())

    start = datetime.strptime(file_json.get('start'), '%Y-%m-%d %H:%M:%S')
    end = datetime.strptime(file_json.get('end'), '%Y-%m-%d %H:%M:%S')

    # TODO: Definir condicoes de consulta
    conds = (
        (ds.field('dh_emi') >= start) 
        & 
        (ds.field('dh_emi') <= end)
    )

    print(f'Period: {start=}, {end=}')
    
    # TODO: Consultar tabela delta
    dt = WriteDelta(bucket_name, 'notas')
    source_data = dt.delta_read(conds=conds).to_pandas()

    print(f'Data: {source_data.shape}')

    # TODO: Athena
    cursor = CursorParquetDuckdb(
        os.getenv('s3_location'),
        result_reuse_enable=True,
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        region_name=os.getenv('region_aws')
    )
    
    # TODO: Saida da tabela iceberg
    table_name = 'notas_xml'
    schema = 'prevencao-perdas'

    location=f"{os.getenv('s3_location_table')}{table_name}/"
    
    # NOTE: somente na pasta tmp é possivel gravar
    with chdir('/tmp'):
        with Athena(cursor=cursor) as cliente:
            # TODO: Verifica se tabela existe
            is_table = cliente.execute(f"""
                select 1 as ok from information_schema.tables
                where table_schema = '{schema}'
                and table_name = '{table_name}' limit 1
            """)

            if is_table.fetchone():
                print(f'MERGE table ATHENA, {datetime.now()}')
                cliente.merge_table_iceberg(
                    table_name,
                    source_data,
                    schema=schema,
                    predicate="t.chave = s.chave and t.item = s.item",
                    location=location
                )
            else:
                print(f'CREATE table ATHENA, {datetime.now()}')
                cliente.write_table_iceberg(
                    source_data,
                    table_name=table_name,
                    schema=schema,
                    location=location
                )
    
    # TODO: Deleta JSON
    blob.delete()
    print(f'End table export ATHENA, {datetime.now()}')