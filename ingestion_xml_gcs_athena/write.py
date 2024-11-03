import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
import os
from ingestion_xml_gcs_athena.cloud import Storage


class Write:
    def __init__(
        self, 
        data: pd.DataFrame | pa.Table,
        bucket_name: str,
        client: Storage
    ) -> None:
        
        self.data = data
        self.bucket_name = bucket_name
        self.client = client

    def write_parquet_buffer(
        self,
        key: str,
        compression: str= 'snappy'
    ) -> None:
        
        if isinstance(self.data, pd.DataFrame):
           self.data = pa.Table.from_pandas(self.data)
        
        buffer = pa.BufferOutputStream()
        pq.write_table(self.data, buffer, compression=compression)

        out_key = os.path.splitext(key)[0] + '.parquet'

        self.client.upload_file(
            data=buffer.getvalue().to_pybytes(),
            bucket_name=self.bucket_name,
            blob_name=out_key
        )