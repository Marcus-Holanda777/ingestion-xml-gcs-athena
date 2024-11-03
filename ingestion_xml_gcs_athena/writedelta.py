from deltalake import (
    write_deltalake, 
    DeltaTable
)
import pyarrow.parquet as pq
import pyarrow.fs as fs
from ingestion_xml_gcs_athena.schemas.notas import schema_nota
import os


class WriteDelta:
    def __init__(
        self,
        table_name: str
    ) -> None:
        
        self.table_name = table_name

    def file_system_gcs(self):
        return fs.GcsFileSystem()
    
    def read_parquet(
        self,
        bucket_name: str,
        medallion: str = 'bronze',
        prefixs: list[str] = []
    ):
        system_gcs = self.file_system_gcs()
        
        path_or_paths = f'{bucket_name}/{medallion}'
        if prefixs:
            alt_ext = lambda f: os.path.splitext(f)[0] + '.parquet'

            path_or_paths = [
                f'{bucket_name}/{medallion}/{alt_ext(pref)}' 
                for pref in prefixs
            ]

        dataset = pq.ParquetDataset(
            path_or_paths,
            schema=schema_nota,
            filesystem=system_gcs
        )

        return dataset.read(use_threads=True)

    def delta_write(
        self,
        bucket_name: str,
        medallion: str = 'silver',
        prefixs: list[str] = []
    ):
        
        data = self.read_parquet(
            bucket_name,
            prefixs=prefixs
        )

        print(data.shape)

        write_deltalake(
            f'gs://{bucket_name}/{medallion}/{self.table_name}',
            data,
            schema=schema_nota,
            mode="overwrite"
        )

    def delta_optimize(
        self,
        bucket_name: str,
        medallion: str = 'silver'
    ):
        dt = DeltaTable(
            f'gs://{bucket_name}/{medallion}/{self.table_name}'
        )
        
        dt.optimize.compact()
        dt.vacuum(
            retention_hours=0, 
            enforce_retention_duration=False, 
            dry_run=False
        )

    def delta_merge(
        self,
        prefixs: list[str],
        bucket_name: str,
        medallion: str = 'silver'
    ):
        data = self.read_parquet(
            bucket_name,
            medallion,
            prefixs=prefixs
        )
        
        dt = DeltaTable(
            f's3://{bucket_name}/{medallion}/{self.table_name}'
        )

        rst = (
            dt.merge(
                data,
                predicate="s.chave = t.chave and s.item = t.item",
                source_alias="s", 
                target_alias="t"
            )
            .when_not_matched_insert_all()
            .when_matched_update_all()
            .execute()
        )

        return rst