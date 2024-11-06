from deltalake import (
    write_deltalake, 
    DeltaTable
)
import pyarrow.parquet as pq
import pyarrow.fs as fs
from notas import schema_nota
import os
from datetime import timedelta
from typing import Any


class WriteDelta:
    def __init__(
        self,
        bucket_name: str,
        table_name: str
    ) -> None:
        
        self.bucket_name = bucket_name
        self.table_name = table_name

    def delta_is_table(
        self,
        medallion: str = 'silver'
    ) -> bool:
        
        uri = f'gs://{self.bucket_name}/{medallion}/{self.table_name}'
        return DeltaTable.is_deltatable(uri)


    def file_system_gcs(self):
        return fs.GcsFileSystem(
            retry_time_limit=timedelta(seconds=3600)
        )
    
    def read_parquet(
        self,
        prefixs: list[str] = [],
        medallion: str = 'bronze'
    ):
        system_gcs = self.file_system_gcs()
        
        path_or_paths = f'{self.bucket_name}/{medallion}'
        if prefixs:
            alt_ext = lambda f: os.path.splitext(f)[0] + '.parquet'

            path_or_paths = [
                f'{self.bucket_name}/{medallion}/{alt_ext(pref)}' 
                if not pref.startswith(medallion) else
                f'{self.bucket_name}/{alt_ext(pref)}' 
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
        prefixs: list[str] = [],
        medallion: str = 'silver'
    ):
        
        data = self.read_parquet(
            prefixs=prefixs
        )

        write_deltalake(
            f'gs://{self.bucket_name}/{medallion}/{self.table_name}',
            data,
            schema=schema_nota,
            mode="overwrite"
        )

    def delta_optimize(
        self,
        medallion: str = 'silver'
    ):
        dt = DeltaTable(
            f'gs://{self.bucket_name}/{medallion}/{self.table_name}'
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
        predicate: str,
        medallion: str = 'silver'
    ):
        data = self.read_parquet(
            prefixs=prefixs
        )
        
        dt = DeltaTable(
            f'gs://{self.bucket_name}/{medallion}/{self.table_name}'
        )

        rst = (
            dt.merge(
                data,
                predicate=predicate,
                source_alias="s",
                target_alias="t"
            )
            .when_not_matched_insert_all()
            .when_matched_update_all()
            .execute()
        )

        return rst
    
    def delta_read(
        self,
        medallion: str = 'silver',
        conds: Any = None
    ) -> Any:
        
        table_uri = f"gs://{self.bucket_name}/{medallion}/{self.table_name}"
        raw_fs, normalized_path = fs.FileSystem.from_uri(table_uri)
        filesystem = fs.SubTreeFileSystem(normalized_path, raw_fs)

        dt = DeltaTable(table_uri)
        table = dt.to_pyarrow_dataset(filesystem=filesystem)
        
        return table.to_table(filter=conds)