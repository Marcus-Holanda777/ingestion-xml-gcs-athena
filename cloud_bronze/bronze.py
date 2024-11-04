from cloud import Storage
from parsexml import ParseXml
from write import Write
import io


def comand_bronze(bucket_name, key) -> str:
    storage = Storage()
    blob = storage.get_blob_file(bucket_name, key)

    file_bytes = io.BytesIO(blob.download_as_bytes())
    __, controle, *__, name = blob.name.split('/')
            
    file_xml = ParseXml(controle, file_bytes)
            
    data = (
        file_xml.df()
        .assign(
            controle = controle.strip().lower(),
            status = int(name.split('_')[1]),
            year = lambda _df: _df.dh_emi.dt.year,
            month = lambda _df: _df.dh_emi.dt.month
        )
    )
            
    key_to = f"bronze/{'/'.join(blob.name.split('/')[1:])}"
    write = Write(data, bucket_name, storage)
    out_key = write.write_parquet_buffer(key_to)

    print(f'Key bronze to: {out_key}')

    return out_key