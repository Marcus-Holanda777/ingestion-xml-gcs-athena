from writedelta import WriteDelta
from datetime import datetime
from cloud import Storage
import json
from itertools import islice


def comand_silver(bucket_name, key) -> None:

    st = Storage()
    blob = st.get_blob_file(bucket_name, key)
    file_json = json.loads(blob.download_as_text())

    data = file_json.get('data', None)

    if data:
        dt = WriteDelta(bucket_name, 'notas')
        is_delta = dt.delta_is_table()
        iter_notes = iter(sorted(data))
        
        while lotes := list(islice(iter_notes, 1_000)):

            is_delta = dt.delta_is_table()

            if is_delta:
                print(f'Merge delta table: {datetime.now()}, lotes = {len(lotes)}')
                dt.delta_merge(
                    lotes, 
                    predicate="s.chave = t.chave and s.item = t.item"
                )
            else:
                print(f'Create delta table: {datetime.now()}, lotes = {len(lotes)}')
                dt.delta_write(lotes)
            
        dt.delta_optimize()

        # TODO: Deletar o arquivo JSON dos lotes ...
        blob.delete()

        print(f'End logs SILVER: {datetime.now()}')