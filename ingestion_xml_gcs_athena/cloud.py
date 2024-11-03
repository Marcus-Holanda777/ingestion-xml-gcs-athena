from google.cloud.storage import (
    Client,
    Blob,
    Bucket
)
from typing import (
    TypeVar,
    Any, 
    Iterator
)
import os
import json


#GsClient = TypeVar(
#    'GsClient', 
#    Client
#)


class Credentials:
    def __init__(
        self,
        client: Client,
        credentials: str = None
    ) -> None:
        
        self.client = client
        self.credentials = credentials
    
    def get_cliente(self) -> Client:
        if self.credentials:

            if all(
                [
                    os.path.isfile(self.credentials),
                    self.credentials.endswith('.json')
                ]
            ):
               return self.client.from_service_account_json(self.credentials)

            json_loads = json.loads(self.credentials)
            return self.client.from_service_account_info(json_loads)
        
        return self.client()


class Storage(Credentials):
    def __init__(
        self, 
        client: Client = Client,
        credentials: str = None
    ) -> None:
        
        super().__init__(client, credentials)

        self.storage = self.get_cliente()

    def upload_streaming(
        self,
        data: Any,
        bucket_name: str,
        blob_name: str,
        content_type: str = 'application/octet-stream'
    ) -> Blob:
        
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.upload_from_file(
            file_obj=data, 
            content_type=content_type
        )

        return blob


    def upload_file(
        self,
        data: Any,
        bucket_name: str,
        blob_name: str,
        content_type: str = 'application/octet-stream'
    ) -> Blob:
        
        bucket = self.storage.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.upload_from_string(
            data=data, 
            content_type=content_type
        )

        return blob

    def list_files(
        self,
        bucket: Bucket | str,
        prefix: str = None,
        delimiter: str = None
    ) -> Iterator[Blob | None]:
        
        blobs = (
            self.storage
             .list_blobs(
                 bucket,
                 prefix=prefix, 
                 delimiter=delimiter
            )
        )
        
        for blob in blobs:
            yield blob