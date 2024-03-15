"""
AWS Example: Upload Download File
-----------------------------------------
Uploads file using multipart upload support. Returns metadata Something object.
```
"""

from io import BytesIO
from typing import List, Optional
from dataclasses import dataclass, field

from hopeit.app.api import event_api
from hopeit.app.logger import app_extra_logger
from hopeit.app.context import EventContext
from hopeit.aws.s3 import ObjectStorage, ObjectStorageSettings
from hopeit.dataobjects import dataobject

object_store: Optional[ObjectStorage] = None
logger, extra = app_extra_logger()


@dataobject
@dataclass
class UploadedFile:
    file_id: str
    file_name: str
    saved_path: str
    size: int


@dataobject
@dataclass
class FileUploadInfo:
    uploaded_files: List[UploadedFile] = field(default_factory=list)


__steps__ = ["upload_item", "download_item"]

__api__ = event_api(
    summary="AWS Example: Upload Download File",
    description="Upload files using Multipart form request",
    responses={
        200: (List[UploadedFile], "list of created Something objects"),
        400: (str, "Missing or invalid fields"),
    },
)


async def __init_event__(context: EventContext):
    global object_store
    if object_store is None:
        settings: ObjectStorageSettings = context.settings(
            key="object_store", datatype=ObjectStorageSettings
        )
        object_store = await ObjectStorage.with_settings(settings)


async def upload_item(payload: None, context: EventContext) -> str:
    """
    Upload file
    """
    assert object_store

    file_name = "hopeit-iso.png"
    src_file_path = f"./apps/examples/aws-example/resources/{file_name}"
    with open(src_file_path, "rb") as file:
        file_path = await object_store.store_file(
            file_name=file_name, value=BytesIO(file.read())
        )
    return file_path


async def download_item(file_name: str, context: EventContext) -> str:
    """
    Create Something objects to be returned for each uploaded file
    """
    assert object_store
    tgt_file_name = "_hopeit-iso.png"
    tgt_file_path = "./apps/examples/aws-example/resources/"
    partition_key = object_store.partition_key(file_name)
    file_name = file_name.rsplit("/", 1)[-1]

    data = await object_store.get_file(file_name=file_name, partition_key=partition_key)
    assert data
    with open(f"{tgt_file_path}{tgt_file_name}", "wb") as file:
        file.write(data)

    with open(f"{tgt_file_path}{tgt_file_name}", "rb") as file1, open(
        f"{tgt_file_path}{file_name}", "rb"
    ) as file2:
        assert file1.read() == file2.read()

    return "Done"
