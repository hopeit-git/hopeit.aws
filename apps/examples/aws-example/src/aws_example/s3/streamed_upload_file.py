"""
AWS Example: Streamed Upload File
-----------------------------------------
Uploads file using multipart upload support. Returns metadata Something object.
```
"""

from typing import List, Optional, Union

from hopeit.app.api import event_api
from hopeit.app.context import EventContext, PreprocessHook
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3 import ObjectStorage, ObjectStorageSettings
from hopeit.dataobjects import BinaryAttachment, dataclass, dataobject, field

object_storage: Optional[ObjectStorage] = None
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


__steps__ = ["create_items"]

__api__ = event_api(
    description="Upload files using Multipart form request",
    fields=[("attachment", BinaryAttachment)],
    responses={
        200: (List[UploadedFile], "list of created Something objects"),
        400: (str, "Missing or invalid fields"),
    },
)


async def __init_event__(context: EventContext):
    global object_storage
    if object_storage is None:
        settings: ObjectStorageSettings = context.settings(
            key="object_storage", datatype=ObjectStorageSettings
        )
        object_storage = await ObjectStorage.with_settings(settings).connect()


# pylint: disable=invalid-name
async def __preprocess__(
    payload: None, context: EventContext, request: PreprocessHook
) -> Union[FileUploadInfo, str]:
    assert object_storage
    uploaded_files: List[UploadedFile] = []
    async for file_hook in request.files():
        file_name = f"{file_hook.name}-{file_hook.file_name}"
        logger.info(context, f"Saving {file_name}...")
        location = await object_storage.store_file(file_name=file_name, value=file_hook)
        uploaded_file = UploadedFile(
            file_hook.name, location, object_storage.bucket, size=file_hook.size
        )
        uploaded_files.append(uploaded_file)
    args = await request.parsed_args()
    if "attachment" not in args:
        request.status = 400
        return "Missing required fields"
    return FileUploadInfo(uploaded_files=uploaded_files)


async def create_items(
    payload: FileUploadInfo, context: EventContext
) -> List[UploadedFile]:
    """
    Create Something objects to be returned for each uploaded file
    """
    return payload.uploaded_files
