"""
AWS Example: Streamed Download File
-------------------------------------------
Download streamed S3 content as file.
The PostprocessHook return the requested resource as stream using `prepare_stream_response`.
"""

from dataclasses import dataclass
from typing import Optional

from hopeit.app.api import event_api
from hopeit.app.context import EventContext, PostprocessHook
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3 import ObjectStorage, ObjectStorageSettings
from hopeit.dataobjects import BinaryDownload

object_storage: Optional[ObjectStorage] = None
logger, extra = app_extra_logger()

__steps__ = ["get_streamed_data"]


@dataclass
class SomeFile(BinaryDownload):
    file_name: str
    partition_key: Optional[str]


__api__ = event_api(
    query_args=[
        ("file_name", str, "expected return file name"),
        ("partition_key", Optional[str], "Partition folder in `YYYY/MM/DD/HH` format"),
    ],
    responses={
        200: (SomeFile, "Return a file"),
        404: (str, "File not found"),
    },
)


async def __init_event__(context):
    global object_storage
    if object_storage is None:
        settings: ObjectStorageSettings = context.settings(
            key="object_storage", datatype=ObjectStorageSettings
        )
        object_storage = await ObjectStorage.with_settings(settings)


async def get_streamed_data(
    payload: None,
    context: EventContext,
    *,
    file_name: str,
    partition_key: Optional[str] = None,
) -> SomeFile:
    """
    Prepare output file name to be streamed
    """
    return SomeFile(file_name=file_name, partition_key=partition_key)


async def __postprocess__(
    file: SomeFile, context: EventContext, response: PostprocessHook
):
    """
    Stream S3 file:
    """
    assert object_storage
    stream_response = None
    async for chunk, content_length in object_storage.get_file_chunked(
        file_name=file.file_name, partition_key=file.partition_key
    ):
        if stream_response is None:
            if chunk is None:
                response.status = 404
                msg = (
                    f"{file.partition_key}/{file.file_name} not found"
                    if file.partition_key
                    else f"{file.file_name} not found"
                )
                return msg
            stream_response = await response.prepare_stream_response(
                context,
                content_disposition=f'attachment; filename="{file.file_name}"',
                content_type=file.content_type,
                content_length=content_length,
            )
        await stream_response.write(chunk)

    return file
