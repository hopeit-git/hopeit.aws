"""
AWS Example: Download File Streamed
-------------------------------------------
Download streamd created content as file.
The PostprocessHook return the requested resource as stream using `prepare_stream_response`.
"""

from dataclasses import dataclass
from typing import Optional

from hopeit.app.api import event_api
from hopeit.app.context import EventContext, PostprocessHook
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3 import ConnectionConfig, ObjectStorage, ObjectStorageSettings
from hopeit.dataobjects import BinaryDownload

__steps__ = ["get_streamed_data"]
logger, extra = app_extra_logger()

object_store: Optional[ObjectStorage] = None


@dataclass
class SomeFile(BinaryDownload):
    file_name: str


__api__ = event_api(
    query_args=[("file_name", str, "expected return file name")],
    responses={200: (SomeFile, "Return content with filename=`file_name`")},
)


async def __init_event__(context):
    global object_store
    if object_store is None:
        conn: ConnectionConfig = context.settings(
            key="s3_conn_config", datatype=ConnectionConfig
        )
        settings: ObjectStorageSettings = context.settings(
            key="object_store", datatype=ObjectStorageSettings
        )
        object_store = (
            await ObjectStorage.with_settings(settings)
            .connect(conn_config=conn, create_bucket=True)
        )


async def get_streamed_data(
    payload: None, context: EventContext, *, file_name: str
) -> SomeFile:
    """
    Prepare output file name to be streamd
    """
    return SomeFile(file_name=file_name)


async def __postprocess__(
    file: SomeFile, context: EventContext, response: PostprocessHook
):
    """
    Stream file:
    """
    assert object_store
    try:
        logger.info(context, f"Getting {file.file_name}...")
        ret = await object_store.get_streamed_file(
            file_name=file.file_name, context=context, response=response
        )
    except Exception as e:
        logger.warning(
            context,
            f"{file.file_name} not found...",
        )
        pass
        response.status = 400
        return "Object not found"
