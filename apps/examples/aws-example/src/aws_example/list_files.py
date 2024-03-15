"""
AWS Example: List Files
--------------------------------------------------------------------
Lists all available Something objects
"""

from typing import List, Optional

from hopeit.app.api import event_api
from hopeit.app.context import EventContext
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3 import ConnectionConfig, ObjectStorage, ObjectStorageSettings
from model import Something

__steps__ = ["load_all"]

__api__ = event_api(
    summary="AWS Example: List Files",
    query_args=[
        (
            "wildcard",
            Optional[str],
            "Wildcard to filter objects by name prefixed "
            "by partition folder in format YYYY/MM/DD/HH/*",
        )
    ],
    responses={
        200: (List[str], "list of Something objects"),
    },
)

logger, extra = app_extra_logger()
object_store: Optional[ObjectStorage] = None


async def __init_event__(context):
    global object_store
    if object_store is None:
        conn: ConnectionConfig = context.settings(
            key="s3_conn_config", datatype=ConnectionConfig
        )
        settings: ObjectStorageSettings = context.settings(
            key="object_store", datatype=ObjectStorageSettings
        )
        object_store = await ObjectStorage.with_settings(settings).connect(
            conn_config=conn, create_bucket=True
        )


async def load_all(
    payload: None, context: EventContext, wildcard: str = "*"
) -> List[str]:
    """
    Load objects that match the given wildcard
    """
    logger.info(context, "load_all", extra=extra(path=object_store.bucket))
    items: List[str] = await object_store.list_files(wildcard)

    return items
