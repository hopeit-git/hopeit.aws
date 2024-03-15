"""
AWS Example: List Files
--------------------------------------------------------------------
Lists all available Something objects
"""

from typing import List, Optional

from hopeit.app.api import event_api
from hopeit.app.context import EventContext
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3 import (
    ItemLocator,
    ObjectStorage,
    ObjectStorageSettings,
)

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
        200: (List[ItemLocator], "list of ItemLocators"),
    },
)

logger, extra = app_extra_logger()
object_store: Optional[ObjectStorage] = None


async def __init_event__(context):
    global object_store
    if object_store is None:
        settings: ObjectStorageSettings = context.settings(
            key="object_store", datatype=ObjectStorageSettings
        )
        object_store = await ObjectStorage.with_settings(settings)


async def load_all(
    payload: None, context: EventContext, wildcard: str = "*"
) -> List[ItemLocator]:
    """
    Load objects that match the given wildcard
    """
    assert object_store
    logger.info(context, "load_all", extra=extra(path=object_store.bucket))
    items: List[ItemLocator] = await object_store.list_files(wildcard)

    return items
