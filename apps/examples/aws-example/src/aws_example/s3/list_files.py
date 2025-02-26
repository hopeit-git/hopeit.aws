"""
AWS Example: List Files
--------------------------------------------------------------------
Lists all available Something objects
"""

from typing import List, Optional

from hopeit.app.api import event_api
from hopeit.app.context import EventContext
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3 import ItemLocator, ObjectStorage

object_storage: Optional[ObjectStorage] = None
logger, extra = app_extra_logger()

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


async def __init_event__(context):
    global object_storage
    if object_storage is None:
        object_storage = await ObjectStorage.with_settings(
            context.settings.extras["object_storage"]
        ).connect()


async def load_all(payload: None, context: EventContext, wildcard: str = "*") -> List[ItemLocator]:
    """
    Load objects that match the given wildcard
    """
    assert object_storage
    logger.info(context, "load_all", extra=extra(path=object_storage.bucket))
    items: List[ItemLocator] = await object_storage.list_files(wildcard)

    return items
