"""
AWS Example: List Objects
--------------------------------------------------------------------
Lists all available Something objects
"""

from typing import List, Optional

from hopeit.app.api import event_api
from hopeit.app.context import EventContext
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3 import ObjectStorage, ObjectStorageSettings
from ..model import Something

object_storage: Optional[ObjectStorage] = None
logger, extra = app_extra_logger()

__steps__ = ["load_all"]

__api__ = event_api(
    summary="AWS Example: List Objects",
    query_args=[
        (
            "wildcard",
            Optional[str],
            "Wildcard to filter objects by name prefixed "
            "by partition folder in format YYYY/MM/DD/HH/*",
        )
    ],
    responses={
        200: (List[Something], "list of Something objects"),
    },
)


async def __init_event__(context):
    global object_storage
    if object_storage is None:
        settings: ObjectStorageSettings = context.settings(
            key="object_storage", datatype=ObjectStorageSettings
        )
        object_storage = await ObjectStorage.with_settings(settings)


async def load_all(
    payload: None, context: EventContext, wildcard: str = "*"
) -> List[Something]:
    """
    Load objects that match the given wildcard
    """
    assert object_storage

    logger.info(context, "load_all", extra=extra(path=object_storage.bucket))
    items: List[Something] = []
    for item_loc in await object_storage.list_objects(wildcard):
        something = await object_storage.get(
            key=item_loc.item_id,
            datatype=Something,
            partition_key=item_loc.partition_key,
        )
        if something is not None:
            items.append(something)
        else:
            logger.warning(
                context,
                "Item not found",
                extra=extra(
                    path=object_storage.bucket,
                    partition_key=item_loc.partition_key,
                    item_id=item_loc.item_id,
                ),
            )
    return items
