"""
AWS Example: Query Something
--------------------------------------------------------------------
Loads Something from s3
"""

from typing import Union, Optional
from datetime import datetime, timezone

from hopeit.app.api import event_api
from hopeit.app.context import EventContext, PostprocessHook
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3 import ObjectStorage, ObjectStorageSettings
from model import Something, StatusType, Status, SomethingNotFound

object_store: Optional[ObjectStorage] = None

__steps__ = ["load", "update_status_history"]

__api__ = event_api(
    summary="AWS Example: Query Something",
    query_args=[
        ("item_id", str, "Item Id to read"),
        ("partition_key", str, "Partition folder in `YYYY/MM/DD/HH` format"),
    ],
    responses={
        200: (Something, "Something object returned when found"),
        404: (SomethingNotFound, "Information about not found object"),
    },
)

logger, extra = app_extra_logger()


async def __init_event__(context):
    global object_store
    if object_store is None:
        settings: ObjectStorageSettings = context.settings(
            key="object_store", datatype=ObjectStorageSettings
        )
        object_store = await ObjectStorage.with_settings(settings)


async def load(
    payload: None,
    context: EventContext,
    *,
    item_id: str,
    partition_key: str,
    update_status: bool = False
) -> Union[Something, SomethingNotFound]:
    """
    Loads json file from filesystem as `Something` instance

    :param payload: unused
    :param context: EventContext
    :param item_id: str, item id to load
    :return: Loaded `Something` object or SomethingNotFound if not found or validation fails

    """
    assert object_store
    logger.info(
        context, "load", extra=extra(something_id=item_id, path=object_store.bucket)
    )
    something = await object_store.get(key=item_id, datatype=Something)

    if something is None:
        logger.warning(
            context,
            "item not found",
            extra=extra(something_id=item_id, path=object_store.bucket),
        )
        return SomethingNotFound(str(partition_key), item_id)
    return something


async def update_status_history(payload: Something, context: EventContext) -> Something:
    if payload.status:
        payload.history.append(payload.status)
    payload.status = Status(ts=datetime.now(tz=timezone.utc), type=StatusType.LOADED)
    return payload


async def __postprocess__(
    payload: Union[Something, SomethingNotFound],
    context: EventContext,
    response: PostprocessHook,
) -> Union[Something, SomethingNotFound]:
    if isinstance(payload, SomethingNotFound):
        response.status = 404
    return payload
