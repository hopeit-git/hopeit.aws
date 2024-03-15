"""
AWS Example: Save Something
--------------------------------------------------------------------
Creates and saves Something
"""

from typing import Optional

from hopeit.app.api import event_api
from hopeit.app.logger import app_extra_logger
from hopeit.app.context import EventContext

from model import Something, User, SomethingParams
from hopeit.aws.s3 import ObjectStorage, ConnectionConfig, ObjectStorageSettings

object_store: Optional[ObjectStorage] = None
logger, extra = app_extra_logger()

__steps__ = ["create_something", "save"]

__api__ = event_api(
    summary="AWS Example: Save Something",
    payload=(SomethingParams, "provide `id` and `user` to create Something"),
    responses={
        200: (str, "path where object is saved"),
        400: (str, "bad request reason"),
    },
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


async def create_something(
    payload: SomethingParams, context: EventContext
) -> Something:
    logger.info(
        context,
        "Creating something...",
        extra=extra(payload_id=payload.id, user=payload.user),
    )
    result = Something(id=payload.id, user=User(id=payload.user, name=payload.user))
    return result


async def save(payload: Something, context: EventContext) -> str:
    """
    Attempts to validate `payload` and save it to disk in json format to s3

    :param payload: Something object
    :param context: EventContext
    """

    assert object_store
    logger.info(context, "saving", extra=extra(something_id=payload.id))
    ret = await object_store.store(key=payload.id, value=payload)
    return f"{ret} saved to s3"
