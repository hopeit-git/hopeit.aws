"""Intial setup resources
"""

from botocore.exceptions import EndpointConnectionError
from hopeit.app.context import EventContext
from hopeit.app.logger import app_extra_logger
from hopeit.aws.s3.object_storage import ObjectStorage, ObjectStorageSettings

logger, extra = app_extra_logger()

__steps__ = [
    "init_storage",
]


async def init_storage(payload: None, context: EventContext) -> None:
    """
    Creates a bucket if it doesn't exist.
    """
    settings: ObjectStorageSettings = context.settings(
        key="object_storage", datatype=ObjectStorageSettings
    )
    try:
        object_storage = await ObjectStorage.with_settings(settings).connect()
        await object_storage.create_bucket(exist_ok=True)
    except EndpointConnectionError as conn_error:
        logger.error(
            context,
            "init_storage: Connection to Object Storage failed. "
            "Consider starting the MinIO server from docker/docker_compose.yaml.",
            extra=extra(conn_error=conn_error),
        )
