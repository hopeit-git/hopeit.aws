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
        if await object_storage.create_bucket():
            logger.info(
                context,
                f"init_storage: Bucket '{object_storage.bucket}' created.",
                extra=extra(path=object_storage.bucket),
            )
        else:
            logger.info(
                context,
                f"init_storage: Bucket '{object_storage.bucket}' already exists, skip creation.",
                extra=extra(path=object_storage.bucket),
            )

    except EndpointConnectionError as conn_error:
        logger.warning(
            context,
            "init_storage: Connection to Object Storage failed. "
            "Consider starting the MinIO server from docker/docker_compose.yaml.",
            extra=extra(conn_error=conn_error),
        )
