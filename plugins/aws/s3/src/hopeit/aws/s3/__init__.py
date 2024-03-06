"""
Storage/persistence asynchronous stores and gets files from object storage.

"""

from typing import Generic, Optional, Type, Union
from io import BytesIO

from aioboto3 import Session
from botocore.exceptions import ClientError
from hopeit.dataobjects import dataclass, dataobject, DataObject
from hopeit.dataobjects.payload import Payload
from hopeit.app.context import (
    EventContext,
    PreprocessFileHook,
    PostprocessHook,
    PostprocessStreamResponseHook,
)
from hopeit.app.logger import app_extra_logger


logger, extra = app_extra_logger()

OBJECT_STORAGE_SERVICE = "s3"

__all__ = ["ObjectStorage", "ObjectStorageSettings", "ObjectStorageConnConfig"]


@dataclass
class FileInfo:
    """
    File info
    """

    bucket: str
    file_name: str
    size: int


@dataobject
@dataclass
class ObjectStorageSettings:
    bucket: str
    create_bucket: bool = False


@dataobject
@dataclass
class ObjectStorageConnConfig:
    aws_access_key_id: str
    aws_secret_access_key: str
    endpoint_url: str
    use_ssl: Union[str, bool]

    def __post_init__(self):
        if isinstance(self.use_ssl, str):
            self.use_ssl = self.use_ssl.lower() in ("true", "1", "t")


class ObjectStorage(Generic[DataObject]):
    """
    Stores and retrieves dataobjects from filesystem
    """

    def __init__(self):
        """
        Stores and retrieves files from an Object Store
        This class must be initialized with the method connect
        Example:
            ```
            object_store = await ObjectStorage().connect(conn_config, bucket)
            ```
        """
        self._conn: Optional[Session] = None

    async def connect(
        self,
        *,
        conn_config: ObjectStorageConnConfig,
        bucket: str,
        create_bucket: bool = False,
    ):
        """
        Creates a ObjectStorage connection pool

        :param config: ObjectStorageConnConfig
        :param bucket: str
        """
        self._conn_config = Payload.to_obj(conn_config)
        self._bucket = bucket
        self._conn = Session()
        if create_bucket:
            async with self._conn.client(
                OBJECT_STORAGE_SERVICE, **self._conn_config
            ) as object_store:
                try:
                    await object_store.create_bucket(Bucket=bucket)
                except ClientError:
                    pass
        return self

    async def get_streamed_file(
        self,
        file_name: str,
        context: EventContext,
        response: PostprocessHook,
        content_disposition: Optional[str] = None,
        content_type: Optional[str] = None,
    ) -> PostprocessStreamResponseHook:
        """
        Retrieves the object from the object store bucket as PostprocessStreamResponseHook

        :param file_name: object id
        :param context: hopeit EventContext
        :param response: PostprocessHook
        :param content_disposition: Optional[str], overwrites the default = f'attachment; filename="{file_name}"'
        :param content_type: Optional[str], overwrites the default `object` content_type
        :return PostprocessStreamResponseHook
        """
        assert self._conn
        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            assert self._bucket
            obj = await object_store.get_object(Bucket=self._bucket, Key=file_name)
            stream_response = await response.prepare_stream_response(
                context=context,
                content_disposition=content_disposition
                or f'attachment; filename="{file_name}"',
                content_type=content_type or obj["ContentType"],
                content_length=obj["ContentLength"],
            )

            async for chunk in obj["Body"]:
                await stream_response.write(chunk)
            return stream_response

    async def store_streamed_file(
        self, file_name: str, file_hook: PreprocessFileHook
    ) -> Optional[FileInfo]:
        """
        Store an object in the object store bucket

        :param file_name: object id
        :param file_hook: PreprocessFileHook
        :return FileInfo
        """
        assert self._conn
        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            assert self._bucket
            await object_store.upload_fileobj(file_hook, self._bucket, file_name)
        return FileInfo(self._bucket, file_name, file_hook.size)

    async def get(self, key: str, *, datatype: Type[DataObject]):
        """
        Download an object from S3 to a file-like object.

        :param key: object id

        The file-like object must be in binary mode.
        This is a managed transfer which will perform a multipart download in
        multiple threads if necessary.
        Usage::
            with open('filename', 'wb') as data:
                object_storage.get('mykey', data)
        """
        assert self._conn
        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            try:
                assert self._bucket
                file_obj = BytesIO()
                await object_store.download_fileobj(self._bucket, key, file_obj)
                file_obj.seek(0)
                if datatype:
                    value = file_obj.read()
                    if len(value):
                        return Payload.from_json(value.decode(), datatype)
                    return None
                return file_obj
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "404":
                    return None
                raise ex

    async def store(self, *, key: str, value):
        """
        Upload a file-like object to S3.

        :param value: File like object
        :param key: object id
        Usage::

        with open('filename', 'rb') as data:
            object_sotage.store(key='mykey', value=data)

        """
        assert self._conn
        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            assert self._bucket
            try:
                await object_store.upload_fileobj(value, self._bucket, key)
            except AttributeError:
                buffer = BytesIO()
                buffer.write(Payload.to_json(value).encode())
                buffer.seek(0)
                await object_store.upload_fileobj(buffer, self._bucket, key)
