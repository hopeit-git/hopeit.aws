"""
Storage/persistence asynchronous stores and gets files from object storage.

"""

import io
from typing import AsyncIterator, Dict, Generic, Optional, Type, Union, List, Tuple, Any
from io import BytesIO
import fnmatch
from dataclasses import dataclass

from aioboto3 import Session  # type: ignore
from botocore.exceptions import ClientError
from hopeit.dataobjects import dataobject, DataObject
from hopeit.dataobjects.payload import Payload
from hopeit.aws.s3.partition import get_file_partition_key, get_partition_key

SUFFIX = ".json"
OBJECT_STORAGE_SERVICE = "s3"

__all__ = ["ObjectStorage", "ObjectStorageSettings", "ConnectionConfig"]

SUFFIX = ".json"


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
class ConnectionConfig:
    aws_access_key_id: str
    aws_secret_access_key: str
    endpoint_url: str
    use_ssl: Union[str, bool]

    def __post_init__(self):
        if isinstance(self.use_ssl, str):
            self.use_ssl = self.use_ssl.lower() in ("true", "1", "t")


@dataobject
@dataclass
class ObjectStorageSettings:
    """
    S3 storage plugin.

    :field: bucket, str: S3 bucket name.
    :field: partition_dateformat, optional str: date format to be used to prefix file name in order
        to partition saved files to different subfolders based on event_ts(). i.e. "%Y/%m/%d"
        will store each files in a folder `base_path/year/month/day/`
    :field: flush_seconds, float: number of seconds to trigger a flush event to save all current
        buffered partitions. Default 0 means flush is not triggered by time.
    :field: flush_max_size: max number of elements to keep in a partition before forcing a flush.
        Default 1. A value of 0 will disable flushing by partition size.
    :field: create_bucket: Flag indicating whether to create the bucket if it does not exist.
    """

    bucket: str
    connection: ConnectionConfig
    partition_dateformat: Optional[str] = None
    flush_seconds: float = 0.0
    flush_max_size: int = 1
    create_bucket: bool = False

    def __post_init__(self):
        if isinstance(self.create_bucket, str):
            self.create_bucket = self.create_bucket.lower() in ("true", "1", "t")


@dataobject
@dataclass
class ItemLocator:
    item_id: str
    partition_key: Optional[str] = None


class ObjectStorage(Generic[DataObject]):
    """
    Stores and retrieves dataobjects and files from S3
    """

    _conn_config: Union[Dict[str, Any], List[Any]] = {}
    _conn: Optional[Session] = None

    def __init__(self, bucket: str, partition_dateformat: Optional[str] = None):
        """
        Initialize ObjectStorage with the bucket name and optional partition_dateformat.
        Example:
            ```
            object_store = await ObjectStorage(bucket).connect(conn_config)
            ```
        """
        self.bucket: str = bucket
        self.partition_dateformat = (partition_dateformat or "").strip("/")

    @classmethod
    async def with_settings(cls, settings: ObjectStorageSettings) -> "ObjectStorage":
        return await cls(
            bucket=settings.bucket, partition_dateformat=settings.partition_dateformat
        ).connect(conn_config=settings.connection)

    async def connect(
        self,
        *,
        conn_config: ConnectionConfig,
    ):
        """
        Creates a ObjectStorage connection pool

        :param config: ObjectStorageConnConfig
        :param bucket: str
        """
        self._conn_config = Payload.to_obj(conn_config)
        self._conn = Session()
        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            try:
                await object_store.create_bucket(Bucket=self.bucket)
            except ClientError:
                pass
        return self

    async def get(
        self,
        key: str,
        *,
        datatype: Type[DataObject],
        partition_key: Optional[str] = None,
    ):
        """
        Retrieves value under specified key, converted to datatype

        :param key: str
        :param datatype: dataclass implementing @dataobject (@see DataObject)
        :param partition_key: partition path to be appended to base path
        :return: instance
        """
        assert self._conn

        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            try:
                assert self.bucket
                key = f"{partition_key}/{key}" if partition_key else key
                file_obj = BytesIO()
                await object_store.download_fileobj(self.bucket, key + SUFFIX, file_obj)
                file_obj.seek(0)
                value = file_obj.read()
                if len(value):
                    return Payload.from_json(value.decode(), datatype)
                return None
            except ClientError as ex:
                if ex.response["Error"]["Code"] == "404":
                    return None
                raise ex

    async def get_file(
        self,
        file_name: str,
        *,
        partition_key: Optional[str] = None,
    ) -> Optional[bytes]:
        """
        Download a file from S3 and return its contents as bytes.

        :param file_name: The name of the file to download.
        :param partition_key: Optional partition key for the file path.
        :return: The contents of the downloaded file as bytes.
        """
        assert self._conn

        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            assert self.bucket
            if self.partition_dateformat:
                file_name = f"{partition_key}/{file_name}"
            try:
                obj = await object_store.get_object(Bucket=self.bucket, Key=file_name)
                ret = BytesIO()
                async for chunk in obj["Body"]:
                    ret.write(chunk)
                return ret.getvalue()
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return None
                raise e

    async def get_file_chunked(
        self,
        file_name: str,
        *,
        partition_key: Optional[str] = None,
    ) -> AsyncIterator[Tuple[Optional[bytes], int]]:
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
            assert self.bucket
            file_name = f"{partition_key}/{file_name}" if partition_key else file_name
            try:
                obj = await object_store.get_object(Bucket=self.bucket, Key=file_name)
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    yield None, 0
                raise e
            content_length = obj["ContentLength"]
            async for chunk in obj["Body"]:
                yield chunk, content_length

    async def store(self, *, key: str, value: DataObject) -> str:
        """
        Upload a file-like object to S3.

        :param value: File like object
        :param key: object id
        """
        assert self._conn
        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            file_path = f"{key}{SUFFIX}"
            if self.partition_dateformat:
                partition_key = get_partition_key(value, self.partition_dateformat)
                file_path = f"{partition_key}{file_path}"
            buffer = BytesIO()
            buffer.write(Payload.to_json(value).encode())
            buffer.seek(0)
            await object_store.upload_fileobj(buffer, self.bucket, file_path)
            return file_path

    async def store_file(self, *, file_name: str, value: io.BytesIO) -> str:
        """
        Stores a file-like object.

        :param file_name: str
        :param value: io.BytesIO, the file-like object to store
        :return: str file location
        """
        assert self._conn
        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            file_path = file_name
            if self.partition_dateformat:
                partition_key = get_file_partition_key(self.partition_dateformat)
                file_path = f"{partition_key}{file_name}"
            await object_store.upload_fileobj(value, self.bucket, file_path)
        return file_path

    async def list_objects(self, wildcard: str = "*") -> List[ItemLocator]:
        """
        Retrieves list of objects keys from the file storage

        :param wildcard: allow filter the listing of objects
        :return: List of objects key
        """
        wildcard = wildcard + SUFFIX
        n_part_comps = len(self.partition_dateformat.split("/"))

        item_list = []
        async for key in self._aioglob(self.bucket, "", True, wildcard=wildcard):
            item_list.append(key)
        return [
            self._get_item_locator(item_path, n_part_comps, SUFFIX)
            for item_path in item_list
        ]

    async def list_files(self, wildcard: str = "*") -> List[ItemLocator]:
        """
        Retrieves list of objects keys from the file storage

        :param wildcard: allow filter the listing of objects
        :return: List of objects key
        """
        n_part_comps = len(self.partition_dateformat.split("/"))
        item_list = []
        async for key in self._aioglob(self.bucket, "", True, wildcard=wildcard):
            item_list.append(key)
        return [
            self._get_item_locator(item_path, n_part_comps) for item_path in item_list
        ]

    async def _aioglob(self, bucket_name, prefix="", recursive=False, wildcard=None):
        """
        A generator function similar to `glob` that lists files in an S3 bucket
        """
        async with self._conn.client(
            OBJECT_STORAGE_SERVICE, **self._conn_config
        ) as object_store:
            paginator = object_store.get_paginator("list_objects_v2")
            async for result in paginator.paginate(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter="/" if not recursive else "",
            ):
                for content in result.get("Contents", []):
                    key = content["Key"]
                    if wildcard and not fnmatch.fnmatch(key, wildcard):
                        continue
                    yield key

    def partition_key(self, path: str) -> str:
        """
        Get the partition key for a given path.

        :param path: str
        :return str, the extracted partition key.
        """
        partition_key = ""
        if self.partition_dateformat:
            partition_key = path.rsplit("/", 1)[0]
        return partition_key

    def _get_item_locator(
        self, item_path: str, n_part_comps: int, suffix: Optional[str] = None
    ) -> ItemLocator:
        """This method generates an `ItemLocator` object from a given `item_path`"""
        comps = item_path.split("/")
        partition_key = (
            "/".join(comps[-n_part_comps - 1: -1])
            if self.partition_dateformat
            else None
        )
        item_id = comps[-1][: -len(suffix)] if suffix else comps[-1]
        return ItemLocator(item_id=item_id, partition_key=partition_key)
