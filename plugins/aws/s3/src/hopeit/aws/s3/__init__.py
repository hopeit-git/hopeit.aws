"""
Storage/persistence asynchronous stores and gets files from object storage.

"""

import fnmatch
from dataclasses import dataclass
from io import BytesIO
import os
from typing import Any, AsyncIterator, Dict, Generic, List, Optional, Tuple, Type, Union

from aioboto3 import Session  # type: ignore
from botocore.exceptions import ClientError

from hopeit.dataobjects import DataObject, dataobject
from hopeit.dataobjects.payload import Payload
from hopeit.aws.s3.partition import get_file_partition_key, get_partition_key


SUFFIX = ".json"
S3 = "s3"

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
    """
    :field aws_access_key_id, str: AWS access key ID
    :field aws_secret_access_key, str: AWS secret access key
    :field aws_session_token, str: AWS temporary session token
    :field region_name, str: Default region when creating new connections
    :field use_ssl, Union[bool, str]: Whether or not to use SSL. By default, SSL is used.
    :field endpoint_url, str: The complete URL to use for the constructed client.
        If this value is provided, then ``use_ssl`` is ignored.

    :field verify, Union[bool, str]: Whether or not to verify SSL certificates.
        By default SSL certificates are verified.  You can provide the following values:
        * False - do not validate SSL certificates.  SSL will still be
            used (unless use_ssl is False), but SSL certificates
            will not be verified.
        * path/to/cert/bundle.pem - A filename of the CA cert bundle to
            uses. You can specify this argument if you want to use a
            different CA cert bundle than the one used by botocore.
    """

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    endpoint_url: Optional[str] = None
    use_ssl: Union[bool, str] = True
    region_name: Optional[str] = None
    verify: Union[bool, str] = True

    def __post_init__(self):
        if isinstance(self.use_ssl, str):
            self.use_ssl = self.use_ssl.lower() in ("true", "1", "t")
        if isinstance(self.verify, str):
            if self.verify.lower() == "true":
                self.verify = True
            elif self.verify.lower() == "false":
                self.verify = False


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
    connection_config: ConnectionConfig
    partition_dateformat: Optional[str] = None
    flush_seconds: float = 0.0
    flush_max_size: int = 1
    create_bucket: Union[bool, str] = True

    def __post_init__(self):
        if isinstance(self.create_bucket, str):
            self.create_bucket = self.create_bucket.lower() in ("true")


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
    _session: Session

    def __init__(
        self,
        bucket: str,
        partition_dateformat: Optional[str] = None,
        create_bucket: bool = False,
    ):
        """
        Initialize ObjectStorage with the bucket name and optional partition_dateformat.
        Example:
            ```
            object_storage = await ObjectStorage(bucket).connect(conn_config)
            ```
        """
        self.bucket: str = bucket
        self.partition_dateformat = (partition_dateformat or "").strip("/")
        self._create_bucket = create_bucket

    @classmethod
    async def with_settings(cls, settings: ObjectStorageSettings) -> "ObjectStorage":
        return await cls(
            bucket=settings.bucket,
            partition_dateformat=settings.partition_dateformat,
            create_bucket=bool(settings.create_bucket),
        ).connect(connection_config=settings.connection_config)

    async def connect(
        self,
        *,
        connection_config: ConnectionConfig,
    ):
        """
        Creates a ObjectStorage connection pool

        :param config: ConnectionConfig
        """
        assert self.bucket
        self._conn_config = Payload.to_obj(connection_config)
        self._session = Session()
        region_name = os.getenv("AWS_DEFAULT_REGION")
        if self._create_bucket:
            kwargs = (
                {
                    "CreateBucketConfiguration": {
                        "LocationConstraint": connection_config.region_name
                        or region_name
                    }
                }
                if connection_config.region_name or region_name
                else {}
            )
            async with self._session.client(S3, **self._conn_config) as object_storage:
                try:
                    await object_storage.create_bucket(Bucket=self.bucket, **kwargs)
                except ClientError as e:
                    if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                        pass
                    else:
                        raise e
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
        :param partition_key, Optional[str]: Optional partition key.
        :return: instance
        """

        async with self._session.client(S3, **self._conn_config) as object_storage:
            try:
                key = f"{partition_key}/{key}" if partition_key else key
                file_obj = BytesIO()
                await object_storage.download_fileobj(
                    self.bucket, key + SUFFIX, file_obj
                )
                obj = file_obj.getvalue()
                if len(obj):
                    return Payload.from_json(obj, datatype)
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

        :param file_name, str: The name of the file to download.
        :param partition_key, Optional[str]: Optional partition key.
        :return: The contents of the requested file as bytes, or None if the file does not exist.
        """

        async with self._session.client(S3, **self._conn_config) as object_storage:
            if self.partition_dateformat:
                file_name = f"{partition_key}/{file_name}"
            try:
                obj = await object_storage.get_object(Bucket=self.bucket, Key=file_name)
                ret = BytesIO()
                async for chunk in obj["Body"]:
                    ret.write(chunk)
                return ret.getvalue()
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
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

        :param file_name str: object id
        :param partition_key, Optional[str]: Optional partition key.

        The file-like object must be in binary mode.
        This is a managed transfer which will perform a multipart download in
        multiple threads if necessary.
        Usage::
            with open('filename', 'wb') as data:
                object_storage.get_file_chunked('mykey', data)
        """

        async with self._session.client(S3, **self._conn_config) as object_storage:
            file_name = f"{partition_key}/{file_name}" if partition_key else file_name
            try:
                obj = await object_storage.get_object(Bucket=self.bucket, Key=file_name)
                content_length = obj["ContentLength"]
                async for chunk in obj["Body"]:
                    yield chunk, content_length
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    yield None, 0
                else:
                    raise e

    async def store(self, *, key: str, value: DataObject) -> str:
        """
        Upload a dataobject object to S3.

        :param key: object id
        :param value: File like object
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            file_path = f"{key}{SUFFIX}"
            if self.partition_dateformat:
                partition_key = get_partition_key(value, self.partition_dateformat)
                file_path = f"{partition_key}{file_path}"

            await object_storage.upload_fileobj(
                BytesIO(Payload.to_json(value).encode()), self.bucket, file_path
            )
            return file_path

    async def store_file(self, *, file_name: str, value: Union[bytes, Any]) -> str:
        """
        Stores a file-like object.

        :param file_name: str
        :param value: bytes or a file-like object to store, it must
            implement the write method and must accept bytes.
        :return: str file location
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            file_path = file_name
            if self.partition_dateformat:
                partition_key = get_file_partition_key(self.partition_dateformat)
                file_path = f"{partition_key}{file_name}"

            if isinstance(value, bytes):
                await object_storage.upload_fileobj(
                    BytesIO(value), self.bucket, file_path
                )
            else:
                await object_storage.upload_fileobj(value, self.bucket, file_path)

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

    async def delete(self, *keys: str, partition_key: Optional[str] = None):
        """
        Delete specified keys

        :param keys: str, keys to be deleted
        :param partition_key, Optional[str]: Optional partition key.
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            for key in keys:
                key = f"{partition_key}/{key}" if partition_key else key
                await object_storage.delete_object(Bucket=self.bucket, Key=key + SUFFIX)

    async def delete_files(self, *file_names: str, partition_key: Optional[str] = None):
        """
        Delete specified file_names

        :param file_names: str, file names to be deleted
        :param partition_key, Optional[str]: Optional partition key.
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            for key in file_names:
                key = f"{partition_key}/{key}" if partition_key else key
                await object_storage.delete_object(Bucket=self.bucket, Key=key)

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
        async with self._session.client(S3, **self._conn_config) as object_storage:
            paginator = object_storage.get_paginator("list_objects_v2")
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
