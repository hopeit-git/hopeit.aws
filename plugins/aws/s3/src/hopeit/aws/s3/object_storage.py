"""
Storage/persistence asynchronous stores and gets files from object storage.

"""

import fnmatch
import os
from dataclasses import dataclass
from io import BytesIO
from typing import (
    IO,
    Any,
    AsyncIterator,
    Dict,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

from aioboto3 import Session  # type: ignore
from botocore.exceptions import ClientError
from hopeit.dataobjects import DataObject, dataobject
from hopeit.dataobjects.payload import Payload

from .partition import get_file_partition_key, get_partition_key

SUFFIX = ".json"
S3 = "s3"

__all__ = ["ObjectStorage", "ObjectStorageSettings", "ConnectionConfig"]

SUFFIX = ".json"


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
    hopeit.aws.s3 `ObjectStorage` settings.

    :field: bucket, str: S3 bucket name.
    :field prefix, Optional[str]: Prefix to be used for every element (object or file) stored in the S3 bucket.
    :field: partition_dateformat, Optional[str]: date format to be used to prefix file name in order
        to partition saved files to different subfolders based on event_ts(). i.e. "%Y/%m/%d"
        will store each files in a folder `/year/month/day/`
    :field connection_config, `ConnectionConfig`: Connection configuration for S3 client.
    :field: create_bucket: Flag indicating whether to create the bucket if it does not exist.
    """

    bucket: str
    connection_config: ConnectionConfig
    prefix: Optional[str] = None
    partition_dateformat: Optional[str] = None
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

    def __init__(
        self,
        bucket: str,
        prefix: Optional[str] = None,
        partition_dateformat: Optional[str] = None,
        create_bucket: bool = False,
    ):
        """
        Initialize ObjectStorage with the bucket name and optional partition_dateformat

        :param bucket, str: The name of the S3 bucket to use for storage
        :param prefix, Optional[str]: Prefix to be used for every element (object or file) stored in the S3 bucket.
        :param partition_dateformat, Optional[str]: Optional format string for partitioning
            dates in the S3 bucket.
        :param create_bucket, bool: Whether to create the S3 bucket if it doesn't exist
        """
        self.bucket: str = bucket
        self.prefix: Optional[str] = prefix
        self.partition_dateformat: str = (partition_dateformat or "").strip("/")
        self.create_bucket: bool = create_bucket
        self._settings: ObjectStorageSettings
        self._conn_config: Dict[str, Any]
        self._session: Session = None

    @classmethod
    def with_settings(
        cls, settings: Union[ObjectStorageSettings, Dict[str, Any]]
    ) -> "ObjectStorage":
        """
        Create an ObjectStorage instance with settings

        :param settings, `ObjectStorageSettings` or `Dict[str, Any]`:
            Either an :class:`ObjectStorageSettings` object or a dictionary
            representing ObjectStorageSettings.
        :return `ObjectStorage`
        """
        if settings and not isinstance(settings, ObjectStorageSettings):
            settings = Payload.from_obj(settings, ObjectStorageSettings)
        assert isinstance(settings, ObjectStorageSettings)
        obj = cls(
            bucket=settings.bucket,
            prefix=settings.prefix,
            partition_dateformat=settings.partition_dateformat,
            create_bucket=bool(settings.create_bucket),
        )
        obj._settings = settings
        return obj

    async def connect(
        self, *, connection_config: Union[ConnectionConfig, Dict[str, Any], None] = None
    ):
        """
        Creates a ObjectStorage connection pool

        :param connection_config: `ConnectionConfig` or `Dict[str, Any]`:
            Either an :class: `ConnectionConfig` object or a dictionary
            representing ConnectionConfig.

        """
        assert self.bucket

        if connection_config and not isinstance(connection_config, ConnectionConfig):
            connection_config = Payload.from_obj(connection_config, ConnectionConfig)

        self._conn_config = Payload.to_obj(  # type: ignore
            connection_config if connection_config else self._settings.connection_config
        )
        self._session = Session()
        region_name = os.getenv("AWS_DEFAULT_REGION")
        if region_name and "region_name" not in self._conn_config:
            self._conn_config["region_name"] = region_name

        if self.create_bucket:
            kwargs = (
                {
                    "CreateBucketConfiguration": {
                        "LocationConstraint": self._conn_config["region_name"]
                    }
                }
                if self._conn_config.get("region_name", None) is not None
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

        :param key, str
        :param datatype: dataclass implementing @dataobject (@see DataObject)
        :param partition_key, Optional[str]: Optional partition key
        :return: instance
        """

        async with self._session.client(S3, **self._conn_config) as object_storage:
            try:
                key = f"{partition_key}/{key}" if partition_key else key
                key = f"{'' if not self.prefix else self.prefix}{key}"
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
        Download a file from S3 and return its contents as bytes

        :param file_name, str: The name of the file to download
        :param partition_key, Optional[str]: Optional partition key
        :return: The contents of the requested file as bytes, or None if the file does not exist
        """

        async with self._session.client(S3, **self._conn_config) as object_storage:
            if self.partition_dateformat:
                file_name = f"{self.prefix or ''}{partition_key + '/' if partition_key else ''}{file_name}"
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
        Download an object from S3 to a file-like object

        :param file_name str: object id
        :param partition_key, Optional[str]: Optional partition key.

        The file-like object must be in binary mode.
        This is a managed transfer which will perform a multipart download in multiple threads if necessary
        Usage::
            with open('filename', 'wb') as data:
                object_storage.get_file_chunked('mykey', data)
        """

        async with self._session.client(S3, **self._conn_config) as object_storage:
            file_name = f"{self.prefix or ''}{partition_key + '/' if partition_key else ''}{file_name}"
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
        Upload a @dataobject object to S3

        :param key: object id
        :param value: hopeit @dataobject
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            file_path = f"{key}{SUFFIX}"
            if self.partition_dateformat:
                partition_key = get_partition_key(value, self.partition_dateformat)
                file_path = f"{partition_key}{file_path}"
            file_path = f"{'' if not self.prefix else self.prefix}{file_path}"
            await object_storage.upload_fileobj(
                BytesIO(Payload.to_json(value).encode()),
                Bucket=self.bucket,
                Key=file_path,
            )
            return file_path

    async def store_file(
        self, *, file_name: str, value: Union[bytes, IO[bytes], Any]
    ) -> str:
        """
        Stores bytes or a file-like object.

        :param file_name, str
        :param value, Union[bytes, any]: bytes or a file-like object to store, it must
            implement the read method and must return bytes.
        :return, str: file location
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            file_path = file_name
            if self.partition_dateformat:
                partition_key = get_file_partition_key(self.partition_dateformat)
                file_path = f"{partition_key}{file_name}"

            file_path = f"{'' if not self.prefix else self.prefix}{file_path}"
            if isinstance(value, bytes):
                await object_storage.upload_fileobj(
                    BytesIO(value),
                    Bucket=self.bucket,
                    Key=file_path,
                )
            else:
                await object_storage.upload_fileobj(
                    value,
                    Bucket=self.bucket,
                    Key=file_path,
                )
        return file_path

    async def list_objects(self, wildcard: str = "*") -> List[ItemLocator]:
        """
        Retrieves list of objects keys from the object storage

        :param wildcard: allow filter the listing of objects
        :return: List of `ItemLocator` with objects location info
        """
        wildcard = wildcard + SUFFIX
        n_part_comps = len(self.partition_dateformat.split("/"))

        item_list = []
        async for key in self._aioglob(True, wildcard=wildcard):
            item_list.append(key)
        return [
            self._get_item_locator(item_path, n_part_comps, SUFFIX)
            for item_path in item_list
        ]

    async def delete(self, *keys: str, partition_key: Optional[str] = None):
        """
        Delete specified keys

        :param keys: str, keys to be deleted
        :param partition_key, Optional[str]: Optional partition key
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            for key in keys:
                key = f"{partition_key}/{key}" if partition_key else key
                await object_storage.delete_object(
                    Bucket=self.bucket,
                    Key=f"{'' if not self.prefix else self.prefix}{key + SUFFIX}",
                )

    async def delete_files(self, *file_names: str, partition_key: Optional[str] = None):
        """
        Delete specified file_names

        :param file_names: str, file names to be deleted
        :param partition_key, Optional[str]: Optional partition key
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            for key in file_names:
                key = f"{partition_key}/{key}" if partition_key else key
                await object_storage.delete_object(
                    Bucket=self.bucket,
                    Key=f"{'' if not self.prefix else self.prefix}{key}",
                )

    async def list_files(self, wildcard: str = "*") -> List[ItemLocator]:
        """
        Retrieves list of files_names from the object storage

        :param wildcard, str: allow filter the listing of objects
        :return: List of `ItemLocator` with file location info
        """
        n_part_comps = len(self.partition_dateformat.split("/"))
        item_list = []
        async for key in self._aioglob(True, wildcard=wildcard):
            item_list.append(key)
        return [
            self._get_item_locator(item_path, n_part_comps) for item_path in item_list
        ]

    async def _aioglob(self, recursive: bool = False, wildcard: Optional[str] = None):
        """
        A generator function similar to `glob` that lists files in an S3 bucket
        """
        async with self._session.client(S3, **self._conn_config) as object_storage:
            paginator = object_storage.get_paginator("list_objects_v2")
            async for result in paginator.paginate(
                Bucket=self.bucket,
                Prefix="" if not self.prefix else self.prefix,
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

        :param path, str
        :return str: the extracted partition key.
        """
        partition_key = ""
        if self.partition_dateformat:
            partition_key = path.rsplit("/", 1)[0]
        ret = partition_key if not self.prefix else partition_key[len(self.prefix) :]
        return ret

    def _get_item_locator(
        self, item_path: str, n_part_comps: int, suffix: Optional[str] = None
    ) -> ItemLocator:
        """This method generates an `ItemLocator` object from a given `item_path`"""
        comps = item_path.split("/")
        partition_key = (
            "/".join(comps[(-n_part_comps - 1) : -1])
            if self.partition_dateformat
            else None
        )
        item_id = comps[-1][: -len(suffix)] if suffix else comps[-1]
        return ItemLocator(item_id=item_id, partition_key=partition_key)
