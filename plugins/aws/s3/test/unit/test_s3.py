"""
hopeit.aws.s3 tests
"""

import io
from time import sleep
from typing import Optional

import pytest
from botocore.exceptions import ClientError
from hopeit.aws.s3 import (
    ConnectionConfig,
    ItemLocator,
    ObjectStorage,
    ObjectStorageSettings,
)
from hopeit.dataobjects import dataclass, dataobject
from moto.server import ThreadedMotoServer


# Fixture to start and stop the Moto server
@pytest.fixture(scope="session")
def moto_server():
    server = ThreadedMotoServer(port=9002)
    server.start()
    sleep(1)
    yield server
    server.stop()


@dataobject
@dataclass
class AwsMockData:
    test: str


@dataobject
@dataclass
class AwsMockEmpty:
    test: Optional[str] = None


expected_aws_mock_data = AwsMockData(test="test_aws")
expected_aws_mock_empty = AwsMockEmpty()


@pytest.mark.asyncio
async def test_bucket_creation(moto_server, monkeypatch):
    """
    This test verifies the behavior of object storage operations when using
    AWS credentials from environment variables.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "hopeit")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "Hopeit#Engine#2020")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")

    settings = ObjectStorageSettings(
        bucket="test",
        prefix="prefix/",
        connection_config=ConnectionConfig(endpoint_url="http://localhost:9002"),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()

    await object_storage.create_bucket(exist_ok=True)
    await object_storage.create_bucket(exist_ok=True)

    with pytest.raises(ClientError) as excinfo:
        await object_storage.create_bucket()

    assert excinfo.value.operation_name == "CreateBucket"
    assert excinfo.value.response["Error"]["Code"] == "BucketAlreadyOwnedByYou"


@pytest.mark.parametrize("prefix", [None, "some_prefix/", "no_slash", "/"])
@pytest.mark.asyncio
async def test_objects(prefix, moto_server, monkeypatch):
    """
    This test verifies the behavior of object storage operations when using
    AWS credentials from environment variables.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "hopeit")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "Hopeit#Engine#2020")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")

    settings = ObjectStorageSettings(
        bucket="test",
        prefix=prefix,
        connection_config=ConnectionConfig(endpoint_url="http://localhost:9002"),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()
    await object_storage.create_bucket(exist_ok=True)

    object_none = await object_storage.get(key="test", datatype=AwsMockData)
    assert object_none is None
    empty_file = io.BytesIO(b"")
    location = await object_storage.store_file(
        file_name="test_none.json", value=empty_file
    )
    assert location == "test_none.json"
    object_none = await object_storage.get(key="test_none", datatype=AwsMockEmpty)
    assert object_none is None
    location = await object_storage.store(key="test", value=expected_aws_mock_data)
    assert location == "test.json"
    object_get = await object_storage.get(key="test", datatype=AwsMockData)
    assert object_get == expected_aws_mock_data
    items = await object_storage.list_objects()

    expected_items = [
        ItemLocator(item_id="test", partition_key=None),
        ItemLocator(item_id="test_none", partition_key=None),
    ]
    for expected_item in expected_items:
        if expected_item not in items:
            assert False, f"Expected item {expected_item} not found in items list"

    await object_storage.delete("test")
    await object_storage.delete("test_none")


@pytest.mark.parametrize("prefix", [None, "some_prefix/", "no_slash", "/"])
@pytest.mark.asyncio
async def test_objects_with_partition_key(prefix, moto_server, monkeypatch):
    """
    This test verifies the behavior of object storage operations when using
    partition_dateformat.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "hopeit")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "Hopeit#Engine#2020")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")

    settings = ObjectStorageSettings(
        bucket="test",
        prefix=prefix,
        partition_dateformat="%Y/%m/%d/%H/",
        connection_config=ConnectionConfig(
            endpoint_url="http://localhost:9002", region_name="eu-central-1"
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()
    await object_storage.create_bucket(exist_ok=True)

    object_none = await object_storage.get(key="test2", datatype=AwsMockData)
    assert object_none is None

    location = await object_storage.store(key="test2", value=expected_aws_mock_data)
    partition_key = object_storage.partition_key(location)
    assert location == f"{partition_key}/test2.json"

    object_get = await object_storage.get(
        key="test2", datatype=AwsMockData, partition_key=partition_key
    )
    assert object_get == expected_aws_mock_data

    items = await object_storage.list_objects("*test2*")
    assert items == []
    items = await object_storage.list_objects("*test2*", recursive=True)
    assert items == [ItemLocator(item_id="test2", partition_key=partition_key)]

    await object_storage.delete("test2", partition_key=partition_key)
    no_file = await object_storage.get(
        key="test2", datatype=AwsMockData, partition_key=partition_key
    )
    assert no_file is None


@pytest.mark.parametrize("prefix", [None, "some_prefix/", "no_slash", "/"])
@pytest.mark.asyncio
async def test_files(prefix, moto_server):
    """
    This test verifies the behavior of file operations when using
    hardcoded AWS credentials.
    """

    object_storage = await ObjectStorage(bucket="test", prefix=prefix).connect(
        connection_config={
            "aws_access_key_id": "hopeit",
            "aws_secret_access_key": "Hopeit#Engine#2020",
            "endpoint_url": "http://localhost:9002",
            "region_name": "eu-central-1",
            "use_ssl": False,
            "verify": "True",
        }
    )
    await object_storage.create_bucket(exist_ok=True)

    binary_file = b"Binary file"

    object_none = await object_storage.get_file(file_name="test3.bin")
    assert object_none is None
    location = await object_storage.store_file(
        file_name="test3.bin", value=io.BytesIO(binary_file)
    )
    assert location == "test3.bin"
    object_get = await object_storage.get_file(file_name="test3.bin")
    assert object_get == binary_file
    items = await object_storage.list_files("*.bin")
    assert items == [ItemLocator(item_id="test3.bin", partition_key=None)]
    await object_storage.delete_files("test3.bin")


@pytest.mark.parametrize("prefix", [None, "some_prefix/", "no_slash", "/"])
@pytest.mark.asyncio
async def test_files_with_partition_keys(prefix, moto_server):
    """File operations with partition_key"""
    settings = {
        "bucket": "test",
        **({"prefix": prefix} if prefix else {}),
        "partition_dateformat": "%Y/%m/%d/%H/",
        "connection_config": {
            "aws_access_key_id": "hopeit",
            "aws_secret_access_key": "Hopeit#Engine#2020",
            "endpoint_url": "http://localhost:9002",
            "region_name": "eu-central-1",
            "use_ssl": False,
            "verify": False,
        },
    }

    object_storage = await ObjectStorage.with_settings(settings).connect()
    await object_storage.create_bucket(exist_ok=True)

    binary_file = b"Binary file"

    object_none = await object_storage.get_file(file_name="test4.bin")
    assert object_none is None

    location = await object_storage.store_file(file_name="test4.bin", value=binary_file)
    partition_key = object_storage.partition_key(location)
    assert location == f"{partition_key}/test4.bin"

    file = await object_storage.get_file(
        file_name="test4.bin", partition_key=partition_key
    )
    assert file == binary_file

    items = await object_storage.list_files("*test4.bin*")
    assert items == []
    items = await object_storage.list_files("*test4.bin*", recursive=True)
    assert items == [ItemLocator(item_id="test4.bin", partition_key=partition_key)]

    await object_storage.delete_files("test4.bin", partition_key=partition_key)
    no_file = await object_storage.get_file(
        file_name="test4.bin", partition_key=partition_key
    )
    assert no_file is None
    await object_storage.delete_files("test4.bin")


@pytest.mark.parametrize("prefix", [None, "some_prefix/", "no_slash", "/"])
@pytest.mark.asyncio
async def test_get_file_chunked(prefix, moto_server):
    """Get file by chunks"""
    settings = ObjectStorageSettings(
        bucket="test",
        prefix=prefix,
        connection_config=ConnectionConfig(
            aws_access_key_id="hopeit",
            aws_secret_access_key="Hopeit#Engine#2020",
            endpoint_url="http://localhost:9002",
            region_name="eu-central-1",
            use_ssl="False",
            verify="False",
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()
    await object_storage.create_bucket(exist_ok=True)

    binary_file = b"Binary file"

    location = await object_storage.store_file(file_name="test6.bin", value=binary_file)
    assert location == "test6.bin"

    data = io.BytesIO()
    async for chunk, file_size in object_storage.get_file_chunked(
        file_name="test6.bin"
    ):
        assert file_size == 11
        data.write(chunk)

    assert data.getvalue() == b"Binary file"

    async for chunk, file_size in object_storage.get_file_chunked(
        file_name="test7.bin"
    ):
        assert file_size == 0
        assert chunk is None

    await object_storage.delete_files("test6.bin")
    await object_storage.delete_files("test7.bin")


@pytest.mark.parametrize("prefix", [None, "some_prefix/", "no_slash", "/"])
@pytest.mark.asyncio
async def test_list(prefix, moto_server):
    """Get file by chunks"""
    settings = ObjectStorageSettings(
        bucket="test",
        prefix=prefix,
        connection_config=ConnectionConfig(
            aws_access_key_id="hopeit",
            aws_secret_access_key="Hopeit#Engine#2020",
            endpoint_url="http://localhost:9002",
            region_name="eu-central-1",
            use_ssl="False",
            verify="False",
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()
    await object_storage.create_bucket(exist_ok=True)

    mock_data = AwsMockData(test="test_aws")

    location = await object_storage.store(key="test01", value=mock_data)
    assert location == "test01.json"

    location = await object_storage.store(key="test02", value=mock_data)
    assert location == "test02.json"

    location = await object_storage.store(key="sub_dir/test03", value=mock_data)
    assert location == "sub_dir/test03.json"

    location = await object_storage.store(key="sub_dir/test04", value=mock_data)
    assert location == "sub_dir/test04.json"

    nofilter_no_recursive = await object_storage.list_objects()
    nofilter_recursive = await object_storage.list_objects(recursive=True)
    filter_no_recursive = await object_storage.list_objects(wildcard="*")
    filter_recursive = await object_storage.list_objects(wildcard="*", recursive=True)

    assert nofilter_no_recursive == [
        ItemLocator(item_id="test01"),
        ItemLocator(item_id="test02"),
    ]

    assert nofilter_recursive == [
        ItemLocator(item_id="sub_dir/test03"),
        ItemLocator(
            item_id="sub_dir/test04",
        ),
        ItemLocator(item_id="test01"),
        ItemLocator(item_id="test02"),
    ]

    assert filter_no_recursive == [
        ItemLocator(item_id="test01"),
        ItemLocator(item_id="test02"),
    ]

    assert filter_recursive == [
        ItemLocator(item_id="sub_dir/test03"),
        ItemLocator(item_id="sub_dir/test04"),
        ItemLocator(item_id="test01"),
        ItemLocator(item_id="test02"),
    ]

    await object_storage.delete("test01")
    await object_storage.delete("test02")
    await object_storage.delete("sub_dir/test03")
    await object_storage.delete("sub_dir/test04")


@pytest.mark.parametrize("prefix", [None, "some_prefix/", "no_slash", "/"])
@pytest.mark.asyncio
async def test_list_files(prefix, moto_server):
    """Get file by chunks"""
    settings = ObjectStorageSettings(
        bucket="test",
        prefix=prefix,
        connection_config=ConnectionConfig(
            aws_access_key_id="hopeit",
            aws_secret_access_key="Hopeit#Engine#2020",
            endpoint_url="http://localhost:9002",
            region_name="eu-central-1",
            use_ssl="False",
            verify="False",
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()
    await object_storage.create_bucket(exist_ok=True)

    binary_file = b"Binary file"

    location = await object_storage.store_file(
        file_name="test01.bin", value=binary_file
    )
    assert location == "test01.bin"

    location = await object_storage.store_file(
        file_name="test02.bin", value=binary_file
    )
    assert location == "test02.bin"

    location = await object_storage.store_file(
        file_name="test01.tmp", value=binary_file
    )
    assert location == "test01.tmp"

    location = await object_storage.store_file(
        file_name="sub_dir/test03.bin", value=binary_file
    )
    assert location == "sub_dir/test03.bin"

    location = await object_storage.store_file(
        file_name="sub_dir/test04.bin", value=binary_file
    )
    assert location == "sub_dir/test04.bin"

    location = await object_storage.store_file(
        file_name="sub_dir/test01.tmp", value=binary_file
    )
    assert location == "sub_dir/test01.tmp"

    nofilter_no_recursive = await object_storage.list_files()
    nofilter_recursive = await object_storage.list_files(recursive=True)
    filter_no_recursive = await object_storage.list_files(wildcard="*.bin")
    filter_recursive = await object_storage.list_files(wildcard="*.bin", recursive=True)
    filter_recursive_subdir = await object_storage.list_files(
        wildcard="sub_dir/*.bin", recursive=True
    )

    assert nofilter_no_recursive == [
        ItemLocator(item_id="test01.bin"),
        ItemLocator(item_id="test01.tmp"),
        ItemLocator(item_id="test02.bin"),
    ]

    assert nofilter_recursive == [
        ItemLocator(item_id="sub_dir/test01.tmp"),
        ItemLocator(item_id="sub_dir/test03.bin"),
        ItemLocator(item_id="sub_dir/test04.bin"),
        ItemLocator(item_id="test01.bin"),
        ItemLocator(item_id="test01.tmp"),
        ItemLocator(item_id="test02.bin"),
    ]

    assert filter_no_recursive == [
        ItemLocator(item_id="test01.bin"),
        ItemLocator(item_id="test02.bin"),
    ]

    assert filter_recursive == [
        ItemLocator(item_id="sub_dir/test03.bin"),
        ItemLocator(item_id="sub_dir/test04.bin"),
        ItemLocator(item_id="test01.bin"),
        ItemLocator(item_id="test02.bin"),
    ]

    assert filter_recursive_subdir == [
        ItemLocator(item_id="sub_dir/test03.bin"),
        ItemLocator(item_id="sub_dir/test04.bin"),
    ]

    await object_storage.delete_files("test01.bin")
    await object_storage.delete_files("test02.bin")
    await object_storage.delete_files("test01.tmp")
    await object_storage.delete_files("sub_dir/test03.bin")
    await object_storage.delete_files("sub_dir/test04.bin")
    await object_storage.delete_files("sub_dir/test01.tmp")
