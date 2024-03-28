import io
from dataclasses import dataclass
from time import sleep
from typing import Optional

import pytest
from hopeit.aws.s3 import (
    ConnectionConfig,
    ItemLocator,
    ObjectStorage,
    ObjectStorageSettings,
)
from hopeit.dataobjects import dataobject
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
async def test_objects(moto_server, monkeypatch):
    """
    This test verifies the behavior of object storage operations when using
    AWS credentials from environment variables.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "hopeit")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "Hopei#Engine#2020")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")

    settings = ObjectStorageSettings(
        bucket="test",
        connection_config=ConnectionConfig(endpoint_url="http://localhost:9002"),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()

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


@pytest.mark.asyncio
async def test_objects_with_partition_key(moto_server, monkeypatch):
    """
    This test verifies the behavior of object storage operations when using
    partition_dateformat.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "hopeit")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "Hopei#Engine#2020")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")

    settings = ObjectStorageSettings(
        bucket="test",
        partition_dateformat="%Y/%m/%d/%H/",
        connection_config=ConnectionConfig(
            endpoint_url="http://localhost:9002", region_name="eu-central-1"
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()

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
    assert items == [ItemLocator(item_id="test2", partition_key=partition_key)]

    await object_storage.delete("test2", partition_key=partition_key)
    no_file = await object_storage.get(
        key="test2", datatype=AwsMockData, partition_key=partition_key
    )
    assert no_file is None


@pytest.mark.asyncio
async def test_objects_with_partition_key_with_prefix(moto_server, monkeypatch):
    """
    This test verifies the behavior of object storage operations when using
    partition_dateformat.
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "hopeit")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "Hopei#Engine#2020")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")

    settings = ObjectStorageSettings(
        bucket="test",
        prefix="my-prefix/",
        partition_dateformat="%Y/%m/%d/%H/",
        connection_config=ConnectionConfig(
            endpoint_url="http://localhost:9002", region_name="eu-central-1"
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()

    object_none = await object_storage.get(key="test2", datatype=AwsMockData)
    assert object_none is None

    location = await object_storage.store(key="test2", value=expected_aws_mock_data)
    partition_key = object_storage.partition_key(location)
    assert location == f"{settings.prefix}{partition_key}/test2.json"

    object_get = await object_storage.get(
        key="test2", datatype=AwsMockData, partition_key=partition_key
    )
    assert object_get == expected_aws_mock_data

    items = await object_storage.list_objects("*test2*")
    assert items == [ItemLocator(item_id="test2", partition_key=partition_key)]

    await object_storage.delete("test2", partition_key=partition_key)
    no_file = await object_storage.get(
        key="test2", datatype=AwsMockData, partition_key=partition_key
    )
    assert no_file is None


@pytest.mark.asyncio
async def test_files(moto_server):
    """
    This test verifies the behavior of file operations when using
    hardcoded AWS credentials.
    """
    settings = ObjectStorageSettings(
        bucket="test",
        create_bucket="true",
        connection_config=ConnectionConfig(
            aws_access_key_id="hopeit",
            aws_secret_access_key="Hopei#Engine#2020",
            endpoint_url="http://localhost:9002",
            region_name="eu-central-1",
            use_ssl=False,
            verify="True",
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()

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


@pytest.mark.asyncio
async def test_files_with_partition_keys(moto_server):
    settings = ObjectStorageSettings(
        bucket="test",
        partition_dateformat="%Y/%m/%d/%H/",
        connection_config=ConnectionConfig(
            aws_access_key_id="hopeit",
            aws_secret_access_key="Hopei#Engine#2020",
            endpoint_url="http://localhost:9002",
            region_name="eu-central-1",
            use_ssl="False",
            verify="False",
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()

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
    assert items == [ItemLocator(item_id="test4.bin", partition_key=partition_key)]

    await object_storage.delete_files("test4.bin", partition_key=partition_key)
    no_file = await object_storage.get_file(
        file_name="test4.bin", partition_key=partition_key
    )
    assert no_file is None


@pytest.mark.asyncio
async def test_files_with_partition_keys_with_prefix(moto_server):
    settings = ObjectStorageSettings(
        bucket="test",
        prefix="prefix2/",
        partition_dateformat="%Y/%m/%d/%H/",
        connection_config=ConnectionConfig(
            aws_access_key_id="hopeit",
            aws_secret_access_key="Hopei#Engine#2020",
            endpoint_url="http://localhost:9002",
            region_name="eu-central-1",
            use_ssl="False",
            verify="False",
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()

    binary_file = b"Binary file"

    object_none = await object_storage.get_file(file_name="test4.bin")
    assert object_none is None

    location = await object_storage.store_file(file_name="test4.bin", value=binary_file)
    partition_key = object_storage.partition_key(location)
    assert location == f"{settings.prefix}{partition_key}/test4.bin"

    file = await object_storage.get_file(
        file_name="test4.bin", partition_key=partition_key
    )
    assert file == binary_file

    items = await object_storage.list_files("*test4.bin*")
    assert items == [ItemLocator(item_id="test4.bin", partition_key=partition_key)]

    await object_storage.delete_files("test4.bin", partition_key=partition_key)
    no_file = await object_storage.get_file(
        file_name="test4.bin", partition_key=partition_key
    )
    assert no_file is None


@pytest.mark.asyncio
async def test_get_file_chunked(moto_server):
    settings = ObjectStorageSettings(
        bucket="test",
        connection_config=ConnectionConfig(
            aws_access_key_id="hopeit",
            aws_secret_access_key="Hopei#Engine#2020",
            endpoint_url="http://localhost:9002",
            region_name="eu-central-1",
            use_ssl="False",
            verify="False",
        ),
    )
    object_storage = await ObjectStorage.with_settings(settings).connect()

    binary_file = b"Binary file"

    location = await object_storage.store_file(file_name="test5.bin", value=binary_file)
    assert location == "test5.bin"

    data = io.BytesIO()
    async for chunk, file_size in object_storage.get_file_chunked(
        file_name="test5.bin"
    ):
        assert file_size == 11
        data.write(chunk)

    assert data.getvalue() == b"Binary file"

    async for chunk, file_size in object_storage.get_file_chunked(
        file_name="test6.bin"
    ):
        assert file_size == 0
        assert chunk is None
