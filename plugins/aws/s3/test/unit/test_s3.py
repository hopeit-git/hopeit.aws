import io
from dataclasses import dataclass
from typing import Optional

import pytest
from hopeit.aws.s3 import (ConnectionConfig, ItemLocator, ObjectStorage,
                           ObjectStorageSettings)
from hopeit.dataobjects import dataobject
from moto.server import ThreadedMotoServer


# Fixture to start and stop the Moto server
@pytest.fixture(scope="session")
def moto_server():
    server = ThreadedMotoServer(port=9002)
    server.start()
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


test_aws = AwsMockData(test="test_aws")
test_aws_none = AwsMockEmpty()


@pytest.mark.asyncio
async def test_objects_related_tasks(moto_server):
    """Simple getting of client."""
    settings = ObjectStorageSettings(
        bucket="test",
        connection_config=ConnectionConfig(endpoint_url="http://localhost:9002"),
    )
    object_store = await ObjectStorage.with_settings(settings)

    object_none = await object_store.get(key="test", datatype=AwsMockData)
    assert object_none is None
    empty_file = io.BytesIO(b"")
    location = await object_store.store_file(
        file_name="test_none.json", value=empty_file
    )
    assert location == "test_none.json"
    object_none = await object_store.get(key="test_none", datatype=AwsMockEmpty)
    assert object_none is None
    location = await object_store.store(key="test", value=test_aws)
    assert location == "test.json"
    object_get = await object_store.get(key="test", datatype=AwsMockData)
    assert object_get == test_aws
    items = await object_store.list_objects()
    assert items == [
        ItemLocator(item_id="test", partition_key=None),
        ItemLocator(item_id="test_none", partition_key=None),
    ]


@pytest.mark.asyncio
async def test_objects_with_partition_key(moto_server):
    """Simple getting of client."""
    settings = ObjectStorageSettings(
        bucket="test",
        partition_dateformat="%Y/%m/%d/%H/",
        connection_config=ConnectionConfig(endpoint_url="http://localhost:9002"),
    )
    object_store = await ObjectStorage.with_settings(settings)

    object_none = await object_store.get(key="test2", datatype=AwsMockData)
    assert object_none is None

    location = await object_store.store(key="test2", value=test_aws)
    partition_key = object_store.partition_key(location)
    assert location == f"{partition_key}/test2.json"

    object_get = await object_store.get(
        key="test2", datatype=AwsMockData, partition_key=partition_key
    )
    assert object_get == test_aws

    items = await object_store.list_objects("*test2*")
    assert items == [ItemLocator(item_id="test2", partition_key=partition_key)]


@pytest.mark.asyncio
async def test_files_related_tasks(moto_server):
    """Simple getting of client."""
    settings = ObjectStorageSettings(
        bucket="test",
        create_bucket="true",
        connection_config=ConnectionConfig(
            endpoint_url="http://localhost:9002", use_ssl=False, verify="True"
        ),
    )
    object_store = await ObjectStorage.with_settings(settings)

    binary_file = b"Binary file"

    object_none = await object_store.get_file(file_name="test3.bin")
    assert object_none is None
    location = await object_store.store_file(
        file_name="test3.bin", value=io.BytesIO(binary_file)
    )
    assert location == "test3.bin"
    object_get = await object_store.get_file(file_name="test3.bin")
    assert object_get == binary_file
    items = await object_store.list_files("*.bin")
    assert items == [ItemLocator(item_id="test3.bin", partition_key=None)]


@pytest.mark.asyncio
async def test_files_with_partition_keys(moto_server):
    """Simple getting of client."""
    settings = ObjectStorageSettings(
        bucket="test",
        partition_dateformat="%Y/%m/%d/%H/",
        connection_config=ConnectionConfig(
            endpoint_url="http://localhost:9002", use_ssl="False", verify="False"
        ),
    )
    object_store = await ObjectStorage.with_settings(settings)

    binary_file = b"Binary file"

    object_none = await object_store.get_file(file_name="test4.bin")
    assert object_none is None

    location = await object_store.store_file(
        file_name="test4.bin", value=io.BytesIO(binary_file)
    )
    partition_key = object_store.partition_key(location)
    assert location == f"{partition_key}/test4.bin"

    file = await object_store.get_file(
        file_name="test4.bin", partition_key=partition_key
    )
    assert file == binary_file

    items = await object_store.list_files("*test4.bin*")
    assert items == [ItemLocator(item_id="test4.bin", partition_key=partition_key)]


@pytest.mark.asyncio
async def test_get_file_chunked(moto_server):
    settings = ObjectStorageSettings(
        bucket="test",
        connection_config=ConnectionConfig(
            endpoint_url="http://localhost:9002", use_ssl="False", verify="False"
        ),
    )
    object_store = await ObjectStorage.with_settings(settings)

    binary_file = b"Binary file"

    location = await object_store.store_file(
        file_name="test5.bin", value=io.BytesIO(binary_file)
    )
    assert location == "test5.bin"

    data = io.BytesIO()
    async for chunk, file_size in object_store.get_file_chunked(file_name="test5.bin"):
        assert file_size == 11
        data.write(chunk)

    assert data.getvalue() == b"Binary file"

    async for chunk, file_size in object_store.get_file_chunked(file_name="test6.bin"):
        assert file_size == 0
        assert chunk is None
