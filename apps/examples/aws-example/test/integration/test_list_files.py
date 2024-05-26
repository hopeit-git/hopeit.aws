import uuid

import pytest
from hopeit.app.config import AppConfig
from hopeit.aws.s3 import ItemLocator, ObjectStorage, ObjectStorageSettings
from hopeit.server.version import APPS_API_VERSION
from hopeit.testing.apps import create_test_context, execute_event
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

APP_VERSION = APPS_API_VERSION.replace(".", "x")


async def sample_file_id(app_config: AppConfig):
    test_id = str(uuid.uuid4())
    test_id1 = test_id + "a"
    test_id2 = test_id + "b"
    binary_file1 = b"Binary file 1"
    binary_file2 = b"Binary file 2"

    context = create_test_context(app_config, "s3.list_files")
    settings = context.settings(key="object_storage", datatype=ObjectStorageSettings)
    storage: ObjectStorage = await ObjectStorage.with_settings(settings).connect()
    await storage.create_bucket()

    await storage.store_file(file_name=test_id1, value=binary_file1)
    ret = await storage.store_file(file_name=test_id2, value=binary_file2)

    return test_id, storage.partition_key(ret)


@pytest.mark.asyncio
async def test_list_files(
    moto_server: ThreadedMotoServer, app_config: AppConfig
):  # pylint: disable=unused-argument
    test_id, partition_key = await sample_file_id(app_config)

    results = await execute_event(
        app_config=app_config,
        event_name="s3.list_files",
        payload=None,
        wildcard=f"{partition_key}/{test_id}*",
    )

    assert len(results) == 2
    assert all(result.item_id.startswith(test_id) for result in results)
    assert all(result.partition_key.startswith(partition_key) for result in results)
    assert all(isinstance(result, ItemLocator) for result in results)
