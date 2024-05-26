import uuid

import pytest
from aws_example.model import Something
from hopeit.app.config import AppConfig
from hopeit.aws.s3 import ObjectStorage, ObjectStorageSettings
from hopeit.dataobjects.payload import Payload
from hopeit.server.version import APPS_API_VERSION
from hopeit.testing.apps import create_test_context, execute_event
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

APP_VERSION = APPS_API_VERSION.replace(".", "x")


async def sample_file_id(app_config: AppConfig):
    test_id = str(uuid.uuid4())
    test_id1 = test_id + "a"
    test_id2 = test_id + "b"
    json_str1 = (
        '{"id": "'
        + test_id1
        + '", "user": {"id": "u1", "name": "test_user"}, '
        + '"status": {"ts": "2020-05-01T00:00:00Z", "type": "NEW"}, "history": []}'
    )
    json_str2 = (
        '{"id": "'
        + test_id2
        + '", "user": {"id": "u1", "name": "test_user"}, '
        + '"status": {"ts": "2020-05-01T00:00:00Z", "type": "NEW"}, "history": []}'
    )

    context = create_test_context(app_config, "s3.list_objects")
    settings = context.settings(key="object_storage", datatype=ObjectStorageSettings)
    storage = await ObjectStorage.with_settings(settings).connect()
    await storage.create_bucket()

    ret = await storage.store(
        key=test_id1, value=Payload.from_json(json_str1, datatype=Something)
    )
    assert ret == f"2020/05/01/00/{test_id1}.json"
    ret = await storage.store(
        key=test_id2, value=Payload.from_json(json_str2, datatype=Something)
    )
    assert ret == f"2020/05/01/00/{test_id2}.json"

    return test_id


@pytest.mark.asyncio
async def test_list_objects(
    moto_server: ThreadedMotoServer, app_config: AppConfig
):  # pylint: disable=unused-argument
    test_id = await sample_file_id(app_config)

    results = await execute_event(
        app_config=app_config,
        event_name="s3.list_objects",
        payload=None,
        wildcard=f"2020/05/01/00/{test_id}*",
    )

    assert len(results) == 2
    assert all(result.id.startswith(test_id) for result in results)
    assert all(isinstance(result, Something) for result in results)
