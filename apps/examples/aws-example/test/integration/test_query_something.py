"""
aws-example tests
"""

import uuid

import pytest
from aws_example.model import Something, SomethingNotFound
from hopeit.app.config import AppConfig
from hopeit.aws.s3 import ObjectStorage, ObjectStorageSettings
from hopeit.dataobjects.payload import Payload
from hopeit.testing.apps import create_test_context, execute_event
from moto.moto_server.threaded_moto_server import ThreadedMotoServer


async def sample_file_id(app_config: AppConfig):
    """Creates sample_file"""
    test_id = str(uuid.uuid4())
    test_id1 = test_id + "a"
    json_str = (
        '{"id": "'
        + test_id1
        + '", "user": {"id": "u1", "name": "test_user"}, '
        + '"status": {"ts": "2020-05-01T00:00:00Z", "type": "NEW"}, "history": []}'
    )
    context = create_test_context(app_config, "s3.query_something")
    settings = context.settings(key="object_storage", datatype=ObjectStorageSettings)
    storage = await ObjectStorage.with_settings(settings).connect()

    ret = await storage.store(key=test_id1, value=Payload.from_json(json_str, datatype=Something))

    return test_id1, storage.partition_key(ret)


@pytest.mark.asyncio
async def test_query_item(moto_server: ThreadedMotoServer, app_config: AppConfig):
    """Test s3.query_something"""
    await execute_event(app_config=app_config, event_name="s3.init", payload=None)

    test_id = await sample_file_id(app_config)

    result, pp_result, _ = await execute_event(
        app_config=app_config,
        event_name="s3.query_something",
        payload=None,
        postprocess=True,
        item_id=test_id[0],
        partition_key=test_id[1],
    )
    assert isinstance(result, Something)
    assert result == pp_result
    assert result.id == test_id[0]


@pytest.mark.asyncio
async def test_query_item_not_found(
    moto_server: ThreadedMotoServer,
    app_config: AppConfig,
):
    """Test s3.query_something"""
    item_id = str(uuid.uuid4())
    result, pp_result, res = await execute_event(
        app_config=app_config,
        event_name="s3.query_something",
        payload=None,
        postprocess=True,
        item_id=item_id,
        partition_key="x",
    )
    assert res.status == 404
    assert result == pp_result
    assert result == SomethingNotFound(path="x", id=item_id)
