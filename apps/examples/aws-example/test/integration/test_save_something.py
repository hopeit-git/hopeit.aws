"""
aws-example tests
"""

import os

import pytest
from aws_example.model import Something
from hopeit.aws.s3 import ObjectStorage
from hopeit.aws.s3.partition import get_partition_key
from hopeit.testing.apps import create_test_context, execute_event
from moto.moto_server.threaded_moto_server import ThreadedMotoServer


@pytest.mark.asyncio
async def test_save_something(
    moto_server: ThreadedMotoServer, app_config, something_params_example
):
    """Test s3.save_something"""
    await execute_event(app_config=app_config, event_name="s3.init", payload=None)

    result = await execute_event(
        app_config=app_config,
        event_name="s3.save_something",
        payload=something_params_example,
    )
    partition_key = get_partition_key(something_params_example, "%Y/%m/%d/%H")
    assert result == f"{partition_key}test.json"

    context = create_test_context(app_config, "s3.save_something")
    storage = await ObjectStorage.with_settings(context.settings.extras["object_storage"]).connect()
    partition_key, file_name = os.path.split(result)
    key, _ = os.path.splitext(file_name)
    saved_object = await storage.get(key=key, partition_key=partition_key, datatype=Something)

    assert isinstance(saved_object, Something)
    assert saved_object.id == something_params_example.id
    assert saved_object.user.name == something_params_example.user
