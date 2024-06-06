"""
aws-example tests
"""

import os

import pytest
from hopeit.aws.s3 import ObjectStorage
from hopeit.testing.apps import create_test_context, execute_event
from moto.moto_server.threaded_moto_server import ThreadedMotoServer


@pytest.mark.asyncio
async def test_streamed_upload_file(
    moto_server: ThreadedMotoServer,
    app_config,
):
    """Test s3.streamed_upload_file"""
    await execute_event(app_config=app_config, event_name="s3.init", payload=None)

    file_name = "test_file_name.bytes"
    file_content = b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    file_size = len(file_content)
    fields = {
        "attachment": file_name,
    }

    upload = {"attachment": file_content}

    result = await execute_event(
        app_config=app_config,
        event_name="s3.streamed_upload_file",
        payload=None,
        fields=fields,
        upload=upload,
        preprocess=True,
    )

    assert result[0].file_id == "attachment"
    assert result[0].saved_path == "hopeit-store"
    assert result[0].size == file_size

    partition_key, file_name = os.path.split(result[0].file_name)

    context = create_test_context(app_config, "s3.streamed_upload_file")

    storage = await ObjectStorage.with_settings(
        context.settings.extras["object_storage"]
    ).connect()

    data = await storage.get_file(file_name=file_name, partition_key=partition_key)

    assert data == upload["attachment"]


@pytest.mark.asyncio
async def test_it_save_something_missing_field(
    moto_server: ThreadedMotoServer,
    app_config,
):
    """Test s3.streamed_upload_file"""
    await execute_event(app_config=app_config, event_name="s3.init", payload=None)

    fields = {}

    result, _, response = await execute_event(
        app_config=app_config,
        event_name="s3.streamed_upload_file",
        payload=None,
        fields=fields,
        preprocess=True,
        postprocess=True,
    )

    assert result == "Missing required fields"
    assert response.status == 400
