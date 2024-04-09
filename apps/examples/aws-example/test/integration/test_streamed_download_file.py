import io

import pytest
from aws_example.s3.streamed_download_file import SomeFile
from hopeit.aws.s3 import ObjectStorage
from hopeit.server.version import APPS_API_VERSION
from hopeit.testing.apps import create_test_context, execute_event
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

APP_VERSION = APPS_API_VERSION.replace(".", "x")


@pytest.mark.asyncio
async def test_streamed_download_file(
    moto_server: ThreadedMotoServer, app_config
):  # pylint: disable=unused-argument
    file_name = "randomfile"

    file = io.BytesIO()
    line = ("x" * 1024 * 1024).encode()
    response_length = 5 * len(line)

    for _ in range(5):
        file.write(line)

    file.seek(0)

    context = create_test_context(app_config, "s3.streamed_download_file")

    storage = await ObjectStorage.with_settings(
        context.settings.extras["object_storage"]
    ).connect()

    file_path = await storage.store_file(file_name=file_name, value=file)

    partition_key = storage.partition_key(file_path)

    result, pp_result, response = await execute_event(
        app_config=app_config,
        event_name="s3.streamed_download_file",
        payload=None,
        postprocess=True,
        file_name=file_name,
        partition_key=partition_key,
    )

    assert result.file_name == file_name
    assert pp_result.file_name == file_name
    assert response.headers == {
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Content-Type": "application/octet-stream",
        "Content-Length": str(response_length),
    }
    assert (
        response.stream_response.resp.data
        == "".join(["x" * 1024 * 1024 for _ in range(5)]).encode()
    )
    assert response.content_type == "application/octet-stream"


@pytest.mark.asyncio
async def test_streamed_download_none(
    moto_server: ThreadedMotoServer, app_config
):  # pylint: disable=unused-argument

    file_name = "randomfile2"

    result, pp_result, response = (  # pylint: disable=unused-variable
        await execute_event(
            app_config=app_config,
            event_name="s3.streamed_download_file",
            payload=None,
            postprocess=True,
            file_name=file_name,
            partition_key="partition_key",
        )
    )

    assert isinstance(result, SomeFile)
    assert result.file_name == file_name
    assert pp_result == "partition_key/randomfile2 not found"
