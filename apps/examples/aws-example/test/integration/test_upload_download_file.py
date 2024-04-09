import pytest
from hopeit.app.config import AppConfig
from hopeit.server.version import APPS_API_VERSION
from hopeit.testing.apps import execute_event
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

APP_VERSION = APPS_API_VERSION.replace(".", "x")


@pytest.mark.asyncio
async def test_upload_download_file(
    moto_server: ThreadedMotoServer, app_config: AppConfig
):  # pylint: disable=unused-argument

    result = await execute_event(
        app_config=app_config, event_name="s3.upload_download_file", payload=None
    )
    assert result == "Done"
