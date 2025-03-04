"""
aws-example tests
"""

import pytest
from hopeit.app.config import AppConfig
from hopeit.testing.apps import execute_event
from moto.moto_server.threaded_moto_server import ThreadedMotoServer


@pytest.mark.asyncio
async def test_upload_download_file(moto_server: ThreadedMotoServer, app_config: AppConfig):
    await execute_event(app_config=app_config, event_name="s3.init", payload=None)

    result = await execute_event(
        app_config=app_config, event_name="s3.upload_download_file", payload=None
    )
    assert result == "Done"
