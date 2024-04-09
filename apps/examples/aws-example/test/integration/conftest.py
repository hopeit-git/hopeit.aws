import time

import pytest
from aws_example.model import SomethingParams
from hopeit.testing.apps import config
from moto.server import ThreadedMotoServer


@pytest.fixture
def something_params_example():
    return SomethingParams(id="test", user="test_user")


@pytest.fixture
def patch_env(monkeypatch):
    monkeypatch.setenv("OBJECT_STORAGE_ACCESS_KEY_ID", "hopeit")
    monkeypatch.setenv("OBJECT_STORAGE_SECRET_ACCESS_KEY", "Hopei#Engine#2020")
    monkeypatch.setenv("OBJECT_STORAGE_ENDPOINT_URL", "http://localhost:9002")
    monkeypatch.setenv("OBJECT_STORAGE_SSL", "false")
    monkeypatch.setenv("HOPEIT_AWS_API_VERSION", "0.1")
    return "patch_env"


@pytest.fixture
def app_config(patch_env):  # pylint: disable=redefined-outer-name
    return config("apps/examples/aws-example/config/app-config.json")


# Fixture to start and stop the Moto server
@pytest.fixture(scope="session")
def moto_server():
    server = ThreadedMotoServer(port=9002)
    server.start()
    time.sleep(1)
    yield server
    server.stop()
