import time
from datetime import datetime, timezone

import pytest
from aws_example.model import Something, SomethingParams, Status, StatusType, User
from hopeit.testing.apps import config
from moto.server import ThreadedMotoServer

EVENT_TS = datetime.strptime("2020-05-01T00:00:00", "%Y-%m-%dT%H:%M:%S").astimezone(
    tz=timezone.utc
)


@pytest.fixture
def something_example():
    return Something(id="test", user=User(id="u1", name="test_user"))


@pytest.fixture
def something_upload_example():
    return Something(id="attachment", user=User(id="test", name="test_user"))


@pytest.fixture
def something_params_example():
    return SomethingParams(id="test", user="test_user")


@pytest.fixture
def something_submitted():
    return Something(
        id="test",
        user=User(id="u1", name="test_user"),
        status=Status(ts=EVENT_TS, type=StatusType.SUBMITTED),
    )


@pytest.fixture
def something_processed():
    return Something(
        id="test",
        user=User(id="u1", name="test_user"),
        status=Status(ts=EVENT_TS, type=StatusType.PROCESSED),
        history=[Status(ts=EVENT_TS, type=StatusType.SUBMITTED)],
    )


@pytest.fixture
def something_with_status_example():
    return Something(
        id="test",
        user=User(id="u1", name="test_user"),
        status=Status(ts=EVENT_TS, type=StatusType.NEW),
    )


@pytest.fixture
def something_with_status_submitted_example():
    return Something(
        id="test",
        user=User(id="u1", name="test_user"),
        status=Status(ts=EVENT_TS, type=StatusType.SUBMITTED),
        history=[Status(ts=EVENT_TS, type=StatusType.NEW)],
    )


@pytest.fixture
def something_with_status_processed_example():
    return Something(
        id="test",
        user=User(id="u1", name="test_user"),
        status=Status(ts=EVENT_TS, type=StatusType.PROCESSED),
        history=[
            Status(ts=EVENT_TS, type=StatusType.NEW),
            Status(ts=EVENT_TS, type=StatusType.SUBMITTED),
        ],
    )


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
