from types import SimpleNamespace

import pytest

from tpuswarm.backend import SkyPilotBackend
from tpuswarm.errors import BackendUnavailableError


class ClusterDoesNotExist(Exception):
    pass


class FakeJobs:
    @staticmethod
    def queue_v2(**kwargs):
        assert kwargs == {
            "refresh": True,
            "skip_finished": False,
            "all_users": False,
            "fields": None,
        }
        return "request-id"


def fake_sky(error: Exception):
    return SimpleNamespace(
        jobs=FakeJobs(),
        exceptions=SimpleNamespace(ClusterDoesNotExist=ClusterDoesNotExist),
        get=lambda request_id: (_ for _ in ()).throw(error),
    )


def test_missing_jobs_controller_is_an_empty_queue(monkeypatch):
    monkeypatch.setattr(
        SkyPilotBackend,
        "_sky",
        staticmethod(lambda: fake_sky(ClusterDoesNotExist("not created"))),
    )

    assert SkyPilotBackend()._list_jobs() == []


def test_other_queue_failures_remain_backend_unavailable(monkeypatch):
    monkeypatch.setattr(
        SkyPilotBackend,
        "_sky",
        staticmethod(lambda: fake_sky(RuntimeError("transport failed"))),
    )

    with pytest.raises(BackendUnavailableError, match="transport failed"):
        SkyPilotBackend()._list_jobs()
