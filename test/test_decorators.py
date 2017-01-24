import pytest

from nameko_amqp_retry.backoff import Backoff
from nameko_amqp_retry.decorators import entrypoint_retry


class TestEntrypointRetry(object):

    class MyService:

        @entrypoint_retry()
        def method1(self, arg1, raises=None):
            if raises:
                raise raises
            return arg1

        @entrypoint_retry(limit=3, schedule=(100, 200))
        def method2(self, arg1, raises=None, kwarg1=None):
            if raises:
                raise raises
            return arg1, kwarg1

        @entrypoint_retry(retry_for=(TypeError, KeyError))
        def method3(self, arg1, raises=None):
            if raises:
                raise raises
            return arg1

    @pytest.fixture
    def service(self):
        return self.MyService()

    def test_default_entrypoint_retry(self, service):
        assert service.method1(5) == 5

        with pytest.raises(Backoff) as exc:
            service.method1(5, raises=ValueError)

        assert exc.value.limit == 20
        assert exc.value.schedule == (
            1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000
        )

    def test_custom_entrypoint_settings(self, service):
        assert service.method2(5, kwarg1=6) == (5, 6)

        with pytest.raises(Backoff) as exc:
            service.method2(5, raises=ValueError, kwarg1=6)

        assert exc.value.limit == 3
        assert exc.value.schedule == (100, 200)

    def test_custom_retry_for(self, service):
        # value error will just be raised (it is not in the `retry_for`)
        with pytest.raises(ValueError):
            service.method3(5, raises=ValueError)

        # but Backoff will be raised for TypeError and KeyError
        with pytest.raises(Backoff):
            service.method3(5, raises=TypeError)

        with pytest.raises(Backoff):
            service.method3(5, raises=KeyError)
