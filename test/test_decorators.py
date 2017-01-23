import pytest

from nameko_amqp_retry.backoff import Backoff
from nameko_amqp_retry.decorators import entrypoint_retry


class TestEntrypointRetry(object):

    class MyService:

        def __init__(self, raises=None):
            self.raises = raises

        @entrypoint_retry()
        def method1(self, arg1):
            if self.raises:
                raise self.raises
            return arg1

        @entrypoint_retry(limit=3, schedule=(100, 200))
        def method2(self, arg1, kwarg1=None):
            if self.raises:
                raise self.raises
            return arg1, kwarg1

        @entrypoint_retry(retry_for=(TypeError, KeyError))
        def method3(self, arg1):
            if self.raises:
                raise self.raises
            return arg1

    @pytest.fixture
    def good_service(self):
        return self.MyService()

    @pytest.fixture
    def value_error_service(self):
        return self.MyService(ValueError())

    @pytest.fixture
    def type_error_service(self):
        return self.MyService(TypeError())

    @pytest.fixture
    def key_error_service(self):
        return self.MyService(KeyError())

    def test_default_entrypoint_retry(self, good_service, value_error_service):
        assert good_service.method1(5) == 5

        with pytest.raises(Backoff) as exc:
            value_error_service.method1(5)

        assert exc.value.limit == 20
        assert exc.value.schedule == (
            1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000
        )

    def test_custom_entrypoint_settings(
        self, good_service, value_error_service
    ):
        assert good_service.method2(5, kwarg1=6) == (5, 6)

        with pytest.raises(Backoff) as exc:
            value_error_service.method2(5, kwarg1=6)

        assert exc.value.limit == 3
        assert exc.value.schedule == (100, 200)

    def test_custom_retry_for(
        self, value_error_service, type_error_service, key_error_service
    ):
        # value error will just be raised (it is not in the `retry_for`)
        with pytest.raises(ValueError):
            value_error_service.method3(5)

        # but Backoff will be raised for TypeError and KeyError
        with pytest.raises(Backoff):
            type_error_service.method3(5)

        with pytest.raises(Backoff):
            key_error_service.method3(5)
