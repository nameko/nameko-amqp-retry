import pytest
from mock import Mock, call, patch

from nameko_amqp_retry import Backoff
from nameko_amqp_retry.backoff import round_to_nearest


BACKOFF_COUNT = 3


class TestGetNextExpiration(object):

    @pytest.fixture
    def backoff(self):
        class CustomBackoff(Backoff):
            schedule = [1000, 2000, 3000]
            random_sigma = 0
            limit = 10

        return CustomBackoff()

    @pytest.fixture
    def backoff_without_limit(self):
        class CustomBackoff(Backoff):
            schedule = [1000, 2000, 3000]
            random_sigma = 0
            limit = 0  # no limit

        return CustomBackoff()

    @pytest.fixture
    def backoff_with_random_sigma(self):
        class CustomBackoff(Backoff):
            schedule = [1000, 2000, 3000]
            random_sigma = 100
            limit = 10

        return CustomBackoff()

    def test_first_backoff(self, backoff):
        message = Mock()
        message.headers = {}
        assert backoff.get_next_expiration(message, "backoff") == 1000

    def test_next_backoff(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 1
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 2000

    def test_last_backoff_single_queue(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 3
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 3000

    def test_last_backoff_multiple_queues(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 2
            }, {
                'exchange': 'backoff',
                'queue': 'backoff--2',
                'count': 1
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 3000

    def test_count_greater_than_schedule_length(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 5
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 3000

    def test_count_greater_than_limit(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 99
            }]
        }
        with pytest.raises(Backoff.Expired) as exc_info:
            backoff.get_next_expiration(message, "backoff")
        # 27 = 1 + 2 + 3 * 8
        assert str(exc_info.value) == (
            "Backoff aborted after '10' retries (~27 seconds)"
        )

    def test_count_equal_to_limit(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 5
            }, {
                'exchange': 'backoff',
                'queue': 'backoff--2',
                'count': 5
            }]
        }
        with pytest.raises(Backoff.Expired) as exc_info:
            backoff.get_next_expiration(message, "backoff")
        # 27 = 1 + 2 + 3 * 8
        assert str(exc_info.value) == (
            "Backoff aborted after '10' retries (~27 seconds)"
        )

    def test_previously_deadlettered_first_backoff(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                # previously deadlettered elsewhere
                'exchange': 'not-backoff',
                'count': 99
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 1000

    def test_previously_deadlettered_next_backoff(self, backoff):
        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 1
            }, {
                'exchange': 'backoff',
                'queue': 'backoff--2',
                'count': 1
            }, {
                # previously deadlettered elsewhere
                'exchange': 'not-backoff',
                'queue': 'irrelevant',
                'count': 99
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 3000

    def test_no_limit(self, backoff_without_limit):

        backoff = backoff_without_limit

        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 999
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 3000

    @patch('nameko_amqp_retry.backoff.random')
    def test_backoff_randomness(self, random_patch, backoff_with_random_sigma):

        random_patch.gauss.return_value = 2205.5

        backoff = backoff_with_random_sigma

        message = Mock()
        message.headers = {
            'x-death': [{
                'exchange': 'backoff',
                'queue': 'backoff--1',
                'count': 1
            }]
        }
        assert backoff.get_next_expiration(message, "backoff") == 2200
        assert random_patch.gauss.call_args_list == [
            call(2000, backoff.random_sigma)
        ]


@pytest.mark.parametrize("value,interval,result", [
    (2000, 1, 2000),
    (2000, 10, 2000),
    (2001, 10, 2000),
    (2009, 10, 2010),
    (0, 10, 0),
    (10, 10, 10),
    (10, -10, 10),
    (-10, 10, -10),
    (-10, -10, -10),
    (2205.5, 20, 2200),  # used in test_backoff_randomness
])
def test_round_to_nearest(value, interval, result):
    assert round_to_nearest(value, interval) == result
