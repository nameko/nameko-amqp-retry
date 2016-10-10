test: flake8 pylint test_lib test_examples

flake8:
	flake8 nameko_amqp_retry test

pylint:
	pylint nameko_amqp_retry -E

test_lib:
	coverage run --concurrency=eventlet --source nameko_amqp_retry --branch -m pytest test
	coverage report --show-missing --fail-under=100

test_examples:
	coverage run --concurrency=eventlet --source docs/examples -m pytest docs/examples/test
	coverage report --show-missing --fail-under=100
