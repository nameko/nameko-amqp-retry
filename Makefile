test: flake8 pylint pytest

flake8:
	flake8 nameko_amqp_retry test

pylint:
	pylint nameko_amqp_retry -E

pytest:
	coverage run --concurrency=eventlet --source nameko_amqp_retry --branch -m pytest test
	coverage report --show-missing --fail-under=100
