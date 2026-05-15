.PHONY: all lint test-unit test-e2e

all: lint test-unit

lint:
	ruff check src/

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

test-e2e:
	pytest tests/e2e/ -v
