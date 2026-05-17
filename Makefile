.PHONY: all lint test-unit test-e2e

all: lint test-unit

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

lint:
	ruff check src/

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

test-e2e:
	pytest tests/e2e/ -v
