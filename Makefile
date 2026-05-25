.PHONY: all lint test-unit test-e2e up down logs switch

COMPOSE_FILE := $(shell cat .compose 2>/dev/null || echo docker-compose.yaml)

switch:
	@printf "Seleccionar query a ejecutar:\n  A) Query 1\n  B) Query 3\n  C) Query 4\n  D) Query 5\n  E) Todas las queries (default)\nIngrese una opción: "; \
	read choice; \
	case "$${choice:-E}" in \
		[Aa]) selected="docker-compose-q1.yaml" ;; \
		[Bb]) selected="docker-compose-q3.yaml" ;; \
		[Cc]) selected="docker-compose-q4.yaml" ;; \
		[Dd]) selected="docker-compose-q5.yaml" ;; \
		[Ee]) selected="docker-compose.yaml" ;; \
		*) echo "Opción inválida"; exit 1 ;; \
	esac; \
	echo "$$selected" > .compose; \
	echo "Usando: $$selected"

up:
	@echo "Usando: $(COMPOSE_FILE)"
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f $(COMPOSE_FILE) up --build --remove-orphans --detach
	docker compose -f $(COMPOSE_FILE) logs --follow

down:
	sudo rm -rf output
	sudo rm -rf src/business/query3/spill_to_disk/*
	docker compose -f $(COMPOSE_FILE) stop -t 5
	docker compose -f $(COMPOSE_FILE) down

logs:
	docker compose -f $(COMPOSE_FILE) logs

all: lint test-unit

lint:
	ruff check src/

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

test-e2e:
	pytest tests/e2e/ -v