.PHONY: all lint test-unit test-e2e test compare up down logs switch generate

COMPOSE_FILE := $(shell cat .compose 2>/dev/null || echo docker-compose.yaml)

switch-query:
	@files=$$(find . -maxdepth 1 -name "*.yaml" -printf "%f\n" | sort); \
	if [ -z "$$files" ]; then echo "No se encontraron archivos .yaml"; exit 1; fi; \
	echo "$$files" | awk '{print "  " NR ") " $$0}'; \
	total=$$(echo "$$files" | wc -l | tr -d ' '); \
	printf "Seleccionar compose [1-$$total]: "; \
	read choice; \
	selected=$$(echo "$$files" | sed -n "$${choice}p"); \
	if [ -z "$$selected" ]; then echo "Opción inválida"; exit 1; fi; \
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
	docker compose -f $(COMPOSE_FILE) down -v --rmi all

logs:
	docker compose -f $(COMPOSE_FILE) logs

all: lint test-unit

lint:
	ruff check src/

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing

test-e2e:
	pytest tests/e2e/ -v

test:
	@mkdir -p output; \
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose-all.yaml up --build --remove-orphans --detach; \
	echo "Esperando que el cliente termine..."; \
	client_exit=$$(docker wait client); \
	if [ "$$client_exit" != "0" ]; then \
		echo "El cliente termino con error (exit code $$client_exit)"; \
		$(MAKE) down COMPOSE_FILE=docker-compose-all.yaml; \
		exit 1; \
	fi; \
	python3 compare_output.py all; \
	compare_exit=$$?; \
	$(MAKE) down COMPOSE_FILE=docker-compose-all.yaml; \
	exit $$compare_exit

compare:
	python3 compare_output.py

generate:
	PYTHONPATH=generate python3 generate/generate_compose_interactive.py