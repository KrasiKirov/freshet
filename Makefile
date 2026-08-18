COMPOSE := docker compose
PYTHON := $(shell command -v python3 2>/dev/null || command -v python)

.PHONY: help up down db-init test test-integration api autopilot

.DEFAULT_GOAL := help

# `make` with no argument lists what exists, grouped. There are ~29 targets;
# without this the only way to discover them is to read the file.
help: ##meta
	@echo "Freshet — make targets"
	@for g in stack dev run demo eval; do \
		list=$$(grep -E "^[a-z][a-z-]*: ##$$g$$" $(MAKEFILE_LIST) | sed "s/: ##.*//" | sort); \
		[ -z "$$list" ] && continue; \
		case $$g in \
			stack) label="Stack lifecycle";; dev) label="Tests and checks";; \
			run) label="Long-running services";; demo) label="Demos (things to watch)";; \
			eval) label="Evaluations (things to measure)";; \
		esac; \
		echo ""; echo "  $$label:"; echo "$$list" | sed "s/^/    make /"; \
	done
	@echo ""

# Bring the stack up and block until both containers report healthy.
up: ##stack
	$(COMPOSE) up -d
	@echo "waiting for services to be healthy..."
	@i=0; until [ "$$(docker inspect -f '{{.State.Health.Status}}' freshet-redpanda 2>/dev/null)" = "healthy" ] \
		&& [ "$$(docker inspect -f '{{.State.Health.Status}}' freshet-postgres 2>/dev/null)" = "healthy" ]; do \
		i=$$((i+1)); \
		if [ $$i -ge 30 ]; then \
			echo "ERROR: stack did not become healthy after 60s"; \
			docker inspect -f '{{.Name}} -> {{.State.Health.Status}}' freshet-redpanda freshet-postgres; \
			exit 1; \
		fi; \
		sleep 2; echo "  ...still waiting ($$i/30)"; \
	done
	@echo "stack healthy."
	@docker exec freshet-redpanda rpk topic create raw.events normalized.events deadletter.events incident.lifecycle -p 3 >/dev/null 2>&1 || true
	@echo "topics ready (3 partitions)."

# Tear down and drop the Postgres volume.
down: ##stack
	COMPOSE_PROFILES=obs $(COMPOSE) down -v

# Apply the schema to a running stack (idempotent).
db-init: ##stack
	docker exec -i freshet-postgres psql -v ON_ERROR_STOP=1 -U freshet -d freshet < db/init.sql

# Run the unit tests (no broker needed; integration tests are excluded by pytest addopts).
test: ##dev
	$(PYTHON) -m pytest -q

# Integration tests against the running stack (make up first).
test-integration: ##dev
	$(PYTHON) -m pytest -q -m integration

# Serve the query API on :8000 (stack must be up; FRESHET_EMBEDDER=stub to skip model).
# Sources .env.local so ANTHROPIC_API_KEY enables the LLM answer composer.
api: ##run
	@if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi; \
	$(PYTHON) -m uvicorn freshet.api.app:app --port 8000

# Autopilot: consume incident.lifecycle and print a cited brief per new incident.
# Sources .env.local so ANTHROPIC_API_KEY enables LLM postmortem narrative synthesis
# (keyless extractive timeline otherwise).
autopilot: ##run
	@if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi; \
	$(PYTHON) -m freshet.autopilot --brokers localhost:9092

