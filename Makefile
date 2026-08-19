COMPOSE := docker compose
# Prefer the repo's own virtualenv. Every run target imports project dependencies
# (confluent_kafka, psycopg, ...), and a bare `python3` on PATH is the system
# interpreter, which does not have them — so an unactivated shell failed with
# ModuleNotFoundError. Falls back to PATH (CI installs into its own env), and
# `make PYTHON=/path/to/python` still overrides both.
PYTHON := $(if $(wildcard $(CURDIR)/.venv/bin/python),$(CURDIR)/.venv/bin/python,$(shell command -v python3 2>/dev/null || command -v python))

.PHONY: help up down db-init test test-integration poller api autopilot

.DEFAULT_GOAL := help

# `make` with no argument lists what exists, grouped. There are ~29 targets;
# without this the only way to discover them is to read the file.
help: ##meta
	@echo "Freshet — make targets"
	@for g in stack dev run demo eval; do \
		list=$$(grep -E "^[a-z][a-z-]*:[^#]*##$$g$$" $(MAKEFILE_LIST) | sed "s/:.*//" | sort); \
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
	@docker exec freshet-redpanda rpk topic create raw.incidents normalized.updates deadletter.events incident.lifecycle -p 3 >/dev/null 2>&1 || true
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
FLINK_VERSION := 1.20.0
FLINK_HOME := .flink/flink-$(FLINK_VERSION)

# One-time: fetch the Flink distribution and the Kafka connector.
# The job is Flink SQL (pure JVM) — PyFlink is not used and not installable here,
# because apache-flink requires apache-beam, which ships no macOS ARM64 wheel.
flink-dist: ##stack
	@mkdir -p .flink
	@test -d $(FLINK_HOME) || (cd .flink && \
	  curl -sSL -o flink.tgz https://archive.apache.org/dist/flink/flink-$(FLINK_VERSION)/flink-$(FLINK_VERSION)-bin-scala_2.12.tgz && \
	  tar -xzf flink.tgz && rm flink.tgz)
	@test -f $(FLINK_HOME)/lib/flink-sql-connector-kafka.jar || curl -sSL -o $(FLINK_HOME)/lib/flink-sql-connector-kafka.jar \
	  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
	@echo "flink $(FLINK_VERSION) ready in $(FLINK_HOME)"

# Start the local Flink cluster and submit the dedup + lifecycle job.
stream: flink-dist ##run
	@$(FLINK_HOME)/bin/start-cluster.sh >/dev/null 2>&1 || true
	@sleep 5
	@# Cancel any job already running. Each submission carries its OWN dedup state,
	@# so a second job does not share the first's — it re-emits every update, and the
	@# topic (and the embedder's workload) multiplies by the number of live jobs.
	@for j in $$(curl -s -m 5 http://localhost:8081/jobs 2>/dev/null \
	    | tr ',' '\n' | grep -B1 RUNNING | grep -o '[0-9a-f]\{32\}'); do \
	  echo "cancelling running job $$j"; \
	  curl -s -X PATCH "http://localhost:8081/jobs/$$j?mode=cancel" >/dev/null; \
	done
	@sleep 3
	$(FLINK_HOME)/bin/sql-client.sh -f freshet/stream/dedup_job.sql

stream-stop: ##run
	@$(FLINK_HOME)/bin/stop-cluster.sh

embedder: ##run
	$(PYTHON) -m freshet.pipeline.embedder

poller: ##run
	$(PYTHON) -m freshet.ingest.poller

api: ##run
	@if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi; \
	$(PYTHON) -m uvicorn freshet.api.app:app --port 8000

# Autopilot: consume incident.lifecycle and print a cited brief per new incident.
# Sources .env.local for ANTHROPIC_API_KEY, which the brief composer requires.
autopilot: ##run
	@if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi; \
	$(PYTHON) -m freshet.autopilot --brokers localhost:9092

review-labels: ##eval
	@# Print a reproducible 20-row sample for HUMAN review; --apply records verdicts.
	$(PYTHON) -m freshet.eval.review_labels $(ARGS)

calibrate-abstention: ##eval
	@# Proposes an abstention floor from paraphrased live labels. Never writes it.
	$(PYTHON) -m freshet.eval.calibrate_abstention

label-live: ##eval
	@# Curate cause labels from the LIVE index (LLM judge; output is draft).
	$(PYTHON) -m freshet.eval.label_live

retrieval-eval: ##eval
	@# Indexes the labeled fixture corpus into a DEDICATED freshet_eval database.
	$(PYTHON) -m freshet.eval.retrieval_eval

# Only meaningful after poller + stream + embedder have run together for HOURS:
# it scores updates POSTED after indexing began, and there are only ~2/hour.
# FRESHNESS_MIN_N=20 make freshness  -> fails instead of reporting a thin sample.
freshness: ##eval
	$(PYTHON) -m freshet.eval.freshness
