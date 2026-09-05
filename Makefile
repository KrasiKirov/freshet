COMPOSE := docker compose
# Prefer the repo's own virtualenv; `make PYTHON=...` overrides.
PYTHON := $(if $(wildcard $(CURDIR)/.venv/bin/python),$(CURDIR)/.venv/bin/python,$(shell command -v python3 2>/dev/null || command -v python))

.PHONY: help up down db-init test test-integration poller api autopilot

.DEFAULT_GOAL := help

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
	@sh deploy/topics.sh >/dev/null
	@echo "topics declared (see deploy/topics.sh)."

down: ##stack
	COMPOSE_PROFILES=obs $(COMPOSE) down -v

db-init: ##stack
	docker exec -i freshet-postgres psql -v ON_ERROR_STOP=1 -U freshet -d freshet < db/init.sql

# db-init only applies the schema; this applies a one-off migration.
db-migrate: ##stack
	@test -n "$(FILE)" || { echo "usage: make db-migrate FILE=db/migrations/....sql"; exit 1; }
	docker exec -i freshet-postgres psql -v ON_ERROR_STOP=1 -U freshet -d freshet < $(FILE)

test: ##dev
	$(PYTHON) -m pytest -q

# Integration tests need the stack up (make up).
test-integration: ##dev
	$(PYTHON) -m pytest -q -m integration


# Load-bearing: if undefined, $(FLINK_HOME) expands to empty and flink-dist
# silently downloads a bogus URL.
FLINK_VERSION := 1.20.0
FLINK_HOME := .flink/flink-$(FLINK_VERSION)

flink-dist: ##stack
	@mkdir -p .flink
	@test -d $(FLINK_HOME) || (cd .flink && \
	  curl -sSL -o flink.tgz https://archive.apache.org/dist/flink/flink-$(FLINK_VERSION)/flink-$(FLINK_VERSION)-bin-scala_2.12.tgz && \
	  tar -xzf flink.tgz && rm flink.tgz)
	@test -f $(FLINK_HOME)/lib/flink-sql-connector-kafka.jar || curl -sSL -o $(FLINK_HOME)/lib/flink-sql-connector-kafka.jar \
	  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
	@echo "flink $(FLINK_VERSION) ready in $(FLINK_HOME)"

stream: flink-dist ##run
	@$(FLINK_HOME)/bin/start-cluster.sh >/dev/null 2>&1 || true
	@sleep 5
	@# Cancel any job already running, or a second submission doubles output.
	@for j in $$(curl -s -m 5 http://localhost:8081/jobs 2>/dev/null \
	    | tr ',' '\n' | grep -B1 RUNNING | grep -o '[0-9a-f]\{32\}'); do \
	  echo "cancelling running job $$j"; \
	  curl -s -X PATCH "http://localhost:8081/jobs/$$j?mode=cancel" >/dev/null; \
	done
	@sleep 3
	$(FLINK_HOME)/bin/sql-client.sh -f freshet/stream/dedup_job.sql

stream-stop: ##run
	@$(FLINK_HOME)/bin/stop-cluster.sh

stream-health: ##run
	@$(PYTHON) -m freshet.stream.health

embedder: ##run
	$(PYTHON) -m freshet.pipeline.embedder

poller: ##run
	$(PYTHON) -m freshet.ingest.poller

# Runs autopilot alone, unsupervised. launchd instead execs deploy/run-live.sh,
# which runs poller + embedder + autopilot under freshet.ops.supervisor.
run-forever: ##run
	@if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi; \
	mkdir -p logs; \
	FRESHET_TORCH_THREADS=$${FRESHET_TORCH_THREADS:-2} \
	FRESHET_LLM_HOURLY_CAP=$${FRESHET_LLM_HOURLY_CAP:-60} \
	FRESHET_LLM_DAILY_CAP=$${FRESHET_LLM_DAILY_CAP:-500} \
	FRESHET_SINK=slack \
	exec $(PYTHON) -m freshet.autopilot --brokers localhost:9092 --sink slack

service-install: ##run
	@mkdir -p $(HOME)/Library/LaunchAgents logs
	@chmod +x deploy/run-live.sh
	@sed 's|__REPO__|$(CURDIR)|g' deploy/com.freshet.autopilot.plist \
	  > $(HOME)/Library/LaunchAgents/com.freshet.autopilot.plist
	@launchctl unload $(HOME)/Library/LaunchAgents/com.freshet.autopilot.plist 2>/dev/null || true
	@launchctl load $(HOME)/Library/LaunchAgents/com.freshet.autopilot.plist
	@echo "loaded. logs: logs/autopilot.log  stop: make service-stop"

service-stop: ##run
	@launchctl unload $(HOME)/Library/LaunchAgents/com.freshet.autopilot.plist 2>/dev/null || true
	@echo "stopped (agent unloaded; plist left in place)"

autopilot: ##run
	@if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi; \
	$(PYTHON) -m freshet.autopilot --brokers localhost:9092

demo-brief: ##run
	@if [ -f .env.local ]; then set -a; . ./.env.local; set +a; fi; \
	$(PYTHON) -m freshet.autopilot.demo_trigger $(ARGS)

index-stats: ##eval
	$(PYTHON) -m freshet.pipeline.index_stats $(ARGS)

live-eval: ##eval
	$(PYTHON) -m freshet.eval.live_retrieval

# FRESHNESS_MIN_N=20 make freshness -> fails instead of reporting a thin sample.
freshness: ##eval
	$(PYTHON) -m freshet.eval.freshness
