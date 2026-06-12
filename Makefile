COMPOSE := docker compose
PYTHON := $(shell command -v python3 2>/dev/null || command -v python)

.PHONY: up up-obs down db-init smoke test test-integration api slice replay scale-demo

# Bring the stack up and block until both containers report healthy.
up:
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
	@docker exec freshet-redpanda rpk topic create raw.events normalized.events deadletter.events -p 3 >/dev/null 2>&1 || true
	@echo "topics ready (3 partitions)."

# Bring up the stack plus Prometheus (:9090) and Grafana (:3000).
up-obs:
	COMPOSE_PROFILES=obs $(MAKE) up

# Tear down and drop the Postgres volume.
down:
	COMPOSE_PROFILES=obs $(COMPOSE) down -v

# Apply the schema to a running stack (idempotent).
db-init:
	docker exec -i freshet-postgres psql -v ON_ERROR_STOP=1 -U freshet -d freshet < db/init.sql

# Run the unit tests (no broker needed; integration tests are excluded by pytest addopts).
test:
	$(PYTHON) -m pytest -q

# Integration tests against the running stack (make up first).
test-integration:
	$(PYTHON) -m pytest -q -m integration

# Produce -> consume -> validate against the real broker, and confirm Postgres.
# --count 60 emits 69 events total (60 noise + 9 scripted incident). A unique
# consumer group makes this re-runnable without tearing the stack down.
smoke:
	$(PYTHON) -m freshet.generator --sink kafka --brokers localhost:9092 --count 60
	$(PYTHON) -m freshet.pipeline.consumer_helloworld --brokers localhost:9092 --max 69 --group smoke-$$(date +%s)
	pg_isready -h localhost -p 5433

# Serve the query API on :8000 (stack must be up; FRESHET_EMBEDDER=stub to skip model).
api:
	$(PYTHON) -m uvicorn freshet.api.app:app --port 8000

# Run the vertical-slice demo end to end (make up first; EMBEDDER=stub to skip model).
slice:
	bash scripts/run_slice.sh

# Re-index the whole corpus under a fresh consumer group (e.g. after a model
# change). Reads normalized.events from the beginning; idempotent upserts
# overwrite rows in place. EMBEDDER=stub skips the model download.
replay:
	$(PYTHON) -m freshet.pipeline.embedder --brokers localhost:9092 --group reindex-$$(date +%s) --embedder $${EMBEDDER:-minilm} --metrics-port 0 --idle-timeout 10

# Throughput demo: WORKERS=1 make scale-demo, then WORKERS=3 make scale-demo.
scale-demo:
	bash scripts/run_scaling_demo.sh
