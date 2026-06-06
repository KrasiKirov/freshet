COMPOSE := docker compose
PY := PYTHONPATH=. python

.PHONY: up down smoke test

# Bring the stack up and block until both containers report healthy.
up:
	$(COMPOSE) up -d
	@echo "waiting for services to be healthy..."
	@i=0; until [ "$$(docker inspect -f '{{.State.Health.Status}}' beacon-redpanda 2>/dev/null)" = "healthy" ] \
		&& [ "$$(docker inspect -f '{{.State.Health.Status}}' beacon-postgres 2>/dev/null)" = "healthy" ]; do \
		i=$$((i+1)); \
		if [ $$i -ge 30 ]; then \
			echo "ERROR: stack did not become healthy after 60s"; \
			docker inspect -f '{{.Name}} -> {{.State.Health.Status}}' beacon-redpanda beacon-postgres; \
			exit 1; \
		fi; \
		sleep 2; echo "  ...still waiting ($$i/30)"; \
	done
	@echo "stack healthy."

# Tear down and drop the Postgres volume.
down:
	$(COMPOSE) down -v

# Run the unit tests (no broker needed).
test:
	$(PY) -m pytest -q

# Produce -> consume -> validate against the real broker, and confirm Postgres.
# --count 60 emits 69 events total (60 noise + 9 scripted incident). A unique
# consumer group makes this re-runnable without tearing the stack down.
smoke:
	$(PY) -m generator --sink kafka --brokers localhost:9092 --count 60
	$(PY) -m pipeline.consumer_helloworld --brokers localhost:9092 --max 69 --group smoke-$$(date +%s)
	pg_isready -h localhost -p 5433
