COMPOSE := docker compose
PY := PYTHONPATH=. python

.PHONY: up down smoke test

# Bring the stack up and block until both containers report healthy.
up:
	$(COMPOSE) up -d
	@echo "waiting for services to be healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' beacon-redpanda 2>/dev/null)" = "healthy" ] \
		&& [ "$$(docker inspect -f '{{.State.Health.Status}}' beacon-postgres 2>/dev/null)" = "healthy" ]; do \
		sleep 2; echo "  ...still waiting"; \
	done
	@echo "stack healthy."

# Tear down and drop the Postgres volume.
down:
	$(COMPOSE) down -v

# Run the unit tests (no broker needed; uses the JSONL sink).
test:
	$(PY) -m pytest -q

# Produce -> consume -> validate against the real broker, and confirm Postgres.
smoke:
	$(PY) -m generator --sink kafka --brokers localhost:9092 --count 60
	$(PY) -m pipeline.consumer_helloworld --brokers localhost:9092 --max 69
	pg_isready -h localhost -p 5433
