COMPOSE := docker compose
PYTHON := $(shell command -v python3 2>/dev/null || command -v python)

.PHONY: up up-obs down db-init test test-integration api slice demo replay scale-demo eval drills rootcause-demo rootcause-eval answer-eval agent-eval agent-demo embedding-compare multiquery-eval live-demo

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


# Serve the query API on :8000 (stack must be up; FRESHET_EMBEDDER=stub to skip model).
api:
	$(PYTHON) -m uvicorn freshet.api.app:app --port 8000

# Run the vertical-slice demo end to end (make up first; EMBEDDER=stub to skip model).
slice:
	bash scripts/run_slice.sh

# One-command demo: ingest the scripted incident, then answer a question about it.
demo:
	bash scripts/run_demo.sh

# Re-index the whole corpus under a fresh consumer group (e.g. after a model
# change). Reads normalized.events from the beginning; idempotent upserts
# overwrite rows in place. EMBEDDER=stub skips the model download.
replay:
	$(PYTHON) -m freshet.pipeline.embedder --brokers localhost:9092 --group reindex-$$(date +%s) --embedder $${EMBEDDER:-minilm} --metrics-port 0 --idle-timeout 10

# Throughput demo: WORKERS=1 make scale-demo, then WORKERS=3 make scale-demo.
scale-demo:
	bash scripts/run_scaling_demo.sh

# Regenerate the committed eval artifacts (results/). Needs the stack up and
# .[embed] .[eval] installed. Deterministic: same inputs -> same numbers.
eval:
	$(PYTHON) -m freshet.eval.run_eval

# Run the failure drills (stack up; .[embed] .[eval]). Writes results/drill_*.png
# and asserts no data loss. Live + timing-sensitive — run deliberately.
drills:
	$(PYTHON) -m freshet.eval.drills


# Stream the richer corpus, then print a cited root-cause timeline (keyless demo).
rootcause-demo:
	bash scripts/run_rootcause_demo.sh

# Keyless completeness eval: hybrid vs hybrid+rerank on cause/fix capture (results/).
rootcause-eval:
	$(PYTHON) -m freshet.eval.rootcause

# Key-gated: extractive timeline vs LLM narrative on faithfulness + relevance (results/).
answer-eval:
	$(PYTHON) -m freshet.eval.answer_eval

# Key-gated: agentic investigator vs single-shot baseline; scores lift in cause/fix recall (results/).
agent-eval:
	$(PYTHON) -m freshet.eval.agent_eval

# Key-gated: investigate one benchmark incident and save the investigation transcript (results/).
agent-demo:
	$(PYTHON) scripts/run_agent_demo.py

# Deterministic MiniLM-vs-bge retrieval comparison (stack up, fresh vector(768) DB).
embedding-compare:
	$(PYTHON) scripts/run_embedding_compare.py

# Key-gated: multi-query vs single-query retrieval recall (needs a key + fresh DB).
multiquery-eval:
	$(PYTHON) -m freshet.eval.multiquery_eval

# Live demo: ingest REAL public status-feed incidents through the pipeline + open the UI.
live-demo:
	bash scripts/run_live_demo.sh
