# Beacon — Real-Time Incident Intelligence (Phase 0)

Freshness-first streaming-RAG system for on-call engineers. See `BRIEF.md` for the
full what/why/how and build order. This repo currently contains **Phase 0**: the
data contract, a deterministic synthetic event generator, Kafka I/O helpers, a
produce→consume hello-world, and tests.

## Run (Phase 0)
    pip install -r requirements.txt
    PYTHONPATH=. python -m generator --sink jsonl --out events.jsonl --count 200
    PYTHONPATH=. pytest -q

With a broker (docker-compose, added in Phase 0 infra step):
    PYTHONPATH=. python -m generator --sink kafka --brokers localhost:9092 --topic raw.events
    PYTHONPATH=. python -m pipeline.consumer_helloworld --brokers localhost:9092

## Layout
    common/      # schemas (the contract) + kafka helpers
    generator/   # synthetic events + scripted incident scenario
    pipeline/    # consumers (Phase 0: hello-world; Phase 1+: normalizer, embedder)
    tests/       # schema + generator tests
