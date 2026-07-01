# Live demo — shot list (for the 60–90s recruiter video)

Run `make up && make live-demo`, wait for the feed to populate, then record
`http://localhost:8000`. Keep it tight; the goal is "real data, answered in
seconds, with citations."

1. **Open cold (0–8s).** Land on the UI. Let the eye catch the header —
   *"ask what's going wrong across live public service-status feeds"* — the pulsing
   **live** chip, and the freshness readout (p50/p95). One line of voiceover:
   *"Freshet ingests real outages from real companies and answers questions about
   them in seconds."*

2. **The live feed (8–20s).** Scroll the right-hand **live incidents** column —
   real Cloudflare / GitHub / OpenAI / Discord incidents, severity chips, status
   (investigating → resolved), and the ticking *"ingested Ns ago"*. Voiceover:
   *"This is real — polled live from public status pages, streamed through a Kafka
   pipeline into pgvector."*

3. **Ask a question (20–45s).** Click the example chip *"what's degraded right
   now?"* (or type a company name). Show the answer render, then the **cited
   evidence** — each citation is a real status update with service + timestamp.
   Voiceover: *"Every answer is grounded in the exact events, with citations — no
   hallucinated summary."*

4. **Freshness beat (45–60s).** Point at the freshness strip / a just-ingested
   incident. Voiceover: *"Event-to-queryable in a few seconds — the whole project
   is built around freshness."*

5. **Close (60–75s).** Cut to the repo / README. Voiceover: *"Hybrid retrieval,
   reranking, an evaluation harness with honest numbers — measured, not just wired
   up. Link in the description."*

**Recording tips:** 1280×800, hide bookmarks bar, do one dry run so the feed is
already populated, keep the cursor deliberate. Export a short GIF of steps 2–3 to
`docs/live-demo.gif` for the README, and the full narrated clip to YouTube/Loom.
