What this repo contains
- app/app.py : FastAPI service implementing the workflow engine.
  - Endpoints: POST /graph/create, POST /graph/run, POST /graph/run_sync, GET /graph/state/{run_id}
  - Tool registry: profile, detect_anomalies, generate_rules, apply_rules
  - Looping and safe termination included

How I tested it (Colab)
1. Start server (in Colab):
   - Use nest_asyncio and uvicorn; example:
     ```
     import nest_asyncio, uvicorn
     nest_asyncio.apply()
     uvicorn.run("app.app:app", host="127.0.0.1", port=8000)
     ```
   - Or run the provided notebook cells which start uvicorn in a separate process.

2. Demo (synchronous run):
   - POST /graph/create with the graph JSON (sample in notebook)
   - POST /graph/run_sync with graph_id and initial_state (sample data)
   - The run_sync response returns final state and execution log.

Notes / what I would improve with more time
- Persist graphs & runs to SQLite so runs survive restarts.
- Add WebSocket streaming for live logs.
- Add unit tests (pytest) for tools and engine stop conditions.
- Add input validation for tool outputs and better error models.


Note: the Colab notebook includes a short synchronous demo (run_sync) that reproduces the exact final state used for evaluation.
