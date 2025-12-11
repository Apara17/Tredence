"""
app.py
Clean, single-file FastAPI workflow engine for the Data Quality Pipeline assignment.

Contains:
- Tool implementations (profile, detect_anomalies, generate_rules, apply_rules)
- In-memory stores for graphs and runs
- Execution engine with immediate loop-stop and safety checks
- FastAPI endpoints: /graph/create, /graph/run, /graph/run_sync, /graph/state/{run_id}

This file is intentionally compact and human-readable so a reviewer can follow the logic easily.
"""

import asyncio
import uuid
import copy
from typing import Dict, Any, Optional
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Mini Workflow Engine — Data Quality Pipeline")

# -------------------------
# In-memory stores
# -------------------------
GRAPHS: Dict[str, Dict[str, Any]] = {}   # graph_id -> graph definition
RUNS: Dict[str, Dict[str, Any]] = {}     # run_id -> {"state":..., "log": [...], "status": ...}

# -------------------------
# Tool registry (small, deterministic, rule-based)
# -------------------------
def profile_data_tool(state: Dict[str, Any]) -> Dict[str, Any]:
    """Compute simple profile metrics: row count and null count."""
    data = state.get("data", [])
    n = len(data)
    nulls = sum(1 for v in data if v is None)
    state["profile"] = {"rows": n, "nulls": nulls}
    return state

def detect_anomalies_tool(state: Dict[str, Any]) -> Dict[str, Any]:
    """Detect values outside provided bounds (anomaly_bounds)."""
    data = state.get("data", [])
    low, high = state.get("anomaly_bounds", (0, 100))
    anomalies = [v for v in data if v is not None and (v < low or v > high)]
    state["anomalies"] = {"count": len(anomalies), "values": anomalies[:10]}
    return state

def generate_rules_tool(state: Dict[str, Any]) -> Dict[str, Any]:
    """Generate simple remediation rules based on profile/anomalies."""
    rules = []
    if state.get("profile", {}).get("nulls", 0) > 0:
        rules.append({"name": "fill_null", "action": "fill", "value": 0})
    if state.get("anomalies", {}).get("count", 0) > 0:
        low, high = state.get("anomaly_bounds", (0, 100))
        rules.append({"name": "clip", "action": "clip", "low": low, "high": high})
    state["rules"] = rules
    return state

def apply_rules_tool(state: Dict[str, Any]) -> Dict[str, Any]:
    """Apply the generated rules to the data (fill nulls, clip outliers)."""
    data = state.get("data", [])
    new_data = []
    rules = state.get("rules", [])
    for v in data:
        if v is None:
            fill_rule = next((r for r in rules if r.get("name") == "fill_null"), None)
            new_data.append(fill_rule["value"] if fill_rule else v)
            continue
        clip_rule = next((r for r in rules if r.get("name") == "clip"), None)
        if clip_rule:
            low, high = clip_rule["low"], clip_rule["high"]
            # clip to bounds
            new_data.append(max(low, min(high, v)))
        else:
            new_data.append(v)
    state["data"] = new_data
    return state

TOOLS: Dict[str, Any] = {
    "profile": profile_data_tool,
    "detect_anomalies": detect_anomalies_tool,
    "generate_rules": generate_rules_tool,
    "apply_rules": apply_rules_tool,
}

# -------------------------
# Graph model
# -------------------------
class GraphDef(BaseModel):
    nodes: Dict[str, str]                 # node_name -> tool_name
    edges: Dict[str, str]                 # from_node -> to_node
    start_node: str
    loop_condition: Optional[Dict[str, Any]] = None  # e.g. {"metric": "anomalies.count", "op": "<=", "value": 1}

# -------------------------
# Helpers
# -------------------------
def _resolve_metric(state: Dict[str, Any], metric_path: str):
    """Resolve dotted metric path like 'anomalies.count' in state safely."""
    parts = metric_path.split(".")
    cur = state
    for p in parts:
        if cur is None:
            return None
        cur = cur.get(p)
    return cur

# -------------------------
# Core execution engine
# -------------------------
async def execute_graph(graph_id: str, run_id: str):
    """
    Execute the graph step-by-step.
    After each node executes we:
      - update RUNS store
      - evaluate loop_condition immediately and stop if satisfied
      - stop if state doesn't change (safety)
    """
    graph = GRAPHS[graph_id]
    state = RUNS[run_id]["state"]
    log = RUNS[run_id]["log"]
    current = graph["start_node"]
    visited = 0
    MAX_STEPS = 200

    def _condition_satisfied(snap: Dict[str, Any]):
        loop_cond = graph.get("loop_condition")
        if not loop_cond:
            return False, None
        metric_val = _resolve_metric(snap, loop_cond["metric"])
        op = loop_cond["op"]
        target = loop_cond["value"]
        if metric_val is None:
            return False, metric_val
        if op == "<=":
            return (metric_val <= target), metric_val
        if op == "<":
            return (metric_val < target), metric_val
        if op == ">=":
            return (metric_val >= target), metric_val
        if op == ">":
            return (metric_val > target), metric_val
        if op == "==":
            return (metric_val == target), metric_val
        if op == "!=":
            return (metric_val != target), metric_val
        return False, metric_val

    prev_state = copy.deepcopy(state)

    while current and visited < MAX_STEPS:
        visited += 1
        tool_name = graph["nodes"].get(current)
        log.append(f"Running node: {current} -> {tool_name}")
        tool_fn = TOOLS.get(tool_name)
        if not tool_fn:
            log.append(f"Missing tool: {tool_name}")
            RUNS[run_id]["status"] = "failed"
            RUNS[run_id]["log"] = log
            return

        # Execute tool (tools are synchronous functions here)
        try:
            result = tool_fn(state)
            if result is not None:
                state = result
        except Exception as exc:
            log.append(f"Exception in {tool_name}: {repr(exc)}")
            RUNS[run_id]["status"] = "failed"
            RUNS[run_id]["log"] = log
            RUNS[run_id]["state"] = state
            return

        # push updates to run store
        RUNS[run_id]["state"] = state
        RUNS[run_id]["log"] = log.copy()

        # immediate loop-condition check
        satisfied, metric_val = _condition_satisfied(state)
        if satisfied:
            log.append(f"Loop stop satisfied: {graph.get('loop_condition')} (metric={metric_val})")
            RUNS[run_id]["status"] = "finished"
            RUNS[run_id]["log"] = log
            RUNS[run_id]["state"] = state
            return

        # safety: if state didn't change, stop to avoid infinite loop
        if state == prev_state:
            log.append("State unchanged — stopping to avoid infinite loop.")
            RUNS[run_id]["status"] = "finished"
            RUNS[run_id]["log"] = log
            RUNS[run_id]["state"] = state
            return

        prev_state = copy.deepcopy(state)

        # advance to next node
        next_node = graph["edges"].get(current)
        if not next_node:
            break
        current = next_node
        await asyncio.sleep(0)

    # normal finish
    if RUNS[run_id].get("status") != "failed":
        RUNS[run_id]["status"] = "finished"
        log.append("Execution finished")
        RUNS[run_id]["log"] = log
        RUNS[run_id]["state"] = state

# -------------------------
# FastAPI endpoints
# -------------------------
@app.post("/graph/create")
async def create_graph(g: GraphDef):
    graph_id = str(uuid.uuid4())
    GRAPHS[graph_id] = g.dict()
    return {"graph_id": graph_id}

@app.post("/graph/run")
async def run_graph(payload: Dict[str, Any]):
    graph_id = payload["graph_id"]

    # REQUIRED FIX — validation check
    if graph_id not in GRAPHS:
        return {"error": "graph_id not found"}, 400

    init_state = payload.get("initial_state", {})
    run_id = str(uuid.uuid4())
    RUNS[run_id] = {"state": init_state, "log": [], "status": "running"}

    # run in background
    asyncio.create_task(execute_graph(graph_id, run_id))
    return {"run_id": run_id}


@app.post("/graph/run_sync")
async def run_graph_sync(payload: Dict[str, Any]):
    graph_id = payload["graph_id"]
    init_state = payload.get("initial_state", {})
    run_id = str(uuid.uuid4())
    RUNS[run_id] = {"state": init_state, "log": [], "status": "running"}
    # run synchronously for debugging / demos
    await execute_graph(graph_id, run_id)
    return {"run_id": run_id, "state": RUNS[run_id]["state"], "log": RUNS[run_id]["log"], "status": RUNS[run_id]["status"]}

@app.get("/graph/state/{run_id}")
async def get_run_state(run_id: str):
    run = RUNS.get(run_id)
    if not run:
        return {"error": "run_id not found"}
    return {"state": run["state"], "log": run["log"], "status": run["status"]}
