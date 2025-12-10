# /app/main.py
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import Dict, Any
from .engine.graph import create_graph, start_run_background, get_run, list_graphs
from .workflows.data_quality import build_data_quality_graph
from .db import init_db, load_graph
from .utils.broadcaster import broadcaster
import uvicorn

app = FastAPI(title="Workflow Engine (Level 1)", version="0.2.0")


class CreateGraphRequest(BaseModel):
    nodes: Dict[str, Dict]
    edges: Dict[str, list]
    start_node: str | None = None
    max_visits: int | None = None


class RunRequest(BaseModel):
    graph_id: str
    initial_state: Dict[str, Any] = {}


@app.on_event("startup")
def startup():
    init_db()


@app.post("/graph/create")
def api_create_graph(req: CreateGraphRequest):
    try:
        graph_id = create_graph(req.dict())
        return {"graph_id": graph_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/graph/run")
def api_run_graph(req: RunRequest):
    from .engine.graph import GRAPHS
    if req.graph_id not in GRAPHS and not load_graph(req.graph_id):
        raise HTTPException(status_code=404, detail="graph not found")
    run_id = start_run_background(req.graph_id, req.initial_state)
    return {"run_id": run_id}


@app.get("/graph/state/{run_id}")
def api_get_run(run_id: str):
    run = get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="run not found")
    # simple serializable dict
    return {
        "run_id": run.run_id,
        "graph_id": run.graph_id,
        "status": run.status.value,
        "state": run.state.data,
        "logs": run.logs,
        "current_node": run.current_node,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "metrics": getattr(run, "metrics", {})
    }


@app.get("/graphs")
def api_list_graphs():
    return list_graphs()


# Demo endpoints retained
@app.post("/demo/create_data_quality")
def demo_create_data_quality(threshold: int = 2):
    graph_def = build_data_quality_graph(threshold=threshold, required_fields=["id", "value"])
    graph_id = create_graph(graph_def)
    return {"graph_id": graph_id}


@app.post("/demo/run_data_quality")
def demo_run_data_quality(threshold: int = 2):
    graph_def = build_data_quality_graph(threshold=threshold, required_fields=["id", "value"])
    graph_id = create_graph(graph_def)
    sample_rows = [
        {"id": 1, "value": 10},
        {"id": 2, "value": -5},
        {"id": 3, "value": None},
        {"id": 4, "value": 12},
        {"id": 5, "value": 7},
    ]
    initial_state = {"rows": sample_rows}
    run_id = start_run_background(graph_id, initial_state)
    return {"run_id": run_id}


# WebSocket endpoint to stream logs for a run (Level 1 feature)
@app.websocket("/ws/logs/{run_id}")
async def websocket_logs(websocket: WebSocket, run_id: str):
    await websocket.accept()
    queue = await broadcaster.subscribe(run_id)
    try:
        # send any existing persisted logs first (so client sees history)
        # attempt to load from DB
        from .db import load_run
        persisted = load_run(run_id)
        if persisted:
            for m in (persisted.get("logs") or []):
                await websocket.send_text(m)
        # then stream new messages as they arrive
        while True:
            msg = await queue.get()
            await websocket.send_text(msg)
    except WebSocketDisconnect:
        broadcaster.unsubscribe(run_id, queue)
    except Exception:
        broadcaster.unsubscribe(run_id, queue)
        await websocket.close()
