# /app/engine/graph.py
import asyncio
import uuid
import time
from typing import Dict, Any, List, Optional
from .node import NodeDef
from .state import RunModel, RunStatus, StateModel
from ..registry.tools import TOOLS, get_tool
from datetime import datetime
from ..db import save_graph, save_run, load_graph, load_run
from ..utils.broadcaster import broadcaster

# In-memory cache mirrors DB for quick listing; primary store is DB via db.py
GRAPHS: Dict[str, Dict] = {}
RUNS_CACHE: Dict[str, RunModel] = {}

# Basic graph validation
def validate_graph_definition(defn: Dict):
    nodes = defn.get("nodes", {})
    edges = defn.get("edges", {})
    start_node = defn.get("start_node") or (next(iter(nodes)) if nodes else None)
    if not nodes:
        raise ValueError("Graph must contain 'nodes'")
    if not edges:
        raise ValueError("Graph must contain 'edges'")
    if start_node not in nodes:
        raise ValueError("start_node must be one of nodes")
    # verify edges reference valid nodes
    for src, outs in edges.items():
        if src not in nodes:
            raise ValueError(f"Edge source '{src}' not in nodes")
        for o in outs:
            nxt = o.get("next")
            if nxt and nxt not in nodes:
                raise ValueError(f"Edge from {src} references unknown next node '{nxt}'")
    # simple unreachable detection (nodes not reachable from start)
    reachable = set()
    stack = [start_node]
    while stack:
        cur = stack.pop()
        if cur in reachable:
            continue
        reachable.add(cur)
        for out in edges.get(cur, []):
            nxt = out.get("next")
            if nxt:
                stack.append(nxt)
    unreachable = set(nodes.keys()) - reachable
    if unreachable:
        raise ValueError(f"Unreachable nodes detected: {unreachable}")
    return True


def create_graph(definition: Dict) -> str:
    # validate
    validate_graph_definition(definition)
    graph_id = str(uuid.uuid4())
    definition["graph_id"] = graph_id
    GRAPHS[graph_id] = definition
    save_graph(graph_id, definition)  # persist
    return graph_id


def evaluate_condition(state: StateModel, cond: Dict) -> bool:
    if not cond:
        return True
    key = cond.get("key")
    op = cond.get("op")
    val = cond.get("value")
    actual = None
    if key:
        parts = key.split(".")
        cur = state.data
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                cur = None
                break
        actual = cur
    if op == "==":
        return actual == val
    if op == "!=":
        return actual != val
    if op == ">":
        return actual is not None and actual > val
    if op == "<":
        return actual is not None and actual < val
    if op == ">=":
        return actual is not None and actual >= val
    if op == "<=":
        return actual is not None and actual <= val
    return bool(actual)


async def run_graph_async(graph_id: str, initial_state: Dict[str, Any]) -> RunModel:
    # load graph from DB if not in memory
    graph = GRAPHS.get(graph_id) or load_graph(graph_id)
    if not graph:
        raise ValueError("graph not found")
    run_id = str(uuid.uuid4())
    run = RunModel(
        run_id=run_id,
        graph_id=graph_id,
        status=RunStatus.RUNNING,
        state=StateModel(data=initial_state.copy()),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        logs=[],
        current_node=None,
    )
    RUNS_CACHE[run_id] = run
    # persist initial run
    save_run({
        "run_id": run_id,
        "graph_id": graph_id,
        "status": run.status.value,
        "state": run.state.data,
        "logs": run.logs,
        "metrics": {},
        "current_node": None
    })
    start_node = graph.get("start_node") or list(graph["nodes"].keys())[0]
    try:
        current = start_node
        visited = 0
        max_visits = graph.get("max_visits", 1000)
        metrics = {}
        while current:
            run.current_node = current
            msg = f"START_NODE:{current}"
            run.logs.append(msg)
            await broadcaster.publish(run_id, msg)
            run.updated_at = datetime.utcnow()
            node_def = NodeDef(**graph["nodes"][current])
            func = get_tool(node_def.func)
            if not func:
                raise RuntimeError(f"Tool {node_def.func} not found for node {current}")
            start_t = time.time()
            if asyncio.iscoroutinefunction(func):
                result = await func(run.state.data, node_def.meta)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, func, run.state.data, node_def.meta)
            elapsed = time.time() - start_t
            metrics[current] = {"time_s": elapsed}
            # merge result
            if isinstance(result, dict):
                run.state.data.update(result)
            end_msg = f"END_NODE:{current} elapsed={elapsed:.4f} state_snapshot={run.state.data}"
            run.logs.append(end_msg)
            await broadcaster.publish(run_id, end_msg)
            run.updated_at = datetime.utcnow()
            # determine next
            edge_options = graph["edges"].get(current, [])
            next_node: Optional[str] = None
            for opt in edge_options:
                cond = opt.get("cond")
                if cond is None or evaluate_condition(run.state, cond):
                    next_node = opt.get("next")
                    break
            if not next_node:
                current = None
            else:
                current = next_node
            visited += 1
            if visited >= max_visits:
                raise RuntimeError("Max visits exceeded; possible infinite loop")
        run.status = RunStatus.SUCCESS
        run.logs.append("RUN_COMPLETE")
        await broadcaster.publish(run_id, "RUN_COMPLETE")
        run.current_node = None
        run.updated_at = datetime.utcnow()
        run.metrics = metrics
        # persist final run
        save_run({
            "run_id": run_id,
            "graph_id": graph_id,
            "status": run.status.value,
            "state": run.state.data,
            "logs": run.logs,
            "metrics": run.metrics,
            "current_node": run.current_node,
        })
    except Exception as e:
        run.status = RunStatus.FAILED
        err = f"ERR:{repr(e)}"
        run.logs.append(err)
        await broadcaster.publish(run_id, err)
        run.updated_at = datetime.utcnow()
        save_run({
            "run_id": run_id,
            "graph_id": graph_id,
            "status": run.status.value,
            "state": run.state.data,
            "logs": run.logs,
            "metrics": {},
            "current_node": run.current_node,
        })
    return run


def start_run_background(graph_id: str, initial_state: Dict[str, Any]) -> str:
    run_id = str(uuid.uuid4())
    # create placeholder run in DB and cache
    placeholder = RunModel(
        run_id=run_id,
        graph_id=graph_id,
        status=RunStatus.PENDING,
        state=StateModel(data=initial_state.copy()),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        logs=[],
        current_node=None,
    )
    RUNS_CACHE[run_id] = placeholder
    save_run({
        "run_id": run_id,
        "graph_id": graph_id,
        "status": placeholder.status.value,
        "state": placeholder.state.data,
        "logs": [],
        "metrics": {},
        "current_node": None
    })

    async def _runner():
        try:
            # replace placeholder by actual run
            actual_run = await run_graph_async(graph_id, initial_state)
            RUNS_CACHE[run_id] = actual_run
        except Exception as e:
            r = RUNS_CACHE.get(run_id, placeholder)
            r.status = RunStatus.FAILED
            r.logs.append(f"ERR:{repr(e)}")
            save_run({
                "run_id": r.run_id,
                "graph_id": r.graph_id,
                "status": r.status.value,
                "state": r.state.data,
                "logs": r.logs,
                "metrics": getattr(r, "metrics", {}),
                "current_node": r.current_node,
            })
            RUNS_CACHE[run_id] = r

    asyncio.create_task(_runner())
    return run_id


def get_run(run_id: str) -> RunModel | None:
    # prefer cache, fallback to DB
    r = RUNS_CACHE.get(run_id)
    if r:
        return r
    loaded = load_run(run_id)
    if not loaded:
        return None
    # convert to RunModel minimally
    rm = RunModel(
        run_id=loaded["run_id"],
        graph_id=loaded["graph_id"],
        status=RunStatus(loaded["status"]),
        state=StateModel(data=loaded.get("state", {})),
        logs=loaded.get("logs", []),
        current_node=loaded.get("current_node"),
        created_at=None,
        updated_at=None,
    )
    RUNS_CACHE[run_id] = rm
    return rm


def list_graphs() -> List[Dict]:
    # return from DB primarily
    return load_graphs_cached()


def load_graphs_cached() -> List[Dict]:
    # try DB list
    try:
        from ..db import list_graphs_db
        return list_graphs_db()
    except Exception:
        return list(GRAPHS.values())
