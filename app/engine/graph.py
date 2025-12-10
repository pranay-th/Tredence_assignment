import asyncio
import uuid
from typing import Dict, Any, List, Optional
from .node import NodeDef
from .state import RunModel, RunStatus, StateModel
from ..registry.tools import TOOLS, get_tool
from datetime import datetime

# Graph is stored as a simple dict:
# {
#   "graph_id": str,
#   "nodes": { "name": NodeDef dict },
#   "edges": { "node_name": [ { "next": "node2", "cond": { "key": "x", "op": "<", "value": 10 } }, ... ] },
#   "start_node": "profile"
# }

GRAPHS: Dict[str, Dict] = {}
RUNS: Dict[str, RunModel] = {}


def create_graph(definition: Dict) -> str:
    graph_id = str(uuid.uuid4())
    if "nodes" not in definition or "edges" not in definition:
        raise ValueError("Graph definition must include 'nodes' and 'edges'")
    definition["graph_id"] = graph_id
    GRAPHS[graph_id] = definition
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
    # default
    return bool(actual)


async def run_graph_async(graph_id: str, initial_state: Dict[str, Any]) -> RunModel:
    graph = GRAPHS.get(graph_id)
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
    )
    RUNS[run_id] = run

    start_node = graph.get("start_node") or list(graph["nodes"].keys())[0]
    try:
        current = start_node
        visited = 0
        max_visits = graph.get("max_visits", 1000)  
        while current:
            run.current_node = current
            run.logs.append(f"START_NODE:{current}")
            run.updated_at = datetime.utcnow()
            node_def = NodeDef(**graph["nodes"][current])
            func = get_tool(node_def.func)
            if not func:
                raise RuntimeError(f"Tool {node_def.func} not found for node {current}")
            if asyncio.iscoroutinefunction(func):
                result = await func(run.state.data, node_def.meta)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(None, func, run.state.data, node_def.meta)
            if isinstance(result, dict):
                run.state.data.update(result)
            run.logs.append(f"END_NODE:{current} -> state_snapshot:{run.state.data}")
            run.updated_at = datetime.utcnow()
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
        run.current_node = None
        run.updated_at = datetime.utcnow()
    except Exception as e:
        run.status = RunStatus.FAILED
        run.logs.append(f"ERR:{repr(e)}")
        run.updated_at = datetime.utcnow()
    return run


def start_run_background(graph_id: str, initial_state: Dict[str, Any]) -> str:
    import asyncio
    run_id = str(uuid.uuid4())
    run = RunModel(
        run_id=run_id,
        graph_id=graph_id,
        status=RunStatus.PENDING,
        state=StateModel(data=initial_state.copy()),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    RUNS[run_id] = run

    async def _runner():
        try:
            RUNS[run_id] = await run_graph_async(graph_id, initial_state)
        except Exception as e:
            r = RUNS.get(run_id, run)
            r.status = RunStatus.FAILED
            r.logs.append(f"ERR:{repr(e)}")
            r.updated_at = datetime.utcnow()
            RUNS[run_id] = r

    asyncio.create_task(_runner())
    return run_id


def get_run(run_id: str) -> RunModel | None:
    return RUNS.get(run_id)


def list_graphs() -> List[Dict]:
    return list(GRAPHS.values())
