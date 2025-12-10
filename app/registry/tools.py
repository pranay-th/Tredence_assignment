# app/registry/tools.py
import asyncio
from typing import Dict, Callable, Any
from random import randint

TOOLS: Dict[str, Callable] = {}


def register(name: str):
    """Decorator to register a tool by name."""
    def _decorator(f):
        TOOLS[name] = f
        return f
    return _decorator


def get_tool(name: str):
    """Return the tool function registered under `name`, or None."""
    return TOOLS.get(name)


# --- Tools used in Data Quality Pipeline ---

@register("profile_data")
async def profile_data(state: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute a very simple profile: row_count, columns, null_counts.
    Returns: {"profile": {...}}
    """
    await asyncio.sleep(0.2)
    data_rows = state.get("rows", [])
    profile = {
        "row_count": len(data_rows),
        "null_counts": {},
        "columns": list(data_rows[0].keys()) if data_rows else [],
    }
    nulls = {}
    for col in profile["columns"]:
        cnt = sum(1 for r in data_rows if r.get(col) is None)
        nulls[col] = cnt
    profile["null_counts"] = nulls
    return {"profile": profile}


@register("identify_anomalies")
async def identify_anomalies(state: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Naive anomaly identification:
    - Missing required fields (from meta["required"])
    - Negative numeric values
    Returns: {"anomalies": [...], "anomaly_count": N}
    """
    await asyncio.sleep(0.2)
    rows = state.get("rows", [])
    anomalies = []
    required = meta.get("required", [])
    for idx, r in enumerate(rows):
        if any(r.get(c) is None for c in required):
            anomalies.append({"idx": idx, "reason": "missing_required"})
        for k, v in r.items():
            if isinstance(v, (int, float)) and v < 0:
                anomalies.append({"idx": idx, "reason": f"negative_{k}"})
    existing = state.get("anomalies", [])
    merged = existing + anomalies
    return {"anomalies": merged, "anomaly_count": len(merged)}


@register("generate_rules")
def generate_rules(state: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Heuristic rule generation:
    - require_fields rule if meta specifies required fields
    - rule to drop negative numeric values
    Returns: {"rules": [...]}
    """
    required = meta.get("required", [])
    rules = state.get("rules", [])
    new_rules = []
    if required:
        new_rules.append({"type": "require_fields", "fields": required})
    new_rules.append({"type": "no_negative_numeric"})
    rules = rules + new_rules
    return {"rules": rules}


@register("apply_rules")
async def apply_rules(state: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply rules to rows:
    - drop rows violating rules
    - recompute anomalies on cleaned data
    Returns: {"rows": [...], "rules_applied": True, "dropped_rows": N, "anomalies": [...], "anomaly_count": N}
    """
    await asyncio.sleep(0.2)
    rows = state.get("rows", [])
    rules = state.get("rules", [])
    new_rows = []
    dropped = 0
    for r in rows:
        drop = False
        for rule in rules:
            if rule["type"] == "require_fields":
                for f in rule["fields"]:
                    if r.get(f) is None:
                        drop = True
            if rule["type"] == "no_negative_numeric":
                for v in r.values():
                    if isinstance(v, (int, float)) and v < 0:
                        drop = True
        if not drop:
            new_rows.append(r)
        else:
            dropped += 1
    anomalies = []
    required = meta.get("required", [])
    for idx, r in enumerate(new_rows):
        if any(r.get(c) is None for c in required):
            anomalies.append({"idx": idx, "reason": "missing_required"})
        for k, v in r.items():
            if isinstance(v, (int, float)) and v < 0:
                anomalies.append({"idx": idx, "reason": f"negative_{k}"})
    return {"rows": new_rows, "rules_applied": True, "dropped_rows": dropped, "anomalies": anomalies, "anomaly_count": len(anomalies)}


@register("mutate_for_demo")
def mutate_for_demo(state: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Small helper to insert/change a value to simulate more anomalies during demo loops.
    Returns partial state updates like {"rows": [...]}
    """
    rows = state.get("rows", [])
    if not rows:
        return {}
    idx = randint(0, len(rows) - 1)
    col = meta.get("col", list(rows[0].keys())[0])
    rows[idx][col] = None
    return {"rows": rows}
